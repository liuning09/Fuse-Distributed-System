package triblab

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
	"trib"
)

type FrontServer struct {
	storageClient trib.BinStorage
	id            string
	lock          sync.Mutex
	usersCache    []string
}

// sorting helper: similar to ref/seq_trib.go
type byClock []*trib.Trib

func (self byClock) Len() int      { return len(self) }
func (self byClock) Swap(i, j int) { self[i], self[j] = self[j], self[i] }
func (self byClock) Less(i, j int) bool {
	if self[i].Clock < self[j].Clock {
		return true
	}
	if self[i].Clock > self[j].Clock {
		return false
	}
	if self[i].Clock == self[j].Clock {
		if self[i].Time.Before(self[j].Time) {
			return true
		}
	}
	return false
}

func (self *FrontServer) Lock(lockName string, client trib.Storage, succ *bool) error {
	if e := client.Set(&trib.KeyValue{lockName, self.id}, succ); e != nil {
		return e
	} else {
		return nil
	}
}

// unlock return true means no other server modified data
func (self *FrontServer) Unlock(lockName string, client trib.Storage) (bool, error) {
	value := ""
	if e := client.Get(lockName, &value); e != nil {
		return false, e
	}
	if value == self.id {
		return true, nil
	} else {
		return false, nil
	}
}

// helper: remove dups
func removeDuplicates(arr []string) []string {
	result := []string{}
	seen := map[string]int{}
	for _, val := range arr {
		if _, ok := seen[val]; !ok {
			result = append(result, val)
			seen[val] = 1
		}
	}
	return result
}

func (self *FrontServer) findUser(user string) (bool, error) {
	// forbid using built-in names
	if user == "GlobalUsersCache" {
		return false, nil
	}

	// search cache first
	for _, cached := range self.usersCache {
		if user == cached {
			return true, nil
		}
	}
	// search backend
	client := self.storageClient.Bin(user)
	value := ""
	if e := client.Get("Existence", &value); e != nil {
		return false, e
	}
	if value == "True" {
		return true, nil
	} else {
		return false, fmt.Errorf("user %q does not exist!", user)
	}
}

func (self *FrontServer) SignUp(user string) error {
	if len(user) > trib.MaxUsernameLen {
		return fmt.Errorf("username %q too long", user)
	}
	if !trib.IsValidUsername(user) {
		return fmt.Errorf("invalid username %q", user)
	}
	self.lock.Lock()
	defer self.lock.Unlock()

	client := self.storageClient.Bin(user)
	// make sure the global buffer is not a user
	anotherClient := self.storageClient.Bin("GlobalUsersCache")
	value := ""
	succ := false
	if e := anotherClient.Get("Existence", &value); e != nil {
		return e
	}
	if value != "True" {
		succ = false
		if e := anotherClient.Set(&trib.KeyValue{"Existence", "True"}, &succ); e != nil {
			return e
		}
	}

	// check existence:
	value = ""
	if e := client.Get("Existence", &value); e != nil {
		return e
	}
	if value == "True" {
		return fmt.Errorf("user %q already exists", user)
	}

	// simulate a lock using key-value store
	succ = false
	if e := self.Lock("SignupLock", client, &succ); e != nil {
		return e
	}

	// try signup
	succ = false
	if e := client.Set(&trib.KeyValue{"Existence", "True"}, &succ); e != nil {
		return e
	}

	// try unlock
	unlock, e := self.Unlock("SignupLock", client)
	if e != nil {
		return e
	}
	if unlock == true {
		// add to global cache
		if len(self.usersCache) < trib.MinListUser {
			self.usersCache = append(self.usersCache, user)
			succ = false
			if e := anotherClient.ListAppend(&trib.KeyValue{"Usernames", user}, &succ); e != nil {
				return e
			}
		}
		return nil
	} else {
		return fmt.Errorf("user %q already signed up by others..", user)
	}
}

func (self *FrontServer) ListUsers() ([]string, error) {
	if len(self.usersCache) >= trib.MinListUser {
		return self.usersCache, nil
	} else {
		// need to fetch users from backend server
		self.usersCache = self.usersCache[:0]
		client := self.storageClient.Bin("GlobalUsersCache")
		list := trib.List{L: []string{}}
		if e := client.ListGet("Usernames", &list); e != nil {
			return nil, e
		} else {
			self.usersCache = append(self.usersCache, list.L...)
		}
		return self.usersCache, nil
	}
}

func (self *FrontServer) Post(who, post string, clock uint64) error {
	if len(post) > trib.MaxTribLen {
		return fmt.Errorf("trib too long")
	}
	self.lock.Lock()
	defer self.lock.Unlock()
	_, e := self.findUser(who)
	if e != nil {
		return e
	}
	client := self.storageClient.Bin(who)
	var newClock uint64
	client.Clock(clock, &newClock)
	newTrib := trib.Trib{User: who, Message: post, Time: time.Now(), Clock: newClock + 1}
	serialized, e := json.Marshal(newTrib)
	if e != nil {
		return e
	}
	value := string(serialized)
	succ := false
	if e := client.ListAppend(&trib.KeyValue{"Tribs", value}, &succ); e != nil {
		return e
	}
	return nil
}

func (self *FrontServer) Tribs(user string) ([]*trib.Trib, error) {
	self.lock.Lock()
	defer self.lock.Unlock()
	_, e := self.findUser(user)
	if e != nil {
		return nil, e
	}
	client := self.storageClient.Bin(user)
	rawList := trib.List{L: []string{}}
	if e := client.ListGet("Tribs", &rawList); e != nil {
		return nil, e
	}
	size := len(rawList.L)
	tribList := make([]*trib.Trib, size)
	for i, entry := range rawList.L {
		if e := json.Unmarshal([]byte(entry), &tribList[i]); e != nil {
			return nil, e
		}
	}

	// sort by logic clock
	sort.Sort(byClock(tribList))

	// garbage collection
	start := 0
	if size > trib.MaxTribFetch {
		for i := 0; i < size-trib.MaxTribFetch; i++ {
			serialized, e := json.Marshal(tribList[i])
			if e != nil {
				return nil, e
			}
			value := string(serialized)
			n := 0
			if e := client.ListRemove(&trib.KeyValue{"Tribs", value}, &n); e != nil {
				return nil, e
			}
		}
		start = size - trib.MaxTribFetch
	}
	return tribList[start:], nil
}

func (self *FrontServer) Follow(who, whom string) error {
	if who == whom {
		return fmt.Errorf("cannot follow oneself")
	}
	self.lock.Lock()
	defer self.lock.Unlock()
	_, e := self.findUser(who)
	if e != nil {
		return e
	}
	_, e = self.findUser(whom)
	if e != nil {
		return e
	}

	isFollowing, e := self.IsFollowing(who, whom)
	if e != nil {
		return e
	}
	if isFollowing {
		return fmt.Errorf("user %q alreay following %q", who, whom)
	}

	// simulate a lock, release is not necessary
	client := self.storageClient.Bin(who)
	//succ := false
	//if e := self.Lock("FollowLock", client, &succ); e != nil {
	//	return e
	//}

	// check size
	followingUsers, e := self.Following(who)
	if e != nil {
		return e
	}
	if len(followingUsers) >= trib.MaxFollowing {
		return fmt.Errorf("user %q is following too many users", who)
	}

	// try follow

	succ := false
	if e := client.ListAppend(&trib.KeyValue{"Followings", whom}, &succ); e != nil {
		return e
	}
	return nil
	/*succ = false
	if e := client.ListAppend(&trib.KeyValue{"Followings", whom}, &succ); e != nil {
		return e
	}
	if succ == true {
		unlock, e := self.Unlock("SignupLock", client)
		if e != nil {
			// undo: wrong! cannot undo by ListRemove
			n := 0
			if e2 := client.ListRemove(&trib.KeyValue{"Followings", whom}, &n); e != nil {
				return e2
			}
			return e
		}
		if unlock == true {
			return nil
		} else {
			// undo
			n := 0
			if e2 := client.ListRemove(&trib.KeyValue{"Followings", whom}, &n); e != nil {
				return e2
			}
			return fmt.Errorf("undo operation becuase concurrency issue, try again later")
		}
	} else {
		return fmt.Errorf("%q fail to follow, succ == false", who)
	}*/
}

func (self *FrontServer) Unfollow(who, whom string) error {
	if who == whom {
		return fmt.Errorf("cannot unfollow oneself")
	}
	self.lock.Lock()
	defer self.lock.Unlock()
	_, e := self.findUser(who)
	if e != nil {
		return e
	}
	_, e = self.findUser(whom)
	if e != nil {
		return e
	}

	isFollowing, e := self.IsFollowing(who, whom)
	if e != nil {
		return e
	}
	if !isFollowing {
		return fmt.Errorf("user %q is not following %q", who, whom)
	}
	client := self.storageClient.Bin(who)
	n := 0
	if e := client.ListRemove(&trib.KeyValue{"Followings", whom}, &n); e != nil {
		return e
	}
	return nil
}

func (self *FrontServer) IsFollowing(who, whom string) (bool, error) {
	if who == whom {
		return false, fmt.Errorf("checking the same user")
	}
	_, e := self.findUser(who)
	if e != nil {
		return false, e
	}
	_, e = self.findUser(whom)
	if e != nil {
		return false, e
	}

	followingUsers, e := self.Following(who)
	if e != nil {
		return false, e
	}
	for _, user := range followingUsers {
		if user == whom {
			return true, nil
		}
	}
	return false, nil
}

func (self *FrontServer) Following(who string) ([]string, error) {
	_, e := self.findUser(who)
	if e != nil {
		return nil, e
	}
	client := self.storageClient.Bin(who)
	followingUsers := trib.List{L: []string{}}
	if e := client.ListGet("Followings", &followingUsers); e != nil {
		return nil, e
	}

	followingUsers.L = removeDuplicates(followingUsers.L)
	return followingUsers.L, nil
}

func (self *FrontServer) Home(user string) ([]*trib.Trib, error) {
	self.lock.Lock()
	defer self.lock.Unlock()
	_, e := self.findUser(user)
	if e != nil {
		return nil, e
	}
	followingUsers, e := self.Following(user)
	if e != nil {
		return nil, e
	}
	followingUsers = append(followingUsers, user)
	wholeRawList := trib.List{L: []string{}}
	for _, followed := range followingUsers {
		client := self.storageClient.Bin(followed)
		rawList := trib.List{L: []string{}}
		if e := client.ListGet("Tribs", &rawList); e != nil {
			return nil, e
		}
		wholeRawList.L = append(wholeRawList.L, rawList.L...)
	}
	size := len(wholeRawList.L)
	tribList := make([]*trib.Trib, size)
	for i, entry := range wholeRawList.L {
		if e := json.Unmarshal([]byte(entry), &tribList[i]); e != nil {
			return nil, e
		}
	}
	// sort by logic clock
	sort.Sort(byClock(tribList))
	l := len(tribList)
	start := 0
	if l > trib.MaxTribFetch {
		start = l - trib.MaxTribFetch
	}

	return tribList[start:], nil
}

func NewFront(s trib.BinStorage) trib.Server {
	rand.Seed(time.Now().UTC().UnixNano())
	bytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		bytes[i] = (byte)(65 + rand.Intn(25))
	}
	str := string(bytes)
	ret := &FrontServer{storageClient: s, id: str, lock: sync.Mutex{}}
	return ret
}
