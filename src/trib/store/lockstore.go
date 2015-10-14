package store

import (
	"log"
	"sync"
	"time"

	"trib"
)

const LockLive = time.Duration(time.Second * 30)

// In-memory storage implementation. All calls always returns nil.
type LockStorage struct {
	clock uint64

	strs  map[string] *trib.Holder

	strLock   sync.Mutex

	lockMap map[string] *sync.Mutex
}

var _ trib.LockStorage = new(LockStorage)

func NewLockStorageId(id int) *LockStorage {
	return &LockStorage{
		strs:  make(map[string] *trib.Holder),
		lockMap: make(map[string] *sync.Mutex),
	}
}

func NewLockStorage() *LockStorage {
	return NewLockStorageId(0)
}

func (self *LockStorage) TestAndSet(kv *trib.KeyValue, succ *bool) error {
	key := kv.Key
	id := kv.Value
	filelock, ok := self.lockMap[key]
	if ok != true {
		self.strLock.Lock()
		_, ok := self.lockMap[key]
		if ok != true {
			// create lock
			self.lockMap[key] = &sync.Mutex{}
		}
		filelock = self.lockMap[key]
		self.strLock.Unlock()
	}

	filelock.Lock()
	defer filelock.Unlock()
	// t & s
	*succ = true
	now := time.Now()
	test, ok := self.strs[key] 
	if ok == false {
		// create entry, get lock
		self.strs[key] = &trib.Holder{Name:id, UpdateTime:now}
	} else {
		// entry exists... check if expired
		diff := now.Sub(test.UpdateTime)
		if diff > LockLive {
			// expired
			self.strs[key] = &trib.Holder{Name:id, UpdateTime:now}
		} else {
			// someone else is holding the lock
			*succ = false
		}
	}
	

	if Logging {
		log.Printf("(%q) Test and Set (%q)", id, key)
	}

	return nil
}

func (self *LockStorage) Renew(kv *trib.KeyValue, succ *bool) error {
	key := kv.Key
	id := kv.Value
	filelock, ok := self.lockMap[key]
	if ok != true {
		*succ = false
		return nil
	}
	filelock.Lock()
	defer filelock.Unlock()
	*succ = true
	now := time.Now()
	test, ok := self.strs[key] 
	if ok == false {
		// no one is holding the lock, should not renew
		*succ = false
	} else {
		// entry exists... if has ownership
		if id == test.Name {
			// just renew it
			self.strs[key] = &trib.Holder{Name:id, UpdateTime:now}
		} else {
			// you are late...
			*succ = false
		}
	}

	if Logging {
		log.Printf("(%q) Renew (%q)", id, key)
	}
	
	return nil
}

func (self *LockStorage) Release(kv *trib.KeyValue, succ *bool) error {
	key := kv.Key
	id := kv.Value
	filelock, ok := self.lockMap[key]
	if ok != true {
		*succ = false
		return nil
	}
	filelock.Lock()
	defer filelock.Unlock()
	test, ok := self.strs[key] 
	*succ = true
	if ok != false {
		if id == test.Name {
			// release my lock
			delete(self.strs, key)
		} else {
			*succ = false
		}
	}

	if Logging {
		log.Printf("(%q) Release (%q)", id, key)
	}
	
	return nil
}

func (self *LockStorage) Debug(key string, value *map[string]*trib.Holder) error {
	*value = self.strs
	return nil
}
