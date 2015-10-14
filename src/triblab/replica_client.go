package triblab

import (
	"trib"
	"trib/colon"
	"strings"
	"sort"
	"fmt"
	"time"
)

type ReplicaBinClient struct {
	addr []string  // <- this is all the back address  :(
	chordAddrs map[string]bool 
}


type ReplicaClient struct {
	addr []string  // <- this is the three clients we need to write
	name  string // <- this is bin name  :(
}

func (self * ReplicaBinClient) FindChord() {
	for _, tryAddr := range self.addr {
		self.chordAddrs[tryAddr] = false
	}
	for _, tryAddr := range self.addr {
		cli := &client{tryAddr}
		pat := trib.Pattern{"CHORD::", ""}
		list := trib.List{[]string{}}
		if err := cli.Keys(&pat, &list); err == nil {
			if len(list.L) != 0 {
				self.chordAddrs[tryAddr] = true
			} else {
				self.chordAddrs[tryAddr] = false
			}
		} else {
			self.chordAddrs[tryAddr] = false
		}
	}
	go func() {
		c := time.Tick(1 * time.Second)
		for _ = range c {
			for _, tryAddr := range self.addr {
				cli := &client{tryAddr}
				pat := trib.Pattern{"CHORD::", ""}
				list := trib.List{[]string{}}
				if err := cli.Keys(&pat, &list); err == nil {
					if len(list.L) != 0 {
						self.chordAddrs[tryAddr] = true
					} else {
						self.chordAddrs[tryAddr] = false
					}
				} else {
					self.chordAddrs[tryAddr] = false
				}
			}
			//fmt.Println("_xxx________DEBUG: ", self.chordAddrs)
		}
	}()
}

func (self * ReplicaBinClient) Bin(name string) trib.Storage {
	//fmt.Println("_____DEBUG : bin name: ", name )
	addrArray := make([]string, 0)
	for _, tryAddr := range self.addr {
		if self.chordAddrs[tryAddr] == true {
			addrArray = append(addrArray, tryAddr)
		}
	}
	clients, err := getClientsByBin(addrArray, name, len(self.addr))
	clients = removeDuplicates(clients)
	//fmt.Println("_________DEBUG: ", clients)
	if err != nil {
		panic("cannot get clients using chord... fatal error")
	}
	bbsClients, err := getClientsByBin(addrArray, "BINSBYSERVER", len(self.addr))
	bbsClients = removeDuplicates(clients)
	success := false
	for _, bbsAddr := range bbsClients {
		cli := &client{bbsAddr}
		for _, clientAddr := range clients {
			// write bbs (index) to replication backend server
			cli.ListAppend(&trib.KeyValue{"BINSBYSERVER::" + clientAddr, name}, &success)
		}
	}
	return &ReplicaClient{clients, name}
}

func (self * ReplicaClient) GetConfig() ([]string, string) {
	return self.addr, self.name
}

func (self * ReplicaClient) Get(key string, value *string) error {
	newkey := colon.Escape(self.name) + "::" + key + "valu" // <- key should escape as well! NVM

	var mylist trib.List
	mylist.L = make([]string, 0)
	// get the list of logs of the key
	for index, ad := range self.addr {
		tempclient := &client{ad}
		err1 := tempclient.ListGet(newkey, &mylist)
		if err1 == nil {
			break
		} else if index < len(self.addr) - 1{
			continue
		} else {
			return err1
		}
	}

	s := ""
	if len(mylist.L) == 0 {
		*value = s
		return nil
	}

	// add dedup
	mylist.L = removeDuplicates(mylist.L)

	loglist := make(LogSlice, len(mylist.L))

	// decode the logs
	for index, l := range mylist.L {
		loglist[index] = DecodeLog(l)
	}

	sort.Sort(loglist)

	index := len(loglist) -1
	result := loglist[index].Val
	*value = result
	return nil
}

func (self * ReplicaClient) Set(kv *trib.KeyValue, succ *bool) error {
	var clk uint64

	err1 := self.Clock(0,&clk)
	if err1 != nil {
		return err1
	}

	var newkv trib.KeyValue
	newkv.Key = colon.Escape(self.name) + "::" + kv.Key + "valu"
	log := BinLog{"append",kv.Value, clk}
	newkv.Value = EncodeLog(log)
	success := false	

	for _, ad := range self.addr{
		tempClient := &client{ad}
		err2 := tempClient.ListAppend(&newkv, succ)
		//fmt.Println("append log in the key-value system")
		if err2 == nil{
			success = true
		}
	}

	if success == true {
		return nil
	} else {
		return fmt.Errorf("ListAppend failure on all servers")
	}
		
}

func (self * ReplicaClient) Keys(p *trib.Pattern, list *trib.List) error {
	// escape colon
	binName := colon.Escape(self.name)
	p.Prefix = binName + "::" + p.Prefix
	p.Suffix = p.Suffix + "valu"

	// RPC call
	// get the list of logs of the key

	// a list used as temp storage
	tmpList := trib.List{make([]string, 0)}
	for index, ad := range self.addr{
		tempclient := &client{ad}
		err1 := tempclient.ListKeys(p, &tmpList)
		if err1 == nil{
			break
		} else if index < len(self.addr) - 1 {
			continue
		} else {
			return err1
		}
	}
	
	blockCh := make(chan string, len(tmpList.L))
	for _, str := range tmpList.L {
		go func(str string) {
			var val string
			s1 := colon.Escape(self.name)+"::"
			s2 := "valu"
			str = strings.TrimPrefix(str, s1)
			str = strings.TrimSuffix(str, s2)
			_ = self.Get(str, &val) // ignore this err?
			if val != "" {
				blockCh <- str
			} else {
				blockCh <- ""
			}
		}(str)
	}

	list.L = make([]string, 0, len(tmpList.L))
	for _ = range tmpList.L {
		result := <- blockCh
		if result != "" {
			list.L = append(list.L, result)
		}
	}

	// unescape and trim
	if len(list.L) > 0 {
		for i, str := range list.L {
			//str = colon.Unescape(str)
			s1 := colon.Escape(self.name)+"::"
			s2 := "valu"
			str = strings.TrimPrefix(str, s1)
			str = strings.TrimSuffix(str, s2)
			list.L[i] = str
		}
	}
	return nil
}

func (self * ReplicaClient) ListGet(key string, list *trib.List) error {

	newkey := colon.Escape(self.name) + "::" + key + "list"

	var mylist trib.List
	mylist.L = make([]string, 0)

	// get the list of logs of the key
	for index, ad := range self.addr{
		tempclient := &client{ad}
		err1 := tempclient.ListGet(newkey, &mylist)
		if err1 == nil{
			break
		} else if index < len(self.addr) - 1{
			continue
		} else {
			return err1
		}
	}

	if len(mylist.L) == 0 {
		list.L = make([]string, 0)
		return nil
	}

	// add dedup
	mylist.L = removeDuplicates(mylist.L)

	loglist := make(LogSlice, len(mylist.L))

	// decode the logs
	for index, l := range mylist.L {
		loglist[index] = DecodeLog(l)
	}

	sort.Sort(loglist)

	var templist trib.List
	//get the real list
	for _, log := range loglist{
		if log.Op == "append"{
			templist.L = append(templist.L, log.Val)
		} else { // remove?
			newList := trib.List{[]string{}}
			for _, entry := range templist.L {
				if log.Val != entry {
					newList.L = append(newList.L, entry)
				}
			}
			templist.L = newList.L
			/*i := 0
			for _, l := range templist.L{
				if l == log.Val {
					if i < len(templist.L) {
						templist.L = append(templist.L[:i], templist.L[i+1:]...)
					} else {
						templist.L = templist.L[:i]
					}
				} else {
					i++
				}
			}*/
		}
	}
	*list = templist
	return nil
}

func (self * ReplicaClient) ListAppend(kv *trib.KeyValue, succ *bool) error {
	var clk uint64
	*succ = true
	err1 := self.Clock(0,&clk)
	if err1 != nil {
		return err1
	}

	var newkv trib.KeyValue
	newkv.Key = colon.Escape(self.name) + "::" + kv.Key + "list"
	log := BinLog{"append",kv.Value, clk}
	newkv.Value = EncodeLog(log)

	success := false
	for _, ad := range self.addr{
		tempclient := &client{ad}
		var b bool
		err2 := tempclient.ListAppend(&newkv, &b)
		if err2 == nil{
			success = true
		}
	}

	if success == true {
		return nil
	} else {
		return fmt.Errorf("ListAppend failure on all servers")
	}
	
}

func (self * ReplicaClient) ListRemove(kv *trib.KeyValue, n *int) error {
	var clk uint64
	err1 := self.Clock(0,&clk)
	if err1 != nil {
		return err1
	}

	var tempList trib.List
    self.ListGet(kv.Key, &tempList)


    r := 0
    for _, element := range tempList.L {
    	if element == kv.Value {
    		r++;
    	}
    }

    *n = r

	var newkv trib.KeyValue
	newkv.Key = colon.Escape(self.name) + "::" + kv.Key + "list"
	log := BinLog{"remove", kv.Value, clk}
	newkv.Value = EncodeLog(log)

	success := false
	for _, ad := range self.addr{
		tempclient := &client{ad}
		var b bool
		err2 := tempclient.ListAppend(&newkv, &b)
		if err2 == nil{
			success = true
		}
	}

	if success == true {
		return nil
	} else {
		return fmt.Errorf("ListRemove failure on all servers")
	}
}

func (self * ReplicaClient) ListKeys(p *trib.Pattern, list *trib.List) error {
	// escape colon
	binName := colon.Escape(self.name)
	p.Prefix = binName + "::" + p.Prefix
	p.Suffix = p.Suffix + "list"

	// RPC call
	// get the list of logs of the key

	// a list used as temp storage
	tmpList := trib.List{make([]string, 0)}
	for index, ad := range self.addr{
		tempclient := &client{ad}
		err1 := tempclient.ListKeys(p, &tmpList)
		if err1 == nil{
			break
		} else if index < len(self.addr) - 1 {
			continue
		} else {
			return err1
		}
	}
	
	blockCh := make(chan string, len(tmpList.L))
	for _, str := range tmpList.L {
		go func(str string) {
			tmpList2 := trib.List{make([]string, 0)}
			s1 := colon.Escape(self.name)+"::"
			s2 := "list"
			str = strings.TrimPrefix(str, s1)
			str = strings.TrimSuffix(str, s2)
			_ = self.ListGet(str, &tmpList2) // ignore this err?
			if len(tmpList2.L) != 0 {
				blockCh <- str
			} else {
				blockCh <- ""
			}
		}(str)
	}

	list.L = make([]string, 0, len(tmpList.L))
	for _ = range tmpList.L {
		result := <- blockCh
		if result != "" {
			list.L = append(list.L, result)
		}
	}

	// unescape and trim
	if len(list.L) > 0 {
		for i, str := range list.L {
			//str = colon.Unescape(str)
			s1 := colon.Escape(self.name)+"::"
			s2 := "valu"
			str = strings.TrimPrefix(str, s1)
			str = strings.TrimSuffix(str, s2)
			list.L[i] = str
		}
	}
	return nil
}

func (self * ReplicaClient) Clock(atLeast uint64, ret *uint64) error {
	var time uint64
	var max uint64
	success := false
	for _, ad := range self.addr{
		tempclient := &client{ad}
		err := tempclient.Clock(atLeast, &time)
		if err == nil {
			success = true
		}
		if time > max {
			max = time
		}
		*ret = max
	}
		
	if success == true {
		return nil
	} else {
		return fmt.Errorf("Clock failure on all servers")
	}
}

type LogSlice []*BinLog

func (l LogSlice) Len() int{ 
	return len(l) 
}

func (l LogSlice) Swap(i, j int){ 
	l[i], l[j] = l[j], l[i] 
}

func (l LogSlice) Less(i, j int) bool { 
	li := l[i]
	lj := l[j]
	return li.Clk < lj.Clk
}
var _ trib.Storage = new(ReplicaClient)

