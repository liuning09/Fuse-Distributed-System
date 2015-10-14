// Package store provides a simple in-memory key value store.
package store

import (
	"log"
	"math"
	"sync"
    "os"
    "fmt"
    "strings"
    "trib/colon"
	"trib"
    "path/filepath"
    "io/ioutil"
)

//var Logging bool

//type strList []string

// In-memory storage implementation. All calls always returns nil.
type PersistentStorage struct {
	clock uint64
	clockLock sync.Mutex
}

//var _ trib.Storage = new(Storage)

func NewPersistentStorageId(id int) *PersistentStorage {
	return &PersistentStorage{}
}

func NewPersistentStorage() *PersistentStorage {
	return NewPersistentStorageId(0)
}

func (self *PersistentStorage) Clock(atLeast uint64, ret *uint64) error {
	self.clockLock.Lock()
	defer self.clockLock.Unlock()

	if self.clock < atLeast {
		self.clock = atLeast
	}

	*ret = self.clock

	if self.clock < math.MaxUint64 {
		self.clock++
	}

	if Logging {
		log.Printf("Clock(%d) => %d", atLeast, *ret)
	}

	return nil
}

func (self *PersistentStorage) Get(key string, value *string) error {
	fmt.Println("Calling Get")
	s := strings.Split(key, "::")
	binname := colon.Unescape(s[0])
	fmt.Println("Bin name is "+binname)
	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	path := dir + "/cse223test/" + binname
	//os.MkdirAll(path, 0777)
	filename := colon.Unescape(s[1])
	fpath := path + "/" + filename
	fmt.Println("file path ", fpath)

	if onDisk(fpath) {
		data, err := ioutil.ReadFile(fpath)
		if err != nil {
			fmt.Println("read error")
			*value = ""
			return nil
		} 
		*value = string(data)
		/*f, err := os.Open(fpath)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		for scanner.Scan(){
			*value = scanner.Text()
			fmt.Println("The value is " + scanner.Text())
		}
		if err := scanner.Err(); err !=nil{
			log.Fatal(err)
		}*/
		
	} else {
		*value = ""
	}

	if Logging {
		log.Printf("Get(%q) => %q", key, *value)
	}
	return nil
}

func (self *PersistentStorage) Set(kv *trib.KeyValue, succ *bool) error {
	fmt.Println("Calling Set")
	s := strings.Split(kv.Key, "::")
	binname := colon.Unescape(s[0])
	fmt.Println("Bin name is " + binname)

	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	path := dir + "/cse223test/" + binname
	os.MkdirAll(path, 0777)
	filename := colon.Unescape(s[1])
	fpath := path + "/" + filename
	fmt.Println("file path ", fpath)

	if onDisk(fpath) {
		os.Remove(fpath)
	}

	if kv.Value != "" {
		f, _ := os.OpenFile(fpath, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, 0777)
		_, err :=  f.WriteString(kv.Value)
		if err != nil {
			*succ = false
		}
	}
	*succ = true
	
	if Logging {
		log.Printf("Set(%q, %q)", kv.Key, kv.Value)
	}
/*
	f, _ := os.OpenFile(fpath, os.O_WRONLY | os.O_CREATE |  os.O_TRUNC, 0777)

	if kv.Value != "" {
		//self.strs[kv.Key] = kv.Value

		_, err2 := f.WriteString(kv.Value + "\n")
		if err2 != nil {
			return err2
		}

	} else {
		delete(self.strs, kv.Key)
	}

	*succ = true

	if Logging {
		log.Printf("Set(%q, %q)", kv.Key, kv.Value)
	}
*/
	return nil
}




/////////////// useless
/*
func (self *PersistentStorage) Keys(p *trib.Pattern, r *trib.List) error {
	self.strLock.Lock()
	defer self.strLock.Unlock()

	ret := make([]string, 0, len(self.strs))

	for k := range self.strs {
		if p.Match(k) {
			ret = append(ret, k)
		}
	}

	r.L = ret

	if Logging {
		log.Printf("Keys(%q, %q) => %d", p.Prefix, p.Suffix, len(r.L))
		for i, s := range r.L {
			log.Printf("  %d: %q", i, s)
		}
	}

	return nil
}

func (self *PersistentStorage) ListKeys(p *trib.Pattern, r *trib.List) error {
	self.listLock.Lock()
	defer self.listLock.Unlock()

	ret := make([]string, 0, len(self.lists))
	for k := range self.lists {
		if p.Match(k) {
			ret = append(ret, k)
		}
	}

	r.L = ret

	if Logging {
		log.Printf("ListKeys(%q, %q) => %d", p.Prefix, p.Suffix, len(r.L))
		for i, s := range r.L {
			log.Printf("  %d: %q", i, s)
		}
	}

	return nil
}

func (self *PersistentStorage) ListGet(key string, ret *trib.List) error {
	self.listLock.Lock()
	defer self.listLock.Unlock()

		//var f *os.File
	s := strings.Split(key, "::")
	binname := colon.Unescape(s[0])
	//fmt.Println("Bin name is "+binname)
	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	path := dir + "/cse223test/" + binname
	filename := s[1] + "list"
	//os.MkdirAll(path, 0777)
	fpath := path + "/" + filename

	if onDisk(fpath) {
		f, err := os.Open(fpath)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		ret.L = make([]string, 0)

		for scanner.Scan(){
			//*value = scanner.Text()
			ret.L = append(ret.L, scanner.Text())
			fmt.Println("The value is " + scanner.Text())
		}
		if err := scanner.Err(); err !=nil{
			log.Fatal(err)
		}
	} else {
		ret.L = []string{}
	}

	if Logging {
		log.Printf("ListGet(%q) => %d", key, len(ret.L))
		for i, s := range ret.L {
			log.Printf("  %d: %q", i, s)
		}
	}


	return nil
}

func (self *PersistentStorage) ListAppend(kv *trib.KeyValue, succ *bool) error {
	fmt.Println("Calling listAppend")
	self.listLock.Lock()
	//var f *os.File
	defer self.listLock.Unlock()
	s := strings.Split(kv.Key, "::")
	binname := colon.Unescape(s[0])
	fmt.Println("Bin name is "+binname)
	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	path := dir + "/cse223test/" + binname
	os.MkdirAll(path, 0777)
	filename := s[1] + "list"
	fpath := path + "/" + filename


	f, _ := os.OpenFile(fpath, os.O_WRONLY | os.O_CREATE | os.O_APPEND, 0777)



	//lst.PushBack(kv.Value)
	//towrite := kv.Value + "\n"
	_, err2 := f.WriteString(kv.Value + "\n")
	if err2 != nil {
		return err2
	}
	*succ = true

	if Logging {
		log.Printf("ListAppend(%q, %q)", kv.Key, kv.Value)
	}

	return nil
}

func (self *PersistentStorage) ListRemove(kv *trib.KeyValue, n *int) error {
	self.listLock.Lock()
	defer self.listLock.Unlock()

	*n = 0

	lst, found := self.lists[kv.Key]
	if !found {
		return nil
	}

	f,err := os.Open(kv.Key)
	if err != nil {
		return err
	}
	_,err2 := f.WriteString(kv.Value)
		if err2 != nil {
			return err2
		}

	i := lst.Front()
	for i != nil {
		if i.Value.(string) == kv.Value {
			hold := i
			i = i.Next()
			lst.Remove(hold)
			*n++
			continue
		}

		i = i.Next()
	}

	if lst.Len() == 0 {
		delete(self.lists, kv.Key)
	}

	if Logging {
		log.Printf("ListRemove(%q, %q) => %d", kv.Key, kv.Value, *n)
	}

	return nil
}

*/

func onDisk(filename string) bool {
	/*
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(dir)
	
	fmt.Println(os.Getwd())
	*/
	fmt.Printf("Call onDisk\n")
	fmt.Println(filename)
	var ret bool
	if _, err := os.Stat(filename); os.IsNotExist(err) {
    	fmt.Printf("no such file or directory: %s\n", filename)
    	ret = false
	} else {
		fmt.Printf("file exists; processing...\n")
		ret = true
	}
	return ret
}
