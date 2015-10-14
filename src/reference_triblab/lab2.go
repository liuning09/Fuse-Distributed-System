package triblab

import (
	"encoding/binary"
//	"fmt"
	"hash/fnv"
	"strings"
//	"time"
	"trib"
	"trib/colon"
)

type BinStorageClient struct {
	backs []string
}

type clientWithPrefix struct {
	bin            string
	originalClient trib.Storage
}

func (self *clientWithPrefix) Get(key string, value *string) error {
	// escape colon
	binName := colon.Escape(self.bin)
	key = colon.Escape(key)
	key = binName + "::" + key

	// RPC call
	return self.originalClient.Get(key, value)
}

func (self *clientWithPrefix) Set(kv *trib.KeyValue, succ *bool) error {
	// escape colon
	binName := colon.Escape(self.bin)
	kv.Key = colon.Escape(kv.Key)
	kv.Key = binName + "::" + kv.Key

	// RPC call
	return self.originalClient.Set(kv, succ)
}

func (self *clientWithPrefix) Keys(p *trib.Pattern, list *trib.List) error {
	// escape colon
	binName := colon.Escape(self.bin)
	p.Prefix = binName + "::" + colon.Escape(p.Prefix)
	p.Suffix = colon.Escape(p.Suffix)

	// RPC call
	err := self.originalClient.Keys(p, list)
	if err != nil {
		return err
	}

	// unescape and trim
	for i, str := range list.L {
		str = colon.Unescape(str)
		list.L[i] = strings.TrimPrefix(str, self.bin+"::")
	}
	// TODO: test
	return nil
}

func (self *clientWithPrefix) ListGet(key string, list *trib.List) error {
	// escape colon
	binName := colon.Escape(self.bin)
	key = colon.Escape(key)
	key = binName + "::" + key

	// RPC call
	return self.originalClient.ListGet(key, list)
}

func (self *clientWithPrefix) ListAppend(kv *trib.KeyValue, succ *bool) error {
	// escape colon
	binName := colon.Escape(self.bin)
	kv.Key = colon.Escape(kv.Key)
	kv.Key = binName + "::" + kv.Key

	// RPC call
	return self.originalClient.ListAppend(kv, succ)
}

func (self *clientWithPrefix) ListRemove(kv *trib.KeyValue, n *int) error {
	// escape colon
	binName := colon.Escape(self.bin)
	kv.Key = colon.Escape(kv.Key)
	kv.Key = binName + "::" + kv.Key

	// RPC call
	return self.originalClient.ListRemove(kv, n)
}

func (self *clientWithPrefix) ListKeys(p *trib.Pattern, list *trib.List) error {
	// escape colon
	binName := colon.Escape(self.bin)
	p.Prefix = binName + "::" + colon.Escape(p.Prefix)
	p.Suffix = colon.Escape(p.Suffix)

	// RPC call
	err := self.originalClient.ListKeys(p, list)
	if err != nil {
		return err
	}

	// unescape and trim
	for i, str := range list.L {
		str = colon.Unescape(str)
		list.L[i] = strings.TrimPrefix(str, self.bin+"::")
	}
	return nil
}

func (self *clientWithPrefix) Clock(atLeast uint64, ret *uint64) error {
	return self.originalClient.Clock(atLeast, ret)
}

var _ trib.Storage = new(clientWithPrefix)

func (self *BinStorageClient) Bin(name string) trib.Storage {
	serverNum := uint32(len(self.backs))
	nameHash := binary.LittleEndian.Uint32(fnv.New32().Sum([]byte(name)))
	index := nameHash % serverNum
	return NewClientWithPrefix(self.backs[index], name)
}

// for lab2
/*func NewBinClient(backs []string) trib.BinStorage {
	return &BinStorageClient{backs}
}*/ 

// for lab3
func NewBinClient(backs []string) trib.BinStorage {
	rClient := &ReplicaBinClient{backs, make(map[string]bool)}
	rClient.FindChord()
	return rClient
}

func NewClientWithPrefix(addr string, binName string) trib.Storage {
	c := NewClient(addr)
	return &clientWithPrefix{bin: binName, originalClient: c}
}

/*
func ServeKeeper(kc *trib.KeeperConfig) error {
	// no initialization for bin storage needed now
	if kc.Ready != nil {
		kc.Ready <- true
	}
	c := time.Tick(1 * time.Second)
	ret := new(uint64)
	for _ = range c {
		var max_time uint64 = 0
		for _, addr := range kc.Backs {
			temp_client := NewClient(addr)
			err := temp_client.Clock(0, ret)
			if err != nil {
				return err
			}
			if max_time < *ret {
				max_time = *ret
			}
		}
		max_time++
		for _, addr := range kc.Backs {
			temp_client := NewClient(addr)
			err := temp_client.Clock(max_time, ret)
			if err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("keeper exits..") 
}
*/
