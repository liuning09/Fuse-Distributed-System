package triblab

import (
	"trib"
	"net/rpc"
//	"fmt"
)

// TODO: use persistent RPC to read, use logging RPC to write

// FS bin client: map fs to serer

// FS client


type FsDiskClient struct {
	// read from disk only
	replicaClient trib.Storage
}

type FsLockClient struct {
	// talk to lock server
	lockServerAddr string
	// TODO: add what we need
}

func NewFsDiskClient(replicaClient trib.Storage) *FsDiskClient {
	return &FsDiskClient{replicaClient}
}

func NewFsLockClient(lockServerAddr string) *FsLockClient {
	return &FsLockClient{lockServerAddr}
}


////// Disk Client
// only get/set, no other operations...
// TODO: use liuning's code in backend
func (self * FsDiskClient) Get(key string, value *string) error {
	//TODO
	/*backs, binName := (self.replicaClient).(*ReplicaClient).GetConfig()
	fmt.Println(backs, binName) */
	return nil
}
func (self * FsDiskClient) Set(kv *trib.KeyValue, succ *bool) error {
	/*backs, binName := (self.replicaClient).(*ReplicaClient).GetConfig()
	newKey := colon.Escape(binName) + "::" + colon.Escape(kv.Key)
	for _, addr := range backs {
		
	}*/
	
	return nil
}
func (self * FsDiskClient) Keys(p *trib.Pattern, list *trib.List) error {
	return nil
}
func (self * FsDiskClient) ListGet(key string, list *trib.List) error {
	return nil
}
func (self * FsDiskClient) ListRemove(kv *trib.KeyValue, n *int) error {
	return nil
}
func (self * FsDiskClient) ListKeys(p *trib.Pattern, list *trib.List) error {
	return nil
}
func (self * FsDiskClient) ListAppend(kv *trib.KeyValue, succ *bool) error {
	return nil
}
func (self * FsDiskClient) Clock (atLeast uint64, ret *uint64) error {
	return self.replicaClient.Clock(atLeast, ret)
}



////// lock client

func (self *FsLockClient) TestAndSet(kv *trib.KeyValue, succ *bool) error {
	conn, e := rpc.DialHTTP("tcp", self.lockServerAddr)
	if e != nil {
		return e
	}

	e = conn.Call("LockStorage.TestAndSet", kv, succ)
	if e != nil {
		conn.Close()
	}
	return conn.Close()
}

func (self *FsLockClient) Renew(kv *trib.KeyValue, succ *bool) error {
	conn, e := rpc.DialHTTP("tcp", self.lockServerAddr)
	if e != nil {
		return e
	}

	e = conn.Call("LockStorage.Renew", kv, succ)
	if e != nil {
		conn.Close()
	}
	return conn.Close()
}

func (self *FsLockClient) Release(kv *trib.KeyValue, succ *bool) error {
	conn, e := rpc.DialHTTP("tcp", self.lockServerAddr)
	if e != nil {
		return e
	}

	e = conn.Call("LockStorage.Release", kv, succ)
	if e != nil {
		conn.Close()
	}
	return conn.Close()
}

func (self *FsLockClient) Debug(key string, value *map[string]*trib.Holder) error {
	conn, e := rpc.DialHTTP("tcp", self.lockServerAddr)
	if e != nil {
		return e
	}

	e = conn.Call("LockStorage.Debug", key, value)
	if e != nil {
		conn.Close()
	}
	return conn.Close()
}
