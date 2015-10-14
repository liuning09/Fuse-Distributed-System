package triblab

import (
	"net/rpc"
	"trib"
)

type diskClient struct {
	addr string
}

func NewDiskClient(addr string) trib.PersistentStorage {
	return &diskClient{addr}
}

func (self *diskClient) Get(key string, value *string) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("PersistentStorage.Get", key, value)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

func (self *diskClient) Set(kv *trib.KeyValue, succ *bool) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("PersistentStorage.Set", kv, succ)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

func (self *diskClient) Clock(atLeast uint64, ret *uint64) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("PersistentStorage.Clock", atLeast, ret)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}
