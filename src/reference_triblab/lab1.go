package triblab

import (
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"trib"
)

type client struct {
	addr string
}

func (self *client) Get(key string, value *string) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("Storage.Get", key, value)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

func (self *client) Set(kv *trib.KeyValue, succ *bool) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("Storage.Set", kv, succ)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

func (self *client) Keys(p *trib.Pattern, list *trib.List) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	list.L = nil
	e = conn.Call("Storage.Keys", p, list)
	if e != nil {
		conn.Close()
		return e
	}
	if list.L == nil {
		list.L = []string{}
	}

	// close the connection
	return conn.Close()
}

func (self *client) ListGet(key string, list *trib.List) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	list.L = nil
	e = conn.Call("Storage.ListGet", key, list)
	if e != nil {
		conn.Close()
		return e
	}
	if list.L == nil {
		list.L = []string{}
	}

	// close the connection
	return conn.Close()
}

func (self *client) ListAppend(kv *trib.KeyValue, succ *bool) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("Storage.ListAppend", kv, succ)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

func (self *client) ListRemove(kv *trib.KeyValue, n *int) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("Storage.ListRemove", kv, n)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

func (self *client) ListKeys(p *trib.Pattern, list *trib.List) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	list.L = nil
	e = conn.Call("Storage.ListKeys", p, list)
	if e != nil {
		conn.Close()
		return e
	}
	if list.L == nil {
		list.L = []string{}
	}

	// close the connection
	return conn.Close()
}

func (self *client) Clock(atLeast uint64, ret *uint64) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("Storage.Clock", atLeast, ret)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

var _ trib.Storage = new(client)

// Creates an RPC client that connects to addr.
func NewClient(addr string) trib.Storage {
	return &client{addr: addr}
}

// Serve as a backend based on the given configuration
func ServeBack(b *trib.BackConfig) error {
	s := rpc.NewServer()
	s.Register(b.Store)
	port := strings.Split(b.Addr, ":")[1]
	tempPort, portErr := strconv.Atoi(port)

	if portErr != nil {
		// port is not a number
		b.Ready <- false
		return portErr
	}
	if tempPort < 0 || tempPort > 65535 {
		// port out of range
		b.Ready <- false
		return nil
	}
	port = ":" + port
	l, e := net.Listen("tcp", port)
	if e != nil {
		if b.Ready != nil {
			b.Ready <- false
			return e
		}
	}
	if b.Ready != nil {
		b.Ready <- true
	}
	http.Serve(l, s)

	return nil
}
