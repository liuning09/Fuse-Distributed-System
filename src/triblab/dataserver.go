// reuse package name
package triblab

import (
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"trib"
)

// Serve as a backend based on the given configuration
func ServeBack(b *trib.BackConfig) error {
	s := rpc.NewServer()

	// using the old store interface
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

func DiskServeBack(b *trib.DiskServerConfig) error {
	s := rpc.NewServer()

	// using the old store interface
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
