package triblab

import (
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"trib"
)

// lock server entrance
func ServeLockServer(l *trib.LockServerConfig) error {
	// TODO: maintain locks and leases
	s := rpc.NewServer()

	// using the old store interface
	s.Register(l.Store)
	port := strings.Split(l.Addr, ":")[1]
	tempPort, portErr := strconv.Atoi(port)

	if portErr != nil {
		// port is not a number
		l.Ready <- false
		return portErr
	}
	if tempPort < 0 || tempPort > 65535 {
		// port out of range
		l.Ready <- false
		return nil
	}
	port = ":" + port
	lis, e := net.Listen("tcp", port)
	if e != nil {
		if l.Ready != nil {
			l.Ready <- false
			return e
		}
	}
	if l.Ready != nil {
		l.Ready <- true
	}
	http.Serve(lis, s)

	return nil

}
