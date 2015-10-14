package trib

// Backend config
type BackConfig struct {
	Addr  string      // listen address
	Store Storage     // the underlying storage it should use
	Ready chan<- bool // send a value when server is ready
}

type KeeperConfig struct {
	// The addresses of back-ends
	Backs []string

	// The addresses of keepers
	Addrs []string

	// The index of this back-end
	This int

	// Non zero incarnation identifier
	Id int64

	// Send a value when the keeper is ready The distributed key-value
	// service should be ready to serve when *any* of the keepers is
	// ready.
	Ready chan<- bool
}

type LockServerConfig struct {
	Addr	string
	Store	LockStorage
	Ready chan<- bool
}

type DiskServerConfig struct {
	Addr	string
	Store	PersistentStorage
	Ready chan<- bool
}

func (c *KeeperConfig) Addr() string {
	return c.Addrs[c.This]
}
