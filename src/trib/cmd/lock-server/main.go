package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"

	"trib"
	"trib/local"
	"trib/ready"
	"trib/store"
	"triblab"
)

var (
	frc       = flag.String("rc", trib.DefaultRCPath, "bin storage config file")
	readyAddr = flag.String("ready", "", "ready notification address")
)

func noError(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func main() {
	flag.Parse()
    
	rc, e := trib.LoadRC(*frc)
	noError(e)

	run := func(i int) {
		if i > len(rc.LockServers) {
			noError(fmt.Errorf("lockserver index out of range: %d", i))
		}

		lockserverConfig := rc.LockServerConfig(i, store.NewLockStorage())
		if *readyAddr != "" {
			lockserverConfig.Ready = ready.Chan(*readyAddr, lockserverConfig.Addr)
		}
		log.Printf("lockserver serving on %s", lockserverConfig.Addr)
		noError(triblab.ServeLockServer(lockserverConfig))
	}

	args := flag.Args()

	n := 0
	if len(args) == 0 {
		// scan for addresses on this machine
		for i, b := range rc.LockServers {
			if local.Check(b) {
				go run(i)
				n++
			}
		}

		if n == 0 {
			log.Fatal("no lockserver found for this host")
		}
	} else {
		// scan for indices for the addresses
		for _, a := range args {
			i, e := strconv.Atoi(a)
			noError(e)
			go run(i)
			n++
		}
	}

	if n > 0 {
		select {}
	}
}
