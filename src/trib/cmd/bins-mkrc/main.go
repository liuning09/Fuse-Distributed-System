package main

import (
	"flag"
	"fmt"
	"log"

	"trib"
	"trib/randaddr"
)

// For now, we assume that we have sequentially-IP'd hosts that don't span more
// than one octet.
const IP_PREFIX = "169.228.66"
const FIRST_IP = 143
const NUM_HOSTS = 10

var (
	local   = flag.Bool("local", false, "always use local ports")
	nback   = flag.Int("nback", 1, "number of back-ends")
	nkeep   = flag.Int("nkeep", 1, "number of keepers")
	frc     = flag.String("rc", trib.DefaultRCPath, "bin storage config file")
	full    = flag.Bool("full", false, "setup of 10 back-ends and 3 keepers")
	fixPort = flag.Bool("fix", false, "fix port numbers; don't use random ones")
)

func main() {
	flag.Parse()

	if *nback > 300 {
		log.Fatal(fmt.Errorf("too many back-ends"))
	}
	if *nkeep > NUM_HOSTS {
		log.Fatal(fmt.Errorf("too many keepers"))
	}

	if *full {
		*nback = NUM_HOSTS
		*nkeep = 3
	}

	p := 3000
	if !*fixPort {
		p = randaddr.RandPort()
	}

	rc := new(trib.RC)
	rc.Backs = make([]string, *nback)
	rc.Keepers = make([]string, *nkeep)

	if !*local {
		const ipOffset = FIRST_IP
		const nmachine = NUM_HOSTS

		for i := 0; i < *nback; i++ {
			host := fmt.Sprintf("%s.%d", IP_PREFIX, ipOffset+i%nmachine)
			rc.Backs[i] = fmt.Sprintf("%s:%d", host, p+i/nmachine)
		}

		p += *nback / nmachine
		if *nback%nmachine > 0 {
			p++
		}

		for i := 0; i < *nkeep; i++ {
			host := fmt.Sprintf("%s.%d", IP_PREFIX, ipOffset+i%nmachine)
			rc.Keepers[i] = fmt.Sprintf("%s:%d", host, p)
		}
	} else {
		for i := 0; i < *nback; i++ {
			rc.Backs[i] = fmt.Sprintf("localhost:%d", p)
			p++
		}

		for i := 0; i < *nkeep; i++ {
			rc.Keepers[i] = fmt.Sprintf("localhost:%d", p)
			p++
		}
	}

	fmt.Println(rc.String())

	if *frc != "" {
		e := rc.Save(*frc)
		if e != nil {
			log.Fatal(e)
		}
	}
}
