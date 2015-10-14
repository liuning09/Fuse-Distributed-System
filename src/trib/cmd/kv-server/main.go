package main

import (
	"flag"
	"log"

	"trib/entries"
//	"trib/randaddr"
	"trib/store"
)

var (
	addr = flag.String("addr", "localhost:rand", "server listen address")
)

func main() {
	flag.Parse()

	*addr = "localhost:9900"//randaddr.Resolve(*addr)

	s := store.NewStorage()

	log.Printf("key-value store serving on %s", *addr)

	e := entries.ServeBackSingle(*addr, s, nil)
	if e != nil {
		log.Fatal(e)
	}
}
