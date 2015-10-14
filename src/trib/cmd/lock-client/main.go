package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
//	"strconv"
	"strings"

	"trib"
	"triblab"
)

func noError(e error) {
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}

func logError(e error) {
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
	}
}

func kv(k, v string) *trib.KeyValue {
	return &trib.KeyValue{k, v}
}

func pat(pre, suf string) *trib.Pattern {
	return &trib.Pattern{pre, suf}
}

func kva(args []string) *trib.KeyValue {
	if len(args) == 1 {
		return kv("", "")
	} else if len(args) == 2 {
		return kv(args[1], "")
	}
	return kv(args[1], args[2])
}

func pata(args []string) *trib.Pattern {
	if len(args) == 1 {
		return pat("", "")
	} else if len(args) == 2 {
		return pat(args[1], "")
	}
	return pat(args[1], args[2])
}

func single(args []string) string {
	if len(args) == 1 {
		return ""
	}
	return args[1]
}

func printList(lst trib.List) {
	for _, e := range lst.L {
		fmt.Println(e)
	}
}

const help = `Usage:
   kv-client <server address> [command <args...>]

With no command specified to enter interactive mode. 
` + cmdHelp

const cmdHelp = `Command list:
   get <key>
   set <key> <value>
   keys [<prefix> [<suffix>]]
   list-get <key>
   list-append <key> <value>
   list-remove <key> <value>
   list-keys [<prefix> [<suffix]]
   clock [<atleast=0>]
   help
   exit
`

func runCmd(s trib.LockStorage, args []string) bool {
//	var v string
	var b bool
//	var lst trib.List
//	var n int
//	var cret uint64
	var m map[string]*trib.Holder

	cmd := args[0]

	switch cmd {
	case "ts":
		logError(s.TestAndSet(kva(args), &b))
		fmt.Println(b)
	case "release":
		logError(s.Release(kva(args), &b))
		fmt.Println(b)
	case "renew":
		logError(s.Renew(kva(args), &b))
		fmt.Println(b)
	case "help":
		fmt.Println(cmdHelp)
	case "debug":
		logError(s.Debug(single(args), &m))
		fmt.Println(m)
	case "exit":
		return true
	default:
		logError(fmt.Errorf("bad command, try \"help\"."))
	}
	return false
}

func fields(s string) []string {
	return strings.Fields(s)
}

func runPrompt(s trib.LockStorage) {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print("> ")

	for scanner.Scan() {
		line := scanner.Text()
		args := fields(line)
		if len(args) > 0 {
			if runCmd(s, args) {
				break
			}
		}
		fmt.Print("> ")
	}

	e := scanner.Err()
	if e != nil {
		panic(e)
	}
}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, help)
		os.Exit(1)
	}

	addr := args[0]
	s := triblab.NewFsLockClient(addr)

	cmdArgs := args[1:]
	if len(cmdArgs) == 0 {
		runPrompt(s)
		fmt.Println()
	} else {
		runCmd(s, cmdArgs)
	}
}
