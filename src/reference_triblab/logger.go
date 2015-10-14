package triblab

import (
	//"trib"
	//"strings"
	"encoding/json"
	"os"
	"fmt"
)
/**
 * helper functions to encode log
 * also means, expand "value" field of kv
*/

type BinLog struct {
	Op  string // operation type
	Val string // value
	Clk uint64 // time
}

func EncodeLog(log BinLog) string {
	//fmt.Println("Call encode function")
	fmt.Printf("op is %s ", log.Op)
	fmt.Printf("val is %s ", log.Val)
	fmt.Printf("clk is %d ", log.Clk)
	fmt.Println(" ")
	b,_ :=json.Marshal(log)
	n := len(b)
	fmt.Printf("log size is %d",n)
	fmt.Println(" ")
	os.Stdout.Write(b)
	fmt.Println(" ")
	return string(b)
}

func DecodeLog (value string) *BinLog {
	//fmt.Println("Call decode function")
	var log BinLog
	json.Unmarshal([]byte(value),&log)
	return &log
}


// TODO: sort by clock

