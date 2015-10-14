package triblab
/*
import (
	"trib"
	"crypto/md5"
	"fmt"
	"math/big"
	"trib/colon"
	"strconv"
	"time"
)

const MAX_SERVER_CNT = 10 

func myHashFunc(name string) int {
	data := []byte(name)
	tmp := md5.Sum(data)
	var c []byte
	for _, j := range tmp{
		//fmt.Println(j)
		c = append(c, j)
	}
	z := new(big.Int).SetBytes(c)
	rt := z.Uint64()
	return int((rt<<33)>>33)
}

func ServeKeeper(kc *trib.KeeperConfig) error {
	// init keeper: build chord config
	binName := "CHORD"
	binNameHash := myHashFunc(binName)
	indexAddr := make([]string, MAX_SERVER_CNT)
	succIndex := make([]string, MAX_SERVER_CNT)
	// rand.Seed(MAX_SERVER_CNT)
	for i := range indexAddr {
		indexAddr[i] = "N/A" // "N/A" means empty
	}
	for j := range succIndex {
		succIndex[j] = "N/A"
	}
	var rtClk uint64
	statusTable := make([]bool, len(kc.Backs))
	for i, addr := range kc.Backs { 
		// addrHash := binary.LittleEndian.Uint32(fnv.New32().Sum([]byte(addr)))
		// not so good as random...
		addrHash := myHashFunc(addr)
		index := addrHash % MAX_SERVER_CNT // 0 to 299 
		initialIndex := index

		err := NewClient(addr).Clock(0, &rtClk)
		if err == nil {
			statusTable[i] = true
		} else {
			statusTable[i] = false
			continue
		}

		// clockwise search slot
		var storedIndex int = MAX_SERVER_CNT + 1
		for {
			if indexAddr[index] != "N/A" {
				index = shiftIndex(index, true)
			} else {
				// index is "N/A"
				indexAddr[index] = addr
				storedIndex = index
				break
			}
			// all the way around !
			if index == initialIndex {
				// full
				kc.Ready <- false
				return fmt.Errorf("exceeds max server count! ") 
			}
		}
		// anti-clockwise change successor
		for {//could be optimized: do this only once after the indexAddr is setup///////////
			if storedIndex == MAX_SERVER_CNT {
				panic("logic wrong")
			}
			succIndex[index] = strconv.Itoa(storedIndex)
			index = shiftIndex(index, false)
			if indexAddr[index] != "N/A" {
				break
			}
		}	
		// 
		// next server
	}

	// write to backends (brute force example...doesn't matter here because of running only once)
	chordIndex := binNameHash % MAX_SERVER_CNT
	tempIndex, _ := strconv.ParseUint(succIndex[chordIndex], 10, 32) 
	firstServer := indexAddr[tempIndex]
	tempIndex, _ = strconv.ParseUint(succIndex[(tempIndex + 1)%MAX_SERVER_CNT], 10, 32)
	secondServer := indexAddr[tempIndex]
	tempIndex, _ = strconv.ParseUint(succIndex[(tempIndex + 1)%MAX_SERVER_CNT], 10, 32)
	thirdServer := indexAddr[tempIndex]
	// debug
	//fmt.Println("chord index: ", binNameHash % MAX_SERVER_CNT, "first: ", firstServer, "second: ", secondServer, "third: ", thirdServer)
	
	test_client1 := NewClient(firstServer)
	test_client2 := NewClient(secondServer)
	test_client3 := NewClient(thirdServer)
	clients := make([]trib.Storage, 3)
	clients[0] = test_client1
	clients[1] = test_client2
	clients[2] = test_client3
	success := false
	for _, cli := range clients {
		for i, a := range indexAddr {
			if err := cli.Set(&trib.KeyValue{"CHORD::INDEX" + strconv.Itoa(i), a}, &success); err != nil {return err}
			if err := cli.Set(&trib.KeyValue{"CHORD::" + a, strconv.Itoa(i)}, &success); err != nil {return err}
		}
		for j, b := range succIndex {
			if err := cli.Set(&trib.KeyValue{"CHORD::SUCC" + strconv.Itoa(j), b}, &success); err != nil {return err}
		}
	}

	// TODO: start real keepers
	// TODO: create server table. map server to bins. (not in keeper)
	//			SERVER + server_id::bin a -> pos,.....  pos = 0 or 1 or 2. 

	chordAddr := make([]string, 3)
	chordAddr[0] = firstServer
	chordAddr[1] = secondServer
	chordAddr[2] = thirdServer
	
	// get from chordAddr
	addrs, err := getClientsPublic(chordAddr, "BINSBYSERVER")
	if err != nil {
		return err
	}
	for _, add := range addrs {
		err = ListAppendHelper(addrs, "BINSBYSERVER::" + add, "BINSBYSERVER")
		if err != nil {
			return err
		}
	}
	for _, add := range chordAddr {
		err = ListAppendHelper(addrs, "BINSBYSERVER::" + add, "CHORD")
		if err != nil {
			return err
		}
	}
	fmt.Println(chordAddr)
	fmt.Println(addrs)

	kp := myKeeper{chordConfigBacks: chordAddr, binsByServerBacks: addrs, maintainBacks: kc.Backs, backStatus: statusTable}
	errChan := make(chan error)
	go kp.realKeeper(errChan)

	if kc.Ready != nil {
		kc.Ready <- true
	}
	// end init keeper

	// debug
	for k := range indexAddr {
		fmt.Println("index", k, indexAddr[k])
		fmt.Println("successor", succIndex[k])
	}
	return <-errChan //blocking when no error
}

type myKeeper struct {
	chordConfigBacks []string //where the chord table is
	binsByServerBacks []string //where the BINSBYSERVER table is
	maintainBacks []string
	backStatus []bool //keep tracking of the status of backs in maintainBacks[]
}

func ListGetHelper(addrs []string, key string) ([]string, error) {
	var err error
	list := new(trib.List)
	for _, add := range addrs {
		err = NewClient(add).ListGet(key, list)
		if err == nil {
			return list.L, nil
		}
	}
	return make([]string, 0), nil
}

func ListAppendHelper(addrs []string, key, value string) error {
	var err error
	var succ bool
	errCnt := 0
	kv := trib.KeyValue{Key: key, Value: value}
	for _, add := range addrs {
		err = NewClient(add).ListAppend(&kv, &succ)
		if err != nil || !succ {
			errCnt++ 
		}
	}
	if errCnt > 1 {
		return fmt.Errorf("ListAppend() fails more than once!")
	}
	return nil
}

func ListRemoveHelper(addrs []string, key, value string) error {
	var err error
	var rmCnt int
	errCnt := 0
	kv := trib.KeyValue{Key: key, Value: value}
	for _, add := range addrs {
		err = NewClient(add).ListRemove(&kv, &rmCnt)
		if err != nil {
			errCnt++ 
		}
	}
	if errCnt > 1 {
		return fmt.Errorf("ListRemove() fails more than once!")
	}
	return nil
}

func (self *myKeeper) binsByServerAddr(addr string) ([]string, error) {
	addrs, err := self.getClients("BINSBYSERVER")
	if err != nil {
		return make([]string, 0), err
	}
	return ListGetHelper(addrs, "BINSBYSERVER::" + addr)
}

func replicateHelper(srcAddr, dstAddr, binName string) error {
	srcClient := NewClient(srcAddr)
	dstClient := NewClient(dstAddr)
	pt := trib.Pattern{Prefix: colon.Escape(binName)+"::", Suffix: ""}
	keyList := new(trib.List)
	// migrate key-value pairs
	err := srcClient.Keys(&pt, keyList)
	if err != nil {
		return err
	}
	var tmpVal string
	var succ bool
	for _, key := range keyList.L {
		err = srcClient.Get(key, &tmpVal)
		if err != nil {
			return err
		}
		err = dstClient.Set(&trib.KeyValue{Key: key, Value: tmpVal}, &succ)
		if err != nil || !succ {
			return fmt.Errorf("Set() fails when migrating") 
		}
	}
	// migrate key-list pairs
	err = srcClient.ListKeys(&pt, keyList)
	if err != nil {
		return err
	}
	var valList trib.List
	for _, key := range keyList.L {
		err = srcClient.ListGet(key, &valList)
		if err != nil {
			return err
		}
		for _, val := range valList.L{
			err = dstClient.ListAppend(&trib.KeyValue{Key: key, Value: val}, &succ)
			if err != nil || !succ {
				return fmt.Errorf("ListAppend() fails when migrating") 
			}
		}
	}
	return nil
}

// do replication when a backend leaves
func (self *myKeeper) replicate(binName string) error {
	newAddrs, err := self.getClients(binName)
	err = replicateHelper(newAddrs[0], newAddrs[2], binName)
	return err
}

// do migration when a backend joins
func (self *myKeeper) migrate(idx int, addr, binName string) error {
	var succAddr string
	succ, err := getChordSuccByIndex(self.chordConfigBacks, shiftIndex(idx, true))
	succAddr, err = getChordAddrByIndex(self.chordConfigBacks, succ)
	err = replicateHelper(succAddr, addr, binName)
	return err
}

func getHelper(addrs []string, key string) (string, error) {
	var rtVal string
	var err error
	for _, add := range addrs {
		err = NewClient(add).Get(key, &rtVal)
		if err == nil {
			return rtVal, nil
		}
	}
	return "", err
}

func getChordIdxByAddr(addrs []string, addr string) (int, error) {
	rtVal, err := getHelper(addrs, "CHORD::" + addr)
	if len(rtVal) == 0 {
		return -1, fmt.Errorf("the index value should not be empty!") 
	}
	var rtInt int
	rtInt, err = strconv.Atoi(rtVal)
	if err != nil {
		return -1, err
	}
	return rtInt, nil
}

func getChordAddrByIndex(addrs []string, idx int) (string, error) {
	rtVal, err := getHelper(addrs, "CHORD::INDEX" + strconv.Itoa(idx))
	if len(rtVal) == 0 {
		return "", fmt.Errorf("the address value should not be empty!") 
	}
	return rtVal, err
}

func getChordSuccByIndex(addrs []string, idx int) (int, error) {
	rtVal, err := getHelper(addrs, "CHORD::SUCC" + strconv.Itoa(idx))
	if len(rtVal) == 0 {
		return -1, fmt.Errorf("the successor value should not be empty!") 
	}
	var rtInt int
	rtInt, err = strconv.Atoi(rtVal)
	if err != nil {
		return -1, err
	}
	return rtInt, nil
}

// this function guarantee to write two backends successfully
func setHelper(addrs []string, key, value string) error {
	var err error
	var succ bool
	kv := trib.KeyValue{Key: key, Value: value}
	errCnt := 0
	for _, add := range addrs {
		err = NewClient(add).Set(&kv, &succ)
		if err != nil || !succ {
			errCnt++
		}
	}
	if errCnt > 1 {
		return fmt.Errorf("more than one Set() in setHelper() fail!") 
	}
	return nil
}

func setChordIdxByAddr (addrs []string, addr string, newIdx int) error {
	return setHelper(addrs, "CHORD::" + addr, strconv.Itoa(newIdx))
}

func setChordAddrByIndex (addrs []string, idx int, newAddr string) error {
	return setHelper(addrs, "CHORD::INDEX" + strconv.Itoa(idx), newAddr)
}

func setChordSuccByIndex (addrs []string, idx, newIdx int) error {
	return setHelper(addrs, "CHORD::SUCC" + strconv.Itoa(idx), strconv.Itoa(newIdx))
}

func (self *myKeeper) getClients(binName string) ([]string, error) {
	hashName := myHashFunc(binName)
	idx := hashName%MAX_SERVER_CNT
	var err error
	var rtStr string
	var rtIdx []int
	rtAddr := make([]string, 0)
	for i:=0; i<3; i++ {
		idx, err = getChordSuccByIndex(self.chordConfigBacks, idx)
		if err != nil {
			return make([]string, 0), err
		}
		rtStr, err = getChordAddrByIndex(self.chordConfigBacks, idx)
		if err != nil {
			return make([]string, 0), err
		}
		
		rtAddr = append(rtAddr, rtStr)
		rtIdx[i] = idx
		idx = shiftIndex(idx, true)
	}
	return rtAddr, nil
}

func getClientsPublic(addrs []string, binName string) ([]string, error) {
	hashName := myHashFunc(binName)
	idx := hashName%MAX_SERVER_CNT
	var err error
	var rtStr string
	// rtIdx := make([]int,3)
	rtAddr := make([]string, 0)
	for i:=0; i<3; i++ {
		idx, err = getChordSuccByIndex(addrs, idx)
		if err != nil {
			return make([]string, 0), err
		}
		rtStr, err = getChordAddrByIndex(addrs, idx)
		if err != nil {
			return make([]string, 0), err
		}
		rtAddr = append(rtAddr, rtStr)
		// rtIdx[i] = idx
		idx = shiftIndex(idx, true)
	}
	return rtAddr, nil
}

// real keeper routine
// FYI: keeper config:
// Backs []string
// Keepers []string
// This int
// Id int64 <- check to see what this is..

func (self *myKeeper) realKeeper(errChan chan error) {
	// TODO:
	// instructions:
	// (1) Keepers also need its job table stored in memory. It should be similar to CHORD table.
	// (2) Keepers need to know that other keepers are alive. Use a status table to do keeper heart beat.
	// (3) Handle keeper job transition.
	// (4) Keep track of its own job table, monitor server heart beat.
	//     Should also keep tracking of the 3 chord config backends
	// (5) Handle server join / leave (Question: how it will know a new server join?)
	//			modify CHORD table immediately!
	//			server x leave: find x's server table. handle pos 0, 1, 2 respectively.
	//			server y join: find the server tables around y's index. handle their server tables...
	// (6) Add more if we think of any...


	c := time.Tick(1 * time.Second)
	ret := new(uint64)
	for _ = range c {
		var max_time uint64 = 0
		for i, addr := range self.maintainBacks {
			temp_client := NewClient(addr)
			err := temp_client.Clock(0, ret)
			if err != nil && self.backStatus[i]{//find a backend left
				err2 := self.leaveHandler(addr)
				if err2 != nil {
					errChan <- err2
				}
			}
			if err == nil && !self.backStatus[i]{//find a backend joined
				err2 := self.joinHandler(addr)
				if err2 != nil {
					errChan <- err2
				}
			}
			if max_time < *ret {
				max_time = *ret
			}
		}
		max_time++
		for i, addr := range self.maintainBacks {
			if self.backStatus[i] {
				temp_client := NewClient(addr)
				err := temp_client.Clock(max_time, ret)
				if err != nil && self.backStatus[i]{
					err2 := self.leaveHandler(addr)
					if err2 != nil {
						errChan <- err2
					}
				}
			}
		}
	}
}

func (self *myKeeper) leaveHandler(addr string) error {
	idx, err := getChordIdxByAddr(self.chordConfigBacks, addr)
	if err != nil {
		return err
	}
	err = self.chordLeave(idx)
	if err != nil {
		return err
	}

	binList, err := self.binsByServerAddr(addr)
	//TODO use map to deduplicate bin name
	for _, binName := range binList {
		ListRemoveHelper(self.binsByServerBacks, "BINSBYSERVER::"+addr, binName)
		self.replicate(binName)
	}

	//mantain self.chordConfigBacks addresses
	flag := false
	for _, v := range self.chordConfigBacks {
		if addr == v {
			flag = true
			break
		}
	}
	if flag {
		self.chordConfigBacks, err = self.getClients("CHORD")
		if err != nil {
			return err
		}
	}
	//mantain self.binsByServerBacks addresses
	flag = false
	for _, v := range self.binsByServerBacks {
		if addr == v {
			flag = true
			break
		}
	}
	if flag {
		self.binsByServerBacks, err = self.getClients("BINSBYSERVER")
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *myKeeper) joinHandler(addr string) error {
	var succIdx int
	var succAddr, rmReplicaAddr string
	newIdx, err := self.chordJoin(addr)
	if err != nil {
		return err
	}

	succIdx, err = getChordSuccByIndex(self.chordConfigBacks, shiftIndex(newIdx,true) )
	if err != nil {
		return err
	}
	succAddr, err =  getChordAddrByIndex(self.chordConfigBacks, succIdx)
	if err != nil {
		return err
	}
	binList, err := self.binsByServerAddr(succAddr)

	for i:=0; i<2; i++ {
		succIdx, err = getChordSuccByIndex(self.chordConfigBacks, shiftIndex(newIdx,true) )
		if err != nil {
			return err
		}
	}
	rmReplicaAddr, err =  getChordAddrByIndex(self.chordConfigBacks, succIdx)
	if err != nil {
		return err
	}

	//TODO use map to deduplicate bin name
	for _, binName := range binList {
		ListRemoveHelper(self.binsByServerBacks, "BINSBYSERVER::" + rmReplicaAddr, binName)
		self.migrate(newIdx, addr, binName)
	}
	//mantain self.chordConfigBacks addresses
	self.chordConfigBacks, err = self.getClients("CHORD")
	if err != nil {
		return err
	}
	//mantain self.binsByServerBacks addresses
	self.binsByServerBacks, err = self.getClients("BINSBYSERVER")
	if err != nil {
		return err
	}
	return nil
}

func (self *myKeeper) chordLeave(oriIdx int) error {
	// step1: set new succ index (search connter-clockwisely), set itself to "N/A"
	// step2: set others using this node as succ (walk anti-clockwisely)
	var err error
	var newSucc, rtIdx int
	idx := oriIdx
	err = setChordAddrByIndex(self.chordConfigBacks, idx, "N/A")
	if err != nil {
		return err
	}
	newSucc, err = getChordSuccByIndex(self.chordConfigBacks, shiftIndex(idx, true) )
	if err != nil {
		return err
	}

	for {//counter-clockwise search
		rtIdx, err = getChordSuccByIndex(self.chordConfigBacks, idx)
		if err != nil {
			return err
		}
		if rtIdx != oriIdx {
			break
		}
		err = setChordSuccByIndex(self.chordConfigBacks, idx, newSucc)
		if err != nil {
			return err
		}
		idx = shiftIndex(idx, false)
	}
	return nil
}

func (self *myKeeper) chordJoin(addr string) (int, error) {
	// similar as init code, find a "N/A", set succ to itself, then walk anti-clockwisely
	newIdx := myHashFunc(addr)
	oriIdx := newIdx
	var err error
	var oldAddr string
	var oldSucc, rtIdx int
	for {
		oldAddr, err = getChordAddrByIndex(self.chordConfigBacks, newIdx)
		if err != nil {
			return -1, err
		}
		if oldAddr == "N/A" {
			break
		}
		newIdx = shiftIndex(newIdx, true)
		if newIdx == oriIdx {
			return -1, fmt.Errorf("exceeds max server count! ") 
		}
	}
	err = setChordAddrByIndex(self.chordConfigBacks, newIdx, addr)
	if err != nil {
		return -1, err
	}
	oldSucc, err = getChordSuccByIndex(self.chordConfigBacks, newIdx)
	if err != nil {
		return -1, err
	}
	idx := newIdx
	for {//counter-clockwise search
		rtIdx, err = getChordSuccByIndex(self.chordConfigBacks, idx)
		if err != nil {
			return -1, err
		}
		if rtIdx != oldSucc {
			break
		}
		err = setChordSuccByIndex(self.chordConfigBacks, idx, newIdx)
		if err != nil {
			return -1, err
		}
		idx = shiftIndex(idx, false)
	}
	return newIdx, nil
}

// TODO: define more helper functions to help handle server join/leave,etc...

// helper function 0 -> max -> 0
func shiftIndex(input int, direction bool) int {
	if input == 0 {
		input = input + MAX_SERVER_CNT
	}
	if direction {
		input = input + 1
	} else {
		input = input - 1
	}
	input = input % MAX_SERVER_CNT
	return input
}*/
