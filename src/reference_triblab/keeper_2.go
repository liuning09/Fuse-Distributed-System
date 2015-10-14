package triblab

import (
	"trib"
	"trib/store"
	"fmt"
	"strconv"
	"time"
	"crypto/md5"
	"math/big"
	"trib/colon"
)


type Keeper struct {
	Backs	[]string
	Keepers	[]string
	This 	int
	Id		int64
	BackStates map[string]bool
	KeeperStates map[string]bool
	BackClients []*client
	KeeperClients []*client
	MyClient *client 
	chordConfigBacks []string //where the chord table is
	binsByServerBacks []string //where the BINSBYSERVER table is
}

func ServeKeeper(kc *trib.KeeperConfig) error {
	// the keeper entry function
	k := NewKeeper(kc.Backs, kc.Addrs, kc.This, kc.Id)
	err := k.Init()
	if err != nil {
		return err
	}
	if kc.Ready != nil {
		kc.Ready <- true
	}
	// forever loop: frequency TBD
	c := time.Tick(1 * time.Second)
	for _ = range c {
		go k.Heartbeat()
	}
	return nil
}

func NewKeeper(backs []string, keepers []string, index int, id int64) *Keeper {
	return &Keeper{backs, keepers, index, id, make(map[string]bool), make(map[string]bool), make([]*client, 0, len(backs)), make([]*client, 0, len(keepers)), &client{keepers[index]}, make([]string, 0, 3), make([]string, 0, 3)}
}

// get the order of this keeper
func (self *Keeper) KeeperOrder() int {
	count := 0
	for i, keeper := range self.Keepers {
		if self.KeeperStates[keeper] == true {
			count++
		}
		if i == self.This {
			break
		}
	}
	count = count - 1
	return count
}

func (self *Keeper) Init() error {
	// start rpc server
	s := store.NewStorage()
	conf := trib.BackConfig{self.Keepers[self.This], s, nil}
	//fmt.Println("serve on keeper ", self.Keepers[self.This])
	go ServeBack(&conf)
	// help init chord: this operation is idempotent, bad efficiency but ok to go...
	err := self.InitChord()
	if err != nil {
		return err
	}
	// sleep to wait for other RPC server to start
	time.Sleep(time.Millisecond * 1)
	// create keeper clients
	for _, keeper := range self.Keepers {
		self.KeeperClients = append(self.KeeperClients, &client{keeper})
	}
	// ping keepers with rpc call
	// change keeper states
	keeperCnt := self.ChangeKeeperStates()
	// create job table
	self.CreateJobTable(keeperCnt)
	// ping backs on the job table
	// change back states
	self.ChangeBackStates()
	
	// done here?

	// debug
	//fmt.Println("Keeper states: ", self.KeeperStates)
	//fmt.Println("Backend states: ", self.BackStates)
	
	return nil
}

func (self *Keeper) InitChord() error {
	// get global status first
	self.CreateJobTable(1) // mod 1 is 0 forever...
	self.ChangeBackStates()
	// len(Backs) is equal to max server count
	maxServerCount := len(self.Backs)
	indexAddr := make([]string, maxServerCount)
	succIndex := make([]string, maxServerCount)
	for i := range indexAddr {
		indexAddr[i] = "N/A"
	}
	for j := range succIndex {
		indexAddr[j] = "N/A"
	}
	for _, backClient := range self.BackClients {
		addr := backClient.addr
		if self.BackStates[addr] == true {
			// server is up
			addrHash := myHashFunc(addr)//rand.Uint32()
			index := addrHash % maxServerCount
			initialIndex := index
			// clockwise search slot
			var storedIndex int = maxServerCount // impossible value
			for {
				if indexAddr[index] != "N/A" {
					index = ShiftIndex(index, true, maxServerCount)
				} else {
					// index is "N/A"
					indexAddr[index] = addr
					storedIndex = index
					break
				}
				// all the way around!
				if index == initialIndex {
					return fmt.Errorf("exceeds max server count!")
				}
			}
			// anti=clockwise change successor
			for {
				if storedIndex == maxServerCount {
					panic("logic wrong, go back and check!")
				}
				succIndex[index] = strconv.Itoa(storedIndex)
				index = ShiftIndex(index, false, maxServerCount)
				if indexAddr[index] != "N/A" {
					break
				}
			}
			// next server
		} // else server is down
	}

	// sync initial chord table to backends...
	//chordIndex := myHashFunc("CHORD") % maxServerCount
	// find the first address in Backs....
/*	chordFirstAddr := "N/A"
	for _, addr := range self.Backs {
		if self.BackStates[addr] == true {
			chordFirstAddr = addr
			break
		}
	}
	if chordFirstAddr == "N/A" {
		panic("no server online!")
	}
	chordIndex := -1
	for i, addr := range indexAddr {
		if chordFirstAddr == addr {
			chordIndex = i
			break
		}
	}
	if chordIndex == -1 {
		panic("logic wrong")
	}*/ // <- bad method

	chordIndex := myHashFunc("CHORD") % maxServerCount
	tempIndex, _ := strconv.Atoi(succIndex[chordIndex])
	firstServer := indexAddr[tempIndex]
	tempIndex, _ = strconv.Atoi(succIndex[(tempIndex+1)%maxServerCount])
	secondServer := indexAddr[tempIndex]
	tempIndex, _ = strconv.Atoi(succIndex[(tempIndex+1)%maxServerCount])
	thirdServer := indexAddr[tempIndex]

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
			if err := cli.Set(&trib.KeyValue{"CHORD::WIP" + a, "0"}, &success); err != nil {return err}
		}
		for j, b := range succIndex {
			if err := cli.Set(&trib.KeyValue{"CHORD::SUCC" + strconv.Itoa(j), b}, &success); err != nil {return err}
		}
	}

	chordAddr := make([]string, 3)
	chordAddr[0] = firstServer
	chordAddr[1] = secondServer
	chordAddr[2] = thirdServer
	
	// get from chordAddr
	addrs, err := getClientsByBin(chordAddr, "BINSBYSERVER", maxServerCount)
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

	self.chordConfigBacks = chordAddr
	self.binsByServerBacks = addrs

	self.PushChordTable(indexAddr, succIndex)
	// debug
	//fmt.Println("CHORD in memory:")
	/*for k := range indexAddr {
		fmt.Println("  index", k, indexAddr[k])
		fmt.Println("  successor", succIndex[k])
	}
	fmt.Println("chord's index: ", myHashFunc("CHORD") % maxServerCount, "first: ", firstServer, "second: ", secondServer, "third: ", thirdServer)
	fmt.Println("chord in keeper: ", self.chordConfigBacks)
	fmt.Println("bbs in keeper: ", self.binsByServerBacks) */
	return nil
}

func (self *Keeper) PushChordTable(indexAddr []string, succIndex []string) {
	success := false
	for _, addr := range self.Backs {
		cli := &client{addr}
		for i, a := range indexAddr {
			if err := cli.Set(&trib.KeyValue{"CHORD::INDEX" + strconv.Itoa(i), a}, &success); err != nil {continue}
			if err := cli.Set(&trib.KeyValue{"CHORD::" + a, strconv.Itoa(i)}, &success); err != nil {continue}
			if err := cli.Set(&trib.KeyValue{"CHORD::WIP" + a, "0"}, &success); err != nil {continue}
		}
		for j, b := range succIndex {
			if err := cli.Set(&trib.KeyValue{"CHORD::SUCC" + strconv.Itoa(j), b}, &success); err != nil {continue}
		}
	}
}

func (self *Keeper) CreateJobTable(keeperCnt int) {
	order := self.KeeperOrder()
	self.BackClients = make([]*client, 0, len(self.Backs))
	if keeperCnt == 1 {
		// everything is yours...
		for _, back := range self.Backs {
			self.BackClients = append(self.BackClients, &client{back})
		}
	} else {
		for i, back := range self.Backs {
			if i % keeperCnt == order {
				self.BackClients = append(self.BackClients, &client{back})
			}
		}
	}
}

// change state only happens at 1) init 2) re-init
func (self *Keeper) ChangeKeeperStates() int {
	keeperCnt := 0
	self.KeeperStates = make(map[string]bool)
	for _, keeperClient := range self.KeeperClients {
		var tmp uint64 = 0
		err := keeperClient.Clock(0, &tmp)
		if err == nil {
			// keeper alive
			self.KeeperStates[keeperClient.addr] = true
			keeperCnt++
		}
		if err != nil {
			// keeper down
			self.KeeperStates[keeperClient.addr] = false
		}
	}
	return keeperCnt
}

// TODO: continue migration
func (self *Keeper) ChangeBackStates() {
	self.BackStates = make(map[string]bool)
	for _, backClient := range self.BackClients {
		var tmp uint64 = 0
		err := backClient.Clock(0, &tmp)
		if err == nil {
			// back alive
			self.BackStates[backClient.addr] = true
			wipFlag, _ := getChordWipFlag(self.chordConfigBacks, backClient.addr)
			// work in progress config found! the keeper is down. do it again. TODO: dedup in clients..
			if wipFlag == 1 {
				// redo migration
				//fmt.Println("!!!!!!!!!! migration was down, need to redo... TODO migrate to new server again")
				self.joinHandler(backClient.addr, self.chordConfigBacks, self.binsByServerBacks)
				setChordWipFlag(self.chordConfigBacks, backClient.addr, 0)
			}
		}	
		if err != nil {
			self.BackStates[backClient.addr] = false
			wipFlag, _ := getChordWipFlag(self.chordConfigBacks, backClient.addr)
			if wipFlag == 1 {
				// redo migration
				//fmt.Println("!!!!!!!!!! migration was down, need to redo... TODO recover from other server")
				self.leaveHandler(backClient.addr, self.chordConfigBacks, self.binsByServerBacks)
				setChordWipFlag(self.chordConfigBacks, backClient.addr, 0)
			}
		}
	}
}

func (self *Keeper) Heartbeat() {
	//fmt.Println("Heartbeat begin ...", self.This)
	binsByServerBacksBackup := self.binsByServerBacks // backup
	chordConfigBacksBackup := self.chordConfigBacks // backup
	// clock oneself :)
	keeperClk := uint64(0)
	self.MyClient.Clock(0, &keeperClk) // use as atLeast
	// clock backends .. blindly
	var max uint64 = 0
	var tmpClk uint64 = 0
	for _, cli := range self.BackClients {
		err := cli.Clock(keeperClk, &tmpClk)
		if err == nil {
			if tmpClk > max {
				max = tmpClk
			}
		}	
	}
	// use buffered channel to do multi-threading optimization
	backhbch := make(chan uint64, len(self.BackClients))
	for _, backClient := range self.BackClients {
		go func(backClient *client, atLeast uint64) {
			backClk := uint64(0)
			err := backClient.Clock(atLeast, &backClk)
			if err == nil {
				// server up
				if self.BackStates[backClient.addr] == false {
					// new server !
					// set up a flag, saying someone's working on it...
					//fmt.Println("new server migration begins ...")
					setChordWipFlag(chordConfigBacksBackup, backClient.addr, 1)
					// mark as true to avoid duplicate migrations
					self.BackStates[backClient.addr] = true 
					self.joinHandler(backClient.addr, chordConfigBacksBackup, binsByServerBacksBackup)
					setChordWipFlag(chordConfigBacksBackup, backClient.addr, 0)
					//fmt.Println("new server up!")
				}
				backhbch <- backClk
			}
			if err != nil {
				// server down
				if self.BackStates[backClient.addr] == true {
					// fault happens! 
					//fmt.Println("server down! migration begins ...")
					setChordWipFlag(chordConfigBacksBackup, backClient.addr, 1)
					self.BackStates[backClient.addr] = false 
					self.leaveHandler(backClient.addr, chordConfigBacksBackup, binsByServerBacksBackup)
					setChordWipFlag(chordConfigBacksBackup, backClient.addr, 0)
					//fmt.Println("old server down!")
				}
				backhbch <- uint64(0)
			}
			backhbch <- uint64(0) // should never reach here...
		}(backClient, max)
	}
	// block for heartbeats to finish...
	for _ = range self.BackClients {
		value := <- backhbch
		if value > keeperClk {
			keeperClk = value
		}
	}

	//mantain self.chordConfigBacks addresses
	self.chordConfigBacks, _ = self.getClients("CHORD", chordConfigBacksBackup)
	//mantain self.binsByServerBacks addresses
	self.binsByServerBacks, _ = self.getClients("BINSBYSERVER", chordConfigBacksBackup)
//	fmt.Println("&&&&& DEBUG: maintain bbs addrs", self.binsByServerBacks)

	// keeper heartbeat
	keeperhbch := make(chan uint64, len(self.KeeperClients))
	var ReInitFlag bool = false
	for _, keeperClient := range self.KeeperClients {
		go func(keeperClient *client, atLeast uint64) {
			otherKeeperClk := uint64(0)
			err := keeperClient.Clock(atLeast, &otherKeeperClk)
			if err == nil {
				// keeper up
				if self.KeeperStates[keeperClient.addr] == false {
					// new keeper !
					ReInitFlag = true
					//fmt.Println("new keeper up!")
					//fmt.Println(keeperClient.addr)
				}
				keeperhbch <- otherKeeperClk
			}
			if err != nil {
				// keeper down
				if self.KeeperStates[keeperClient.addr] == true {
					// fault happens !
					ReInitFlag = true
					//fmt.Println("old keeper down!")
				}
				keeperhbch <- uint64(0)
			}
			keeperhbch <- uint64(0)
		}(keeperClient, keeperClk)
	}
	// block for hearbeats to finish...
	for _ = range self.KeeperClients {
		value := <- keeperhbch
		if value > keeperClk {
			keeperClk = value
		}
	}
	self.MyClient.Clock(keeperClk, &keeperClk) // use as atLeast

	
	// re-init
	if (ReInitFlag) {
		//fmt.Println("+++++++++++ re-init +++++++++++")
		// ping keepers with rpc call
		// change keeper states
		keeperCnt := self.ChangeKeeperStates()
		// create job table
		//fmt.Println(keeperCnt)
		self.InitChord()
		self.CreateJobTable(keeperCnt)
		// ping backs on the job table
		// change back states
		self.ChangeBackStates()
		//fmt.Println("after re-init:")
		//fmt.Println(self.KeeperStates)
		//fmt.Println(self.BackStates)
	}

	// debug
	//fmt.Println(self.KeeperStates)
	//fmt.Println(self.BackStates)
	//fmt.Println("end of heartbeat")
}

func ShiftIndex(input int, direction bool, maxServerCount int) int {
	if input == 0 {
		input = input + maxServerCount 
	}
	if direction {
		input = input + 1
	} else {
		input = input - 1
	}
	input = input % maxServerCount
	return input
}

func myHashFunc(name string) int {
	data := []byte(name)
	tmp := md5.Sum(data)
	var c []byte
	for i := 14; i < 16; i++ {
		c = append(c, tmp[i])
	}
	z := new(big.Int).SetBytes(c)
	return int(z.Uint64())
}

// this function guarantee to write two backends successfully <- try write to all servers, no error will be punished
func setHelper(addrs []string, key, value string) error { 
	//var err error
	var succ bool
	kv := trib.KeyValue{Key: key, Value: value}
	//errCnt := 0
	for _, add := range addrs {
		NewClient(add).Set(&kv, &succ)
		/*if err != nil || !succ {
			errCnt++
		}*/
	}
	/*if errCnt > 1 {
		return fmt.Errorf("more than one Set() in setHelper() fail!") 
	}*/
	return nil
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

func setChordIdxByAddr(addrs []string, addr string, newIdx int) error {
	return setHelper(addrs, "CHORD::" + addr, strconv.Itoa(newIdx))
}

func setChordAddrByIndex(addrs []string, idx int, newAddr string) error {
	return setHelper(addrs, "CHORD::INDEX" + strconv.Itoa(idx), newAddr)
}

func setChordSuccByIndex(addrs []string, idx, newIdx int) error {
	return setHelper(addrs, "CHORD::SUCC" + strconv.Itoa(idx), strconv.Itoa(newIdx))
}

// Work in progress flag, 0 means not wip, 1 means wip
func setChordWipFlag(addrs []string, addr string, flag int) error {
	return setHelper(addrs, "CHORD::WIP" + addr, strconv.Itoa(flag))
}

func getChordWipFlag(addrs []string, addr string) (int, error) {
	flag, err := getHelper(addrs, "CHORD::WIP" + addr)
	if err != nil {
		return 0, err
	}
	if flag == "" {
		return 0, nil
	} else {
		retFlag, err := strconv.Atoi(flag)
		return retFlag, err
	}
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

func getClientsByBin(addrs []string, binName string, maxServerCount int) ([]string, error) {
	hashName := myHashFunc(binName)
	idx := hashName % maxServerCount
	var err error
	var rtStr string
	// rtIdx := make([]int,3)
	rtAddr := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
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
		idx = ShiftIndex(idx, true, maxServerCount)
	}
	return rtAddr, nil
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
	//fmt.Println("in list append : ", addrs, " ", key, value)
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
	//fmt.Println("in list remove : ", addrs, " ", key, value)
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

func (self *Keeper) leaveHandler(addr string, chordConfigBacksBackup []string, binsByServerBacksBackup []string) error {
	idx, err := getChordIdxByAddr(chordConfigBacksBackup, addr)
	if err != nil {
		return err
	}
	if idx == -1 {
		fmt.Println("!!!!!!!!!!!!! -1")
	}
	err = self.chordLeave(idx, chordConfigBacksBackup)
	if err != nil {
		return err
	}

	binList, err := self.binsByServerAddr(addr, binsByServerBacksBackup)
	binList = removeDuplicates(binList)
	bbsFlag := false
	for _, binName := range binList {
		if binName != "BINSBYSERVER" {
			ListRemoveHelper(binsByServerBacksBackup, "BINSBYSERVER::" + addr, binName)
			self.replicate(binName, chordConfigBacksBackup)
		} else {
			bbsFlag = true
		}
	}
	// move BinsByServer bin last
	if bbsFlag == true {
		ListRemoveHelper(binsByServerBacksBackup, "BINSBYSERVER::" + addr, "BINSBYSERVER")
		self.replicate("BINSBYSERVER", chordConfigBacksBackup)
	}
	

	//mantain self.chordConfigBacks addresses
	/*flag := false
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
	}*/
	//mantain self.binsByServerBacks addresses
	/*flag = false
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
	}*/
	return nil
}

func (self *Keeper) joinHandler(addr string, chordConfigBacksBackup []string, binsByServerBacksBackup []string) error {
	maxServerCount := len(self.Backs)
	var succIdx int
	var succAddr, rmReplicaAddr string
	newIdx, err := self.chordJoin(addr, chordConfigBacksBackup)
	if err != nil {
		return err
	}

	succIdx, err = getChordSuccByIndex(chordConfigBacksBackup, ShiftIndex(newIdx,true, maxServerCount))
	if err != nil {
		return err
	}
	succAddr, err =  getChordAddrByIndex(chordConfigBacksBackup, succIdx)
	if err != nil {
		return err
	}
	binList, err := self.binsByServerAddr(succAddr, binsByServerBacksBackup)
	//fmt.Println("&&& DEBUG: successor id", succIdx)
	//fmt.Println("&&& DEBUG: successor addr ", succAddr)
	//fmt.Println("&&& DEBUG : ", binList)

	// for i:=0; i<3; i++ {//////////////////////
	// 	succIdx, err = getChordSuccByIndex(self.chordConfigBacks, ShiftIndex(newIdx, true, maxServerCount))
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	// rmReplicaAddr, err =  getChordAddrByIndex(self.chordConfigBacks, succIdx)
	// if err != nil {
	// 	return err
	// }

	binList = removeDuplicates(binList)
	bbsFlag := false
	for _, binName := range binList {
		if binName != "BINSBYSERVER" {
			addrs, _ := self.getClients(binName, chordConfigBacksBackup)
			//fmt.Println("&&&& DEBUG: planned addrs ", addrs)
			changeFlag := false
			for _, v := range addrs {
				if v == addr {
					changeFlag = true
					break
				}
			}
			if !changeFlag {
				continue
			}

			tmpIdx, _ := getChordIdxByAddr(chordConfigBacksBackup, addrs[2])
			if tmpIdx == -1 {
				fmt.Println("!!!!!!!! -1")
			}
			rmIdx, _ := getChordSuccByIndex(chordConfigBacksBackup, ShiftIndex(tmpIdx, true, maxServerCount))
			//fmt.Println("### debug: succ ", rmIdx)
			rmReplicaAddr, err = getChordAddrByIndex(chordConfigBacksBackup, rmIdx) // ??? <- this is wrong!
			//fmt.Println("### debug: rmAddr ", rmReplicaAddr)///////////////////

			ListRemoveHelper(binsByServerBacksBackup, "BINSBYSERVER::" + rmReplicaAddr, binName)
			self.migrate(newIdx, addr, binName, chordConfigBacksBackup)
		} else {
			bbsFlag = true
		}
	}
	if bbsFlag == true {
		addrs, _ := self.getClients("BINSBYSERVER", chordConfigBacksBackup)
		changeFlag := false
		for _, v := range addrs {
			if v == addr {
				changeFlag = true
				break
			}
		}
		if changeFlag {
			tmpIdx, _ := getChordIdxByAddr(chordConfigBacksBackup, addrs[2])
			if tmpIdx == -1 {
				fmt.Println("!!!!!!!! -1")
			}
			rmIdx, _ := getChordSuccByIndex(chordConfigBacksBackup, ShiftIndex(tmpIdx, true, maxServerCount))
			rmReplicaAddr, err = getChordAddrByIndex(chordConfigBacksBackup, rmIdx)

			ListRemoveHelper(binsByServerBacksBackup, "BINSBYSERVER::" + rmReplicaAddr, "BINSBYSERVER")
			self.migrate(newIdx, addr, "BINSBYSERVER", chordConfigBacksBackup)
		}
	}
/*
	//mantain self.chordConfigBacks addresses
	self.chordConfigBacks, err = self.getClients("CHORD")
	if err != nil {
		return err
	}
	//mantain self.binsByServerBacks addresses
	self.binsByServerBacks, err = self.getClients("BINSBYSERVER")
	fmt.Println("&&&&& DEBUG: maintain bbs addrs", self.binsByServerBacks)
	if err != nil {
		return err
	}*/
	return nil
}

func (self *Keeper) chordLeave(oriIdx int, chordConfigBacksBackup []string) error {
	maxServerCount := len(self.Backs)


	// step1: set new succ index (search connter-clockwisely), set itself to "N/A"
	// step2: set others using this node as succ (walk anti-clockwisely)
	var err error
	var newSucc, rtIdx int
	idx := oriIdx
	voidAddr, _ := getChordAddrByIndex(chordConfigBacksBackup, idx)
	if voidAddr == "N/A" {
		return nil
	}
	//err = setChordAddrByIndex(chordConfigBacksBackup, idx, "N/A")
	err = setChordAddrByIndex(self.Backs, idx, "N/A")
	//setChordIdxByAddr(chordConfigBacksBackup, voidAddr, -1)
	setChordIdxByAddr(self.Backs, voidAddr, -1)
	if err != nil {
		return err
	}
	newSucc, err = getChordSuccByIndex(chordConfigBacksBackup, ShiftIndex(idx, true, maxServerCount))
	if err != nil {
		return err
	}

	for {//counter-clockwise search
		rtIdx, err = getChordSuccByIndex(chordConfigBacksBackup, idx)
		if err != nil {
			return err
		}
		if rtIdx != oriIdx {
			break
		}
		//err = setChordSuccByIndex(chordConfigBacksBackup, idx, newSucc)
		err = setChordSuccByIndex(self.Backs, idx, newSucc)
		if err != nil {
			return err
		}
		idx = ShiftIndex(idx, false, maxServerCount)
	}
	return nil
}

func (self *Keeper) chordJoin(addr string, chordConfigBacksBackup []string) (int, error) {
	maxServerCount := len(self.Backs)
	// similar as init code, find a "N/A", set succ to itself, then walk anti-clockwisely
	newIdx := myHashFunc(addr)%maxServerCount
	oriIdx := newIdx
	var err error
	var oldAddr string
	var oldSucc, rtIdx int
	for {
		oldAddr, err = getChordAddrByIndex(chordConfigBacksBackup, newIdx)
		if oldAddr == addr {
			return newIdx, nil
		}
		if err != nil {
			return -1, err
		}
		if oldAddr == "N/A" {
			break
		}
		newIdx = ShiftIndex(newIdx, true, maxServerCount)
		if newIdx == oriIdx {
			return -1, fmt.Errorf("exceeds max server count! ") 
		}
	}
	//err = setChordAddrByIndex(chordConfigBacksBackup, newIdx, addr)
	err = setChordAddrByIndex(self.Backs, newIdx, addr)
	//setChordIdxByAddr(chordConfigBacksBackup, addr, newIdx)
	setChordIdxByAddr(self.Backs, addr, newIdx)
	if err != nil {
		return -1, err
	}
	oldSucc, err = getChordSuccByIndex(chordConfigBacksBackup, newIdx)
	if err != nil {
		return -1, err
	}
	idx := newIdx
	for {//counter-clockwise search
		rtIdx, err = getChordSuccByIndex(chordConfigBacksBackup, idx)
		if err != nil {
			return -1, err
		}
		if rtIdx != oldSucc {
			break
		}
		//err = setChordSuccByIndex(chordConfigBacksBackup, idx, newIdx)
		err = setChordSuccByIndex(self.Backs, idx, newIdx)
		if err != nil {
			return -1, err
		}
		idx = ShiftIndex(idx, false, maxServerCount)
	}
	return newIdx, nil
}

func (self *Keeper) getClients(binName string, chordConfigBacksBackup []string) ([]string, error) {
	maxServerCount := len(self.Backs)
	hashName := myHashFunc(binName)
	idx := hashName % maxServerCount
	var err error
	var rtStr string
	//var rtIdx []int
	rtAddr := make([]string, 0)
	for i:=0; i<3; i++ {
		idx, err = getChordSuccByIndex(chordConfigBacksBackup, idx)
		if err != nil {
			return make([]string, 0), err
		}
		rtStr, err = getChordAddrByIndex(chordConfigBacksBackup, idx)
		if err != nil {
			return make([]string, 0), err
		}
		
		rtAddr = append(rtAddr, rtStr)
		//rtIdx[i] = idx
		idx = ShiftIndex(idx, true, maxServerCount)
	}
	return rtAddr, nil
}

func (self *Keeper) binsByServerAddr(addr string, binsByServerBacksBackup []string) ([]string, error) {
	//addrs, err := self.getClients("BINSBYSERVER")
	addrs := binsByServerBacksBackup
	//fmt.Println("^^^ DEBUG : in binsbyserveraddr: ", addrs)
	//if err != nil {
	//	return make([]string, 0), err
	//}
	return ListGetHelper(addrs, "BINSBYSERVER::" + addr)
}

// do replication when a backend leaves
func (self *Keeper) replicate(binName string, chordConfigBacksBackup []string) error {
	newAddrs, err := self.getClients(binName, chordConfigBacksBackup)
	//ListAppendHelper(self.binsByServerBacks, "BINSBYSERVER::" + newAddrs[2], binName)
	ListAppendHelper(newAddrs, "BINSBYSERVER::" + newAddrs[2], binName)
	err = replicateHelper(newAddrs[0], newAddrs[2], binName)
	return err
}

// do migration when a backend joins
func (self *Keeper) migrate(idx int, addr, binName string, chordConfigBacksBackup []string) error {
	maxServerCount := len(self.Backs)
	var succAddr string
	succ, err := getChordSuccByIndex(chordConfigBacksBackup, ShiftIndex(idx, true, maxServerCount))
	succAddr, err = getChordAddrByIndex(chordConfigBacksBackup, succ)
	addrs, _ := self.getClients(binName, chordConfigBacksBackup)
	ListAppendHelper(addrs, "BINSBYSERVER::" + addr, binName)
	//ListAppendHelper(self.binsByServerBacks, "BINSBYSERVER::" + addr, binName)
	err = replicateHelper(succAddr, addr, binName)
	return err
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
