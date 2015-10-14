package main

import (
	"bufio"
	"fuse"
	"trib"
	"triblab"
	"time"
	"os"
	"io"
	"flag"
	"math"
	"path/filepath"
	"math/rand"
	"strings"

	"encoding/json"
	"strconv"
	"fmt"
)

var frc = flag.String("rc", trib.DefaultRCPath, "bin storage config file")//////////////////

const cmdHelp = `Command List:
   upload filename chunksize FSname username
   help
   exit
`

func noError(e error) {
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}

func uploadChunk(addr, dir, fileName string, ino, ckSz, off, size int64, dCh chan bool, eCh chan error) {
	buf := make([]byte, ckSz)
	f, err := os.Open(dir+"/"+fileName)
	if err != nil {
		fmt.Println("open error")
		dCh <- false
		eCh <- err
		return
	}

	readLen, err := f.ReadAt(buf, off)
	if err == io.EOF {
		buf = buf[:readLen]
	}else if err != nil {
		fmt.Println("ReadAt error")
		dCh <- false
		eCh <- err
		return
	}
	// fmt.Println("upload buf:", buf)

	var succ bool
	myclient := triblab.NewClient(addr)
	err = myclient.Set(&trib.KeyValue{Key: "__file_segment_"+strconv.FormatInt(ino, 10)+"_"+strconv.FormatInt(off, 10), Value: string(buf)}, &succ)
	if err != nil {
		fmt.Println("set error")
		dCh <- false
		eCh <- err
		return
	} else {
		dCh <- true
	}
}

func uploadFile(file, fsNa, userNa string, chunkSz int64) {
	var fileName string
	slashPos := strings.LastIndexAny(file, "/")
	if slashPos == -1 {
		fileName = file
	} else {
		fileName = file[slashPos+1:]
	}
	dir, err := filepath.Abs(filepath.Dir(file))
	noError(err)
	fmt.Println(dir)
	fmt.Println(fileName)

	var fsName, userName string
	fsName = fsNa
	userName = userNa

	nextId, err := triblab.GetNextIdHelper(fsName, userName)
	noError(err)
	err = triblab.SetNextIdHelper(nextId+1, fsName, userName)
	noError(err)

	fileHandle, err := os.Open(dir+"/"+fileName)
	fileInfo, err := fileHandle.Stat()
	noError(err)
	fiSz := fileInfo.Size()
	
	rc, err := trib.LoadRC(*frc)
	noError(err)
	partNum := int(math.Ceil(float64(fiSz)/float64(chunkSz)))
	doneChan := make(chan bool, partNum+10)
	errChan := make(chan error, partNum+10)

	var addr string
	var curOffset int64
	fiConf := triblab.FileConf{
		FileName: fileName, 
		PartNum: partNum,
		ChunkSize: chunkSz,
		OffAddrPairs: make([]*triblab.OffsetAddr, partNum),
	}
	// fmt.Println("partNum", partNum)
	for i:=0; i<partNum; i++ {
		curOffset = int64(i) * chunkSz
		addr = rc.Backs[rand.Intn(len(rc.Backs))]
		fiConf.OffAddrPairs[i] = &triblab.OffsetAddr{Offset: curOffset, Addr: addr}
		uploadChunk(addr, dir, fileName, nextId, chunkSz, curOffset, fiSz, doneChan, errChan)
	}

	var doneFlag bool
	for i:=0; i<partNum; i++ {
		doneFlag = <-doneChan
		if !doneFlag {
			fmt.Fprintln(os.Stderr, <-errChan)
			os.Exit(1)
		}
	}

	now := time.Now()
	ind := triblab.Inode{
		Dir: false,
		File: true,
		Parent: 1,
		Ctime: now,
		Mtime: now,
		Atime: now,
		Mode: 0444 | fuse.S_IFREG,
		Size: fiSz,
		Inos: make([]triblab.NameIdPair, 0),
		LargeFlag: true,
	}
	// fmt.Println("step 1")
	err = triblab.SetInodeHelper(nextId, &ind, fsName, userName)
	noError(err)

	inodir, err := triblab.GetInodeHelper(1)
	noError(err)
	inodir.Inos = append(inodir.Inos, triblab.NameIdPair{Name: fileName, InoID: nextId, DirFlag: false})
	err = triblab.SetInodeHelper(1, inodir, fsName, userName)
	noError(err)

	// fmt.Println("step 2")
	jsonStr, err := json.Marshal(fiConf)
	noError(err)
	fmt.Println(string(jsonStr))
	// fmt.Println("step 3")
	err = triblab.SetFileHelper(nextId, string(jsonStr), fsName, userName)
	// fmt.Println("step 4")
	noError(err)

	
}

func runCmd(args []string) bool {
	var chunkSz int64
	var fileName, fsName, userName string
	var err error
	cmd := args[0]

	switch cmd {
	case "upload":
			if len(args) >= 2 {
			fileName = args[1]
		}
		if len(args) >= 3 {
			chunkSz, err = strconv.ParseInt(args[2], 10, 64)
			noError(err)
		} else {
			chunkSz = 1024
		}
		if len(args) == 5 {
			fsName = args[3]
			userName = args[4]
		} else {
			fsName = "FS1"
			userName = "user1"
		}
		uploadFile(fileName, fsName, userName, chunkSz)
	case "help":
		fmt.Println(cmdHelp)
	case "exit":
		return true
	default:
		fmt.Println("bad command, try \"help\".")
	}
	return false
}

func fields(s string) []string {
	return strings.Fields(s)
}

func runPrompt() {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print("> ")

	for scanner.Scan() {
		line := scanner.Text()
		args := fields(line)
		if len(args) > 0 {
			if runCmd(args) {
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

// var (
// 	frc = flag.String("rc", trib.DefaultRCPath, "bin storage config file")
// )

func main() {
	args := os.Args
	go func(args []string){
		fs_name := "FS1" // mock bin name
		user_name := "user1"
		ops := &triblab.MyFileSystem{}
		ops.FsName = fs_name
		ops.UserName = user_name
		fuse.MountAndRun(args, ops)
		}(args)

	flag.Parse()

	// rc, e := trib.LoadRC(*frc)
	// noError(e)

	runPrompt()
	fmt.Println()
}

