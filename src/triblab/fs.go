package triblab

import(
	"fuse"
	"trib"
	"time"
	"os"
	"flag"
	"io/ioutil"

	"encoding/json"
	"strconv"
	"fmt"
)

var cacheDir = "/tmp/cse223proj/"

func noError(e error) {
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}

type OffsetAddr struct {
	Offset int64
	Addr string
}

type FileConf struct {
	FileName string
	PartNum int
	ChunkSize int64
	OffAddrPairs []*OffsetAddr
}

// add fs name  as bin name; use bin name with client
// add user name, multiple users can share the same fs
type MyFileSystem struct {
	FsName	string
	UserName string
	fuse.DefaultRawFileSystem
}

type NameIdPair struct {
	Name string
	InoID int64
	DirFlag bool // true means dir
}

type Inode struct {
	Dir bool
	File bool
	Parent int64
	Ctime time.Time // inode change time
	Mtime time.Time // content modification time
	Atime time.Time // access time
	Mode int        // permission bits
	Size int64      // file size
	Inos []NameIdPair  // the inode it has (only apply to dir)
	LargeFlag bool
}

const tmpAddr = "localhost:9900"
var (
	frc = flag.String("rc path", trib.DefaultRCPath, "bin storage config file")
)
var binClient trib.BinStorage
var lockClient FsLockClient
var LockTimeTable map[string]time.Time //lock time table keeps what locks you have and when they expire
var LockLive = time.Duration(time.Second * 25)

func getWriteLogClient(binName string) trib.Storage {
	return binClient.Bin(binName)
}

func getReadDiskClient(binName string) *FsDiskClient {
	return NewFsDiskClient(binClient.Bin(binName))
}

func getLockClient() *FsLockClient {
	// TODO: use more lock servers....
	rc, e := trib.LoadRC(*frc)
	noError(e)
	lockServerAddr := rc.LockServers[0]
	return NewFsLockClient(lockServerAddr)
}

// TODO: 1) use lock, 2) use new client
// get next ID number
func GetNextIdHelper(FsName string, UserName string) (int64, error) {
	var tns bool
	getLockClient().TestAndSet(&trib.KeyValue{FsName + "::" + "__nextId__", UserName}, &tns)
	if tns == false {
		return -1, fmt.Errorf("Someone else is holding the lock")
	}
	// TODO: use FsDiskClient to get nextId 
	var nextIdStr string
	myclient := NewClient(tmpAddr)
	err := myclient.Get("__nextId__", &nextIdStr)
	if err != nil {
		return -1, err
	}
	rtInt, err := strconv.ParseInt(nextIdStr, 10, 64)
	if err != nil {
		fmt.Println(err)
	}
	return rtInt, nil
}

// set next ID num
func SetNextIdHelper(newNextId int64, FsName string, UserName string) error {
	var succ bool
	newNextIdStr := strconv.FormatInt(newNextId, 10)
	// TODO: use FsDiskClient to set nextId
	myclient := NewClient(tmpAddr)
	err := myclient.Set(&trib.KeyValue{Key: "__nextId__", Value: newNextIdStr}, &succ)
	if err != nil {
		return err
	}
	getLockClient().Release(&trib.KeyValue{FsName + "::" + "__nextId__", UserName}, &succ)
	return nil
}

// get a inode by id
func GetInodeHelper(inoId int64) (*Inode, error) {
	var jsonStr string
	var ind Inode
	myClient := NewClient(tmpAddr)
	// TODO: use FsDiskClient
	err := myClient.Get("__inode"+strconv.FormatInt(inoId, 10), &jsonStr)
	if err != nil {
		return nil, err
	}
	if len(jsonStr) == 0 {
		return nil, nil
	}
	json.Unmarshal([]byte(jsonStr), &ind)
	return &ind, nil
}

// set a inode by id
func SetInodeHelper(inoId int64, ind *Inode, FsName string, UserName string) error {
	var tns bool
	getLockClient().TestAndSet(&trib.KeyValue{FsName + "::" + "__inode" + strconv.FormatInt(inoId, 10), UserName}, &tns)
	if tns == false {
		return fmt.Errorf("Someone else is holding the lock")
	}
	var succ bool
	jsonStr, _ := json.Marshal(*ind)
	// TODO: use FsDiskClient
	myclient := NewClient(tmpAddr)
	err := myclient.Set(&trib.KeyValue{Key: "__inode"+strconv.FormatInt(inoId, 10), Value: string(jsonStr)}, &succ)
	if err != nil {
		return err
	}
	getLockClient().Release(&trib.KeyValue{FsName + "::" + "__inode" + strconv.FormatInt(inoId, 10), UserName}, &succ)
	return nil
}

// delete a inode
func DeleteInodeHelper(inoId int64, FsName string, UserName string) error {
	var tns bool
	getLockClient().TestAndSet(&trib.KeyValue{FsName + "::" + "__inode" + strconv.FormatInt(inoId, 10), UserName}, &tns)
	if tns == false {
		return fmt.Errorf("Someone else is holding the lock")
	}
	var succ bool
	// TODO: use FsDiskClient
	myclient := NewClient(tmpAddr)
	err := myclient.Set(&trib.KeyValue{Key: "__inode"+strconv.FormatInt(inoId, 10), Value: ""}, &succ)
	if err != nil {
		return err
	}
	getLockClient().Release(&trib.KeyValue{FsName + "::" + "__inode" + strconv.FormatInt(inoId, 10), UserName}, &succ)
	return nil
}

// retrieve a file segment by inode id
func GetFileSegmentHelper(ino, off int64, addr string) (string, error) {
	var data string
	myClient := NewClient(addr)
	err := myClient.Get("__file_segment_"+strconv.FormatInt(ino, 10)+"_"+strconv.FormatInt(off, 10), &data)
	if err != nil {
		return "", err
	}
	return data, nil
}

// retrieve a file by inode id
func GetFileHelper(ino int64) (string, error) {
	var data string
	myClient := NewClient(tmpAddr)
	err := myClient.Get("__file"+strconv.FormatInt(ino, 10), &data)
	if err != nil {
		return "", err
	}
	return data, nil
}

// set a file
func SetFileHelper(inoId int64, data string, FsName string, UserName string) error {
	// check lock time table
	now := time.Now()
	locktime, OK := LockTimeTable["__file"+strconv.FormatInt(inoId, 10)]
	if OK == false {
		// no lock
		var tns bool
		getLockClient().TestAndSet(&trib.KeyValue{FsName + "::" + "__file" + strconv.FormatInt(inoId, 10), UserName}, &tns)
		if tns == false {
			return fmt.Errorf("Someone else is holding the lock")
		}
	} else {
		// check lock
		diff := now.Sub(locktime)
		if diff > LockLive {
			// renew
			var renew bool
			getLockClient().Renew(&trib.KeyValue{FsName + "::" + "__file" + strconv.FormatInt(inoId, 10), UserName}, &renew)
			if renew == false {
				// t & s again
				var tns bool
				getLockClient().TestAndSet(&trib.KeyValue{FsName + "::" + "__file" + strconv.FormatInt(inoId, 10), UserName}, &tns)
				if tns == false {
					return fmt.Errorf("Someone else is holding the lock")
				}
			}
		}
	}
	LockTimeTable["__file"+strconv.FormatInt(inoId, 10)] = now
	var succ bool
	myclient := NewClient(tmpAddr)
	err := myclient.Set(&trib.KeyValue{Key: "__file"+strconv.FormatInt(inoId, 10), Value: data}, &succ)
	if err != nil {
		return err
	}
	return nil
}

// delete a file
func DeleteFileHelper(inoId int64, FsName string, UserName string) error {
	return SetFileHelper(inoId, "", FsName, UserName)
}

// get attrbutes by inode id
func getAttrHelper(inoId int64) (*fuse.InoAttr, error) {
	ind, err := GetInodeHelper(inoId)
	if err != nil {
		return nil, err
	}
	if ind == nil {
		return nil, nil
	}
	return &fuse.InoAttr{
		Ino:     inoId,
		Timeout: 5.0,
		Mode:    ind.Mode,
		Nlink:   1,
		Ctim:    ind.Ctime,
		Mtim:    ind.Mtime,
		Atim:    ind.Atime,
		Size:    ind.Size,
	}, nil
}

// Init initializes a filesystem.
// Called before any other filesystem method.
func (d *MyFileSystem) Init(*fuse.ConnInfo) {
	fmt.Println("Init func: ")

	rc, e := trib.LoadRC(*frc)
	noError(e)
	binClient = NewBinClient(rc.Backs)
	LockTimeTable = make(map[string]time.Time) //lock time table keeps what locks you have and when they expire

	rootDir, _ := GetInodeHelper(1)
	if rootDir == nil {
		now := time.Now()
		rootDir := Inode{
			Dir: true, 
			File: false, 
			Parent: -1, 
			Ctime: now, 
			Mtime: now, 
			Atime: now,
			Mode: 0777 | fuse.S_IFDIR, 
			Inos: make([]NameIdPair, 0),
			LargeFlag: false,
		}
		SetInodeHelper(1, &rootDir, d.FsName, d.UserName)
		// GetNextIdHelper(d.FsName, d.UserName) // just to avoid to release an empty lock
		SetNextIdHelper(2, d.FsName, d.UserName)//root is 1
	}
	fmt.Println("end of init")
}

// Destroy cleans up a filesystem.
// Called on filesystem exit.
//func (d *MyFileSystem) Destroy() {}

// StatFs gets file system statistics.
func (d *MyFileSystem) StatFs(ino int64) (stat *fuse.StatVfs, status fuse.Status) {
	fmt.Println("StatFs::ino:"+strconv.FormatInt(ino, 10))

	stat = &fuse.StatVfs{//TODO
		Files: int64(99),
	}
	status = fuse.OK
	return
}

// Lookup finds a directory entry by name and get its attributes.
func (d *MyFileSystem) Lookup(parent int64, name string) (*fuse.Entry, fuse.Status) {
	fmt.Println("Lookup::parent:"+strconv.FormatInt(parent, 10)+",name:"+name)
	// check parent dir
	inodir, err := GetInodeHelper(parent)
	if err != nil {
		return nil, fuse.ENETUNREACH
	}
	if inodir == nil {
		return nil, fuse.ENOENT
	}
	if inodir.Dir == false {
		return nil, fuse.ENOTDIR
	}
	//find inode by name
	for i:=0; i<len(inodir.Inos); i++ {
		if inodir.Inos[i].Name == name {
			curInoId := inodir.Inos[i].InoID
			attr, err := getAttrHelper(curInoId)
			if err != nil {
				return nil, fuse.ENETUNREACH
			}
			e := &fuse.Entry{
				Ino:          curInoId,
				Attr:         attr,
				AttrTimeout:  5.0,
				EntryTimeout: 5.0,
			}
			return e, fuse.OK
		}
	}
	return nil, fuse.ENOENT
}

// Mknod creates a file node.
//
// This is used to create a regular file, or special files such as character devices, block
// devices, fifo or socket nodes.
func (d *MyFileSystem) Mknod(parent int64, name string, mode int, rdev int) (*fuse.Entry, fuse.Status) {
	fmt.Println("Mknod::parent:"+strconv.FormatInt(parent, 10)+",name:"+name)
	// check parent dir
	inodir, err := GetInodeHelper(parent)
	if err != nil {
		return nil, fuse.ENETUNREACH
	}
	if inodir == nil {
		return nil, fuse.ENOENT
	}
	if inodir.Dir == false {
		return nil, fuse.ENOTDIR
	}
	for i:=0; i<len(inodir.Inos); i++ {
		if inodir.Inos[i].Name == name {
			return nil, fuse.EEXIST
		}
	}
	// Increase nextId by 1
	nextId, err := GetNextIdHelper(d.FsName, d.UserName)
	if err != nil {
		return nil, fuse.ENETUNREACH
	}
	err = SetNextIdHelper(nextId+1, d.FsName, d.UserName)
	if err != nil {
		return nil, fuse.ENETUNREACH
	}
	// Update the parent dir's contents
	inodir.Inos = append(inodir.Inos, NameIdPair{Name: name, InoID: nextId, DirFlag: false})
	err = SetInodeHelper(parent, inodir, d.FsName, d.UserName)
	if err != nil {
		return nil, fuse.ENETUNREACH
	}
	// Create an inode for current file
	now := time.Now()
	curMode := 0777 | fuse.S_IFREG
	newinofile := Inode{
		Dir: false, 
		File: true, 
		Parent: parent, 
		Ctime: now, 
		Mtime: now, 
		Atime: now,
		Mode: curMode,
		Inos: make([]NameIdPair, 0),
		LargeFlag: false,
	}
	err = SetInodeHelper(nextId, &newinofile, d.FsName, d.UserName)
	if err != nil {
		return nil, fuse.ENETUNREACH
	}

	tmpAttr := &fuse.InoAttr{
		Ino:     nextId,
		Timeout: 5.0,
		Mode:    curMode,
		Nlink:   1,
		Ctim:    now,
		Mtim:    now,
		Atim:    now,
	}
	e := &fuse.Entry{
		Ino:          nextId,
		Attr:         tmpAttr,
		AttrTimeout:  5.0,
		EntryTimeout: 5.0,
	}
	return e, fuse.OK
}

// Mkdir creates a directory.
func (d *MyFileSystem) Mkdir(parent int64, name string, mode int) (*fuse.Entry, fuse.Status) {
	fmt.Println("Mkdir::parent:"+strconv.FormatInt(parent, 10)+",name:"+name)
	//check parent dir
	inodir, err := GetInodeHelper(parent)
	if err != nil {
		return nil, fuse.ENETUNREACH
	}
	if inodir == nil {
		return nil, fuse.ENOENT
	}
	if inodir.Dir == false {
		return nil, fuse.ENOTDIR
	}
	for i:=0; i<len(inodir.Inos); i++ {
		if inodir.Inos[i].Name == name {
			return nil, fuse.EEXIST
		}
	}
	// Increase nextId by 1
	nextId, err := GetNextIdHelper(d.FsName, d.UserName)
	if err != nil {
		return nil, fuse.ENETUNREACH
	}
	err = SetNextIdHelper(nextId+1, d.FsName, d.UserName)
	if err != nil {
		return nil, fuse.ENETUNREACH
	}
	// Update the parent dir's contents
	inodir.Inos = append(inodir.Inos, NameIdPair{Name: name, InoID: nextId, DirFlag: true})
	err = SetInodeHelper(parent, inodir, d.FsName, d.UserName)
	if err != nil {
		return nil, fuse.ENETUNREACH
	}
	// Create an inode for current dir
	now := time.Now()
	curMode := 0777 | fuse.S_IFDIR
	newinodir := Inode{
		Dir: true, 
		File: false, 
		Parent: parent, 
		Ctime: now, 
		Mtime: now, 
		Atime: now,
		Mode: curMode,
		Inos: make([]NameIdPair, 0),
		LargeFlag: false,
	}
	err = SetInodeHelper(nextId, &newinodir, d.FsName, d.UserName)
	if err != nil {
		return nil, fuse.ENETUNREACH
	}

	tmpAttr := &fuse.InoAttr{
		Ino:     nextId,
		Timeout: 5.0,
		Mode:    curMode,
		Nlink:   1,
		Ctim:    now,
		Mtim:    now,
		Atim:    now,
	}
	e := &fuse.Entry{
		Ino:          nextId,
		Attr:         tmpAttr,
		AttrTimeout:  5.0,
		EntryTimeout: 5.0,
	}
	return e, fuse.OK
}

// Open makes a file available for read or write.
//
// Open flags are available in fi.Flags
//
// Filesystems may store an arbitrary file handle in fh.Handle and use this in other file
// operations (read, write, flush, release, fsync). Filesystems may also implement stateless file
// I/O and not store anything in fi.Handle.
func (d *MyFileSystem) Open(ino int64, fi *fuse.FileInfo) fuse.Status {
	fmt.Println("Open::ino:"+strconv.FormatInt(ino, 10))

	inoStr := strconv.FormatInt(ino, 10)

	// simply check if the inode is a file
	inofile, err := GetInodeHelper(ino)
	if err != nil {
		return fuse.ENETUNREACH
	}
	if inofile == nil {
		return fuse.ENOENT
	}
	if inofile.Dir {
		return fuse.EISDIR
	}
	if inofile.LargeFlag {
		fileConfigStr, err := GetFileHelper(ino)
		if err != nil {
			return fuse.ENETUNREACH
		}
		var fileConfig FileConf
		var iterStr string
		json.Unmarshal([]byte(fileConfigStr), &fileConfig)
		doneChan := make(chan bool, fileConfig.PartNum)
		for i:=0; i<fileConfig.PartNum; i++ { //download the file segments to tmp files
			go func(addr string, i int, done chan bool) {
				fileStr, err := GetFileSegmentHelper(ino, fileConfig.OffAddrPairs[i].Offset, fileConfig.OffAddrPairs[i].Addr)
				iterStr = strconv.Itoa(i)
				f, err := os.OpenFile(cacheDir+"/"+d.FsName+"/"+d.UserName+"/"+inoStr+"_"+iterStr, os.O_WRONLY | os.O_CREATE, 0777)
				if err != nil{
					done <- false
				}
				noError(err)
				fmt.Println("write file seg cache ", []byte(fileStr))
				_, err =  f.WriteString(fileStr)
				if err != nil{
					done <- false
				}
				noError(err)
				done <- true
			}(fileConfig.OffAddrPairs[i].Addr, i, doneChan)
		}
		for i:=0; i<fileConfig.PartNum; i++ {
			if !<-doneChan {
				return fuse.EIO
			}
		}
		f_all, err := os.OpenFile(cacheDir+"/"+d.FsName+"/"+d.UserName+"/"+inoStr, os.O_WRONLY | os.O_CREATE, 0777)
		for i:=0; i<fileConfig.PartNum; i++ { //concatenate the files
			iterStr = strconv.Itoa(i)
			// f, err := os.OpenFile(cacheDir+"/"+d.FsName+"/"+d.UserName+"/"+inoStr+"_"+iterStr, os.O_RDONLY | os.O_APPEND | os.O_TRUNC, 0777)
			fileStr, _ := ioutil.ReadFile(cacheDir+"/"+d.FsName+"/"+d.UserName+"/"+inoStr+"_"+iterStr)
			fmt.Println("read and append file seg cache ", fileStr)
			_, err =  f_all.WriteString(string(fileStr))
			noError(err)
		}
	}
	return fuse.OK
}

// Rmdir removes a directory.
func (d *MyFileSystem) Rmdir(parent int64, name string) fuse.Status {
	fmt.Println("Rmdir::parent:"+strconv.FormatInt(parent, 10)+",name:"+name)
	//check parent dir
	inodir, err := GetInodeHelper(parent)
	if err != nil {
		return fuse.ENETUNREACH
	}
	if inodir == nil {
		return fuse.ENOENT
	}
	if inodir.Dir == false {
		return fuse.ENOTDIR
	}
	// find inode by name
	for i:=0; i<len(inodir.Inos); i++ {
		if inodir.Inos[i].Name == name {
			if !inodir.Inos[i].DirFlag {
				return fuse.ENOTDIR
			}
			rmId := inodir.Inos[i].InoID
			rmDir, err := GetInodeHelper(rmId)
			if err != nil {
				return fuse.ENETUNREACH
			}
			if inodir == nil {
				return fuse.ENOENT
			}
			// check if the dir is mepty
			if len(rmDir.Inos) > 0 {
				return fuse.ENOTEMPTY
			}
			// delete inode
			err = DeleteInodeHelper(rmId, d.FsName, d.UserName)
			if err != nil {
				return fuse.ENETUNREACH
			}
			// update parent dir
			inodir.Inos = append(inodir.Inos[:i], inodir.Inos[i+1:]...)
			err = SetInodeHelper(parent, inodir, d.FsName, d.UserName)
			if err != nil {
				return fuse.ENETUNREACH
			}
			return fuse.OK
		}
	}
	return fuse.ENOENT
}

// Unlink removes a file.
func (d *MyFileSystem) Unlink(parent int64, name string) fuse.Status {
	fmt.Println("Unlink::parent:"+strconv.FormatInt(parent, 10)+",name:"+name)
	// check parent dir
	inodir, err := GetInodeHelper(parent)
	if err != nil {
		return fuse.ENETUNREACH
	}
	if inodir == nil {
		return fuse.ENOENT
	}
	if inodir.Dir == false {
		return fuse.ENOTDIR
	}
	// find inode by name
	for i:=0; i<len(inodir.Inos); i++ {
		if inodir.Inos[i].Name == name {
			if inodir.Inos[i].DirFlag {
				return fuse.EISDIR
			}
			// delete inode
			rmId := inodir.Inos[i].InoID
			err = DeleteInodeHelper(rmId, d.FsName, d.UserName)
			if err != nil {
				return fuse.ENETUNREACH
			} 
			// update parent dir
			inodir.Inos = append(inodir.Inos[:i], inodir.Inos[i+1:]...)
			err = SetInodeHelper(parent, inodir, d.FsName, d.UserName)
			if err != nil {
				return fuse.ENETUNREACH
			}
			// delete file
			err = DeleteFileHelper(rmId, d.FsName, d.UserName)
			if err != nil {
				return fuse.ENETUNREACH
			}
			return fuse.OK
		}
	}
	return fuse.ENOENT
}

// Rename renames a file or directory.
func (d *MyFileSystem) Rename(dir int64, name string, newdir int64, newname string) fuse.Status {
	fmt.Println("Rename::dir:"+strconv.FormatInt(dir, 10)+",name:"+name+",newname:"+newname)
	// check if the old dir exists
	oldDir, err := GetInodeHelper(dir)
	if err != nil {
		return fuse.ENETUNREACH
	}
	if oldDir == nil {
		return fuse.ENOENT
	}
	if oldDir.Dir == false {
		return fuse.ENOTDIR
	}
	// retrive inode info and update old dir, including: contents and mtime
	findFlag := false
	now := time.Now()
	var renameId int64
	for i:=0; i<len(oldDir.Inos); i++ {
		if oldDir.Inos[i].Name == name {
			renameId = oldDir.Inos[i].InoID
			oldDir.Inos = append(oldDir.Inos[:i], oldDir.Inos[i+1:]...)
			oldDir.Mtime = now
			err = SetInodeHelper(dir, oldDir, d.FsName, d.UserName)
			if err != nil {
				return fuse.ENETUNREACH
			}
			findFlag = true
		}
	}
	if !findFlag {
		return fuse.ENOENT
	}
	// check if the new dir exists
	newDir, err := GetInodeHelper(newdir)
	if err != nil {
		return fuse.ENETUNREACH
	}
	if newDir == nil {
		return fuse.ENOENT
	}
	if newDir.Dir == false {
		return fuse.ENOTDIR
	}
	// Update the new dir
	newDir.Inos = append(newDir.Inos, NameIdPair{Name: newname, InoID: renameId, DirFlag: false})
	err = SetInodeHelper(newdir, newDir, d.FsName, d.UserName)
	if err != nil {
		return fuse.ENETUNREACH
	}

	//update inode's info including its parent dir
	renameIno, err := GetInodeHelper(renameId)
	if err != nil {
		return fuse.ENETUNREACH
	}
	if renameIno == nil {
		return fuse.ENOENT
	}
	renameIno.Parent = newdir
	err = SetInodeHelper(renameId, renameIno, d.FsName, d.UserName)
	if err != nil {
		return fuse.ENETUNREACH
	}
	return fuse.OK
}

// Getattr gets file attributes.
//
// fi is for future use, currently always nil.
func (d *MyFileSystem) GetAttr(ino int64, fi *fuse.FileInfo) (*fuse.InoAttr, fuse.Status) {
	fmt.Println("GetAttr::ino:"+strconv.FormatInt(ino, 10))

	attr, err := getAttrHelper(ino)
	if err != nil {
		return nil, fuse.ENETUNREACH
	}
	if attr == nil {
		return nil, fuse.ENOENT
	}
	return attr, fuse.OK
}

// Setattr sets file attributes.
//
// In the 'attr' argument, only members indicated by the mask contain valid values.  Other
// members contain undefined values.
//
// If the setattr was invoked from the ftruncate() system call, the fi.Handle will contain the
// value set by the open method.  Otherwise, the fi argument may be nil.
func (d *MyFileSystem) SetAttr(ino int64, attr *fuse.InoAttr, mask fuse.SetAttrMask, fi *fuse.FileInfo) (*fuse.InoAttr, fuse.Status) {
	fmt.Println("SetAttr::ino:"+strconv.FormatInt(ino, 10))

	ind, err := GetInodeHelper(ino)
	if err != nil {
		return nil, fuse.ENETUNREACH
	}
	if ind == nil {
		return nil, fuse.ENOENT
	}
	if mask & fuse.SET_ATTR_MODE != 0 {
		ind.Mode = attr.Mode
	}
	if mask & fuse.SET_ATTR_MTIME != 0 {
		ind.Mtime = attr.Mtim
	}
	if mask & fuse.SET_ATTR_MTIME_NOW != 0 {
		ind.Mtime = time.Now()
	}
	if mask & fuse.SET_ATTR_ATIME != 0 {
		ind.Atime = attr.Atim
	}
	if mask & fuse.SET_ATTR_ATIME_NOW != 0 {
		ind.Atime = time.Now()
	}

	newAttr, err := getAttrHelper(ino)
	if err != nil {
		return nil, fuse.ENETUNREACH
	}
	if newAttr == nil {
		return nil, fuse.ENOENT
	}
	return newAttr, fuse.OK
}

// ReadDir reads a directory.
//
// fi.Handle will contain the value set by the opendir method, or will be undefined if the
// opendir method didn't set any value.
//
// DirEntryWriter is used to add entries to the output buffer.
func (d *MyFileSystem) ReadDir(ino int64, fi *fuse.FileInfo, off int64, size int, w fuse.DirEntryWriter) fuse.Status {
	fmt.Println("ReadDir::ino:"+strconv.FormatInt(ino, 10)+",off:"+strconv.FormatInt(off, 10))

	var attr *fuse.InoAttr
	ind, err := GetInodeHelper(ino)
	if err != nil {
		return fuse.ENETUNREACH
	}
	if ind == nil {
		return fuse.ENOENT
	}
	if ind.Dir == false {
		return fuse.ENOTDIR
	}
	idx := int64(1)
	if idx > off {
		attr, err = getAttrHelper(ino)
		if err != nil {
			return fuse.ENETUNREACH
		}
		if !w.Add(".", ino, attr.Mode, idx) {
			return fuse.OK
		}
	}
	idx++
	if ind.Parent != -1 {
		if idx > off {
			attr, err = getAttrHelper(ind.Parent)
			if err != nil {
				return fuse.ENETUNREACH
			}
			if !w.Add("..", ind.Parent, attr.Mode, idx) {
				return fuse.OK
			}
		}
		idx++
	}
	for i:=0; i<len(ind.Inos); i++ {
		if idx > off {
			node := ind.Inos[i]
			attr, err = getAttrHelper(ind.Inos[i].InoID)
			if err != nil {
				return fuse.ENETUNREACH
			}
			if !w.Add(node.Name, node.InoID, attr.Mode, idx) {
				return fuse.OK
			}
		}
		idx++
	}
	return fuse.OK
}

// Read reads data from an open file.
//
// Read should return exactly the number of bytes requested except on EOF or error.
//
// fi.Handle will contain the value set by the open method, if any.
func (d *MyFileSystem) Read(p []byte, ino int64, off int64, fi *fuse.FileInfo) (int, fuse.Status) {
	fmt.Println("Read::ino:"+strconv.FormatInt(ino, 10)+",off:"+strconv.FormatInt(off, 10))

	// check this inode
	inofile, err := GetInodeHelper(ino)
	if err != nil {
		return 0, fuse.ENETUNREACH
	}
	if inofile == nil {
		return 0, fuse.ENOENT
	}
	if !inofile.File {
		return 0, fuse.EISDIR
	}
	if inofile.LargeFlag {
		//read from the local predownload file which is done in Open()
		path := cacheDir+"/"+d.FsName+"/"+d.UserName+"/"
		err := os.MkdirAll(path, 0777)
		noError(err)
		f, err := os.Open(path+strconv.FormatInt(ino, 10))
		readLen, _ := f.ReadAt(p, off)
		return readLen, fuse.OK
	} else {
		// update access time
		inofile.Atime = time.Now()
		err = SetInodeHelper(ino, inofile, d.FsName, d.UserName)
		if err != nil {
			return 0, fuse.ENETUNREACH
		}
		// get the file content
		// TODO: check lock, use disk client
		str, err := GetFileHelper(ino)
		byteArr := []byte(str)
		if err != nil {
			return 0, fuse.ENETUNREACH
		}
		l := len(byteArr) - int(off)
		if l >= 0 {
	        copy(p, byteArr[off:])
			return l, fuse.OK
		} else {
			return 0, fuse.OK
		}
	}
}

// Write writes data to an open file.
//
// Write should return exactly the number of bytes requested except on error.
//
// fi.handle will contain the value set by the open method, if any.
func (d *MyFileSystem) Write(p []byte, ino int64, off int64, fi *fuse.FileInfo) (int, fuse.Status) {
	fmt.Println("Write::ino:"+strconv.FormatInt(ino, 10)+",off:"+strconv.FormatInt(off, 10))
	// check this inode
	inofile, err := GetInodeHelper(ino)
	if err != nil {
		return 0, fuse.ENETUNREACH
	}
	if inofile == nil {
		return 0, fuse.ENOENT
	}
	if !inofile.File {
		return 0, fuse.EISDIR
	}
	// get old file content
	str, err := GetFileHelper(ino)
	if err != nil {
		return 0, fuse.ENETUNREACH
	}
	// update the file content
	oldByteArr := []byte(str)
	newByteArr := append(oldByteArr[:off], p...)
	newData := string(newByteArr[:])
	sz := len(newData)
	err = SetFileHelper(ino, newData, d.FsName, d.UserName)
	if err != nil {
		return 0, fuse.ENETUNREACH
	}
	// update the access time and file size
	now := time.Now()
	inofile.Atime = now
	inofile.Mtime = now
	inofile.Size = int64(sz)
	err = SetInodeHelper(ino, inofile, d.FsName, d.UserName)
	if err != nil {
		return 0, fuse.ENETUNREACH
	}
	return len(p), fuse.OK
}



// Forget limits the lifetime of an inode.
//
// The n parameter indicates the number of lookups previously performed on this inode.
// The filesystem may ignore forget calls if the inodes don't need to have a limited lifetime.
// On unmount it is not guaranteed that all reference dinodes will receive a forget message.

//func (d *MyFileSystem) Forget(ino int64, n int) {}

// Release drops an open file reference.
//
// Release is called when there are no more references to an open file: all file descriptors are
// closed and all memory mappings are unmapped.
//
// For every open call, there will be exactly one release call.
//
// A filesystem may reply with an error, but error values are not returned to the close() or
// munmap() which triggered the release.
//
// fi.Handle will contain the value set by the open method, or will be undefined if the open
// method didn't set any value.
// fi.Flags will contain the same flags as for open.

func (d *MyFileSystem) Release(ino int64, fi *fuse.FileInfo) fuse.Status {
	fmt.Println("in Release: ino ", ino)
	var succ bool
	getLockClient().Release(&trib.KeyValue{d.FsName + "::" + "__file" + strconv.FormatInt(ino, 10), d.UserName}, &succ)	
	delete(LockTimeTable, "__file" + strconv.FormatInt(ino, 10))
	return fuse.OK
}

// Flush is called on each close() of an opened file.
//
// Since file descriptors can be duplicated (dup, dup2, fork), for one open call there may be
// many flush calls.
//
// fi.Handle will contain the value set by the open method, or will be undefined if the open
// method didn't set any value.
//
// The name of the method is misleading. Unlike fsync, the filesystem is not forced to flush
// pending writes.
//func (d *MyFileSystem) Flush(ino int64, fi *fuse.FileInfo) fuse.Status {return *new(fuse.Status)}

// Fsync synchronizes file contents.
//
// If the dataOnly parameter is true, then only the user data should be flushed, not the
// metdata.
//func (d *MyFileSystem) FSync(ino int64, dataOnly bool, fi *fuse.FileInfo) fuse.Status {return *new(fuse.Status)}


// ReadLink reads a symbolic link.
//func (d *MyFileSystem) ReadLink(ino int64) (string, fuse.Status) {return "", *new(fuse.Status)}

// OpenDir opens a directory.
//
// Filesystems may store an arbitrary file handle in fh.Handle and use this in other directory
// operations (ReadDir, ReleaseDir, FsyncDir). Filesystems may not store anything in fi.Handle,
// though that makes it impossible to implement standard conforming directory stream operations
// in case the contents of the directory can change between opendir and releasedir.
//func (d *MyFileSystem) OpenDir(ino int64, fi *fuse.FileInfo) fuse.Status {return *new(fuse.Status)}

// ReleaseDir drops an open file reference.
//
// For every OpenDir call, there will be exactly one ReleaseDir call.
//
// fi.Handle will contain the value set by the OpenDir method, or will be undefined if the
// OpenDir method didn't set any value.

func (d *MyFileSystem) ReleaseDir(ino int64, fi *fuse.FileInfo) fuse.Status {
	fmt.Println("in ReleaseDir: ino ", ino)
	return fuse.OK
}

// FsyncDir synchronizes directory contents.
//
// If the dataOnly parameter is true, then only the user data should be flushed, not the
// metdata.
//func (d *MyFileSystem) FSyncDir(ino int64, dataOnly bool, fi *fuse.FileInfo) fuse.Status {return *new(fuse.Status)}

// Symlink creates a symbolic link.
//func (d *MyFileSystem) Symlink(link string, parent int64, name string) (*fuse.Entry, fuse.Status) {return nil, *new(fuse.Status)}

// Link creates a hard link.
//func (d *MyFileSystem) Link(ino int64, newparent int64, name string) (*fuse.Entry, fuse.Status) {return nil, *new(fuse.Status)}

// Access checks file access permissions.
//
// This will be called for the access() system call.  If the 'default_permissions' mount option
// is given, this method is not called.
//func (d *MyFileSystem) Access(ino int64, mask int) fuse.Status {return *new(fuse.Status)}

// Create creates and opens a file.
//
// If the file does not exist, first create it with the specified mode and then open it.
//
// Open flags are available in fi.Flags.
//
// Filesystems may store an arbitrary file handle in fi.Handle and use this in all other file
// operations (Read, Write, Flush, Release, FSync).
//
// If this method is not implemented, then Mknod and Open methods will be called instead.
//func (d *MyFileSystem) Create(parent int64, name string, mode int, fi *fuse.FileInfo) (*fuse.Entry, fuse.Status) {return nil, *new(fuse.Status)}

// Returns a list of the extended attribute keys.
//func (d *MyFileSystem) ListXattrs(ino int64) ([]string, fuse.Status) {return nil, *new(fuse.Status)}

// Returns the size of the attribute value.
//func (d *MyFileSystem) GetXattrSize(ino int64, name string) (int, fuse.Status) {return 0, *new(fuse.Status)}

// Get an extended attribute.
// Result placed in out buffer.
// Returns the number of bytes copied.
//func (d *MyFileSystem) GetXattr(ino int64, name string, out []byte) (int, fuse.Status) {return 0, *new(fuse.Status)}

// Set an extended attribute.
//func (d *MyFileSystem) SetXattr(ino int64, name string, value []byte, flags int) fuse.Status {return *new(fuse.Status)}

// Remove an extended attribute.
//func (d *MyFileSystem) RemoveXattr(ino int64, name string) fuse.Status {return *new(fuse.Status)}


