package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	files    sync.Map // [filename] 是否已经分配
	sucFiles sync.Map
	mids     map[string]int
	ids      []fileConf

	nReduce      int
	rInName      []map[string]bool
	done         bool
	reduceSucNum int
	mapdone      bool
}

var cmutex sync.Mutex

type Conf struct {
	NReduce int
}

type Map struct {
	File    string
	NReduce int
	Mid     int
}

type Reduce struct {
	FilePaths      []string
	NReduce        int
	Id             int
	OutputFileName string
}

type fileConf struct {
	id    int
	st    bool // map中看是否分配出去，reduce中看是否成功
	time_ time.Time
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetConf(_ string, reply *Conf) error {
	reply.NReduce = c.nReduce
	return nil
}

func check(time_ time.Time) bool {
	return time.Now().Sub(time_).Seconds() > 10
}

func (c *Coordinator) Map(args int, reply *Map) error {
	var filename string
	filename = ""
	*reply = Map{}

	c.files.Range(func(k, v interface{}) bool {
		tmp := v.(fileConf)
		// suc, ok := c.sucFiles.Load(k)
		// fmt.Println(k, suc)
		// 如果 没有分配 || 已经分配了，并且没有成功，但是已经过了10分钟
		// if !tmp.st || check(tmp.time_) {
		if !tmp.st {
			filename = k.(string)
			return false
		}
		return true
	})
	reply.File = filename
	reply.NReduce = c.nReduce
	// 分配编号
	mutex.Lock()
	if id, ok := c.mids[filename]; ok {
		reply.Mid = id
	} else {
		reply.Mid = len(c.mids)
		c.mids[filename] = reply.Mid
	}
	mutex.Unlock()
	c.files.Store(filename, fileConf{reply.Mid, true, time.Now()})

	return nil
}

// 返回已经成功调用的文件名
func (c *Coordinator) MCallBack(args MapResultInfo, reply *bool) error {
	_, ok := c.sucFiles.Load(args.MapInputFile)
	if !ok {
		str := fmt.Sprintf("返回文件名不存在 %v", args.MapInputFile)
		return errors.New(str)
	}
	c.sucFiles.Store(args.MapInputFile, fileConf{args.Mid, true, time.Now()})
	*reply = true

	// 存储中间文件名 mr-tmp/0-2

	for k, _ := range args.MapOutputFilePaths {
		tmps := strings.Split(k, "/")
		tmp := strings.Split(tmps[1], "-")
		reduceId, _ := strconv.Atoi(tmp[2])
		// reduceId 文件名  "%v/mr-%v-%v", outputpath, id, hid
		cmutex.Lock()
		c.rInName[reduceId][k] = false
		cmutex.Unlock()
	}

	return nil
}

func (c *Coordinator) MapSuc(_ string, reply *bool) error {
	*reply = true
	c.sucFiles.Range(func(k, v interface{}) bool {
		tmp := v.(fileConf)
		if tmp.st {
			return true
		} else {
			*reply = false
			if check(tmp.time_) {
				c.files.Store(k, fileConf{tmp.id, false, time.Now()})
			}
			return false
		}
	})
	return nil
}

func (c *Coordinator) Reduce(args int, reply *Reduce) error {
	// var filename string
	// filename = ""
	// c.files.Store(filename, true)
	id := -1
	cmutex.Lock()
	for i, _ := range c.ids {
		if c.ids[i].id == 0 || check(c.ids[i].time_) {
			// fmt.Println(c.ids[i].time_)
			// fmt.Println("check", check(c.ids[i].time_), "i: ", i)
			// fmt.Println("reduceI:", i)
			c.ids[i].id = 1
			c.ids[i].time_ = time.Now()
			id = i
			break
		}
	}
	cmutex.Unlock()
	reply.Id = id
	if id == -1 {
		// log.Printf("所有任务分配完成")
		return nil
	}
	files := []string{}
	for k, _ := range c.rInName[id] {
		files = append(files, k)
	}
	reply.FilePaths = files
	return nil
}

func (c *Coordinator) RCallBack(args Reduce, reply *bool) error {
	oname := fmt.Sprintf("mr-out-%v", args.Id)
	err := os.Rename(args.OutputFileName, oname)
	if err != nil {
		log.Println(err)
		return err
	}
	c.ids[args.Id].st = true
	return nil
}

func (c *Coordinator) ReduceDone(args int, reply *bool) error {
	for _, v := range c.ids {
		if v.st {
			continue
		} else {
			// fmt.Println("i:", i)
			*reply = false
			return nil
		}
	}
	// 全部成功才会结束
	c.done = true
	*reply = true
	return nil
}

func (c *Coordinator) IsDone(args bool, reply *bool) error {
	*reply = c.done
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.done
}

func (c *Coordinator) T() {
	// Your code here.
	fmt.Println(c.reduceSucNum)
}

// func (c *Coordinator) checkMap() {
// 	for {
// 		c.mapdone = true
// 		c.sucFiles.Range(func(k, v interface{}) bool {
// 			if v.(bool) {
// 				return true
// 			} else {
// 				c.mapdone = false

// 				return false
// 			}
// 		})
// 		if c.mapdone {
// 			break
// 		}
// 		time.Sleep(10 * time.Second)
// 	}
// }

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	for _, file := range files {
		c.files.Store(file, fileConf{-1, false, time.Now()})
		c.sucFiles.Store(file, fileConf{-1, false, time.Now()})
	}
	c.nReduce = nReduce
	c.rInName = make([]map[string]bool, nReduce)
	c.ids = make([]fileConf, nReduce)
	c.mids = make(map[string]int)
	for i := 0; i < c.nReduce; i++ {
		c.rInName[i] = make(map[string]bool)
	}
	// go checkMap()
	c.server()
	return &c
}
