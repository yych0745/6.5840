package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

var mutex sync.Mutex

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type MapResultInfo struct {
	MapOutputFilePaths map[string]struct{}
	MapInputFile       string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var currentTime int64

//
// main/mrworker.go calls this function.
//
var tmr string

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	log.SetFlags(log.Lshortfile | log.LstdFlags)
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// for i := 0; i <= 2; i++ {
	// 	go CallExample(i)
	// }

	var wg sync.WaitGroup
	n := 1
	currentTime = time.Now().Unix()
	os.RemoveAll("mr-tmp")
	tmr = fmt.Sprintf("mr-tmp-%v", currentTime)
	os.Mkdir(tmr, os.ModePerm)
	wg.Add(n)
	conf := Conf{}
	tmp := ""
	ok := call("Coordinator.GetConf", tmp, &conf)
	if ok {
	} else {
		log.Println("call failed")
	}

	for i := 0; i < n; i++ {
		go WorkerMap(mapf, i, &wg)
	}
	wg.Wait()

	// 开始走reduce

	log.Println("reduce start!!!!")
	wg.Add(n)
	for i := 0; i < n; i++ {
		go WorkerReduce(n, reducef, &wg)
	}
	wg.Wait()
	done := false
	ok = call("Coordinator.IsDone", done, &done)
	if !ok {
		log.Fatal("call fail")
		return
	}
	for !done {
		ok = call("Coordinator.IsDone", done, &done)
		if !ok {
			log.Fatal("call fail")
			return
		}
	}
}

func WorkerReduce(id int, reducef func(string, []string) string, waitGroup *sync.WaitGroup) error {
	defer waitGroup.Done()
	for {
		var ReduceConf Reduce
		var filename string
		// 初始化获得reduce的id和需要读取的文件名
		ok := call("Coordinator.Reduce", id, &ReduceConf)
		if ok {
			// reply.Y should be 100.
			// log.Printf("get reduce id:%v\n", ReduceConf.Id)
			// 回调
		} else {
			log.Println("call failed")
			return errors.New("call failed")
		}

		if ReduceConf.Id == -1 {
			break
			// fmt.Println(1111)
			continue
		}
		// 读取文件
		intermediate := []KeyValue{}
		for _, filename = range ReduceConf.FilePaths {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
				return err
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
		}
		sort.Sort(ByKey(intermediate))
		oname := fmt.Sprintf("mr-out-%v", ReduceConf.Id)
		ofile, _ := os.OpenFile(oname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
			i = j
		}
		ofile.Close()
		t := false
		ok = call("Coordinator.RCallBack", t, &t)
		if ok {
		} else {
			return errors.New("call failed")
		}
	}
	return nil
}

func getTempPath(dir string) (string, error) {
	file, err := ioutil.TempFile(dir, "sss")
	name := ""
	if err != nil {
		return name, err
	}
	name = file.Name()
	file.Close()
	return name, nil
}

func WorkerMap(mapf func(string, string) []KeyValue, id int, waitGroup *sync.WaitGroup) error {
	defer waitGroup.Done()
	for {
		var filename string
		tmpFilepaths := make(map[int]string)
		outputResults := MapResultInfo{make(map[string]struct{}), ""}
		outputpath := fmt.Sprintf("%v", tmr)
		var m_map Map

		ok := call("Coordinator.Map", id, &m_map)
		if !ok {
			return errors.New("call failed")
		}

		filename = m_map.File
		nReduce := m_map.NReduce
		if filename == "" {
			var suc bool
			ok := call("Coordinator.MapSuc", "", &suc)
			if !ok {
				return errors.New("call failed")
			}
			if suc {
				break
			}
			continue
		}

		id = m_map.Mid
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))

		for _, kv := range kva {

			hid := ihash(kv.Key) % nReduce

			if _, ok := tmpFilepaths[hid]; !ok {
				name, err := getTempPath(outputpath)
				if err != nil {
					return err
				}
				tmpFilepaths[hid] = name
				// println(ok, hid, tmpFilepaths[hid])
			}
			// filePath := fmt.Sprintf("%v/mr-%v-%v", outputpath, id, hid)
			filePath := tmpFilepaths[hid]
			file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
			enc := json.NewEncoder(file)
			if err != nil {
				log.Println(err)
				return err
			}

			err = enc.Encode(&kv)
			if err != nil {
				log.Println(err)
				return err
			}

			err = file.Close()
			if err != nil {
				log.Println(err)
				return err
			}
		}

		for hid, file := range tmpFilepaths {

			filePath := fmt.Sprintf("%v/mr-%v-%v", outputpath, m_map.Mid, hid)
			err = os.Rename(file, filePath)
			if err != nil {
				log.Println(err)
				return err
			}
			outputResults.MapOutputFilePaths[filePath] = struct{}{}
		}
		if err != nil {
			log.Printf("文件打开失败%v\n", err)
		}

		t := false
		outputResults.MapInputFile = m_map.File
		ok = call("Coordinator.MCallBack", outputResults, &t)
		if ok {
			// log.Printf("MCallBack succ")
		} else {
			// log.Printf("call failed!\n")
			return errors.New("call failed")
		}
	}
	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample(x int) {
	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
