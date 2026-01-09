package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var req GetTaskArgs
	var resp GetTaskReply
	// Your worker implementation here.

	// 尝试获得任务
	for {
		ok := call("Coordinator.TryGetTask", &GetTaskArgs{}, &resp)
		if ok {
			break
		}
		time.Sleep(2 * time.Second)
	}
	//获得任务
	req.Task = resp.Task
	req.TaskId = resp.TaskId
	for {
		ok := call("Coordinator.DoGetTask", &req, &GetTaskReply{})
		if ok {
			break
		}
		time.Sleep(2 * time.Second)
	}

	//任务处理
	switch resp.Task {
	case "Map":
		handleMap(&req, &resp, mapf)
	case "Reduce":
		handleReduce(&req, &resp, reducef)
	}
}

func handleMap(req *GetTaskArgs, resp *GetTaskReply, mapf func(string, string) []KeyValue) {
	var nReduce int
	for {
		ok := call("Coordinator.GetNReduce", &GetTaskArgs{}, &nReduce)
		if ok {
			break
		}
	}
	outFileMap := make(map[int]string)
	tmpFileMap := make(map[int]*os.File)
	for i := 0; i < nReduce; i++ {
		outFileName := fmt.Sprintf("mr-%v-%v", resp.TaskId, i)
		outFileMap[i] = outFileName
		tmpFile, err := os.OpenFile(fmt.Sprintf("tmp/tmp-%s", outFileName), os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0777)
		if err != nil {
			fmt.Printf("create tmp file err %v\n", err)
			return
		}
		defer tmpFile.Close()
		tmpFileMap[i] = tmpFile
	}
	intermediate := mapf(resp.MapFile, "")
	for _, keyVal := range intermediate {
		index := ihash(keyVal.Key) % nReduce
		json.NewEncoder(tmpFileMap[index]).Encode(keyVal)
	}

	for idx := range tmpFileMap {
		fileName := outFileMap[idx]
		os.Rename(fmt.Sprintf("tmp/tmp-%s", fileName), fmt.Sprintf("intermediate/%s", fileName))
	}
	for {
		ok := call("Coordinator.FinishTask", &req, &GetTaskReply{})
		if ok {
			break
		}
		time.Sleep(2 * time.Second)
	}
}
func handleReduce(req *GetTaskArgs, resp *GetTaskReply, reducef func(string, []string) string) {
	var nMap int
	for {
		ok := call("Coordinator.GetNReduce", &GetTaskArgs{}, &nMap)
		if ok {
			break
		}
	}
	totalKeyValList := []KeyValue{}
	for i := 0; i < nMap; i++ {
		intermediateFile, err := os.OpenFile(fmt.Sprintf("intermediate/mr-%v-%v", i, resp.ReduceFile), os.O_RDONLY, 0)
		if err != nil {
			fmt.Printf("open file failed %v\n", err)
			return
		}
		defer intermediateFile.Close()
		keyValueList := []KeyValue{}
		err = json.NewDecoder(intermediateFile).Decode(&keyValueList)
		if err != nil {
			fmt.Printf("json Decode err %v\n", err)
			return
		}
		totalKeyValList = append(totalKeyValList, keyValueList...)
	}

	oname := fmt.Sprintf("output/mr-out-%v", resp.TaskId)
	ofile, err := os.Create(oname)
	if err != nil {
		fmt.Printf("Create output file err %v\n", err)
		return
	}
	defer ofile.Close()
	sort.Sort(ByKey(totalKeyValList))
	i := 0
	for i < len(totalKeyValList) {
		j := i + 1
		for j < len(totalKeyValList) && totalKeyValList[j].Key == totalKeyValList[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, totalKeyValList[k].Value)
		}
		output := reducef(totalKeyValList[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", totalKeyValList[i].Key, output)
		i = j
	}
	for {
		ok := call("Coordinator.FinishTask", &req, &GetTaskReply{})
		if ok {
			break
		}
		time.Sleep(2 * time.Second)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
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
