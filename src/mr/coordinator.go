package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	Todo     TaskState = 0
	Handling TaskState = 1
	Finished TaskState = 2
)

type Task struct {
	FileName string
	State    TaskState
}
type Coordinator struct {
	// Your definitions here.
	NReduce           int
	MapRemainTasks    map[int]*Task
	ReduceRemainTasks map[int]*Task
	mu                *sync.RWMutex
	// mapTaskIdLock, reduceTaskIdLock *sync.Mutex
	finishedMapTask    int //已完成数目
	finishedReduceTask int
	MaxTaskId          int
	nextMapTaskId      int  // 下一个分配的maptaskid
	nextReduceTaskId   int  // 下一个分配的reduce task id
	finishMapProcedure bool // 是否完成map阶段
	isFinished         bool // 所有阶段完成
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) getNextMapTaskId() int {
	// c.mapTaskIdLock.Lock()
	// defer c.mapTaskIdLock.Unlock()

	ret := c.nextMapTaskId
	c.nextMapTaskId++
	c.nextMapTaskId = c.nextMapTaskId % c.MaxTaskId
	return ret
}

func (c *Coordinator) getNextReduceTaskId() int {
	// c.reduceTaskIdLock.Lock()
	// defer c.reduceTaskIdLock.Unlock()

	ret := c.nextReduceTaskId
	c.nextReduceTaskId++
	c.nextReduceTaskId = c.nextReduceTaskId % c.NReduce
	return ret
}
func (c *Coordinator) TryGetTask(args GetTaskArgs, reply *GetTaskReply) error {
	// c.mu.RLock()
	// defer c.mu.RUnlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finishedMapTask < c.MaxTaskId {
		reply.Task = "Map"
		// DEBUG
		reply.TaskId = c.getNextMapTaskId()
		cnt := 0
		for task, exist := c.MapRemainTasks[reply.TaskId]; !exist || task.State != Todo; {
			reply.TaskId = c.getNextMapTaskId()
			cnt++
			if cnt == c.MaxTaskId {
				return fmt.Errorf("TryGetTask Failed")
			}
		}
		reply.MapFile = c.MapRemainTasks[reply.TaskId].FileName
	} else if c.finishedReduceTask < c.NReduce && c.finishMapProcedure {
		reply.Task = "Reduce"
		reply.TaskId = c.getNextReduceTaskId()
		cnt := 0
		for task, exist := c.ReduceRemainTasks[reply.TaskId]; !exist || task.State != Todo; {
			reply.TaskId = c.getNextReduceTaskId()
			cnt++
			if cnt == c.NReduce {
				return fmt.Errorf("TryGetTask Failed")
			}
		}
		for {
			task, exist := c.ReduceRemainTasks[reply.TaskId]
			if !exist || task.State == Todo {
				break
			}
			reply.TaskId = c.getNextReduceTaskId()
		}
		reply.ReduceFile = c.ReduceRemainTasks[reply.TaskId].FileName
	} else {
		reply.Task = "Exit"
	}

	// tmp_resp, _ := json.Marshal(*reply)
	// fmt.Printf("TryGetTask success! %s\n", tmp_resp)
	return nil
}

func (c *Coordinator) DoGetTask(args GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// // fmt.Printf("DoGetTask Start %v\n", args)
	var task *Task
	switch args.Task {
	case "Map":
		if task, exist := c.MapRemainTasks[args.TaskId]; !exist || task.State != Todo {
			return fmt.Errorf("DoGetTask Failed")
		}
		task = c.MapRemainTasks[args.TaskId]
		task.State = Handling
		// // fmt.Printf("DoGetTask success %v\n", args)
		go func() {
			timer := time.NewTimer(10 * time.Second)
			<-timer.C
			c.mu.Lock()
			defer c.mu.Unlock()
			if oldTask, exist := c.MapRemainTasks[args.TaskId]; exist && oldTask.State != Finished {
				oldTask.State = Todo
			}
		}()
	case "Reduce":
		if task, exist := c.ReduceRemainTasks[args.TaskId]; !exist || task.State != Todo {
			return fmt.Errorf("DoGetTask Failed")
		}
		task = c.ReduceRemainTasks[args.TaskId]
		task.State = Handling
		// // fmt.Printf("DoGetTask success %v\n", args)
		go func() {
			timer := time.NewTimer(10 * time.Second)
			<-timer.C
			// // fmt.Printf("Try Rollback Task %v\n", args)
			c.mu.Lock()
			defer c.mu.Unlock()
			if oldTask, exist := c.ReduceRemainTasks[args.TaskId]; exist && oldTask.State != Finished {
				c.ReduceRemainTasks[args.TaskId].State = Todo
			}
		}()
	}
	return nil
}

func (c *Coordinator) FinishTask(args GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// fmt.Printf("FinishTask %v\n", args)
	switch args.Task {
	case "Map":
		if _, exist := c.MapRemainTasks[args.TaskId]; !exist {
			return fmt.Errorf("FinishTask Failed")
		}
		task := c.MapRemainTasks[args.TaskId]
		if task.State != Handling {
			return fmt.Errorf("FinishTask Failed")
		}
		c.MapRemainTasks[args.TaskId].State = Finished
		c.finishedMapTask++

		if c.finishedMapTask == c.MaxTaskId {
			c.finishMapProcedure = true
			for i := 0; i < c.NReduce; i++ {
				c.ReduceRemainTasks[i] = &Task{
					FileName: fmt.Sprintf("%v", i),
					State:    Todo,
				}
			}
		}
	case "Reduce":
		if _, exist := c.ReduceRemainTasks[args.TaskId]; !exist {
			return fmt.Errorf("FinishTask Failed")
		}
		task := c.ReduceRemainTasks[args.TaskId]
		if task.State != Handling {
			return fmt.Errorf("FinishTask Failed")
		}
		c.ReduceRemainTasks[args.TaskId].State = Finished
		c.finishedReduceTask++
		if c.finishedReduceTask == c.NReduce {
			c.isFinished = true
		}
	default:
	}
	// fmt.Printf("FinishTask finished %v %v %v\n", args, c.finishedMapTask, c.finishedReduceTask)
	return nil
}

func (c *Coordinator) GetNReduce(args GetTaskArgs, reply *int) error {
	*reply = c.NReduce
	return nil
}

func (c *Coordinator) GetNMap(args GetTaskArgs, reply *int) error {
	*reply = c.MaxTaskId
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.isFinished {
		fmt.Printf("ALL task done!!\n")
		return true
	}
	return c.isFinished
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		NReduce:           nReduce,
		mu:                &sync.RWMutex{},
		MapRemainTasks:    make(map[int]*Task),
		ReduceRemainTasks: make(map[int]*Task),
		// mapTaskIdLock:     &sync.Mutex{},
		// reduceTaskIdLock:  &sync.Mutex{},
		MaxTaskId: len(files),
	}
	for idx, file := range files {
		c.MapRemainTasks[idx] = &Task{
			FileName: file,
			State:    Todo,
		}
	}
	// fmt.Printf("Coordinator Start! MaxTaskId %v  NReduce %v \n", c.MaxTaskId, c.NReduce)
	c.server()
	return &c
}
