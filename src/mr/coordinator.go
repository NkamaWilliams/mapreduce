package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu                 sync.Mutex
	mapTasks           []Task
	reduceTasks        []Task
	mapDone            map[int]bool
	reduceDone         map[int]bool
	mapTaskAssigned    map[int]bool
	reduceTaskAssigned map[int]bool
	nreduce            int
	nmap               int
	done               bool
}
type Task struct {
	Type    TaskType
	File    string
	TaskID  int
	NReduce int
	NMap    int
	Start   time.Time
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type RequestTaskArgs struct{}
type RequestTaskReply struct {
	Task Task
}
type ReportTaskArgs struct {
	TaskID int
	Type   TaskType
}
type ReportTaskReply struct{}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	//Assign map tasks first
	for i, task := range c.mapTasks {
		if !c.mapDone[task.TaskID] && !c.mapTaskAssigned[task.TaskID] {
			c.mapTasks[i].Start = time.Now()
			reply.Task = c.mapTasks[i]
			c.mapTaskAssigned[task.TaskID] = true
			return nil
		}
	}

	//All maps done
	allMapDone := len(c.mapDone) == len(c.mapTasks)

	//Assign reduce tasts
	if allMapDone {
		for i, task := range c.reduceTasks {
			if !c.reduceDone[task.TaskID] && !c.reduceTaskAssigned[task.TaskID] {
				c.reduceTasks[i].Start = time.Now()
				reply.Task = c.reduceTasks[i]
				c.reduceTaskAssigned[task.TaskID] = true
				return nil
			}
		}
	}

	allReduceDone := len(c.reduceDone) == len(c.reduceTasks)
	if allMapDone && allReduceDone {
		reply.Task.Type = ExitTask
		c.done = true
	} else {
		reply.Task.Type = WaitTask
	}

	return nil
}

func (c *Coordinator) ReportTaskCompletion(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.Type {
	case MapTask:
		c.mapDone[args.TaskID] = true
	case ReduceTask:
		c.reduceDone[args.TaskID] = true
	}

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
	//l, e := net.Listen("tcp", ":1234")
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
	ret := true

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	ret = c.done

	return ret
}

// Reassign tasks if they take more than 5 seconds to complete
func (c *Coordinator) CheckStatus() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.mapTasks) != len(c.mapDone) {
		for val := range c.mapTaskAssigned {
			if c.mapDone[val] {
				continue
			}
			task := c.mapTasks[val]
			if time.Since(task.Start) > 5*time.Second {
				log.Printf("Reassigning map task %d due to worker timeout", val)
				c.mapTaskAssigned[val] = false
				task.Start = time.Now()
			}
		}
	}

	if len(c.reduceTasks) != len(c.reduceDone) {
		for val := range c.reduceTaskAssigned {
			if c.reduceDone[val] {
				continue
			}
			task := c.reduceTasks[val]
			if time.Since(task.Start) > 5*time.Second {
				log.Printf("Reassigning reduce task %d due to worker timeout", val)
				c.reduceTaskAssigned[val] = false
				task.Start = time.Now()
			}
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nmap:               len(files),
		nreduce:            nReduce,
		mapDone:            make(map[int]bool),
		reduceDone:         make(map[int]bool),
		mapTaskAssigned:    make(map[int]bool),
		reduceTaskAssigned: make(map[int]bool),
		done:               false,
	}

	// Your code here.

	//Make map tasks
	for i, file := range files {
		c.mapTasks = append(c.mapTasks, Task{
			Type:    MapTask,
			File:    file,
			TaskID:  i,
			NReduce: nReduce,
			NMap:    len(files),
		})
	}

	//Make reduce tasks
	for i := 0; i < nReduce; i += 1 {
		c.reduceTasks = append(c.reduceTasks, Task{
			Type:    ReduceTask,
			TaskID:  i,
			NReduce: nReduce,
			NMap:    len((files)),
		})
	}

	go func() {
		for {
			if c.done {
				break
			}
			c.CheckStatus()
			time.Sleep(time.Second)
		}
	}()

	c.server()
	return &c
}
