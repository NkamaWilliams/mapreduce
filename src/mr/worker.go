package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

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

	// Your worker implementation here.

	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}

		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			log.Fatal("Failed to Request Task!")
		}
		task := reply.Task
		switch task.Type {
		case MapTask:
			handleMapTask(task, mapf)
			reportTaskDone(task)
		case ReduceTask:
			handleReduceTask(task, reducef)
			reportTaskDone(task)
		case WaitTask:
			time.Sleep(1e6)
		case ExitTask:
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func readFile(filepath string) string {
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("Error opening file %v", filepath)
	}
	contents, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Error reading contents of file %v", filepath)
	}
	return string(contents)
}

func handleMapTask(task Task, mapf func(string, string) []KeyValue) {
	content := readFile(task.File)
	kva := mapf(task.File, content)

	// Partition kva into `task.NReduce` intermediate files (one per reducer)
	// Write to intermediate files like: mr-X-Y (X=MapID, Y=ReduceID)

	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		i := ihash(kv.Key) % task.NReduce
		buckets[i] = append(buckets[i], kv)
	}

	for i, kvs := range buckets {
		filename := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
		file, err := os.Create(filename)

		if err != nil {
			log.Fatalf("Failed to create file %q", filename)
		}
		enc := json.NewEncoder(file)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("error encoding KV: %v", err)
			}
		}
		file.Close()
	}
}

func handleReduceTask(task Task, reducef func(string, []string) string) {
	kvs := []KeyValue{}

	for m := 0; m < task.NMap; m++ {
		filename := fmt.Sprintf("mr-%d-%d", m, task.TaskID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open file named %q", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		file.Close()
	}

	//Group by key
	kvmap := make(map[string][]string)
	for _, kv := range kvs {
		kvmap[kv.Key] = append(kvmap[kv.Key], kv.Value)
	}

	//Sort keys
	var keys []string
	for k := range kvmap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	//Create output file
	outputname := fmt.Sprintf("mr-out-%d", task.TaskID)
	tempFile, err := os.CreateTemp("", "mr-out-*")
	if err != nil {
		log.Fatalf("Error creating temp outfile %q", outputname)
	}

	for _, key := range keys {
		result := reducef(key, kvmap[key])
		fmt.Fprintf(tempFile, "%v %v\n", key, result)

	}
	tempFile.Close()

	os.Rename(tempFile.Name(), outputname)
}

func reportTaskDone(task Task) {
	args := ReportTaskArgs{
		TaskID: task.TaskID,
		Type:   task.Type,
	}
	reply := ReportTaskReply{}

	ok := call("Coordinator.ReportTaskCompletion", &args, &reply)

	if !ok {
		log.Fatalf("Failed to report task with id %v as done", task.TaskID)
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
