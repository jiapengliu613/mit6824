package mr

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var workerID uint32
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	gob.Register(new(Task))
	gob.Register(new(MapTask))
	gob.Register(new(ReduceTask))
	gob.Register(new(ShutdownTask))
	gob.Register(new(Report))
	gob.Register(new(MapReport))
	gob.Register(new(ReduceReport))

	// Your worker implementation here.
	workerID = Register()
	go func() {
		req := &PingRequest{
			WorkerID:workerID,
		}
		res := new(PingRes)
		tc := time.NewTicker(10 * time.Second)
		defer tc.Stop()
		for {
			<-tc.C
			call("Master.Ping", req, res)
		}

	}()
	for {
		task := GetTask(workerID)
		if task == nil {
			return
		}
		ExecuteTask(mapf, reducef, workerID, task)

		tc := time.NewTicker(time.Second)
		tc.Stop()
	}

}

// Register will register a worker
func Register() uint32 {
	req := new(RegisterReq)
	res := new(RegisterRes)
	if call("Master.Register", req, res) {
		return  res.WorkerID
	}
	log.Fatalf("unable to register")
	return 0
}

func GetTask(workerID uint32) Task {
	req := &GetTaskReq{
		WorkerID:workerID,
	}
	res := new(GetTaskRes)
	if call("Master.GetTask", req, res) {
		return res.Task
	}
	log.Fatalf("cannot get task")
	return nil
}

func ExecuteTask(mapF func(string, string) []KeyValue, reduceF func(string, []string) string, workerID uint32, task Task) {
	switch kind := task.(type) {
	case *MapTask:
		files := doMap(mapF,  kind)
		req := &ReportTaskReq{
			WorkerID:workerID,
			Report: &MapReport{
				Files:files,
				MapTaskID:uint32(kind.Id),
			},
		}
		res := new(ReportTaskRes)
		call("Master.ReportTask", req, res)
	case *ReduceTask:
		req := &ReportTaskReq{
			WorkerID: workerID,
			Report: &ReduceReport{
				ReduceTaskID: kind.ShardIdx,
			},
		}
		res := new(ReportTaskRes)
		doReduce(reduceF, kind)
		call("Master.ReportTask", req, res)
	case *ShutdownTask:
		os.Exit(0)
	default:
		log.Fatalf("invalid task")
	}
}



type Task interface {
	IsTask()
}

type MapTask struct {
	FileName string
	Id int32
	NReduce int
}

func (mt *MapTask) IsTask() {}

type ReduceTask struct {
	Files []string
	ShardIdx int
}

func (rt *ReduceTask) IsTask() {}

type ShutdownTask struct {}
func (st *ShutdownTask) IsTask() {}

func doMap(mapF func(string, string) []KeyValue,  task *MapTask) []string {
	var res []string
	f, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}
	defer f.Close()
	content, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}
	kvs := mapF(task.FileName, string(content))
	shuffled := make([][]KeyValue, task.NReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % task.NReduce
		shuffled[idx] = append(shuffled[idx], kv)
	}
	for i, vals := range shuffled {
		//sort.Sort(ByKey(vals))
		v, err := json.Marshal(vals)
		if err != nil {
			log.Fatalf("cannot marshal: %s", err)
		}
		name := fmt.Sprintf("worker-%d-shuffled-%d", task.Id, i)
		out, err := os.Create(name)
		if err != nil {
			log.Fatalf("cannot create %s", name)
		}
		out.Write(v)
		out.Close()
		res = append(res, name)
	}
	return res
}

func doReduce(reducef func(string, []string) string, task *ReduceTask) {
	var kvs []KeyValue
	for _, file := range task.Files {
		f, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open: %s", err)
		}
		content, err := ioutil.ReadAll(f)
		if err != nil {
			log.Fatalf("cannot read: %s", err)
		}
		var vals []KeyValue
		err = json.Unmarshal(content, &vals)
		if err != nil {
			log.Fatalf("cannot unmarshal: %s", err)
		}
		kvs = append(kvs, vals...)
	}
	sort.Sort(ByKey(kvs))

	oname := fmt.Sprintf("mr-out-%d", task.ShardIdx)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

		i = j
	}

	ofile.Close()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}


//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
