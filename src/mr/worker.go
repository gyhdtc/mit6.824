package mr

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Work struct {
	Id      int64
	Mapf    func(string, string) []KeyValue
	Reducef func(string, []string) string
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

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	gob.Register(&MapTask{})
	gob.Register(&ReduceTask{})
	worker := &Work{
		Mapf:    mapf,
		Reducef: reducef,
	}
	worker.Run()
}

func (w *Work) Run() {
	for {
		reply := w.PullTask()
		if reply == nil {
			return
		}
		switch reply.Master_Phase {
		case Phase_Complete:
			return
		case Phase_Map:
			if reply.Task == nil {
				if debug {
					log.Printf("Cannot apply a map Task")
				}
				time.Sleep(time.Second)
			} else {
				w.RunMapTask(reply.Task.(*MapTask))
			}
		case Phase_Reduce:
			if reply.Task == nil {
				if debug {
					log.Printf("Cannot apply a reduce Task")
				}
				time.Sleep(time.Second)
			} else {
				w.RunReduceTask(reply.Task.(*ReduceTask))
			}
		}
	}
}

func (w *Work) RunMapTask(task *MapTask) {
	if debug {
		log.Printf("Receive a map task: %v\n", *task)
	}
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}
	file.Close()
	kva := w.Mapf(task.FileName, string(content))
	sort.Sort(ByKey(kva))

	kvas := make([][]KeyValue, task.NReduce)

	for _, kv := range kva {
		outk := ihash(kv.Key) % task.NReduce
		kvas[outk] = append(kvas[outk], kv)
	}
	intermediatefiles := make([]string, 0)
	for i := 0; i < task.NReduce; i++ {
		os.Mkdir("maptmp", os.ModePerm)
		filename := "maptmp/mr-" + strconv.Itoa(task.Id) + "-" + strconv.Itoa(i)
		file, _ := os.Create(filename)
		enc := json.NewEncoder(file)
		for _, kv := range kvas[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Printf("write intermediate file name")
			}
		}
		intermediatefiles = append(intermediatefiles, filename)
	}
	w.ReportTaskFinish(task.Id, Phase_Map, intermediatefiles)
}
func (w *Work) RunReduceTask(task *ReduceTask) {
	if debug {
		log.Printf("Receive a reduce task: %v\n", *task)
	}
	files := task.FileNames
	kva := make(map[string][]string)
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva[kv.Key] = append(kva[kv.Key], kv.Value)
		}
	}
	ofile, _ := os.Create("mr-out-" + strconv.Itoa(task.Id))

	for k := range kva {
		output := w.Reducef(k, kva[k])
		fmt.Fprintf(ofile, "%v %v\n", k, output)
	}
	w.ReportTaskFinish(task.Id, Phase_Reduce, nil)
}
func (w *Work) ReportTaskFinish(taskId int, taskPhase int, intermediates []string) {
	args := ReportTaskFinishArgs{
		TaskId:            taskId,
		TaskPhase:         taskPhase,
		IntermediateFiles: intermediates,
	}
	reply := ReportTaskFinishReply{}
	call("Master.ReportTaskFinish", &args, &reply)
}
func (w *Work) PullTask() *PullTaskReply {
	args := PullTaskArgs{}
	reply := PullTaskReply{}
	if call("Master.AssignTask", &args, &reply) {
		return &reply
	} else {
		return nil
	}
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
