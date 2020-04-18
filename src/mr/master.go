package mr

import (
	"encoding/gob"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	debug = false
	// master phase
	Phase_Map      = 1
	Phase_Reduce   = 2
	Phase_Complete = 3

	// task status
	Status_Error     = -1
	Status_Idel      = 0
	Status_Progess   = 1
	Status_Completed = 2
)

type WorkerMachine struct {
	Id     int64
	Status int
}
type MapTask struct {
	Id       int
	FileName string
	WorkerId int64
	Status   int
	NReduce  int
}
type ReduceTask struct {
	Id        int
	FileNames []string
	WorkerId  int64
	Status    int
}
type Master struct {
	// Your definitions here.
	NReduce int
	NMap    int

	MapTasks    []*MapTask
	ReduceTasks []*ReduceTask

	Phase int

	MapTaskChan    chan *MapTask
	ReduceTaskChan chan *ReduceTask

	IntermediateFiles [][]string

	NCompleteMap    int
	NCompleteReduce int

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	return m.Phase == Phase_Complete
}

func (m *Master) GenerateMapTask(files []string) {
	for i, file := range files {
		mapTask := MapTask{
			Id:       i,
			FileName: file,
			Status:   Status_Idel,
			NReduce:  m.NReduce,
		}
		m.MapTasks = append(m.MapTasks, &mapTask)
		m.MapTaskChan <- (&mapTask)
	}
}
func (m *Master) GenerateReduceMap() {
	for i, files := range m.IntermediateFiles {
		reduceTask := ReduceTask{
			Id:        i,
			FileNames: files,
			Status:    Status_Idel,
		}
		m.ReduceTasks = append(m.ReduceTasks, &reduceTask)
		m.ReduceTaskChan <- (&reduceTask)
	}
}
func (m *Master) AssignTask(args *PullTaskArgs, reply *PullTaskReply) error {
	select {
	case mapTask := <-m.MapTaskChan:
		if debug {
			log.Printf("Assign a map task: %v\n", *mapTask)
		}
		mapTask.Status = Status_Progess
		reply.Task = *mapTask
		go m.MonitorMapTask(mapTask)
	case reduceTask := <-m.ReduceTaskChan:
		if debug {
			log.Printf("Assign a reduceTask : %v\n", *reduceTask)
		}
		reduceTask.Status = Status_Progess
		reply.Task = *reduceTask
		go m.MonitorReduceTask(reduceTask)
	default:
		if debug {
			log.Println("No task to assign")
		}
		reply.Task = nil
	}
	reply.Master_Phase = m.Phase
	return nil
}
func (m *Master) ReportTaskFinish(args *ReportTaskFinishArgs, reply *ReportTaskFinishReply) error {
	switch args.TaskPhase {
	case Phase_Map:
		if debug {
			log.Printf("Complete map task %d\n", args.TaskId)
		}
		m.mu.Lock()
		m.MapTasks[args.TaskId].Status = Status_Completed
		m.NCompleteMap++
		for i := 0; i < m.NReduce; i++ {
			m.IntermediateFiles[i] = append(m.IntermediateFiles[i], args.IntermediateFiles[i])
		}
		if m.NCompleteMap == m.NMap {
			if debug {
				log.Printf("All Map Tasks finished")
			}
			m.Phase = Phase_Reduce
			go m.GenerateReduceMap()
		}
		m.mu.Unlock()
	case Phase_Reduce:
		if debug {
			log.Printf("Complete reduce task %d\n", args.TaskId)
		}
		m.mu.Lock()
		m.ReduceTasks[args.TaskId].Status = Status_Completed
		m.NCompleteReduce++
		if m.NCompleteReduce == m.NReduce {
			if debug {
				log.Println("All Reduce Tasks finished")
			}
			m.Phase = Phase_Complete
		}
		m.mu.Unlock()
	}
	return nil
}
func (m *Master) MonitorMapTask(task *MapTask) {
	t := time.NewTicker(time.Second * 10)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			m.mu.Lock()
			task.Status = Status_Idel
			m.MapTaskChan <- task
			m.mu.Unlock()
		default:
			if task.Status == Status_Completed {
				return
			}
		}
	}
}
func (m *Master) MonitorReduceTask(task *ReduceTask) {
	t := time.NewTicker(time.Second * 10)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			m.mu.Lock()
			task.Status = Status_Idel
			m.ReduceTaskChan <- task
			m.mu.Unlock()
		default:
			if task.Status == Status_Completed {
				return
			}
		}
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.NReduce = nReduce
	m.MapTasks = make([]*MapTask, 0)
	m.MapTaskChan = make(chan *MapTask)
	m.ReduceTaskChan = make(chan *ReduceTask)
	m.NCompleteMap = 0
	m.NCompleteReduce = 0
	m.IntermediateFiles = make([][]string, nReduce)
	m.NMap = len(files)
	m.Phase = Phase_Map

	gob.Register(&MapTask{})
	gob.Register(&ReduceTask{})

	go m.GenerateMapTask(files)

	m.server()
	return &m
}