package mr

import (
	"encoding/gob"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	tasks chan Task

	nMap int32 // total number of map tasks
	mapkDone int32

	nReduce int32
	reduceDone int32
	reduceSource [][]string // each reduce will have a list of shuffled files



	nextWorkerID uint32
	workerPool *sync.Map

	recycle chan uint32 // a channel to recycle task from non-responding worker

	reports chan Report
}

type workerSession struct {
	workID uint32
	idle bool
	task Task
	mutex sync.Mutex
	pingChan chan struct{}
}



// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

type RegisterReq struct{}

type RegisterRes struct{
	WorkerID uint32
}

type Report interface {
	IsReport()
}

type MapReport struct {
	Files []string
	MapTaskID uint32
}

func (mr *MapReport) IsReport() {}

type ReduceReport struct {
	ReduceTaskID int
}

func (rr *ReduceReport) IsReport() {}

type ReportTaskReq struct{
	WorkerID uint32
	Report Report

}
type ReportTaskRes struct {

}
func (m *Master) ReportTask(req *ReportTaskReq, res *ReportTaskRes) error {
	v, ok := m.workerPool.Load(req.WorkerID)
	if !ok {
		return nil
	}

	session := v.(*workerSession)
	session.mutex.Lock()
	session.task = nil
	session.idle = true
	session.mutex.Unlock()
	m.reports <- req.Report
	return nil
}

type PingRequest struct {
	WorkerID uint32
}

type PingRes struct{}

func (m *Master) Ping(req *PingRequest, res *PingRes) error {
	v, ok := m.workerPool.Load(req.WorkerID)
	if !ok {
		return  nil
	}
	session := v.(*workerSession)
	session.pingChan <- struct{}{}
	return nil
}

func (m *Master) handleReport() {
	for {
		select {
		case report := <-m.reports:
			switch kind := report.(type) {
			case *MapReport:
				for i := 0; i < len(kind.Files); i++ {
					m.reduceSource[i] = append(m.reduceSource[i], kind.Files[i])
				}
				atomic.AddInt32(&m.mapkDone, 1)
				if m.mapkDone == m.nMap {
					for i := 0; i < len(m.reduceSource); i++ {
						task := &ReduceTask{
							Files: m.reduceSource[i],
							ShardIdx: i,
						}
						m.tasks <- task
					}
				}
			case *ReduceReport:
				atomic.AddInt32(&m.reduceDone, 1)

			}
		}
	}
}


func (m *Master) Register(req *RegisterReq, res *RegisterRes) error {
	workerID := atomic.LoadUint32(&m.nextWorkerID)
	for {
		if atomic.CompareAndSwapUint32(&m.nextWorkerID, workerID, workerID + 1) {
			session := &workerSession{
				workID: workerID,
				idle:   true,
				task:   nil,
			}

			m.workerPool.Store(workerID, session)
			res.WorkerID = workerID
			return  nil
		}
		time.Sleep(time.Second)
	}

}

type GetTaskReq struct {
	WorkerID uint32
}

type GetTaskRes struct {
	Task Task
}

func (m *Master) GetTask(req *GetTaskReq, res *GetTaskRes) error {
	val, ok := m.workerPool.Load(req.WorkerID)
	if !ok {
		res.Task = new(ShutdownTask)
		return nil
	}
	session := val.(*workerSession)
	if !session.idle {
		return nil
	}

	timer := time.After(5 * time.Second)
	select {
	case task, ok := <- m.tasks:
		if !ok {
			res.Task = new(ShutdownTask)
			m.workerPool.Delete(req.WorkerID)
			return nil
		}
		session.mutex.Lock()
		session.idle = false
		session.task = task
		session.mutex.Unlock()
		res.Task = task
		// start watching
		go m.watch(session)
		return nil
	case <-timer:
		return nil
	}
}

func (m *Master) watch(session *workerSession) {
	timer := time.NewTimer(10 *time.Second)
	for {
		select {
		case <-timer.C:
			m.recycle <- session.workID
		case <-session.pingChan:
			fmt.Println("ping signal received from worker ", session.workID)
			timer.Reset(10 * time.Second)
		}
	}
}

func (m *Master) recycleTask() {
	for {
		select {
		case worker, ok := <-m.recycle:
			if !ok {
				return
			}
			v, ok := m.workerPool.Load(worker)
			if ok {


				session := v.(*workerSession)
				m.tasks <- session.task
				m.workerPool.Delete(worker)
				fmt.Println("recycling worker ", worker, session.task)
			}
		}
	}
}



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

	// Your code here.
	if m.reduceDone != m.nReduce {
		return false

	}
	m.workerPool.Range(func(key interface{}, val interface{}) bool {
		m.workerPool.Delete(key)
		return true
	})

	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	gob.Register(new(Task))
	gob.Register(new(MapTask))
	gob.Register(new(ReduceTask))
	gob.Register(new(ShutdownTask))
	gob.Register(new(Report))
	gob.Register(new(MapReport))
	gob.Register(new(ReduceReport))
	m := Master{
		tasks: make(chan Task, len(files)),
		nMap: int32(len(files)),
		nReduce: int32(nReduce),
		reduceSource: make([][]string, nReduce),
		workerPool: &sync.Map{},
		recycle: make(chan uint32, len(files)),
		reports: make(chan Report, len(files)),
	}

	// Your code here.

	for i, file := range files {
		task := &MapTask{
			NReduce:nReduce,
			Id: int32(i),
			FileName:file,
		}
		m.tasks <- task
	}
	go m.recycleTask()
	go m.handleReport()

	m.server()
	return &m
}
