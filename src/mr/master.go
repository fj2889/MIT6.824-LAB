package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskInterface interface {
	GenerateTaskInfo() TaskInfo
	SetBeginTime()
	GetMapIndex() int
	GetReduceIndex() int
	IsTimeOut() bool
}

type Task struct {
	beginTime   time.Time
	fileName    string
	mapIndex    int
	reduceIndex int
	nFile       int
	nReduce     int
}

type MapTask struct {
	Task
}

type ReduceTask struct {
	Task
}

func (this *MapTask) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		Filename:    this.fileName,
		MapIndex:    this.mapIndex,
		ReduceIndex: this.reduceIndex,
		NFile:       this.nFile,
		NReduce:     this.nReduce,
		State:       TaskMap,
	}
}

func (this *ReduceTask) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		Filename:    this.fileName,
		MapIndex:    this.mapIndex,
		ReduceIndex: this.reduceIndex,
		NFile:       this.nFile,
		NReduce:     this.nReduce,
		State:       TaskReduce,
	}
}

func (this *Task) SetBeginTime() {
	this.beginTime = time.Now()
}

func (this *Task) GetMapIndex() int {
	return this.mapIndex
}

func (this *Task) GetReduceIndex() int {
	return this.reduceIndex
}

func (this *Task) IsTimeOut() bool {
	if time.Now().Sub(this.beginTime) > time.Duration(10)*time.Second {
		return true
	}
	return false
}

type TaskQueue struct {
	taskArray []TaskInterface
	mutex     sync.Mutex
}

func (this *TaskQueue) lock() {
	this.mutex.Lock()
}

func (this *TaskQueue) unlock() {
	this.mutex.Unlock()
}

func (this *TaskQueue) Push(taskInterface TaskInterface) {
	this.lock()
	if taskInterface == nil {
		fmt.Println("master: cannot push an empty task")
		return
	}
	this.taskArray = append(this.taskArray, taskInterface)
	this.unlock()
	return
}

func (this *TaskQueue) Pop() TaskInterface {
	this.lock()
	taskLen := len(this.taskArray)
	if taskLen == 0 {
		return nil
	}
	ret := this.taskArray[taskLen-1]
	this.taskArray = this.taskArray[:taskLen-1]
	this.unlock()
	return ret
}

func (this *TaskQueue) Remove(mapIndex int, reduceIndex int) {
	this.lock()
	for i := 0; i < this.Len(); i++ {
		if this.taskArray[i].GetMapIndex() == mapIndex && this.taskArray[i].GetReduceIndex() == reduceIndex {
			this.taskArray = append(this.taskArray[:i], this.taskArray[i+1:]...)
			break
		}
	}
	this.unlock()
}

func (this *TaskQueue) Len() int {
	return len(this.taskArray)
}

type Master struct {
	// Your definitions here.
	files   []string
	nReduce int
	nFile   int

	// map queue
	mapWaiting TaskQueue
	mapRunning TaskQueue

	// reduce queue
	reduceWaiting TaskQueue
	reduceRunning TaskQueue
}

func (m *Master) IsDone() bool {
	if m.mapWaiting.Len() == 0 && m.mapRunning.Len() == 0 && m.reduceWaiting.Len() == 0 && m.reduceRunning.Len() == 0 {
		return true
	}
	return false
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AskForTask(request *TaskInfo, response *TaskInfo) error {
	// tell worker to quit
	if m.IsDone() {
		*response = TaskInfo{
			State: TaskEnd,
		}
		return nil
	}

	// reduce task first
	if m.reduceWaiting.Len() != 0 {
		taskInterface := m.reduceWaiting.Pop()
		taskInterface.SetBeginTime()
		m.reduceRunning.Push(taskInterface)
		*response = taskInterface.GenerateTaskInfo()
		return nil
	}

	// give map task
	if m.mapWaiting.Len() != 0 {
		taskInterface := m.mapWaiting.Pop()
		taskInterface.SetBeginTime()
		m.mapRunning.Push(taskInterface)
		*response = taskInterface.GenerateTaskInfo()
		return nil
	}

	// just wait
	*response = TaskInfo{
		State: TaskWait,
	}
	return nil
}

func (m *Master) TellDone(request *TaskInfo, response *TaskInfo) error {
	switch request.State {
	case TaskMap:
		fmt.Printf("master: get the message that map task %v done\n", request.MapIndex)
		m.mapRunning.Remove(request.MapIndex, request.ReduceIndex)
		if m.mapWaiting.Len() == 0 && m.mapRunning.Len() == 0 {
			m.createReduceTask()
		}
		return nil
	case TaskReduce:
		fmt.Printf("master: get the message that reduce task %v done\n", request.ReduceIndex)
		m.reduceRunning.Remove(request.MapIndex, request.ReduceIndex)
		return nil
	default:
		fmt.Println("master: unknown task done")
		return nil
	}
}

func (m *Master) createReduceTask() {
	for i := 0; i < m.nReduce; i++ {
		reduceTask := ReduceTask{
			Task{
				reduceIndex: i,
				nReduce:     m.nReduce,
				nFile:       m.nFile,
			},
		}
		m.reduceWaiting.Push(&reduceTask)
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	fmt.Println("master server start complete")
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	// Your code here.
	return m.IsDone()
}

func (m *Master) CheckTimeOut() {
	for {
		if m.IsDone() {
			return
		}
		for _, taskInterface := range m.reduceRunning.taskArray {
			if taskInterface.IsTimeOut() {
				fmt.Printf("reduce task %v time out\n", taskInterface.GetReduceIndex())
				m.reduceRunning.Remove(taskInterface.GetMapIndex(), taskInterface.GetReduceIndex())
				m.reduceWaiting.Push(taskInterface)
			}
		}
		for _, taskInterface := range m.mapRunning.taskArray {
			if taskInterface.IsTimeOut() {
				fmt.Printf("map task %v time out\n", taskInterface.GetMapIndex())
				m.mapRunning.Remove(taskInterface.GetMapIndex(), taskInterface.GetReduceIndex())
				m.mapWaiting.Push(taskInterface)
			}
		}
		time.Sleep(time.Duration(5) * time.Second)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.
	m := Master{
		files:   files,
		nReduce: nReduce,
		nFile:   len(files),
	}

	// create map task
	fmt.Println("master: start to create map task")
	for mapIndex, filename := range files {
		mapTask := MapTask{
			Task{
				fileName: filename,
				mapIndex: mapIndex,
				nFile:    len(files),
				nReduce:  nReduce,
			},
		}
		m.mapWaiting.Push(&mapTask)
	}
	fmt.Println("master: complete to create map task and start the master server")

	go m.CheckTimeOut()

	m.server()
	return &m
}
