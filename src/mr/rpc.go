package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskType int

const (
	TaskMap TaskType = iota
	TaskReduce
	TaskWait
	TaskEnd
)

func (this TaskType) String() string {
	switch this {
	case TaskMap:
		return "map_task"
	case TaskReduce:
		return "reduce_task"
	case TaskWait:
		return "wait_task"
	case TaskEnd:
		return "task_done"
	default:
		return "unknown_task"
	}
}

type TaskInfo struct {
	Filename    string
	MapIndex    int
	ReduceIndex int
	NFile       int // reduce task need
	NReduce     int
	State       TaskType
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
