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
const (
	CoordinatorStateMap    = 1
	CoordinatorStateReduce = 2
	CoordinatorStateDone   = 3

	CoordinatorStateWait = 6
)

type Job struct {
	ID       string
	Index    int
	JobType  string
	FileName string
	NReduce  int
	NMap     int
}

type PullJobReq struct {
}

type PullJobRsp struct {
	State int
	Job   *Job
}

type CommitJobReq struct {
	JobID string
	Suc   bool
}

type CommitJobRsp struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
