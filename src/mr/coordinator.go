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

type Coordinator struct {
	// Your definitions here.
	nMap, nReduce int

	// 这里要保证 waitingJobs、runningJobMap和state的同步，
	// 分发任务这里性能不重要，加一把大锁就行了
	lock sync.Mutex

	state int

	waitingJobs   *JobQueue
	runningJobMap map[string]*JobChan
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) PullJob(_ *PullJobReq, rsp *PullJobRsp) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	switch c.state {
	case CoordinatorStateMap,
		CoordinatorStateReduce:
		{

			job, ok := c.dispatchJob()
			if !ok {
				rsp.State = CoordinatorStateWait
				return nil
			}

			rsp.Job = job
			rsp.State = c.state
		}
	case CoordinatorStateDone:
		rsp.State = CoordinatorStateDone
	default:
		log.Fatalf("un know state: %d\n", c.state)
	}

	return nil
}

func (c *Coordinator) CommitJob(req *CommitJobReq, _ *CommitJobRsp) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	log.Printf("commit: %s, %b, %+v", req.JobID, req.Suc, req)

	if !req.Suc {
		c.jobFail(req.JobID)
	}

	c.jobSuc(req.JobID)
	if len(c.runningJobMap) == 0 && c.waitingJobs.Empty() {
		c.nextState()
	}

	return nil
}

func (c *Coordinator) nextState() {
	if c.state < CoordinatorStateDone {
		c.state = c.state + 1
	}
	if c.state == CoordinatorStateReduce {
		c.produceReduceJob()
	}
}

func (c *Coordinator) produceMapJob(files []string) {
	for i, fileName := range files {
		c.waitingJobs.Push(&Job{
			ID:       fmt.Sprintf("m_%d", i),
			Index:    i,
			FileName: fileName,
			NReduce:  c.nReduce,
			NMap:     len(files),
		})
	}
}

func (c *Coordinator) produceReduceJob() {
	for i := 0; i < c.nReduce; i++ {
		c.waitingJobs.Push(&Job{
			ID:       fmt.Sprintf("reduce_%d", i),
			Index:    i,
			FileName: "tmp_%d_%d",
			NReduce:  c.nReduce,
			NMap:     c.nMap,
		})
	}
}

func (c *Coordinator) dispatchJob() (*Job, bool) {
	job, ok := c.waitingJobs.Pop()
	if !ok {
		return nil, false
	}

	jobChn := BuildJobChan(job)
	c.runningJobMap[job.ID] = jobChn

	go func() {
		timer := time.NewTimer(10 * time.Second)

		select {
		case <-timer.C:
			c.lock.Lock()
			defer c.lock.Unlock()

			c.jobFail(job.ID)
		case <-jobChn.Done():
			timer.Stop()
		}
	}()

	return job, true
}

func (c *Coordinator) jobSuc(jobID string) {
	jobChn, ok := c.runningJobMap[jobID]
	if !ok {
		return
	}

	jobChn.Release()
	delete(c.runningJobMap, jobID)
}

func (c *Coordinator) jobFail(jobID string) {
	jobChn, ok := c.runningJobMap[jobID]
	if !ok {
		return
	}

	jobChn.Release()
	delete(c.runningJobMap, jobID)

	c.waitingJobs.Push(jobChn.job)
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.lock.Lock()
	ret = c.state == CoordinatorStateDone
	defer c.lock.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nMap = len(files)
	c.nReduce = nReduce
	c.state = CoordinatorStateMap

	c.waitingJobs = &JobQueue{}
	c.runningJobMap = make(map[string]*JobChan)

	c.produceMapJob(files)

	c.server()
	return &c
}
