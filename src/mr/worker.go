package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		jobRsp, ok := PullJob()
		if !ok {
			continue
		}

		var err error

		switch jobRsp.State {
		case CoordinatorStateMap:
			err = workerMap(jobRsp.Job, mapf)
		case CoordinatorStateReduce:
			err = workReduce(jobRsp.Job, reducef)
		case CoordinatorStateWait:
			time.Sleep(100 * time.Millisecond)
			continue
		case CoordinatorStateDone:
			return
		default:
			log.Fatalf("un know state: %d\n", jobRsp.State)
		}

		if err != nil {
			log.Printf("worker err: %+v", err)
			CommitJob(false, jobRsp.Job.ID)
		}

		CommitJob(true, jobRsp.Job.ID)
	}

}

func workerMap(job *Job, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(job.FileName)

	if err != nil {
		log.Printf("cannot open %v", job.FileName)
		return err
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", job.FileName)
		return err
	}
	_ = file.Close()

	kva := mapf(job.FileName, string(content))

	nFile := make([]*os.File, job.NReduce)
	nIntermediate := make([][]KeyValue, job.NReduce)

	for _, kv := range kva {
		index := ihash(kv.Key) % job.NReduce
		nIntermediate[index] = append(nIntermediate[index], kv)
	}

	_ = os.Mkdir("tmp", 0777)

	for i := 0; i < job.NReduce; i++ {
		bytes, _ := json.Marshal(nIntermediate[i])

		nFile[i], _ = os.CreateTemp("tmp", fmt.Sprintf("mr-tmp-%d", job.Index))
		tmpFileName := filepath.Join(nFile[i].Name())

		_ = ioutil.WriteFile(tmpFileName, bytes, 0777)

		_ = nFile[i].Close()
		_ = os.Rename(tmpFileName, fmt.Sprintf("tmp/mr-%d-%d", job.Index, i))
	}

	return nil
}

func workReduce(job *Job, reducef func(string, []string) string) error {
	var intermediate []KeyValue

	for i := 0; i < job.NMap; i++ {
		file, _ := os.Open(fmt.Sprintf("tmp/mr-%d-%d", i, job.Index))
		content, _ := ioutil.ReadAll(file)
		tmp := []KeyValue{}
		_ = json.Unmarshal(content, &tmp)

		intermediate = append(intermediate, tmp...)

		_ = file.Close()
	}

	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	oname := fmt.Sprintf("mr-out-%d", job.Index)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	return nil
}

func PullJob() (*PullJobRsp, bool) {
	req := &PullJobReq{}
	rsp := &PullJobRsp{}

	ok := call("Coordinator.PullJob", req, rsp)
	return rsp, ok
}

func CommitJob(suc bool, jobID string) bool {
	req := &CommitJobReq{
		JobID: jobID,
		Suc:   suc,
	}
	rsp := &CommitJobRsp{}

	ok := call("Coordinator.CommitJob", req, rsp)
	return ok
}

//
// example function to show how to make an RPC call to the coordinator.
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
