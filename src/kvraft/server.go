package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false
const timeOut = 1 * time.Second

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType      OpType
	Key, Value  string
	ClerkID     int64
	SequenceNum int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state              *KvState
	lastSequenceNumMap map[int64]int64
	resultChnMap       map[string]chan result
}

type result struct {
	err        Err
	key, value string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	getOp := Op{
		OpType:      GET,
		Key:         args.Key,
		ClerkID:     args.ClerkID,
		SequenceNum: args.SequenceNum,
	}

	res := kv.startOp(getOp)

	reply.Err = res.err
	reply.Value = res.value
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if lastSeq, ok := kv.lastSequenceNumMap[args.ClerkID]; ok && lastSeq >= args.SequenceNum {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	kv.mu.Unlock()

	op := Op{
		OpType:      args.Op,
		Key:         args.Key,
		Value:       args.Value,
		ClerkID:     args.ClerkID,
		SequenceNum: args.SequenceNum,
	}
	res := kv.startOp(op)

	reply.Err = res.err
	return
}

func (kv *KVServer) startOp(op Op) result {
	var res result
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.err = ErrWrongLeader
		return res
	}

	resChn := make(chan result)
	resultKey := getResultChnKey(term, index)
	kv.mu.Lock()
	kv.resultChnMap[resultKey] = resChn
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.resultChnMap, resultKey)
		kv.mu.Unlock()
		close(resChn)
	}()

	select {
	case res = <-resChn:
		return res
	case <-time.After(timeOut):
		res.err = ErrTimeOut
		return res
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyMsg() {
	for !kv.killed() {
		for msg := range kv.applyCh {

			DPrintf("applyMsg msg: %+v", msg)

			kv.applyCommandMsg(msg)
		}
	}
}

func (kv *KVServer) applyCommandMsg(msg raft.ApplyMsg) {
	op, _ := msg.Command.(Op)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	res := kv.applyMsgToState(op)

	key := getResultChnKey(msg.CommandTerm, msg.CommandIndex)
	resultChn, ok := kv.resultChnMap[key]

	if ok {
		select {
		case resultChn <- res:
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (kv *KVServer) applyMsgToState(op Op) result {
	var res result
	lastSeqID, ok := kv.lastSequenceNumMap[op.ClerkID]
	if ok && lastSeqID >= op.SequenceNum && op.OpType != GET {
		res.err = OK
		return res
	}

	switch op.OpType {
	case GET:
		res.value, res.err = kv.state.Get(op.Key)
	case PUT:
		kv.state.Put(op.Key, op.Value)
		res.err = OK
	case APPEND:
		kv.state.Append(op.Key, op.Value)
		res.err = OK
	default:
		log.Fatalf("un know op type: %s", op.OpType)
	}

	kv.lastSequenceNumMap[op.ClerkID] = op.SequenceNum

	return res
}

func (kv *KVServer) generateSnapshot() *bytes.Buffer {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(kv.state)
	_ = e.Encode(kv.lastSequenceNumMap)
	return w
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.state = NewKvState()
	kv.lastSequenceNumMap = map[int64]int64{}
	kv.resultChnMap = map[string]chan result{}

	go kv.applyMsg()
	return kv
}

func getResultChnKey(term int, index int) string {
	return fmt.Sprintf("%d_%d", term, index)
}
