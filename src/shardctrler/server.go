package shardctrler

import (
	"6.824/raft"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const timeOut = 1 * time.Second

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here
	dead int32

	state *ConfigState // indexed by config num

	lastSequenceNumMap map[int64]int64
	resultChnMap       map[string]chan result
}

type result struct {
	err Err

	config Config
}

type Op struct {
	// Your data here.
	OpType      OpType
	ClerkID     int64
	SequenceNum int64

	// query req
	Num int
	// join req
	Groups map[int][]string
	// leave req
	Gids []int
	// move req
	Shard int
	Gid   int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	joinOp := Op{
		OpType:      JOIN,
		ClerkID:     args.ClerkID,
		SequenceNum: args.SequenceNum,
		Groups:      args.Servers,
	}

	res := sc.startOp(joinOp)

	reply.Err = res.err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	leaveOp := Op{
		OpType:      LEAVE,
		ClerkID:     args.ClerkID,
		SequenceNum: args.SequenceNum,
		Gids:        args.GIDs,
	}

	res := sc.startOp(leaveOp)

	reply.Err = res.err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	moveOp := Op{
		OpType:      MOVE,
		ClerkID:     args.ClerkID,
		SequenceNum: args.SequenceNum,
		Shard:       args.Shard,
		Gid:         args.GID,
	}

	res := sc.startOp(moveOp)

	reply.Err = res.err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	if args.Num >= 0 && args.Num < len(sc.state.configs) {
		reply.Err = OK
		reply.Config = sc.state.Query(args.Num)
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	queryOp := Op{
		OpType:      QUERY,
		ClerkID:     args.ClerkID,
		SequenceNum: args.SequenceNum,
		Num:         args.Num,
	}

	res := sc.startOp(queryOp)

	reply.Err = res.err
	reply.Config = res.config
}

func (sc *ShardCtrler) startOp(op Op) result {
	var res result

	sc.mu.Lock()
	if lastSeq, ok := sc.lastSequenceNumMap[op.ClerkID]; ok && lastSeq >= op.SequenceNum {
		sc.mu.Unlock()
		res.err = OK
		return res
	}
	sc.mu.Unlock()

	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		res.err = ErrWrongLeader
		return res
	}

	resChn := make(chan result)
	resultKey := getResultChnKey(term, index)
	sc.mu.Lock()
	sc.resultChnMap[resultKey] = resChn
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.resultChnMap, resultKey)
		sc.mu.Unlock()
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

func getResultChnKey(term int, index int) string {
	return fmt.Sprintf("%d_%d", term, index)
}

func (sc *ShardCtrler) applyMsg() {
	for !sc.killed() {
		for msg := range sc.applyCh {
			sc.applyCommandMsg(msg)
		}
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applyCommandMsg(msg raft.ApplyMsg) {
	op, _ := msg.Command.(Op)

	sc.mu.Lock()
	defer sc.mu.Unlock()

	res := sc.applyMsgToState(op)

	key := getResultChnKey(msg.CommandTerm, msg.CommandIndex)
	resultChn, ok := sc.resultChnMap[key]

	if ok {
		select {
		case resultChn <- res:
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (sc *ShardCtrler) applyMsgToState(op Op) result {
	var res result

	lastSeqID, ok := sc.lastSequenceNumMap[op.ClerkID]
	if ok && lastSeqID >= op.SequenceNum && op.OpType != QUERY {
		res.err = OK
		return res
	}

	switch op.OpType {
	case QUERY:
		res.config = sc.state.Query(op.Num)
	case JOIN:
		sc.state.Join(op.Groups)
	case LEAVE:
		sc.state.Leave(op.Gids)
	case MOVE:
		sc.state.Move(op.Shard, op.Gid)
	default:
		log.Fatalf("un know op type: %s", op.OpType)
	}

	sc.lastSequenceNumMap[op.ClerkID] = op.SequenceNum

	res.err = OK
	return res
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.state = MakeConfigState()

	sc.lastSequenceNumMap = map[int64]int64{}
	sc.resultChnMap = map[string]chan result{}

	go sc.applyMsg()

	return sc
}
