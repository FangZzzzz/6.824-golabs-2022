package shardkv

import (
	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"log"
	"time"
)

type result struct {
	err         Err
	key, value  string
	index, term int
}

func (kv *ShardKV) startOp(op Op) result {
	const applyResultTimeout = 1 * time.Second

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
	}()

	select {
	case res = <-resChn:
		return res
	case <-time.After(applyResultTimeout):
		res.err = ErrTimeOut
		return res
	}
}

func (kv *ShardKV) applyMsg() {
	for !kv.killed() {
		for msg := range kv.applyCh {
			if msg.SnapshotValid {
				kv.applySnapshotMsg(msg)
				continue
			}
			kv.applyCommandMsg(msg)

			kv.doSnapshot()
		}
	}
}

func (kv *ShardKV) doSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.maxraftstate == -1 {
		return
	}

	if kv.maxraftstate <= kv.rf.GetCurrentStateSize() {
		snapshot := kv.generateSnapshot()
		kv.rf.Snapshot(kv.appliedIndex, snapshot)
	}
}

func (kv *ShardKV) applySnapshotMsg(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.readStateFromSnapshot(msg.Snapshot)
		kv.appliedIndex = msg.SnapshotIndex
	}
}

func (kv *ShardKV) applyCommandMsg(msg raft.ApplyMsg) {
	op, _ := msg.Command.(Op)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 理论上在raft层已经做过严格保证有序了，不会出现这种情况。
	if kv.appliedIndex >= msg.CommandIndex {
		return
	}

	var res result
	switch op.OpModule {
	case StateOp:
		res = kv.applyMsgToState(op)
	case ConfigOp:
		res = kv.applyMsgToCfg(op)
	case ShardOp:
		res = kv.applyMsgToShard(op)
	}

	kv.appliedIndex = msg.CommandIndex

	key := getResultChnKey(msg.CommandTerm, msg.CommandIndex)
	resultChn, ok := kv.resultChnMap[key]

	if !ok {
		return
	}

	go func() {
		select {
		case resultChn <- res:
		case <-time.After(10 * time.Millisecond):
		}
	}()
}

func (kv *ShardKV) applyMsgToCfg(op Op) result {
	var res result
	newCfg := op.Cfg
	if newCfg.Num != kv.curCfg.Num+1 {
		res.err = ErrCfgNum
		return res
	}

	kv.updateStateManager(newCfg)
	kv.preCfg = kv.curCfg
	kv.curCfg = newCfg

	res.err = OK
	return res
}

func (kv *ShardKV) applyMsgToState(op Op) result {
	var res result

	switch op.OpType {
	case Get:
		res.value, res.err = kv.state.Get(op.Key, op.ClerkID, op.SequenceNum)
	case Put:
		kv.state.Put(op.Key, op.Value, op.ClerkID, op.SequenceNum)
		res.err = OK
	case Append:
		res.value, res.err = kv.state.Append(op.Key, op.Value, op.ClerkID, op.SequenceNum)
	default:
		log.Fatalf("un know op type: %s", op.OpType)
	}

	return res
}

func (kv *ShardKV) applyMsgToShard(op Op) result {
	var res result
	if op.CfgNum != kv.curCfg.Num {
		res.err = ErrCfgNum
		return res
	}

	switch op.OpType {
	case AddShard:
		kv.state.AddShard(op.Shards)
	case DeleteShard:
		kv.state.DeleteShard(op.ShardIDs)
	}

	res.err = OK
	return res
}

func (kv *ShardKV) generateSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	_ = e.Encode(kv.preCfg)
	_ = e.Encode(kv.curCfg)
	_ = e.Encode(kv.state)
	_ = e.Encode(kv.appliedIndex)
	return w.Bytes()
}

func (kv *ShardKV) readStateFromSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)

	d := labgob.NewDecoder(r)

	var preCfg, curCfg shardctrler.Config
	newState := NewShardState()
	newAppliedIndex := 0

	d.Decode(&preCfg)
	d.Decode(&curCfg)
	d.Decode(&newState)
	d.Decode(&newAppliedIndex)

	kv.preCfg = preCfg
	kv.curCfg = curCfg
	kv.state = newState
	kv.appliedIndex = newAppliedIndex
}

func getResultChnKey(term int, index int) string {
	return fmt.Sprintf("%d_%d", term, index)
}
