package shardkv

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.state.Serving(key2shard(args.Key)) {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	getOp := Op{
		OpModule:    StateOp,
		OpType:      Get,
		Key:         args.Key,
		ClerkID:     args.ClerkID,
		SequenceNum: args.SequenceNum,
	}

	res := kv.startOp(getOp)

	reply.Err = res.err
	reply.Value = res.value
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var res result

	kv.mu.Lock()
	if !kv.state.Serving(key2shard(args.Key)) {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	repeat := kv.state.IsIsRepeat(args.Key, args.ClerkID, args.SequenceNum)
	if repeat {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	kv.mu.Unlock()

	op := Op{
		OpModule:    StateOp,
		OpType:      args.Op,
		Key:         args.Key,
		Value:       args.Value,
		ClerkID:     args.ClerkID,
		SequenceNum: args.SequenceNum,
	}
	res = kv.startOp(op)

	reply.Err = res.err
	return
}

func (kv *ShardKV) PullShards(args *PullShardArgs, reply *PullShardReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.curCfg.Num < args.CfgNum {
		reply.Err = ErrCfgNum
		return
	}

	shards := map[int]*KvState{}
	for _, shardID := range args.ShardIDs {
		shards[shardID] = kv.state.CopyShard(shardID)
	}

	reply.Err = OK
	reply.Shards = shards
}

func (kv *ShardKV) IsServing(args *IsServingArgs, reply *IsServingReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.curCfg.Num < args.CfgNum {
		reply.Err = ErrCfgNum
		return
	}

	reply.Err = OK

	for _, shardID := range args.ShardIDs {
		if kv.state.NeedPull(shardID) {
			reply.Serving = false
			return
		}
	}

	reply.Serving = true
	return
}
