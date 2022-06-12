package shardkv

import "6.824/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
	ErrCfgNum      = "ErrCfgNum"
)

const (
	StateOp  = "STATE"
	ConfigOp = "CONFIG"
	ShardOp  = "SHARD"
)

const (
	Get    = "GET"
	Put    = "PUT"
	Append = "APPEND"

	UpdateConfig = "UPDATE_CONFIG"

	AddShard    = "ADD_SHARD"
	DeleteShard = "DELETE_SHARD"
)

const (
	Empty    = ""
	Serving  = "SERVING"
	NeedPull = "NEED_PULL"
	NeedGc   = "NEED_GC"
)

type StateStatus string
type Err string
type OpModule string
type OpType string

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpModule    OpModule
	OpType      OpType
	Key, Value  string
	ClerkID     int64
	SequenceNum int64
	Cfg         shardctrler.Config
	CfgNum      int
	Shards      map[int]*KvState
	ShardIDs    []int
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    OpType // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkID     int64
	SequenceNum int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkID     int64
	SequenceNum int64
}

type GetReply struct {
	Err   Err
	Value string
}

type PullShardArgs struct {
	CfgNum   int
	ShardIDs []int
}

type PullShardReply struct {
	Err    Err
	Shards map[int]*KvState
}

type IsServingArgs struct {
	CfgNum   int
	ShardIDs []int
}

type IsServingReply struct {
	Err     Err
	Serving bool
}
