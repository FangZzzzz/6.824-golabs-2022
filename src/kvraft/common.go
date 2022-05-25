package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
)

const (
	GET    = "GET"
	PUT    = "PUT"
	APPEND = "APPEND"
)

type Err string
type OpType string

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
