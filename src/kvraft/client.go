package kvraft

import (
	"6.824/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	serverLeaderID int64
	clerkID        int64
	sequenceNum    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkID = nrand()
	ck.serverLeaderID = 0
	ck.sequenceNum = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.sequenceNum += 1

	args := &GetArgs{
		Key:         key,
		ClerkID:     ck.clerkID,
		SequenceNum: ck.sequenceNum,
	}

	DPrintf("get: %+v", args)

	for {
		reply := &GetReply{}
		if ok := ck.servers[ck.serverLeaderID].Call("KVServer.Get", args, reply); ok {
			if reply.Err == OK ||
				reply.Err == ErrNoKey {
				return reply.Value
			}
		}

		ck.serverLeaderID = (ck.serverLeaderID + 1) % int64(len(ck.servers))
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op OpType) {
	// You will have to modify this function.
	ck.sequenceNum += 1

	args := &PutAppendArgs{
		Key:         key,
		Value:       value,
		Op:          op,
		ClerkID:     ck.clerkID,
		SequenceNum: ck.sequenceNum,
	}

	DPrintf("put append: %+v", args)

	for {
		reply := &PutAppendReply{}
		if ok := ck.servers[ck.serverLeaderID].Call("KVServer.PutAppend", args, reply); ok && reply.Err == OK {
			return
		}

		ck.serverLeaderID = (ck.serverLeaderID + 1) % int64(len(ck.servers))
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
