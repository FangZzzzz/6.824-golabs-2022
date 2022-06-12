package shardkv

import (
	"time"
)

const (
	debugShardStatusInterval = 1 * time.Second
)

func (kv *ShardKV) debugShardStatus() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("me: %d, cftNum: %d, status: %+v", kv.gid, kv.curCfg.Num, kv.state.ShardManager)
}
