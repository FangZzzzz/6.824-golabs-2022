package shardkv

import (
	"6.824/shardctrler"
	"time"
)

const (
	pullConfigInterval = 100 * time.Millisecond
)

func (kv *ShardKV) pullConfig() {
	kv.mu.Lock()
	canPull := kv.state.AllServing()
	kv.mu.Unlock()
	if !canPull {
		return
	}

	curCfgNum := kv.curCfg.Num
	newCfg := kv.mck.Query(curCfgNum + 1)
	if newCfg.Num != curCfgNum+1 {
		return
	}

	op := Op{
		OpModule: ConfigOp,
		OpType:   UpdateConfig,
		Cfg:      newCfg,
	}
	_ = kv.startOp(op)
}

func (kv *ShardKV) updateStateManager(newCfg shardctrler.Config) {
	for shard, gid := range newCfg.Shards {
		if gid == kv.gid && kv.curCfg.Shards[shard] == 0 {
			kv.state.ShardManager[shard] = Serving
		} else if gid == kv.gid && kv.curCfg.Shards[shard] != kv.gid {
			kv.state.ShardManager[shard] = NeedPull
		} else if gid != kv.gid && kv.curCfg.Shards[shard] == kv.gid {
			kv.state.ShardManager[shard] = NeedGc
		}
	}
}
