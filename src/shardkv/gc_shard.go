package shardkv

import (
	"sync"
	"time"
)

const (
	gcShardInterval = 100 * time.Millisecond
)

func (kv *ShardKV) gcShard() {
	var wg sync.WaitGroup

	kv.mu.Lock()
	gid2Shard := kv.GetNeedGcGID2ShardIDs()

	if len(gid2Shard) == 0 {
		kv.mu.Unlock()
		return
	}

	for gid, shards := range gid2Shard {
		wg.Add(1)
		go func(servers []string, curCfgNum int, shardIDs []int) {
			defer wg.Done()
			kv.doGcShard(servers, curCfgNum, shardIDs)
		}(kv.curCfg.Groups[gid], kv.curCfg.Num, shards)
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) GetNeedGcGID2ShardIDs() map[int][]int {
	res := map[int][]int{}

	for shard, status := range kv.state.ShardManager {
		if status != NeedGc {
			continue
		}
		gid := kv.curCfg.Shards[shard]
		res[gid] = append(res[gid], shard)
	}
	return res
}

func (kv *ShardKV) doGcShard(servers []string, cfgNum int, shardIDs []int) {
	for _, server := range servers {
		req := &IsServingArgs{
			CfgNum:   cfgNum,
			ShardIDs: shardIDs,
		}
		rsp := &IsServingReply{}

		svrCli := kv.make_end(server)
		ok := svrCli.Call("ShardKV.IsServing", req, rsp)
		if !ok || rsp.Err != OK {
			continue
		}

		if !rsp.Serving {
			return
		}

		deleteShardOp := Op{
			OpModule: ShardOp,
			OpType:   DeleteShard,
			CfgNum:   cfgNum,
			ShardIDs: shardIDs,
		}

		_ = kv.startOp(deleteShardOp)
		return
	}
}
