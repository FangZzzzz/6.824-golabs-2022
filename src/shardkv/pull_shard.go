package shardkv

import (
	"6.824/shardctrler"
	"sync"
	"time"
)

const (
	pullShardInterval = 100 * time.Millisecond
)

func (kv *ShardKV) pullShard() {
	var wg sync.WaitGroup

	kv.mu.Lock()
	gid2Shard := kv.GetNeedPullGID2ShardIDs()

	if len(gid2Shard) == 0 {
		kv.mu.Unlock()
		return
	}

	for gid, shards := range gid2Shard {
		wg.Add(1)
		go func(servers []string, curCfgNum int, shardIDs []int) {
			defer wg.Done()
			kv.doPullShards(servers, curCfgNum, shardIDs)
		}(kv.preCfg.Groups[gid], kv.curCfg.Num, shards)
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) doPullShards(servers []string, cfgNum int, shardIDs []int) {
	for _, server := range servers {
		req := &PullShardArgs{
			CfgNum:   cfgNum,
			ShardIDs: shardIDs,
		}
		rsp := &PullShardReply{}

		svrCli := kv.make_end(server)
		ok := svrCli.Call("ShardKV.PullShards", req, rsp)
		if !ok || rsp.Err != OK {
			continue
		}

		addShardOp := Op{
			OpModule: ShardOp,
			OpType:   AddShard,
			Cfg:      shardctrler.Config{},
			CfgNum:   cfgNum,
			Shards:   rsp.Shards,
		}

		_ = kv.startOp(addShardOp)
		return
	}
}

func (kv *ShardKV) GetNeedPullGID2ShardIDs() map[int][]int {
	res := map[int][]int{}

	for shard, status := range kv.state.ShardManager {
		if status != NeedPull {
			continue
		}
		gid := kv.preCfg.Shards[shard]
		res[gid] = append(res[gid], shard)
	}
	return res
}
