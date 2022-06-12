package shardkv

import (
	"6.824/shardctrler"
)

type ShardState struct {
	KvState      map[int]*KvState
	ShardManager [shardctrler.NShards]StateStatus
}

func NewShardState() *ShardState {
	return &ShardState{
		KvState: map[int]*KvState{},
	}
}

func (s *ShardState) AllServing() bool {
	for _, status := range s.ShardManager {
		if status != Serving && status != Empty {
			return false
		}
	}
	return true
}

func (s *ShardState) Serving(shard int) bool {
	return s.ShardManager[shard] == Serving
}

func (s *ShardState) NeedGc(shard int) bool {
	return s.ShardManager[shard] == NeedGc
}

func (s *ShardState) NeedPull(shard int) bool {
	return s.ShardManager[shard] == NeedPull
}

func (s *ShardState) IsIsRepeat(key string, clientID, seq int64) bool {
	shardID := key2shard(key)

	shard := s.getShard(shardID)
	return shard.IsRepeat(clientID, seq)
}

func (s *ShardState) CopyShard(shardID int) *KvState {
	newState := NewKvState()

	shard := s.getShard(shardID)
	for k, v := range shard.Kv {
		newState.Kv[k] = v
	}
	for k, v := range shard.LastSequenceNumMap {
		newState.LastSequenceNumMap[k] = v
	}

	return newState
}

func (s *ShardState) AddShard(shards map[int]*KvState) {
	for shardID, shard := range shards {
		if !s.NeedPull(shardID) {
			DPrintf("ERROR: shard: %d not pull status, status: %s", shardID, s.ShardManager[shardID])
			return
		}

		s.KvState[shardID] = shard
		s.ShardManager[shardID] = Serving
	}
}

func (s *ShardState) DeleteShard(shardIDs []int) {
	for _, shardID := range shardIDs {
		if !s.NeedGc(shardID) {
			DPrintf("ERROR: shard: %d not gc status, status: %s", shardID, s.ShardManager[shardID])
			return
		}

		delete(s.KvState, shardID)
		s.ShardManager[shardID] = Empty
	}
}

func (s *ShardState) Get(key string, clientID, seq int64) (string, Err) {
	shardID := key2shard(key)
	if !s.Serving(shardID) {
		return "", ErrWrongGroup
	}

	shard := s.getShard(shardID)
	return shard.Get(key, clientID, seq)
}

func (s *ShardState) Put(key, value string, clientID, seq int64) Err {
	shardID := key2shard(key)
	if !s.Serving(shardID) {
		return ErrWrongGroup
	}

	shard := s.getShard(shardID)
	shard.Put(key, value, clientID, seq)
	return OK
}

func (s *ShardState) Append(key, value string, clientID, seq int64) (string, Err) {
	shardID := key2shard(key)
	if !s.Serving(shardID) {
		return "", ErrWrongGroup
	}

	shard := s.getShard(shardID)
	return shard.Append(key, value, clientID, seq), OK
}

func (s *ShardState) getShard(shardID int) *KvState {
	shard, ok := s.KvState[shardID]
	if !ok {
		shard = NewKvState()
		s.KvState[shardID] = shard
	}
	return shard
}

type KvState struct {
	Kv                 map[string]string
	LastSequenceNumMap map[int64]int64
}

func NewKvState() *KvState {
	return &KvState{
		Kv:                 map[string]string{},
		LastSequenceNumMap: map[int64]int64{},
	}
}

func (k *KvState) IsRepeat(clientID, seq int64) bool {
	if lastSeq, ok := k.LastSequenceNumMap[clientID]; ok && seq <= lastSeq {
		return true
	}
	return false
}

func (k *KvState) Get(key string, clientID, seq int64) (string, Err) {
	value, ok := k.Kv[key]
	if !ok {
		return "", ErrNoKey
	}

	if k.IsRepeat(clientID, seq) {
		return value, OK
	}

	k.LastSequenceNumMap[clientID] = seq

	return value, OK
}

func (k *KvState) Put(key, value string, clientID, seq int64) {
	if k.IsRepeat(clientID, seq) {
		return
	}

	k.Kv[key] = value

	k.LastSequenceNumMap[clientID] = seq
}

func (k *KvState) Append(key, value string, clientID, seq int64) string {
	if k.IsRepeat(clientID, seq) {
		return k.Kv[key]
	}

	k.Kv[key] += value

	k.LastSequenceNumMap[clientID] = seq

	return k.Kv[key]
}
