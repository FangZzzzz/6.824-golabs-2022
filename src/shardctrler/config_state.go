package shardctrler

import "sort"

type ConfigState struct {
	configs []Config
}

func MakeConfigState() *ConfigState {
	configs := make([]Config, 1)
	configs[0].Groups = map[int][]string{}

	return &ConfigState{
		configs: configs,
	}
}

func (cs *ConfigState) Query(num int) Config {
	var c Config

	if num >= 0 && num < len(cs.configs) {
		c = cs.configs[num]
	}

	if num == -1 {
		c = cs.configs[len(cs.configs)-1]
	}

	return c
}

func (cs *ConfigState) Join(groups map[int][]string) {
	newConfig := cs.newLastConfig()

	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newConfig.Groups[gid] = newServers
	}

	g2s := gid2Shard(newConfig)
	for {
		source, target := getGIDWithMaximumShards(g2s), getGIDWithMinimumShards(g2s)
		if source != 0 && len(g2s[source])-len(g2s[target]) <= 1 {
			break
		}
		g2s[target] = append(g2s[target], g2s[source][0])
		g2s[source] = g2s[source][1:]
	}

	for gid, shards := range g2s {
		for _, shard := range shards {
			newConfig.Shards[shard] = gid
		}
	}

	cs.configs = append(cs.configs, newConfig)
}

func (cs *ConfigState) Leave(gids []int) {
	newConfig := cs.newLastConfig()

	g2s := gid2Shard(newConfig)
	orphanShards := make([]int, 0)
	for _, gid := range gids {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := g2s[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(g2s, gid)
		}
	}

	if len(newConfig.Groups) == 0 {
		for i := range newConfig.Shards {
			newConfig.Shards[i] = 0
		}
	} else {
		for _, shard := range orphanShards {
			target := getGIDWithMinimumShards(g2s)
			g2s[target] = append(g2s[target], shard)
		}
		for gid, shards := range g2s {
			for _, shard := range shards {
				newConfig.Shards[shard] = gid
			}
		}
	}

	cs.configs = append(cs.configs, newConfig)
}

func (cs *ConfigState) Move(shard int, gid int) {
	newConfig := cs.newLastConfig()

	if _, ok := newConfig.Groups[gid]; ok {
		newConfig.Shards[shard] = gid
	}

	cs.configs = append(cs.configs, newConfig)
}

func (cs *ConfigState) newLastConfig() Config {
	lastConfig := cs.configs[len(cs.configs)-1]

	newConfig := Config{
		Num:    len(cs.configs),
		Shards: [NShards]int{},
		Groups: map[int][]string{},
	}

	for i, gid := range lastConfig.Shards {
		newConfig.Shards[i] = gid
	}

	for k, v := range lastConfig.Groups {
		newConfig.Groups[k] = v
	}

	return newConfig
}

func gid2Shard(config Config) map[int][]int {
	g2s := map[int][]int{}
	for gid := range config.Groups {
		g2s[gid] = []int{}
	}

	for shard, gid := range config.Shards {
		g2s[gid] = append(g2s[gid], shard)
	}
	return g2s
}

func getGIDWithMinimumShards(g2s map[int][]int) int {
	// make iteration deterministic
	var keys []int
	for k := range g2s {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find GID with minimum shards
	index, min := -1, NShards+1
	for _, gid := range keys {
		if gid != 0 && len(g2s[gid]) < min {
			index, min = gid, len(g2s[gid])
		}
	}
	return index
}

func getGIDWithMaximumShards(g2s map[int][]int) int {
	// always choose gid 0 if there is any
	if shards, ok := g2s[0]; ok && len(shards) > 0 {
		return 0
	}
	// make iteration deterministic
	var keys []int
	for k := range g2s {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find GID with maximum shards
	index, max := -1, -1
	for _, gid := range keys {
		if len(g2s[gid]) > max {
			index, max = gid, len(g2s[gid])
		}
	}
	return index
}
