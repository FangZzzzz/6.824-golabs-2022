package raft

type Log struct {
	Entries           []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
}

// LogEntry 日志项
type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

func (lo *Log) get(i int) LogEntry {
	if i == lo.LastIncludedIndex {
		return LogEntry{Term: lo.LastIncludedTerm, Index: lo.LastIncludedIndex}
	}
	if i > lo.LastIncludedIndex+lo.size() {
		panic("ERROR: index greater than log length!\n")
	}
	if i < lo.LastIncludedIndex {
		panic("ERROR: index smaller than log snapshot!\n")
	}
	return lo.Entries[i-lo.LastIncludedIndex-1]
}

func (lo *Log) size() int {
	return len(lo.Entries)
}

func (lo *Log) getLastLogIndex() int {
	return lo.LastIncludedIndex + lo.size()
}

func (lo *Log) getLastLogTerm() int {
	return lo.get(lo.getLastLogIndex()).Term
}

func (lo *Log) findFirstLogIndex(term int) int {
	l, r := 0, lo.size()
	for l < r {
		mid := l + (r-l)/2
		if lo.Entries[mid].Term >= term {
			r = mid
		} else if lo.Entries[mid].Term < term {
			l = mid + 1
		}
	}
	return l + 1 + lo.LastIncludedIndex
}
