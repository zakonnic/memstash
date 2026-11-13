package memstash

import "github.com/puzpuzpuz/xsync/v4"

// counters are the operation counters behind WithStats, striped so an increment stays contention-free. Each add
// method is one branch around one Add, which keeps it inlinable; a second call would push it over the budget and cost
// every read a CALL.
type counters struct {
	enabled      bool
	memHits      xsync.Counter
	memMisses    xsync.Counter
	l2Hits       xsync.Counter
	l2Misses     xsync.Counter
	sets         xsync.Counter
	deletes      xsync.Counter
	tableGrowths xsync.Counter
	rejected     xsync.Counter
}

func newCounters(enabled bool) counters {
	if !enabled {
		return counters{}
	}
	return counters{
		enabled:      true,
		memHits:      *xsync.NewCounter(),
		memMisses:    *xsync.NewCounter(),
		l2Hits:       *xsync.NewCounter(),
		l2Misses:     *xsync.NewCounter(),
		sets:         *xsync.NewCounter(),
		deletes:      *xsync.NewCounter(),
		tableGrowths: *xsync.NewCounter(),
		rejected:     *xsync.NewCounter(),
	}
}

func (s *counters) addMemHits(n int64) {
	if s.enabled {
		s.memHits.Add(n)
	}
}

func (s *counters) addMemMisses(n int64) {
	if s.enabled {
		s.memMisses.Add(n)
	}
}

func (s *counters) addL2Hits(n int64) {
	if s.enabled {
		s.l2Hits.Add(n)
	}
}

func (s *counters) addL2Misses(n int64) {
	if s.enabled {
		s.l2Misses.Add(n)
	}
}

func (s *counters) addSets(n int64) {
	if s.enabled {
		s.sets.Add(n)
	}
}

func (s *counters) addDeletes(n int64) {
	if s.enabled {
		s.deletes.Add(n)
	}
}

func (s *counters) addTableGrowths(n int64) {
	if s.enabled {
		s.tableGrowths.Add(n)
	}
}

func (s *counters) addRejected(n int64) {
	if s.enabled {
		s.rejected.Add(n)
	}
}

// snapshot reads every counter once; a disabled set reads all zero.
func (s *counters) snapshot() Stats {
	if !s.enabled {
		return Stats{}
	}
	return Stats{
		memHits:      s.memHits.Value(),
		memMisses:    s.memMisses.Value(),
		l2Hits:       s.l2Hits.Value(),
		l2Misses:     s.l2Misses.Value(),
		sets:         s.sets.Value(),
		deletes:      s.deletes.Value(),
		tableGrowths: s.tableGrowths.Value(),
		rejected:     s.rejected.Value(),
	}
}

// Stats is a snapshot of the cache's operation counters (see Cache.Stats): it does not change after it is taken, so
// two of them subtract to what happened in between. Batch operations count once per key.
type Stats struct {
	memHits, memMisses, l2Hits, l2Misses, sets, deletes, tableGrowths, rejected int64
}

// MemoryHits is the number of read keys served from memory.
func (s Stats) MemoryHits() int64 { return s.memHits }

// MemoryMisses is the number of read keys that missed memory and stopped there, never reaching L2: reads on a cache
// without an L2, GetFromMemory, and calls that joined another key's in-flight load.
func (s Stats) MemoryMisses() int64 { return s.memMisses }

// L2Hits is the number of read keys that missed memory and were found in L2. Always 0 without an L2.
func (s Stats) L2Hits() int64 { return s.l2Hits }

// L2Misses is the number of read keys L2 was asked for and did not return, an L2 error included. Always 0 without
// an L2.
func (s Stats) L2Misses() int64 { return s.l2Misses }

// L2Gets is the number of read keys forwarded to L2: L2Hits() + L2Misses().
func (s Stats) L2Gets() int64 { return s.l2Hits + s.l2Misses }

// Hits is the number of read keys found in the cache: MemoryHits() + L2Hits().
func (s Stats) Hits() int64 { return s.memHits + s.l2Hits }

// Misses is the number of read keys found in neither level: MemoryMisses() + L2Misses().
func (s Stats) Misses() int64 { return s.memMisses + s.l2Misses }

// Sets is the number of values written into the cache, per key: Set, SetWithTTL, BatchSet and stored loader results,
// whether or not memory kept them (see Rejected) or an L2 write failed (see OnL2Error). L2-to-memory promotions are
// not counted - the value was already in the cache.
func (s Stats) Sets() int64 { return s.sets }

// Deletes is the number of keys passed to Delete and BatchDelete, whether or not the cache held them.
func (s Stats) Deletes() int64 { return s.deletes }

// TableGrowths is the number of shard item tables rebuilt into a larger table. Always 0 with WithPreallocatedSize:
// a preallocated table is never grown.
func (s Stats) TableGrowths() int64 { return s.tableGrowths }

// Rejected is the number of values memory did not take, though they count in Sets() and went to L2: heavier than
// their shard's capacity (MemoryCapacity or MemoryBudget over the shard count), or bound for a table that cannot grow
// with one free slot left.
func (s Stats) Rejected() int64 { return s.rejected }

// TablePurges is always 0: a deletion compacts its probe run on the spot, so a table is never rebuilt at the same
// size. Kept for callers that read it.
func (s Stats) TablePurges() int64 { return 0 }

// Gets returns the total number of read keys: Hits() + Misses().
func (s Stats) Gets() int64 { return s.Hits() + s.Misses() }

// HitRate returns Hits() / Gets(); 0 when nothing has been read yet.
func (s Stats) HitRate() float64 {
	if gets := s.Gets(); gets > 0 {
		return float64(s.Hits()) / float64(gets)
	}
	return 0
}

// MemoryHitRate returns MemoryHits() / Gets(); 0 when nothing has been read yet.
func (s Stats) MemoryHitRate() float64 {
	if gets := s.Gets(); gets > 0 {
		return float64(s.MemoryHits()) / float64(gets)
	}
	return 0
}

// L2HitRate returns L2Hits() / L2Gets(), the share of the reads that reached L2; 0 when nothing reached L2 yet.
func (s Stats) L2HitRate() float64 {
	if gets := s.L2Gets(); gets > 0 {
		return float64(s.L2Hits()) / float64(gets)
	}
	return 0
}

// MissRate returns Misses() / Gets(); 0 when nothing has been read yet.
func (s Stats) MissRate() float64 {
	if gets := s.Gets(); gets > 0 {
		return float64(s.Misses()) / float64(gets)
	}
	return 0
}

// Stats returns a snapshot of the cache's operation counters, all zero if collection was not enabled with WithStats.
func (c *Cache[K, V]) Stats() Stats {
	return c.stats.snapshot()
}
