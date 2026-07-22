package memstash

import "github.com/puzpuzpuz/xsync/v3"

// Stats is a snapshot of the cache's operation counters (see Cache.Stats). Batch operations count once per key.
type Stats struct {
	enabled   bool
	memHits   xsync.Counter
	memMisses xsync.Counter
	l2Hits    xsync.Counter
	l2Misses  xsync.Counter
	sets      xsync.Counter
	deletes   xsync.Counter
}

func newStats(enabled bool) Stats {
	if !enabled {
		return Stats{}
	}
	return Stats{
		enabled:   true,
		memHits:   *xsync.NewCounter(),
		memMisses: *xsync.NewCounter(),
		l2Hits:    *xsync.NewCounter(),
		l2Misses:  *xsync.NewCounter(),
		sets:      *xsync.NewCounter(),
		deletes:   *xsync.NewCounter(),
	}
}

func (s *Stats) addMemHits(n int64) {
	if s.enabled {
		s.memHits.Add(n)
	}
}

func (s *Stats) addMemMisses(n int64) {
	if s.enabled {
		s.memMisses.Add(n)
	}
}

func (s *Stats) addL2Hits(n int64) {
	if s.enabled {
		s.l2Hits.Add(n)
	}
}

func (s *Stats) addL2Misses(n int64) {
	if s.enabled {
		s.l2Misses.Add(n)
	}
}

func (s *Stats) addSets(n int64) {
	if s.enabled {
		s.sets.Add(n)
	}
}

func (s *Stats) addDeletes(n int64) {
	if s.enabled {
		s.deletes.Add(n)
	}
}

// MemoryHits is the number of read keys served from memory.
func (s *Stats) MemoryHits() int64 { return s.memHits.Value() }

// MemoryMisses is the number of read keys that missed memory and stopped there, never reaching L2: reads on a cache
// without an L2, GetFromMemory, and calls that joined another key's in-flight load.
func (s *Stats) MemoryMisses() int64 { return s.memMisses.Value() }

// L2Hits is the number of read keys that missed memory and were found in L2. Always 0 without an L2.
func (s *Stats) L2Hits() int64 { return s.l2Hits.Value() }

// L2Misses is the number of read keys L2 was asked for and did not return, an L2 error included. Always 0 without
// an L2.
func (s *Stats) L2Misses() int64 { return s.l2Misses.Value() }

// L2Gets is the number of read keys forwarded to L2: L2Hits() + L2Misses().
func (s *Stats) L2Gets() int64 { return s.L2Hits() + s.L2Misses() }

// Hits is the number of read keys found in the cache: MemoryHits() + L2Hits().
func (s *Stats) Hits() int64 { return s.MemoryHits() + s.L2Hits() }

// Misses is the number of read keys found in neither level: MemoryMisses() + L2Misses().
func (s *Stats) Misses() int64 { return s.MemoryMisses() + s.L2Misses() }

// Sets is the number of values written into the cache: Set, BatchSet and stored loader results. L2-to-memory
// promotions are not counted - the value was already in the cache.
func (s *Stats) Sets() int64 { return s.sets.Value() }

// Deletes is the number of keys removed by Delete and BatchDelete.
func (s *Stats) Deletes() int64 { return s.deletes.Value() }

// Gets returns the total number of read keys: Hits() + Misses().
func (s *Stats) Gets() int64 { return s.Hits() + s.Misses() }

// HitRate returns Hits() / Gets(); 0 when nothing has been read yet.
func (s *Stats) HitRate() float64 {
	if gets := s.Gets(); gets > 0 {
		return float64(s.Hits()) / float64(gets)
	}
	return 0
}

// MemoryHitRate returns MemoryHits() / Gets(); 0 when nothing has been read yet.
func (s *Stats) MemoryHitRate() float64 {
	if gets := s.Gets(); gets > 0 {
		return float64(s.MemoryHits()) / float64(gets)
	}
	return 0
}

// L2HitRate returns L2Hits() / L2Gets(); 0 when nothing reached L2 yet.
func (s *Stats) L2HitRate() float64 {
	if gets := s.L2Gets(); gets > 0 {
		return float64(s.L2Hits()) / float64(gets)
	}
	return 0
}

// MissRate returns Misses() / Gets(); 0 when nothing has been read yet.
func (s *Stats) MissRate() float64 {
	if gets := s.Gets(); gets > 0 {
		return float64(s.Misses()) / float64(gets)
	}
	return 0
}

// Stats returns the cache's operation counters, or zero if collection was not enabled with WithStats.
func (c *Cache[K, V]) Stats() Stats {
	return c.stats
}
