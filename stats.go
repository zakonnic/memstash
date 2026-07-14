package memstash

import "github.com/puzpuzpuz/xsync/v3"

// Stats is a snapshot of the cache's operation counters (see Cache.Stats). Batch operations count once per key.
type Stats struct {
	enabled bool
	hits    xsync.Counter
	misses  xsync.Counter
	sets    xsync.Counter
	deletes xsync.Counter
}

func newStats(enabled bool) Stats {
	if !enabled {
		return Stats{}
	}
	return Stats{
		enabled: true,
		hits:    *xsync.NewCounter(),
		misses:  *xsync.NewCounter(),
		sets:    *xsync.NewCounter(),
		deletes: *xsync.NewCounter(),
	}
}

func (s *Stats) incHits() {
	if s.enabled {
		s.hits.Add(1)
	}
}

func (s *Stats) addHits(n int64) {
	if s.enabled {
		s.hits.Add(n)
	}
}

func (s *Stats) incMisses() {
	if s.enabled {
		s.misses.Add(1)
	}
}

func (s *Stats) addMisses(n int64) {
	if s.enabled {
		s.misses.Add(n)
	}
}

func (s *Stats) incSets() {
	if s.enabled {
		s.sets.Add(1)
	}
}

func (s *Stats) addSets(n int64) {
	if s.enabled {
		s.sets.Add(n)
	}
}

func (s *Stats) incDeletes() {
	if s.enabled {
		s.deletes.Add(1)
	}
}

func (s *Stats) addDeletes(n int64) {
	if s.enabled {
		s.deletes.Add(n)
	}
}

// Hits is the number of read keys found in the cache (in memory or in L2).
func (s *Stats) Hits() int64 { return s.hits.Value() }

// Misses is the number of read keys found in neither level (absent, resolved by a loader, or failed with an error).
func (s *Stats) Misses() int64 { return s.misses.Value() }

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
