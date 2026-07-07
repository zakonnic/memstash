// Package itemstate holds the per-item bookkeeping primitives of the cache's first level: a State record per cached
// item (key + eviction metadata), a Pool that recycles those records slab-style, and the chunked FIFO queue of nodes
// that reference them. A memory hit costs one atomic metadata read - no locks and no allocations.
package itemstate

import "sync/atomic"

// Layout of a state record's 64-bit meta word:
//
//	[dead:1][secondChance1:1][secondChance2:1][expireOff:29][gen:32]
//
// dead          - tombstone: the item was deleted or evicted, so the eviction queue skips it.
// secondChance1 - first bit of the "unary" reference counter (reading the item sets this bit, marking it as active).
// secondChance2 - second bit of the unary reference counter.
// expireOff     - expiration time in seconds since the cache epoch; 0 means "no TTL".
// gen           - occupancy generation: bumped every time the state record is handed out to a new key. The map keeps
// this generation next to the state pointer, so a single atomic load of meta gives Get a consistent "the record is
// still mine and alive" answer.
//
// Packing all four facts into one word is what makes the lock-free Get possible: splitting any of them (gen in
// particular) into a separate field would require two loads that could observe a record reused in between. The word
// is handled as a raw uint64 rather than a wrapper type to keep the hot path free of any abstraction cost.
//
// The unary reference counter is a deliberate choice: increment is an idempotent OR (Go 1.23+ compiles
// atomic.Uint64.Or into a single LOCK OR instruction) and decrement is an AND. A saturated counter performs no
// writes at all, so hot keys never bounce the cache line between cores.
const (
	// Dead marks an item as a tombstone.
	Dead uint64 = 1 << 63

	SecondChance1 uint64 = 1 << 62
	SecondChance2 uint64 = 1 << 61
	// ChanceMask isolates the unary reference counter (with three states: 00, 10 and 11).
	ChanceMask uint64 = SecondChance1 | SecondChance2

	ExpireShift        = 32
	ExpireMask  uint64 = (1<<29 - 1) << ExpireShift
	// ExpireMax is the maximum expiration offset (29 bits, ~17 years).
	ExpireMax = 1<<29 - 1
)

// State is a cached item's bookkeeping record: its key and eviction metadata. The value itself is not stored here -
// it lives in the owning cache's map, which keeps the record compact, lets it sit by value inside a pool chunk, and
// makes it reusable without allocations.
type State[K comparable] struct {
	Key  K
	meta atomic.Uint64
}

// Gen returns the record's current occupancy generation.
func (s *State[K]) Gen() uint32 { return uint32(s.meta.Load()) }

// Load returns the current meta word. A single atomic load checks generation, tombstone and expiration consistently.
func (s *State[K]) Load() uint64 { return s.meta.Load() }

// TouchWith sets the next bit of the unary reference counter. It takes an already-loaded meta word and does nothing
// once the counter is saturated. A false positive on a reused record is possible (Get racing with eviction) and is
// harmless: some other key gets one extra second chance, correctness is not affected.
func (s *State[K]) TouchWith(metaWord uint64) {
	switch {
	case metaWord&SecondChance1 == 0:
		s.meta.Or(SecondChance1)
	case metaWord&SecondChance2 == 0:
		s.meta.Or(SecondChance2)
	}
}

// RevokeChance withdraws a single second chance. Called only under the shard mutex.
func (s *State[K]) RevokeChance(metaWord uint64) {
	if metaWord&SecondChance2 != 0 {
		s.meta.And(^SecondChance2)
		return
	}
	s.meta.And(^SecondChance1)
}

// ResetChances clears the reference counter (used when promoting an item from small to main in S3-FIFO). Called only
// under the shard mutex.
func (s *State[K]) ResetChances() {
	s.meta.And(^ChanceMask)
}

// Kill marks the item as a tombstone; it is wait-free (a single LOCK OR instruction). It returns true if this very
// call performed the alive -> dead transition. Every kill runs under the shard mutex, so for a live item the result
// is always true; the return value is kept to document the "whoever kills it accounts for its weight" protocol.
func (s *State[K]) Kill() bool {
	return s.meta.Or(Dead)&Dead == 0
}

// RefreshExpire extends the item's TTL while preserving its generation and reference counter. A race with a
// concurrent touch may lose one second chance - that is harmless.
func (s *State[K]) RefreshExpire(expireOff uint32) {
	metaWord := s.meta.Load()
	s.meta.Store(metaWord&^ExpireMask | uint64(expireOff)<<ExpireShift)
}

// Expired reports whether the TTL has elapsed for the given loaded meta word.
func Expired(metaWord uint64, nowOff uint32) bool {
	expireOff := uint32((metaWord & ExpireMask) >> ExpireShift)
	return expireOff != 0 && expireOff <= nowOff
}
