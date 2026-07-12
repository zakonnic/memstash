// Package itemstate holds the per-item bookkeeping primitives of the cache's first level: a State record per cached
// item (an immutable key/value box plus eviction metadata), a Pool that recycles those records slab-style, and the
// chunked FIFO queue of nodes that reference them. A memory hit costs one atomic metadata read and one atomic box
// load - no locks and no allocations.
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
// gen           - occupancy generation: bumped every time the state record is handed out to a new key. The lock-free
// read path identifies items by their immutable Entry box, so gen is not consulted on Get; it remains the cheap
// occupancy stamp visible in tests and debugging.
//
// The unary reference counter is a deliberate choice: increment is an idempotent OR (Go 1.23+ compiles
// atomic.Uint64.Or into a single LOCK OR instruction) and decrement is an AND. A saturated counter performs no
// writes at all, so hot keys never bounce the cache line between cores.
const (
	// Dead marks an item as a tombstone.
	Dead uint64 = 1 << 63

	SecondChance uint64 = 1 << 62
	ThirdChance  uint64 = 1 << 61
	// ChanceMask isolates the unary reference counter (with three states: 00, 10 and 11).
	ChanceMask uint64 = SecondChance | ThirdChance

	ExpireShift        = 32
	ExpireMask  uint64 = (1<<29 - 1) << ExpireShift
	// ExpireMax is the maximum expiration offset (29 bits, ~17 years).
	ExpireMax = 1<<29 - 1

	// GenMask isolates the occupancy generation.
	GenMask uint64 = 1<<32 - 1
	// AliveGenMask Dead|GenMask lets a single mask-and-compare check "alive and still this generation".
	AliveGenMask = Dead | GenMask
)

// Entry is the immutable key/value box of one cached item. It is allocated once per Set and never mutated after
// being published, which is what makes the lock-free read path safe for arbitrary key and value types: a reader
// either sees the whole box or a different (older/newer) whole box - never a torn mix. Overwriting a key swaps the
// record's box pointer; the record, its queue node and its table slot stay put.
type Entry[K comparable, V any] struct {
	Key   K
	Value V
}

// State is a cached item's bookkeeping record: an atomic pointer to the item's immutable Entry box plus eviction
// metadata. The record is a fixed 16 bytes for every key/value type, sits by value inside a pool chunk and is reused
// without allocations.
type State[K comparable, V any] struct {
	entry atomic.Pointer[Entry[K, V]]
	meta  atomic.Uint64
}

// Entry returns the record's current key/value box: nil only while the record rests on the pool freelist. Safe to
// call lock-free; the box itself is immutable.
func (s *State[K, V]) Entry() *Entry[K, V] { return s.entry.Load() }

// SetEntry publishes a new key/value box (an in-place overwrite of the item's value). Called only under the shard
// mutex; concurrent readers see either the old or the new box.
func (s *State[K, V]) SetEntry(e *Entry[K, V]) { s.entry.Store(e) }

// Gen returns the record's current occupancy generation.
func (s *State[K, V]) Gen() uint32 { return uint32(s.meta.Load()) }

// Load returns the current meta word. A single atomic load checks tombstone and expiration consistently.
func (s *State[K, V]) Load() uint64 { return s.meta.Load() }

// TouchWith sets the next bit of the unary reference counter. It takes an already-loaded meta word and does nothing
// once the counter is saturated. A false positive on a reused record is possible (Get racing with eviction) and is
// harmless: some other key gets one extra second chance, correctness is not affected.
func (s *State[K, V]) TouchWith(metaWord uint64) {
	switch {
	case metaWord&SecondChance == 0:
		s.meta.Or(SecondChance)
	case metaWord&ThirdChance == 0:
		s.meta.Or(ThirdChance)
	}
}

// RevokeChance withdraws a single second chance. Called only under the shard mutex.
func (s *State[K, V]) RevokeChance(metaWord uint64) {
	if metaWord&ThirdChance != 0 {
		s.meta.And(^ThirdChance)
		return
	}
	s.meta.And(^SecondChance)
}

// ResetChances clears the reference counter (used when promoting an item from small to main in S3-FIFO). Called only
// under the shard mutex.
func (s *State[K, V]) ResetChances() {
	s.meta.And(^ChanceMask)
}

// Kill marks the item as a tombstone; it is wait-free (a single LOCK OR instruction). It returns true if this very
// call performed the alive -> dead transition. Every kill runs under the shard mutex, so for a live item the result
// is always true; the return value is kept to document the "whoever kills it accounts for its weight" protocol.
func (s *State[K, V]) Kill() bool {
	return s.meta.Or(Dead)&Dead == 0
}

// RefreshExpire extends the item's TTL while preserving its generation and reference counter. A race with a
// concurrent touch may lose one second chance - that is harmless.
func (s *State[K, V]) RefreshExpire(expireOff uint32) {
	metaWord := s.meta.Load()
	s.meta.Store(metaWord&^ExpireMask | uint64(expireOff)<<ExpireShift)
}

// Expired reports whether the TTL has elapsed for the given loaded meta word.
func Expired(metaWord uint64, nowOff uint32) bool {
	expireOff := uint32((metaWord & ExpireMask) >> ExpireShift)
	return expireOff != 0 && expireOff <= nowOff
}
