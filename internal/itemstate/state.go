// Package itemstate holds the per-item bookkeeping primitives of the cache's first level: a state record per item
// (its key/value Entry next to an atomic meta word), a Pool that recycles those records slab-style, and the chunked
// FIFO queue of nodes that reference them.
package itemstate

// Layout of a state record's 64-bit meta word:
//
//	[dead:1][secondChance1:1][secondChance2:1][expireOff:29][gen:32]
//
// dead marks a tombstone the eviction queue skips; the two chance bits form a unary reference counter; expireOff is
// the expiration time in seconds since the cache epoch (0 = no TTL); gen counts the record's occupancies and is
// what lock-free readers validate their Entry snapshot against (see State.Snapshot).
//
// The unary counter is deliberate: increment is an idempotent OR and decrement an AND, and a saturated counter
// performs no writes at all, so hot keys never bounce the cache line between cores.
const (
	// Dead marks an item as a tombstone.
	Dead uint64 = 1 << 63

	SecondChance uint64 = 1 << 62
	ThirdChance  uint64 = 1 << 61
	// ChanceMask isolates the unary reference counter (states 00, 10 and 11).
	ChanceMask uint64 = SecondChance | ThirdChance

	ExpireShift        = 32
	ExpireMask  uint64 = (1<<29 - 1) << ExpireShift
	// ExpireMax is the maximum expiration offset (29 bits, ~17 years).
	ExpireMax = 1<<29 - 1

	// GenMask isolates the occupancy generation.
	GenMask uint64 = 1<<32 - 1
	// AliveGenMask checks "alive and still this generation" with one mask-and-compare.
	AliveGenMask = Dead | GenMask
)

// Entry is the key/value pair of one cached item. Lock-free readers never touch it directly - they take a whole
// copy through State.Snapshot and validate it against the record's generation.
type Entry[K comparable, V any] struct {
	Key   K
	Value V
}

// Gen returns the record's current occupancy generation.
func (s *State[K, V]) Gen() uint32 { return uint32(s.meta.Load()) }

// Load returns the current meta word.
func (s *State[K, V]) Load() uint64 { return s.meta.Load() }

// TouchWith sets the next bit of the reference counter, given an already-loaded meta word; a saturated counter
// writes nothing. A false positive on a reused record (Get racing eviction) just grants a stranger one extra chance.
func (s *State[K, V]) TouchWith(metaWord uint64) {
	switch {
	case metaWord&SecondChance == 0:
		s.meta.Or(SecondChance)
	case metaWord&ThirdChance == 0:
		s.meta.Or(ThirdChance)
	}
}

// RevokeChance withdraws one second chance. Called under the shard mutex.
func (s *State[K, V]) RevokeChance(metaWord uint64) {
	if metaWord&ThirdChance != 0 {
		s.meta.And(^ThirdChance)
		return
	}
	s.meta.And(^SecondChance)
}

// ResetChances clears the reference counter (S3-FIFO promotion from small to main). Called under the shard mutex.
func (s *State[K, V]) ResetChances() {
	s.meta.And(^ChanceMask)
}

// Kill marks the item as a tombstone (wait-free) and reports whether this call performed the alive -> dead
// transition - whoever kills the item accounts for its weight.
func (s *State[K, V]) Kill() bool {
	return s.meta.Or(Dead)&Dead == 0
}

// RefreshExpire extends the TTL, preserving the generation and reference counter. A race with a concurrent touch may
// lose one second chance - harmless.
func (s *State[K, V]) RefreshExpire(expireOff uint32) {
	metaWord := s.meta.Load()
	s.meta.Store(metaWord&^ExpireMask | uint64(expireOff)<<ExpireShift)
}

// TouchAndRefreshExpire extends the TTL and grants the touch in one CAS from the lock-free read path.
func (s *State[K, V]) TouchAndRefreshExpire(metaWord uint64, expireOff uint32) bool {
	newWord := metaWord&^ExpireMask | uint64(expireOff)<<ExpireShift
	switch {
	case metaWord&SecondChance == 0:
		newWord |= SecondChance
	case metaWord&ThirdChance == 0:
		newWord |= ThirdChance
	}
	return s.meta.CompareAndSwap(metaWord, newWord)
}

// Expired reports whether the TTL has elapsed for the given loaded meta word.
func Expired(metaWord uint64, nowOff uint32) bool {
	expireOff := uint32((metaWord & ExpireMask) >> ExpireShift)
	return expireOff != 0 && expireOff <= nowOff
}
