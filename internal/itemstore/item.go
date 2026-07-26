// Package itemstore holds the first-level bookkeeping: the slot table (linear hash map) where slots are the item
// records (FlatHashMap), and a chunked FIFO queue of nodes that reference them by position.
package itemstore

import (
	"sync/atomic"
	"unsafe"
)

// Layout of a state record's 64-bit meta word:
//
//	[dead:1][secondChance:1][thirdChance:1][free:1][expireOff:18][tag:14][gen:28]
//
// dead marks a tombstone the eviction queue skips; free marks a dead slot whose queue node is already gone, so
// inserts may reuse it; the two chance bits form a unary reference counter; expireOff is the expiration time in
// ExpireResolution units on a wrapping scale (0 = no TTL); tag prefilters probe candidates without touching the key;
// gen counts the record's occupancies and lock-free readers validate their Entry snapshot against it (see Snapshot).
// A meta word of 0 is a never-occupied slot and terminates probes.
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

	// Free marks a dead slot no eviction queue references anymore: reusable by inserts.
	Free uint64 = 1 << 60

	ExpireShift        = 42
	ExpireMask  uint64 = (1<<18 - 1) << ExpireShift
	// ExpireWrapMask wraps offsets on the 18-bit scale.
	ExpireWrapMask uint32 = 1<<18 - 1
	// ExpireMax is the maximum TTL in offset units: half the wrapping scale, so "behind now" stays unambiguous.
	ExpireMax = 1<<17 - 1

	TagShift        = 28
	TagMask  uint64 = (1<<14 - 1) << TagShift
	// DeadOrTag matches a probe candidate in one compare: alive and carrying the probed tag.
	DeadOrTag = Dead | TagMask

	// GenMask isolates the occupancy generation - a cyclic counter incremented on every write.
	GenMask uint64 = 1<<28 - 1
	// AliveGenMask checks "alive and still this generation" with one mask-and-compare.
	AliveGenMask = Dead | GenMask
)

// TagOf extracts a key hash's slot tag, positioned for direct meta-word compares. Hash bits 14..27: disjoint from the
// low bits picking the shard and the high bits picking the home slot.
func TagOf(hash uint64) uint64 { return hash >> 14 << TagShift & TagMask }

// Entry is the key/value pair of one cached item.
type Entry[K comparable, V any] struct {
	Key   K
	Value V
}

// Item is a cached item's record: an atomic meta word plus its Entry inline. It is also the item's hash-table slot,
// so a probe that finds the record has its Entry in the same cache line.
//
// Readers are lock-free through a seqlock on the meta word's generation: Snapshot copies the Entry and validates the
// copy, writers (under the shard mutex) advance the generation. Aligned words are single-copy atomic everywhere Go
// runs, so a snapshotted pointer is always whole - old or new, never a mix.
type Item[K comparable, V any] struct {
	meta  atomic.Uint64
	entry Entry[K, V]
}

// Entry returns the record's pair in place. Callers must hold the shard mutex; lock-free readers use Snapshot.
func (s *Item[K, V]) Entry() *Entry[K, V] { return &s.entry }

// SnapshotInto copies the record's Entry into dst and reports whether it's consistent with metaWord.
// false means a recycle or overwrite raced the copy: reload meta word and retry, or stop if it shows a tombstone.
// On false, the copy must not be used - in particular, no pointer it carries may be dereferenced.
// The out-parameter avoids returning the Entry, keeping the hot path to a single copy of the pair.
//
// The odd generation rejected here is SetValue's write-in-progress marker: without it, a reader living entirely
// inside one write window would validate against a stable-looking word and accept a torn value.
func (s *Item[K, V]) SnapshotInto(dst *Entry[K, V], metaWord uint64) bool {
	s.snapshotEntryInto(dst) // platform-specific, seats under build flags
	return (s.meta.Load()^metaWord)&AliveGenMask == 0 && metaWord&1 == 0
}

// Snapshot is SnapshotInto returning the copy by value.
func (s *Item[K, V]) Snapshot(metaWord uint64) (Entry[K, V], bool) {
	var entry Entry[K, V]
	ok := s.SnapshotInto(&entry, metaWord)
	return entry, ok
}

// Publish fills an empty or free slot with a fresh occupancy: the Entry lands strictly before the meta word goes
// live, so a reader that sees the record alive always finds the pair in place. Called under the shard mutex.
func (s *Item[K, V]) Publish(entry Entry[K, V], tag uint64, expireOff uint32) {
	s.entry = entry
	gen := (s.meta.Load() + 2) & GenMask &^ 1
	s.meta.Store(uint64(expireOff)<<ExpireShift | tag | gen)
}

// MakeFree hands a dead slot back to inserts once its queue node is dropped, zeroing the Entry so the key and value
// do not outlive the item. Called under the shard mutex; the record is already a tombstone, so a stale snapshot of
// the mid-zero state fails validation.
func (s *Item[K, V]) MakeFree() {
	s.entry = Entry[K, V]{}
	s.meta.Or(Free)
}

// Reusable reports whether an insert may claim this slot: dead and released by its queue.
func Reusable(metaWord uint64) bool { return metaWord&(Dead|Free) == Dead|Free }

// SetValue overwrites the value in place (the key stays). Called under the shard mutex. Word-sized values skip the
// bracketing: the Go memory model forbids tearing reads up to one machine word, so a reader gets the old or the new.
func (s *Item[K, V]) SetValue(value V) {
	// unsafe.Sizeof is a compile-time constant; the branch disappears.
	if unsafe.Sizeof(value) > 8 {
		// protects against concurrent reads during writes with && metaWord&1
		s.beginWrite()
		s.entry.Value = value
		s.endWrite()
		return
	}
	s.entry.Value = value
}

// beginWrite makes the generation odd to signal a write in progress.
func (s *Item[K, V]) beginWrite() {
	// The retry also keeps a concurrent touch from being lost.
	for {
		metaWord := s.meta.Load()
		// readers must see the odd marker before any bytes of the new value - no Store to prevent reordering.
		if s.meta.CompareAndSwap(metaWord, metaWord&^GenMask|(metaWord+1)&GenMask) {
			return
		}
	}
}

// endWrite makes the generation even again; the store's release ordering keeps the value stores before it. A
// concurrent touch may lose its chance bit here - harmless, as everywhere else.
func (s *Item[K, V]) endWrite() {
	metaWord := s.meta.Load()
	s.meta.Store(metaWord&^GenMask | (metaWord+1)&GenMask)
}

// Gen returns the record's current occupancy generation.
func (s *Item[K, V]) Gen() uint32 { return uint32(s.meta.Load() & GenMask) }

// Load returns the current meta word.
func (s *Item[K, V]) Load() uint64 { return s.meta.Load() }

// TouchWith sets the next bit of the reference counter, given an already-loaded meta word; a saturated counter
// writes nothing. A false positive on a reused record (Get racing eviction) just grants a stranger one extra chance.
func (s *Item[K, V]) TouchWith(metaWord uint64) {
	switch {
	case metaWord&SecondChance == 0:
		s.meta.Or(SecondChance)
	case metaWord&ThirdChance == 0:
		s.meta.Or(ThirdChance)
	}
}

// RevokeChance withdraws one second chance. Called under the shard mutex.
func (s *Item[K, V]) RevokeChance(metaWord uint64) {
	if metaWord&ThirdChance != 0 {
		s.meta.And(^ThirdChance)
		return
	}
	s.meta.And(^SecondChance)
}

// ResetChances clears the reference counter (S3-FIFO promotion from small to main). Called under the shard mutex.
func (s *Item[K, V]) ResetChances() {
	s.meta.And(^ChanceMask)
}

// Kill marks the item as a tombstone (wait-free) and reports whether this call performed the alive -> dead
// transition - whoever kills the item accounts for its weight.
func (s *Item[K, V]) Kill() bool {
	return s.meta.Or(Dead)&Dead == 0
}

// RefreshExpire extends the TTL, preserving the generation and reference counter. A race with a concurrent touch may
// lose one second chance - harmless.
func (s *Item[K, V]) RefreshExpire(expireOff uint32) {
	metaWord := s.meta.Load()
	s.meta.Store(metaWord&^ExpireMask | uint64(expireOff)<<ExpireShift)
}

// TouchAndRefreshExpire extends the TTL and grants the touch in one CAS from the lock-free read path.
func (s *Item[K, V]) TouchAndRefreshExpire(metaWord uint64, expireOff uint32) bool {
	newWord := metaWord&^ExpireMask | uint64(expireOff)<<ExpireShift
	switch {
	case metaWord&SecondChance == 0:
		newWord |= SecondChance
	case metaWord&ThirdChance == 0:
		newWord |= ThirdChance
	}
	return s.meta.CompareAndSwap(metaWord, newWord)
}

// Expired reports whether the TTL has elapsed for the given loaded meta word. Offsets live on a wrapping scale:
// "expired" is a zero or backward distance from nowOff, which is why TTLs are capped at half the scale (ExpireMax).
func Expired(metaWord uint64, nowOff uint32) bool {
	expireOff := uint32(metaWord & ExpireMask >> ExpireShift)
	if expireOff == 0 {
		return false
	}
	diff := (expireOff - nowOff) & ExpireWrapMask
	return diff == 0 || diff > ExpireMax
}
