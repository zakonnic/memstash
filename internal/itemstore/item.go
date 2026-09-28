package itemstore

import (
	"sync/atomic"
	"unsafe"
)

// Layout of an item's 64-bit meta word, high bit first:
//
//	[dead:1][secondChance:1][thirdChance:1][main:1][expireOff:18][hasNext:1][tag:13][disp:6][gen:22]
//
// An empty slot carries nothing but its gen. A never-occupied slot is all zero; a cleared one keeps counting, so no
// snapshot of a previous occupant validates against the next.
//
// The reference counter is unary: increment is an OR, decrement an AND, and a saturated counter writes nothing. So hot
// keys never bounce the cache line between cores.
const (
	// Dead marks a killed item whose slot has not been reclaimed yet.
	Dead uint64 = 1 << 63

	SecondChance uint64 = 1 << 62
	ThirdChance  uint64 = 1 << 61
	// ChanceMask isolates the unary reference counter (states 00, 10 and 11).
	ChanceMask uint64 = SecondChance | ThirdChance

	// InMainQueue is set while the item sits in eviction queue 1.
	InMainQueue uint64 = 1 << 60

	ExpireShift        = 42
	ExpireMask  uint64 = (1<<18 - 1) << ExpireShift
	// ExpireWrapMask wraps offsets on the 18-bit scale.
	ExpireWrapMask uint32 = 1<<18 - 1
	// ExpireMax is the maximum TTL in offset units: half the wrapping scale, so "behind now" stays unambiguous.
	ExpireMax = 1<<17 - 1

	// HasNext is the probe terminator. Clear means the slot is uncovered: no key homed at or before it lives past
	// it, so a probe that got here without a match may stop. Inserts set it on the slots a key flies over; Reclaim
	// recomputes it exactly over the run a deletion compacts.
	HasNext uint64 = 1 << 41

	TagShift        = 28
	TagMask  uint64 = (1<<13 - 1) << TagShift
	// TagOfZero is what TagOf yields for a hash whose tag bits are all zero: the field's top bit alone.
	TagOfZero uint64 = 1 << (TagShift + 12)
	// DeadOrTag matches a probe candidate in one compare: alive and carrying the probed tag.
	DeadOrTag = Dead | TagMask

	// DispShift and DispMask place the item's distance from its home slot, so a run compaction moves items without
	// rehashing them. DispMax stands for anything farther; then the distance comes from the key's hash.
	DispShift        = 22
	DispMax   uint32 = 1<<6 - 1
	DispMask  uint64 = uint64(DispMax) << DispShift

	// GenMask isolates the generation, which advances on every occupancy and every in-place value write. At 22 bits a
	// reader stalled mid-snapshot validates a stale word only if that one slot is rewritten 2M times meanwhile.
	GenMask uint64 = 1<<22 - 1
	// AliveGenMask checks "alive and still this generation" with one mask-and-compare.
	AliveGenMask = Dead | GenMask
)

// TagOf returns a key hash's tag, positioned for meta-word compares. It takes hash bits 14..26, disjoint from the low
// bits that pick the shard and the high bits that pick the home slot.
//
// A tag is never 0, so an empty slot never passes the candidate compare and probes need no empty test. A zero tag maps
// to TagOfZero, whose low bits are still the hash's, so BucketOf reads them back.
func TagOf(hash uint64) uint64 {
	tag := hash >> 14 << TagShift & TagMask
	if tag == 0 {
		tag = TagOfZero
	}
	return tag
}

// DispOf returns the item's distance from its home slot, or DispMax when it has to come from the key's hash.
func DispOf(metaWord uint64) uint32 { return uint32(metaWord>>DispShift) & DispMax }

// BucketOf returns the item's OverflowBucket, read back from its tag.
func BucketOf(metaWord uint64) uint32 { return uint32(metaWord>>TagShift) & (overflowBuckets - 1) }

func dispBits(disp uint32) uint64 { return uint64(min(disp, DispMax)) << DispShift }

// Empty reports whether the slot holds no item: never occupied, or cleared since.
func Empty(metaWord uint64) bool { return metaWord&^GenMask == 0 }

// Entry is the key/value pair of one cached item.
type Entry[K comparable, V any] struct {
	Key   K
	Value V
}

// Item is one cached item: an atomic meta word plus its Entry inline. It is also the item's hash-table slot, so a
// probe that finds it has the Entry right there, with no second lookup.
//
// Readers are lock-free through a seqlock on the meta word's generation: Snapshot copies the Entry and validates the
// copy, writers (under the shard mutex) advance the generation. Aligned words are single-copy atomic everywhere Go
// runs, so a snapshotted pointer is always whole - old or new, never a mix.
type Item[K comparable, V any] struct {
	meta  atomic.Uint64
	entry Entry[K, V]
}

// Entry returns the item's pair in place. Callers must hold the shard mutex; lock-free readers use Snapshot.
func (s *Item[K, V]) Entry() *Entry[K, V] { return &s.entry }

// SnapshotInto copies the item's Entry into dst and reports whether the copy is consistent with metaWord. False means
// a recycle or an overwrite raced the copy: dst must not be used, not even to dereference a pointer in it, and the
// caller reloads the meta word and starts over. The out-parameter keeps the hot path to one copy of the pair.
//
// An odd generation fails too. It is SetValue's write-in-progress mark: without the check, a reader entirely inside
// one write would see a steady word and accept a torn value.
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

// Publish fills an empty slot. The Entry lands before the meta word goes live, so a reader that sees the item alive
// finds the pair in place. HasNext is kept: the slot may lie on another key's probe chain.
func (s *Item[K, V]) Publish(entry Entry[K, V], tag uint64, expireOff, disp uint32) {
	s.entry = entry
	prev := s.meta.Load()
	storeMeta(&s.meta, prev&HasNext|uint64(expireOff)<<ExpireShift|tag|dispBits(disp)|liveGen((prev+2)&^1))
}

func (s *Item[K, V]) setMain(set bool) {
	if set {
		orMeta(&s.meta, InMainQueue)
		return
	}
	andMeta(&s.meta, ^InMainQueue)
}

// PublishMoved fills an empty slot with the item at src, as Publish does. The item keeps its tag, TTL, reference
// counter and queue bit.
func (s *Item[K, V]) PublishMoved(src *Item[K, V], disp uint32) {
	s.entry = src.entry
	prev := s.meta.Load()
	srcMeta := src.meta.Load()
	storeMeta(&s.meta, prev&HasNext|srcMeta&(ChanceMask|ExpireMask|TagMask|InMainQueue)|dispBits(disp)|
		liveGen((prev+2)&^1))
}

// liveGen wraps a generation into its field, skipping 0: gen 0 belongs to never-occupied slots. Probes don't depend
// on it, since a live word is non-zero through its tag.
func liveGen(gen uint64) uint64 {
	gen &= GenMask
	if gen == 0 {
		return 2
	}
	return gen
}

// MarkHasNext records that a key homed at or before this slot lives past it. A probe that reads the bit just before
// it appears behaves like a Get racing the Set.
func (s *Item[K, V]) MarkHasNext() { orMeta(&s.meta, HasNext) }

// Clear empties a dead slot. It zeroes the pair, then advances the generation once more, so no snapshot of the old
// occupant validates. No live key's probe chain may run through the slot: a probe that stops here must stop correctly.
func (s *Item[K, V]) Clear() {
	s.entry = Entry[K, V]{}
	prev := s.meta.Load()
	storeMeta(&s.meta, liveGen((prev+2)&^1))
}

// ClearHasNext retires a chain bit whose key is gone, on a slot proven uncovered. A reader that still sees the bit
// walks one slot too far, nothing worse.
func (s *Item[K, V]) ClearHasNext() { andMeta(&s.meta, ^HasNext) }

func (s *Item[K, V]) SetHasNext(set bool) {
	if set {
		orMeta(&s.meta, HasNext)
	} else {
		andMeta(&s.meta, ^HasNext)
	}
}

// SetValue overwrites the value in place. A value wider than a word, or made of several parts, is written between
// beginWrite and endWrite; a single scalar of up to a word is one store, so a reader gets the old or the new.
func (s *Item[K, V]) SetValue(value V) {
	// Constant per instantiation. Alignof < Sizeof when the value has several parts.
	if unsafe.Sizeof(value) > unsafe.Sizeof(uintptr(0)) || unsafe.Alignof(value) < unsafe.Sizeof(value) {
		s.beginWrite()
		s.entry.Value = value
		s.endWrite()
		return
	}
	s.entry.Value = value
}

// endWrite makes the generation even again; the store's release ordering keeps the value stores before it. A
// concurrent touch may lose its chance bit here. The reread must stay atomic: gc would forward a plain one from
// beginWrite's store, then delete that store as overwritten, and no reader would ever see the odd gen.
func (s *Item[K, V]) endWrite() {
	metaWord := s.meta.Load()
	storeMeta(&s.meta, metaWord&^GenMask|liveGen(metaWord+1))
}

// Gen returns the item's current occupancy generation.
func (s *Item[K, V]) Gen() uint32 { return uint32(s.meta.Load() & GenMask) }

// Metadata returns the current meta word.
func (s *Item[K, V]) Metadata() uint64 { return s.meta.Load() }

// TouchWith sets the next bit of the reference counter, given the meta word the caller loaded. A saturated counter
// writes nothing. It is a CAS rather than an OR, so a reader lagging behind a kill cannot stamp a bit onto a slot
// that has since been cleared or refilled; the touch is dropped instead.
func (s *Item[K, V]) TouchWith(metaWord uint64) {
	switch {
	case metaWord&SecondChance == 0:
		s.meta.CompareAndSwap(metaWord, metaWord|SecondChance)
	case metaWord&ThirdChance == 0:
		s.meta.CompareAndSwap(metaWord, metaWord|ThirdChance)
	}
}

// RevokeChance withdraws one second chance. Called under the shard mutex.
func (s *Item[K, V]) RevokeChance(metaWord uint64) {
	if metaWord&ThirdChance != 0 {
		andMeta(&s.meta, ^ThirdChance)
		return
	}
	andMeta(&s.meta, ^SecondChance)
}

// ResetChances clears the reference counter (S3-FIFO promotion from small to main). Called under the shard mutex.
func (s *Item[K, V]) ResetChances() {
	andMeta(&s.meta, ^ChanceMask)
}

// Kill marks the item dead and reports whether it was alive. FlatHashMap.Reclaim frees the slot.
func (s *Item[K, V]) Kill() bool {
	return orMeta(&s.meta, Dead)&Dead == 0
}

// RefreshExpire extends the TTL, preserving the generation and reference counter. A race with a concurrent touch may
// lose one second chance - harmless.
func (s *Item[K, V]) RefreshExpire(expireOff uint32) {
	metaWord := s.meta.Load()
	storeMeta(&s.meta, metaWord&^ExpireMask|uint64(expireOff)<<ExpireShift)
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
