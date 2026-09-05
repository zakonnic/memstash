package itemstore

import (
	"sync/atomic"
	"unsafe"
)

// groupShift sizes the overflow-count groups at 8 slots: home clusters at the working load factor rarely outgrow one
// group, so a zero count usually proves absence right at the group edge.
const groupShift = 3

// chunkShift splits the slot space into chunks of 64Ki slots - a few MiB each, whatever the item type. A big table is
// then a handful of those instead of one multi-gigabyte block the allocator has to find contiguous space for.
const (
	chunkShift = 16
	chunkSlots = 1 << chunkShift
	chunkMask  = chunkSlots - 1
)

// MaxItems caps the item count: 5/6 (the rebuild load factor) of the 2^32 slot positions.
const MaxItems = int64(1) << 32 * 5 / 6

// FlatHashMap is one shard's memory storage: an open-addressing (https://en.wikipedia.org/wiki/Linear_probing) linear
// hash table whose slots are the items themselves, so a probe that lands has meta, key and value right there, with no
// second lookup. Queue nodes address items by slot position; a rebuild moves items and re-links the queues.
//
// No buckets: the hash picks the starting slot, and a taken slot just means stepping forward with pos++ until the key
// or an empty slot turns up. The 5/6 fill cap keeps the walk short - ~2 slots on a hit, 2 to 5 on a miss.
// Readers probe lock-free against atomically published meta words; all mutations happen under the shard mutex.
// A rebuild swaps the whole table: readers mid-probe finish on the superseded one and may miss a write that landed
// after the swap - indistinguishable from the Get racing the Set.
type FlatHashMap[K comparable, V any] struct {
	short     []Item[K, V]              // the whole table while it fits one chunk - most tables do
	chunks    []*[chunkSlots]Item[K, V] // past that: the slot's high bits pick the chunk, the low ones the slot in it
	overflows *atomic.Uint32            // per group: how many keys homed in it live past its edge (SwissTable/f14)
	size      uint32                    // slot count, as TableSlots and GrowSlots size it
}

// NewFlatHashMap allocates a table of itemCount slots: a whole number of chunks, or a power of two below one chunk,
// where the table is one flat run of exactly that many slots and a small shard costs no more than it used to.
func NewFlatHashMap[K comparable, V any](itemCount int) *FlatHashMap[K, V] {
	t := &FlatHashMap[K, V]{size: uint32(itemCount)}
	if itemCount <= chunkSlots {
		t.short = make([]Item[K, V], itemCount)
	} else {
		t.chunks = make([]*[chunkSlots]Item[K, V], itemCount>>chunkShift)
		for i := range t.chunks {
			t.chunks[i] = new([chunkSlots]Item[K, V])
		}
	}
	over := make([]atomic.Uint32, itemCount>>groupShift)
	t.overflows = &over[0]
	return t
}

// At resolves a probe position into its item. A probe walks forward from a slot index, so pos stays below twice the
// slot count and one subtraction brings it back in range.
func (t *FlatHashMap[K, V]) At(pos uint32) *Item[K, V] {
	if pos >= t.size {
		pos -= t.size
	}
	if t.size <= chunkSlots { // the slot count says which of the two layouts is in use
		return &t.short[pos]
	}
	return &t.chunks[pos>>chunkShift][pos&chunkMask]
}

// Home returns a key hash's home slot: the high 32 hash bits scaled into the slot range, so it stays independent of
// the tag and of the shard bits. Multiply-shift rather than a mask, because the slot count is not a power of two.
func (t *FlatHashMap[K, V]) Home(hash uint64) uint32 {
	return uint32((hash >> 32) * uint64(t.size) >> 32)
}

// Wrap normalizes a probe position into a slot index.
func (t *FlatHashMap[K, V]) Wrap(pos uint32) uint32 {
	if pos >= t.size {
		pos -= t.size
	}
	return pos
}

// Dist is the number of steps a probe takes from one slot to another, the wrap included.
func (t *FlatHashMap[K, V]) Dist(from, to uint32) uint32 {
	if to >= from {
		return to - from
	}
	return to + t.size - from
}

// Covers reports whether the probe chain of a key homed at home, living at pos, runs through slot q.
func (t *FlatHashMap[K, V]) Covers(home, pos, q uint32) bool {
	return t.Dist(home, q) < t.Dist(home, pos)
}

// A group's overflow word is 8 counters of 4 bits, picked by the key's tag, so a probe asks whether a key with ITS
// tag left the group rather than whether any key did. Same word, same 4 bytes, but it turns 43% of misses that must
// walk past the group edge into 9%. A counter sticks at 15 instead of wrapping into its neighbour; measured peak is
// 5, and a stuck counter only makes the group over-walk that one tag.
const (
	overflowBuckets = 8
	overflowWidth   = 4
	overflowMax     = uint32(1)<<overflowWidth - 1
)

// OverflowBucket picks a key hash's overflow counter: the tag's low three bits.
func OverflowBucket(hash uint64) uint32 { return uint32(hash>>14) & (overflowBuckets - 1) }

// OverflowedTag reports whether any key homed in home's group and carrying bucket's tag lives past the group's edge.
// False means a probe that scanned the group without a match may stop: the key is absent.
func (t *FlatHashMap[K, V]) OverflowedTag(home, bucket uint32) bool {
	return t.overflowAt(home).Load()>>(bucket*overflowWidth)&overflowMax != 0
}

// NoteDisplaced counts a key that settled outside its home group, under the counter its tag picks.
func (t *FlatHashMap[K, V]) NoteDisplaced(keyHash uint64, pos uint32) {
	home := t.Home(keyHash)
	if home>>groupShift == t.Wrap(pos)>>groupShift {
		return
	}
	word, shift := t.overflowAt(home), OverflowBucket(keyHash)*overflowWidth
	if cur := word.Load(); cur>>shift&overflowMax != overflowMax {
		word.Store(cur + 1<<shift)
	}
}

// ForgetDisplaced undoes NoteDisplaced when the key at pos dies.
func (t *FlatHashMap[K, V]) ForgetDisplaced(keyHash uint64, pos uint32) {
	home := t.Home(keyHash)
	if home>>groupShift == t.Wrap(pos)>>groupShift {
		return
	}
	word, shift := t.overflowAt(home), OverflowBucket(keyHash)*overflowWidth
	if cur := word.Load(); cur>>shift&overflowMax != overflowMax {
		word.Store(cur - 1<<shift)
	}
}

// overflowAt takes a home that Home already reduced into range.
func (t *FlatHashMap[K, V]) overflowAt(home uint32) *atomic.Uint32 {
	return (*atomic.Uint32)(unsafe.Add(unsafe.Pointer(t.overflows), uintptr(home>>groupShift)*4))
}

// MarkChain sets HasNext on every slot between a new key's home and the slot it landed in. Most of those already
// carry the bit, so it reads first and writes only where the bit is missing.
func (t *FlatHashMap[K, V]) MarkChain(home, placed uint32) {
	for d, steps := uint32(0), t.Dist(home, placed); d < steps; d++ {
		if item := t.At(home + d); item.Metadata()&HasNext == 0 {
			item.MarkHasNext()
		}
	}
}

// cascadeLimit caps the walk left of a freed slot. The walk stops on its own within a slot or two; the cap only
// keeps a table that is almost entirely tombstones from turning one free into a long scan.
const cascadeLimit = 64

// TableSlots rounds a wanted slot count up to a size a table can take: whole chunks once it needs more than one,
// and whole overflow groups below that, since every group owns a counter.
func TableSlots(want int) int {
	const groupRound = 1<<groupShift - 1
	if want <= chunkSlots {
		return (want + groupRound) &^ groupRound
	}
	return (want + chunkMask) &^ chunkMask
}

// GrowSlots is the next size up for a table that ran out of room: a quarter more, rounded down to whole chunks and
// then one chunk further, so the ladder past the first chunk runs 1, 2, 3, 4, 6, 8, 11... Below three quarters of a
// chunk the table is small enough that doubling costs nothing worth saving.
func GrowSlots(size int) int {
	switch {
	case size < chunkSlots*3/4:
		return size * 2
	case size < chunkSlots:
		return chunkSlots
	default:
		return (size*5/4/chunkSlots + 1) * chunkSlots
	}
}

// Release hands a dead slot back once its queue node is gone. When no live key's probe chain runs through it the slot
// goes back to a zero meta word instead of staying a tombstone, and so do the slots to its left that can no longer be
// covered. Returns how many slots it reclaimed, so the shard can drop them from its tombstone count.
//
// uncovered says the caller has established that nothing homed at or before idx lives past it. Everything from there
// needs no further test: if a slot is uncovered and holds no live key, its neighbour to the left is uncovered too,
// because the only key that could have covered the neighbour is the one that lived in the slot itself. So the walk
// stops at the first live item - whose stale bit it still clears - and at the first slot that is already empty.
// Called under the shard mutex.
func (t *FlatHashMap[K, V]) Release(idx uint32, uncovered bool) int {
	item := t.At(idx)
	if !uncovered {
		item.MakeFree()
		return 0
	}
	item.Clear()
	freed, pos := 1, idx
	for back := 0; back < cascadeLimit; back++ {
		if pos == 0 {
			pos = t.size
		}
		pos--
		item = t.At(pos)
		metaWord := item.Metadata()
		if metaWord == 0 {
			break // already empty and its bit is clear: nothing left of here can be stale through it
		}
		if metaWord&HasNext != 0 {
			item.ClearHasNext()
		}
		if metaWord&Dead == 0 {
			break // a live key: its bit is fixed, but it may still cover what lies further left
		}
		if Reusable(metaWord) {
			item.Clear()
			freed++
		}
	}
	return freed
}

// InsertFresh copies a live item into a table nobody reads yet (rebuilds) and returns its slot index.
func (t *FlatHashMap[K, V]) InsertFresh(keyHash uint64, src *Item[K, V]) uint32 {
	home := t.Home(keyHash)
	for pos := home; ; pos++ {
		dst := t.At(pos)
		if dst.meta.Load() == 0 {
			dst.entry = src.entry
			// The chains are the old table's; this one gets its own from MarkChain.
			dst.meta.Store(src.meta.Load() &^ HasNext)
			t.NoteDisplaced(keyHash, pos)
			t.MarkChain(home, t.Wrap(pos))
			return t.Wrap(pos)
		}
	}
}

func (t *FlatHashMap[K, V]) Len() int { return int(t.size) }

func (t *FlatHashMap[K, V]) Bytes() int64 {
	length := int64(t.Len())
	chunkPtrs := int64(len(t.chunks)) * int64(unsafe.Sizeof(uintptr(0)))
	return int64(unsafe.Sizeof(*t)) + chunkPtrs + length*int64(unsafe.Sizeof(Item[K, V]{})) + (length>>groupShift)*4
}

// StorageProxy is a shard's stable handle to its current FlatHashMap: rebuilds swap the table underneath while the shard's
// policy keeps the ref. Implements the public ItemStates resolver.
type StorageProxy[K comparable, V any] struct {
	storage atomic.Pointer[FlatHashMap[K, V]]
}

func (r *StorageProxy[K, V]) GetStorage() *FlatHashMap[K, V] { return r.storage.Load() }
func (r *StorageProxy[K, V]) Store(t *FlatHashMap[K, V])     { r.storage.Store(t) }

// At resolves a queue-node index into its item via the current table.
func (r *StorageProxy[K, V]) At(idx uint32) *Item[K, V] { return r.storage.Load().At(idx) }
