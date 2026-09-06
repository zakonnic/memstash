package itemstore

import (
	"hash/maphash"
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

// MaxItems caps the item count: 5/6 (the default growth load factor) of the 2^32 slot positions.
const MaxItems = int64(1) << 32 * 5 / 6

// FlatHashMap is one shard's memory storage: an open-addressing (https://en.wikipedia.org/wiki/Linear_probing) linear
// hash table whose slots are the items themselves, so a probe that lands has meta, key and value right there, with no
// second lookup. Queue nodes address items by key hash (see QNode) and are resolved with Find, so items are free to
// move: a deletion compacts its run in place (Reclaim) and the table never holds a tombstone.
//
// No buckets: the hash picks the starting slot, and a taken slot just means stepping forward with pos++ until the key
// or an empty slot turns up. The load factor keeps the walk short - ~2.5 slots on a hit at 0.75.
// Readers probe lock-free against atomically published meta words; all mutations happen under the shard mutex.
// A growth swaps the whole table: readers mid-probe finish on the superseded one and may miss a write that landed
// after the swap - indistinguishable from the Get racing the Set.
type FlatHashMap[K comparable, V any] struct {
	short     []Item[K, V]              // the whole table while it fits one chunk - most tables do
	chunks    []*[chunkSlots]Item[K, V] // past that: the slot's high bits pick the chunk, the low ones the slot in it
	overflows *atomic.Uint32            // per group: how many keys homed in it live past its edge (SwissTable/f14)
	seed      maphash.Seed              // the cache's key hash seed: Find and Reclaim rehash the keys they meet
	size      uint32                    // slot count, as TableSlots and GrowSlots size it
}

// NewFlatHashMap allocates a table of itemCount slots: a whole number of chunks, or a power of two below one chunk,
// where the table is one flat run of exactly that many slots and a small shard costs no more than it used to. seed is
// the seed the cache hashes keys with.
func NewFlatHashMap[K comparable, V any](itemCount int, seed maphash.Seed) *FlatHashMap[K, V] {
	t := &FlatHashMap[K, V]{size: uint32(itemCount), seed: seed}
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
	return t.homeOfHigh(uint32(hash >> 32))
}

// homeOfHigh is Home for the high hash word alone - what a queue node carries.
func (t *FlatHashMap[K, V]) homeOfHigh(high uint32) uint32 {
	return uint32(uint64(high) * uint64(t.size) >> 32)
}

// hashOf rehashes a slot's key with the cache's seed. Called under the shard mutex.
func (t *FlatHashMap[K, V]) hashOf(key K) uint64 { return maphash.Comparable(t.seed, key) }

// Wrap normalizes a probe position into a slot index.
func (t *FlatHashMap[K, V]) Wrap(pos uint32) uint32 {
	if pos >= t.size {
		pos -= t.size
	}
	return pos
}

// back steps a slot index back by n, which must be less than the slot count, wrapping at the table's start.
func (t *FlatHashMap[K, V]) back(pos, n uint32) uint32 {
	if pos < n {
		return pos + t.size - n
	}
	return pos - n
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
// walk past the group edge into 9%. A counter sticks at 15 instead of wrapping into its neighbor; measured peak is
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
	t.NoteDisplacedAt(t.Home(keyHash), pos, OverflowBucket(keyHash))
}

// NoteDisplacedAt is NoteDisplaced for a caller that already has the key's home and bucket. It loads and stores
// rather than adds: the shard mutex excludes other writers, and a reader only needs the word to land whole.
func (t *FlatHashMap[K, V]) NoteDisplacedAt(home, pos, bucket uint32) {
	if home>>groupShift == t.Wrap(pos)>>groupShift {
		return
	}
	word, shift := t.overflowAt(home), bucket*overflowWidth
	if cur := word.Load(); cur>>shift&overflowMax != overflowMax {
		word.Store(cur + 1<<shift)
	}
}

// ForgetDisplaced undoes NoteDisplaced when the key at pos dies or moves.
func (t *FlatHashMap[K, V]) ForgetDisplaced(keyHash uint64, pos uint32) {
	t.ForgetDisplacedAt(t.Home(keyHash), pos, OverflowBucket(keyHash))
}

// ForgetDisplacedAt is ForgetDisplaced for a caller that already has the key's home and bucket.
func (t *FlatHashMap[K, V]) ForgetDisplacedAt(home, pos, bucket uint32) {
	if home>>groupShift == t.Wrap(pos)>>groupShift {
		return
	}
	word, shift := t.overflowAt(home), bucket*overflowWidth
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

// Find resolves a queue node to the item it was enqueued for: the item and its current slot. false means the key has
// left the cache since. The walk is the reader's probe, run under the shard mutex, plus one rehash of every candidate
// to confirm the node's 40 identity bits - a false candidate is a rehash every ~256 probes.
func (t *FlatHashMap[K, V]) Find(node QNode) (*Item[K, V], uint32, bool) {
	return t.resolve(node, true)
}

// Locate is Find without the rehash: the first live item on the node's probe path carrying its tag byte, which is the
// node's item, or once in ~256 probes a neighbor sharing the byte. For read-only questions where a neighbor costs
// nothing - a sweep asking whether the node still resolves, an admission filter peeking at main's head. A policy
// that writes to the item it examines (a reference counter is written on every eviction step) resolves with Find:
// writing to a neighbor would cost that neighbor its standing. Called under the shard mutex.
func (t *FlatHashMap[K, V]) Locate(node QNode) (*Item[K, V], uint32, bool) {
	return t.resolve(node, false)
}

func (t *FlatHashMap[K, V]) resolve(node QNode, exact bool) (*Item[K, V], uint32, bool) {
	home := t.homeOfHigh(node.home)
	tagged := uint64(node.tag())<<TagShift | TagForced
	bucket := uint32(node.tag()) & (overflowBuckets - 1)
	groupEnd := (home | 7) + 1
	for pos := home; ; pos++ {
		if pos == groupEnd && !t.OverflowedTag(home, bucket) {
			return nil, 0, false
		}
		item := t.At(pos)
		metaWord := item.Metadata()
		if metaWord&TagMatchMask == tagged {
			if !exact {
				return item, t.Wrap(pos), true
			}
			if hash := t.hashOf(item.Entry().Key); uint32(hash>>32) == node.home && uint8(hash>>14) == node.tag() {
				return item, t.Wrap(pos), true
			}
		}
		if metaWord&HasNext == 0 {
			return nil, 0, false
		}
	}
}

// ShiftEntry is Reclaim's record of one live slot in the run it compacts: where the slot is and where its key is
// homed, both as offsets from the reclaimed slot so the table wrap never enters the arithmetic. homeOff is negative
// when the home lies before the reclaimed slot.
type ShiftEntry struct{ off, homeOff int64 }

// Reclaim closes the hole a kill left at idx (the item there is Dead, its weight and overflow count already
// accounted). It is backward-shift deletion (Knuth, TAOCP 6.4, Algorithm R) with one twist: of the items to the
// right whose probe chains run through the hole, the farthest one slides into it - not the nearest. Its old slot is
// the hole now, and so on until no item to the right can cover the hole; that slot is cleared. The occupied slots come
// out the same either way (linear probing's occupied set does not depend on order), but the farthest item is the most
// displaced one, so this halves the moves per deletion (~1 instead of ~2 at load 0.75) and trims the long chains.
// The run walked is then given exact HasNext bits, which also repairs the stale ones earlier churn left behind. The
// table ends in the state the live keys alone would produce: never a tombstone.
//
// Called under the shard mutex. The moves - and only they - run inside a bracket on moves (odd while open): a reader
// whose probe overlapped it and missed re-probes, since a key sliding toward its home can cross a probe. The scan and
// the bit repass stay outside, so the bracket is a few tens of nanoseconds, and an uncovered hole opens none. Every
// moved item is live in its new slot before its old one is killed, so a reader finds it at one or the other at every
// instant. scratch is the caller's buffer, reused across calls.
func (t *FlatHashMap[K, V]) Reclaim(idx uint32, scratch *[]ShiftEntry, moves *atomic.Uint32) {
	if t.At(idx).Metadata()&HasNext == 0 {
		t.Release(idx) // uncovered: nothing to move
		return
	}
	// The run: every live slot from the hole to the first slot nothing is chained past, with its key's home.
	entries := (*scratch)[:0]
	end := int64(0)
	for d := int64(1); ; d++ {
		if d >= int64(t.size) {
			end = d // the whole table is one run: nothing left to examine
			break
		}
		pos := idx + uint32(d)
		item := t.At(pos)
		metaWord := item.Metadata()
		if Empty(metaWord) {
			end = d
			break
		}
		if metaWord&Dead == 0 {
			disp := DispOf(metaWord)
			if disp == DispMax { // saturated: ask the hash
				disp = t.Dist(t.Home(t.hashOf(item.Entry().Key)), t.Wrap(pos))
			}
			entries = append(entries, ShiftEntry{off: d, homeOff: d - int64(disp)})
		}
		if metaWord&HasNext == 0 {
			end = d // nothing further can cover the hole
			break
		}
	}
	*scratch = entries

	// Fill the hole with the farthest item whose chain runs through it, then the hole it leaves, until none does.
	hole, moved := int64(0), false
	for {
		pick := -1
		for i := len(entries) - 1; i >= 0 && entries[i].off > hole; i-- {
			if entries[i].homeOff <= hole {
				pick = i
				break
			}
		}
		if pick < 0 {
			break
		}
		if !moved {
			moves.Add(1)
			moved = true
		}
		src, dst := t.At(idx+uint32(entries[pick].off)), t.At(idx+uint32(hole))
		disp := uint32(entries[pick].off - entries[pick].homeOff)
		itemHome := t.back(t.Wrap(idx+uint32(entries[pick].off)), disp)
		bucket := BucketOf(src.Metadata())
		// Counted at the new slot before it is forgotten at the old one, so a reader never sees the group
		// under-counted.
		dst.PublishMoved(src, uint32(hole-entries[pick].homeOff))
		t.NoteDisplacedAt(itemHome, idx+uint32(hole), bucket)
		t.ForgetDisplacedAt(itemHome, idx+uint32(entries[pick].off), bucket)
		src.Kill()
		hole, entries[pick].off = entries[pick].off, hole
	}
	if moved {
		moves.Add(1)
	}

	// Back to slot order. Only the moved entries are out of place, so this is a few swaps.
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0 && entries[j].off < entries[j-1].off; j-- {
			entries[j], entries[j-1] = entries[j-1], entries[j]
		}
	}
	// Exact chain bits over [idx, idx+end): a slot's bit is earned by an item beyond it homed at or before it. Every
	// such item lies in the run, since one past end would contradict the clear bit that ended the scan.
	minHome, i := int64(1)<<62, len(entries)-1
	for s := end - 1; s >= 0; s-- {
		for ; i >= 0 && entries[i].off > s; i-- {
			minHome = min(minHome, entries[i].homeOff)
		}
		slot := t.At(idx + uint32(s))
		if want := minHome <= s; (slot.Metadata()&HasNext != 0) != want {
			slot.SetHasNext(want)
		}
	}
	t.Release(t.Wrap(idx + uint32(hole)))
}

// cascadeLimit caps the walk left of a freed slot. The walk stops on its own within a slot or two; the cap only
// keeps a run of stale chain bits from turning one release into a long scan.
const cascadeLimit = 64

// Release empties an uncovered dead slot and retires the chain bits to its left that nothing can earn any more. If a
// slot is uncovered and holds no live key, its left neighbor is uncovered too: the only key that could cover the
// neighbor lived in the slot itself. So the walk goes left until a live item, whose stale bit it still clears, or an
// empty slot.
func (t *FlatHashMap[K, V]) Release(idx uint32) {
	t.At(idx).Clear()
	pos := idx
	for back := 0; back < cascadeLimit; back++ {
		if pos == 0 {
			pos = t.size
		}
		pos--
		item := t.At(pos)
		metaWord := item.Metadata()
		if Empty(metaWord) {
			break // already empty and its bit is clear: nothing left of here can be stale through it
		}
		if metaWord&HasNext != 0 {
			item.ClearHasNext()
		}
		if metaWord&Dead == 0 {
			break // a live key: its bit is fixed, but it may still cover what lies further left
		}
		item.Clear()
	}
}

// InsertFresh copies a live item into a table nobody reads yet (growth) and returns its slot index.
func (t *FlatHashMap[K, V]) InsertFresh(keyHash uint64, src *Item[K, V]) uint32 {
	home := t.Home(keyHash)
	for pos := home; ; pos++ {
		dst := t.At(pos)
		if Empty(dst.meta.Load()) {
			dst.entry = src.entry
			// The chains and the distances are the old table's; this one gets its own from MarkChain and here.
			dst.meta.Store(src.meta.Load()&^(HasNext|DispMask) | dispBits(t.Dist(home, t.Wrap(pos))))
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

// StorageProxy is a shard's stable handle to its current FlatHashMap: growth swaps the table underneath while the
// shard's policy keeps the ref. Implements the public Items resolver.
type StorageProxy[K comparable, V any] struct {
	storage atomic.Pointer[FlatHashMap[K, V]]
}

func (r *StorageProxy[K, V]) GetStorage() *FlatHashMap[K, V] { return r.storage.Load() }
func (r *StorageProxy[K, V]) Store(t *FlatHashMap[K, V])     { r.storage.Store(t) }

// At resolves a slot index into its item via the current table.
func (r *StorageProxy[K, V]) At(idx uint32) *Item[K, V] { return r.storage.Load().At(idx) }

// Find resolves a queue node into its item and current slot via the current table. Called under the shard mutex.
func (r *StorageProxy[K, V]) Find(node QNode) (*Item[K, V], uint32, bool) {
	return r.storage.Load().Find(node)
}

// Locate is FlatHashMap.Locate on the current table. Called under the shard mutex.
func (r *StorageProxy[K, V]) Locate(node QNode) (*Item[K, V], uint32, bool) {
	return r.storage.Load().Locate(node)
}
