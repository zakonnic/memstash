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

// MaxItems caps the item count: 3/4 (the rebuild load factor) of the 2^32 slot positions.
const MaxItems = int64(3) << 30

// FlatHashMap is one shard's memory storage: an open-addressing (https://en.wikipedia.org/wiki/Linear_probing) linear
// hash table whose slots are the items themselves, so a probe that lands has meta, key and value right there, with no
// second lookup. Queue nodes address items by slot position; a rebuild moves items and re-links the queues.
//
// No buckets: the hash picks the starting slot, and a taken slot just means stepping forward with pos++ until the key
// or an empty slot turns up. The 3/4 fill cap keeps the walk short - ~2 slots on a hit, ~6 on a miss.
// Readers probe lock-free against atomically published meta words; all mutations happen under the shard mutex.
// A rebuild swaps the whole table: readers mid-probe finish on the superseded one and may miss a write that landed
// after the swap - indistinguishable from the Get racing the Set.
type FlatHashMap[K comparable, V any] struct {
	short     []Item[K, V]              // the whole table while it fits one chunk - most tables do
	chunks    []*[chunkSlots]Item[K, V] // past that: the slot's high bits pick the chunk, the low ones the slot in it
	overflows *atomic.Uint32            // per group: how many keys homed in it live past its edge (SwissTable/f14)
	mask      uint32                    // slot count minus one; the count is a power of two
}

// NewFlatHashMap allocates a table of itemCount slots, a power of two. Below chunkSlots the table is one flat run of
// exactly that many slots, so a small shard costs no more than it used to.
func NewFlatHashMap[K comparable, V any](itemCount int) *FlatHashMap[K, V] {
	t := &FlatHashMap[K, V]{mask: uint32(itemCount - 1)}
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

// At resolves a probe position (any uint32; wrapped by the mask) into its item.
func (t *FlatHashMap[K, V]) At(idx uint32) *Item[K, V] {
	idx &= t.mask
	if t.mask < chunkSlots { // the mask already says which of the two layouts is in use
		return &t.short[idx]
	}
	return &t.chunks[idx>>chunkShift][idx&chunkMask]
}

// Home is the probe start for a key hash: the high hash bits, so the slot position stays uncorrelated with the tag
// and the lowest bits picking the shard.
func (t *FlatHashMap[K, V]) Home(hash uint64) uint32 { return uint32(hash>>32) & t.mask }

// Wrap normalizes a probe position into a slot index.
func (t *FlatHashMap[K, V]) Wrap(pos uint32) uint32 { return pos & t.mask }

// Overflowed reports whether any key homed in home's group lives past the group's edge. False means a probe that
// scanned the group without a match may stop: the key is definitely absent.
func (t *FlatHashMap[K, V]) Overflowed(home uint32) bool { return t.overflowAt(home).Load() != 0 }

// NoteDisplaced increments the overflow count for home's group if the key settled outside its home group.
func (t *FlatHashMap[K, V]) NoteDisplaced(home, pos uint32) {
	if (home&t.mask)>>groupShift != (pos&t.mask)>>groupShift {
		t.overflowAt(home).Add(1)
	}
}

// ForgetDisplaced undoes NoteDisplaced when the key at pos dies.
func (t *FlatHashMap[K, V]) ForgetDisplaced(home, pos uint32) {
	if (home&t.mask)>>groupShift != (pos&t.mask)>>groupShift {
		t.overflowAt(home).Add(^uint32(0))
	}
}

func (t *FlatHashMap[K, V]) overflowAt(home uint32) *atomic.Uint32 {
	return (*atomic.Uint32)(unsafe.Add(unsafe.Pointer(t.overflows), uintptr((home&t.mask)>>groupShift)*4))
}

// InsertFresh copies a live item into a table nobody reads yet (rebuilds) and returns its slot index.
func (t *FlatHashMap[K, V]) InsertFresh(keyHash uint64, src *Item[K, V]) uint32 {
	home := t.Home(keyHash)
	for pos := home; ; pos++ {
		dst := t.At(pos)
		if dst.meta.Load() == 0 {
			dst.entry = src.entry
			dst.meta.Store(src.meta.Load())
			t.NoteDisplaced(home, pos)
			return pos & t.mask
		}
	}
}

func (t *FlatHashMap[K, V]) Len() int { return int(t.mask) + 1 }

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
