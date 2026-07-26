package itemstore

import (
	"sync/atomic"
	"unsafe"
)

// groupShift sizes the overflow-count groups at 8 slots: home clusters at the working load factor rarely outgrow one
// group, so a zero count usually proves absence right at the group edge.
const groupShift = 3

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
	base      *Item[K, V]    // first slot; the backing array stays alive through this interior pointer
	overflows *atomic.Uint32 // per group: how many keys homed in it live past its edge (overflows counts)
	mask      uint32         // slot count minus one; the count is a power of two
}

func NewFlatHashMap[K comparable, V any](itemCount int) *FlatHashMap[K, V] {
	items := make([]Item[K, V], itemCount)
	over := make([]atomic.Uint32, itemCount>>groupShift)
	return &FlatHashMap[K, V]{base: &items[0], overflows: &over[0], mask: uint32(itemCount - 1)}
}

// At resolves a probe position (any uint32; wrapped by the mask) into its item.
func (t *FlatHashMap[K, V]) At(idx uint32) *Item[K, V] {
	return (*Item[K, V])(unsafe.Add(unsafe.Pointer(t.base), uintptr(idx&t.mask)*unsafe.Sizeof(Item[K, V]{})))
}

// Home is the probe start for a key hash: the high hash bits, so the slot position stays uncorrelated with the tag
// and the lowest bits picking the shard.
func (t *FlatHashMap[K, V]) Home(hash uint64) uint32 { return uint32(hash>>32) & t.mask }

// Wrap normalizes a probe position into a slot index.
func (t *FlatHashMap[K, V]) Wrap(pos uint32) uint32 { return pos & t.mask }

// Overflowed reports whether any key homed in home's group lives past the group's edge. False means a probe that
// scanned the group without a match may stop: the key is definitely absent.
func (t *FlatHashMap[K, V]) Overflowed(home uint32) bool { return t.overflowAt(home).Load() != 0 }

// NoteDisplaced records that home's key settled at pos; a no-op inside the home group.
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

func (t *FlatHashMap[K, V]) SlotCount() int { return int(t.mask) + 1 }

func (t *FlatHashMap[K, V]) Bytes() int64 {
	slots := int64(t.SlotCount())
	return int64(unsafe.Sizeof(*t)) + slots*int64(unsafe.Sizeof(Item[K, V]{})) + (slots>>groupShift)*4
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
