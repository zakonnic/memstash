package memstash

import (
	"sync/atomic"
	"unsafe"
)

// slotTable is one shard's index: an open-addressing (linear probing) table whose slot packs a 32-bit key tag with
// the 32-bit pool index of the item's record - 8 bytes per slot in one flat allocation.
//
// Mutations happen under the shard mutex, but every slot is read and written atomically, so readers probe lock-free.
// The shard publishes its current table through an atomic pointer; growth and purges swap in a fresh table, and a
// straggler on the superseded one still resolves records correctly - at worst it misses a key inserted after the
// swap, indistinguishable from the Get racing the Set.
//
// The tag doubles as the occupancy marker: 0 ends a probe chain, 1 is a tombstone (keeps the chain alive, reusable
// by inserts). Growth keeps occupancy at or below 3/4, so every probe terminates.
type slotTable struct {
	slots []atomic.Uint64
	mask  uint32
}

const (
	// minTableSlots is the initial table size of every shard.
	minTableSlots = 64

	emptyTag uint32 = 0
	tombTag  uint32 = 1
	// minKeyTag is the smallest tag a real key may carry.
	minKeyTag uint32 = 2

	tombSlot = uint64(tombTag) << 32
)

func newSlotTable(slotCount int) *slotTable {
	return &slotTable{slots: make([]atomic.Uint64, slotCount), mask: uint32(slotCount - 1)}
}

func (t *slotTable) bytes() int64 {
	return int64(unsafe.Sizeof(*t)) + int64(len(t.slots))*int64(unsafe.Sizeof(atomic.Uint64{}))
}

// tagOf derives the slot tag from the low half of the key hash (the high half seeds the probe). It only prefilters
// chain collisions; the exact match is always the Entry key comparison.
func tagOf(h uint64) uint32 {
	tag := uint32(h)
	if tag < minKeyTag {
		tag += minKeyTag
	}
	return tag
}

// slotHome is the probe start position for a key hash.
func slotHome(h uint64, mask uint32) uint32 { return uint32(h>>32) & mask }

func packSlot(tag, idx uint32) uint64 { return uint64(tag)<<32 | uint64(idx) }
