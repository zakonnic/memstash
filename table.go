package memstash

import (
	"sync/atomic"
	"unsafe"
)

// slotTable is one shard's index of the first level: an open-addressing (linear probing) hash table whose slot packs
// a 32-bit key tag with the 32-bit pool index of the item's state record - 8 bytes per slot in one flat allocation,
// which is what replaced the previous xsync map (64-byte buckets plus a heap-allocated entry per item).
//
// Concurrency contract: every mutation happens under the shard mutex, but every slot is read and written atomically,
// so readers probe completely lock-free. A shard publishes its current table through an atomic pointer; growth and
// tombstone purges build a fresh table and swap the pointer, and a straggler reading the superseded table still
// resolves records correctly (their liveness and identity are re-verified against the record itself) - at worst it
// misses a key inserted after the swap, which is indistinguishable from the Get racing the Set.
//
// A slot's tag is derived from the key's hash and doubles as the occupancy marker: 0 is a never-used slot (ends a
// probe chain), 1 is a tombstone (keeps the chain alive, reusable by inserts). Because the table is never allowed to
// run out of empty slots (grow keeps the load at or below 3/4), every probe loop terminates.
type slotTable struct {
	slots []atomic.Uint64
	mask  uint32
}

const (
	// minTableSlots is the initial table size of every shard; kept small so an underfilled cache pays almost nothing.
	minTableSlots = 64

	emptyTag uint32 = 0
	tombTag  uint32 = 1
	// minKeyTag is the smallest tag value a real key may carry (values below are the markers above).
	minKeyTag uint32 = 2

	tombSlot = uint64(tombTag) << 32
)

func newSlotTable(slotCount int) *slotTable {
	return &slotTable{slots: make([]atomic.Uint64, slotCount), mask: uint32(slotCount - 1)}
}

func (t *slotTable) bytes() int64 {
	return int64(unsafe.Sizeof(*t)) + int64(len(t.slots))*int64(unsafe.Sizeof(atomic.Uint64{}))
}

// tagOf derives the slot tag from a key hash. The shard is chosen by the hash's lowest bits and the probe start by
// its highest, so the tag takes the low half - within one shard its entropy is what is left after the shard bits,
// which is still plenty for a prefilter that only exists to skip pulling the record's cache line on chain collisions.
func tagOf(h uint64) uint32 {
	tag := uint32(h)
	if tag < minKeyTag {
		tag += minKeyTag
	}
	return tag
}

// slotHome is the probe start position for a key hash: the high half of the hash, independent from the bits that
// picked the shard.
func slotHome(h uint64, mask uint32) uint32 { return uint32(h>>32) & mask }

func packSlot(tag, idx uint32) uint64 { return uint64(tag)<<32 | uint64(idx) }
