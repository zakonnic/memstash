package memstash

import (
	"sync/atomic"
	"unsafe"
)

// hashSlots is one shard's index: an open-addressing (https://en.wikipedia.org/wiki/Linear_probing) table.
//
// Mutations happen under the shard mutex, but every slot is read and written atomically, so readers probe lock-free.
// The shard publishes its current table through an atomic pointer; growth and purges swap in a fresh table, and a
// straggler on the superseded one still resolves records correctly - at worst it misses a key inserted after the
// swap, indistinguishable from the Get racing the Set.
type hashSlots struct {
	slots []atomic.Uint64 // [32-bit key short hash][32-bit pool index]
	mask  uint32
}

const (
	// minTableSlots is the initial table size of every shard.
	minTableSlots = 64

	emptyShortHash uint32 = 0
	tombShortHash  uint32 = 1
	// minKeyShortHash is the smallest short hash a real key may carry.
	minKeyShortHash uint32 = 2

	tombSlot = uint64(tombShortHash) << 32
)

func newHashSlots(slotCount int) *hashSlots {
	return &hashSlots{slots: make([]atomic.Uint64, slotCount), mask: uint32(slotCount - 1)}
}

func (t *hashSlots) bytes() int64 {
	return int64(unsafe.Sizeof(*t)) + int64(len(t.slots))*int64(unsafe.Sizeof(atomic.Uint64{}))
}

func shortHashOf(h uint64) uint32 {
	shortHash := uint32(h)
	if shortHash < minKeyShortHash {
		shortHash += minKeyShortHash
	}
	return shortHash
}

func homeSlot(hash uint64, mask uint32) uint32 { return uint32(hash>>32) & mask }

func packSlot(shortHash, idx uint32) uint64 { return uint64(shortHash)<<32 | uint64(idx) }
