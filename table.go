package memstash

import (
	"sync/atomic"
	"unsafe"
)

// hashSlots is one shard's index: an open-addressing (https://en.wikipedia.org/wiki/Linear_probing) linear hash table
// of 8-byte slots packed as [32-bit key short hash][32-bit pool index].
// It's like normal hash table, but without buckets - we start at position, calculated by hash, walk around a plain
// array with pos++ on collisions, but table is filled by 3/4 maximum and hash function gives us a very smooth
// distribution, so the average probe count is small; typically 1–2 steps on collisions, long chains are rare.
// Every slot is read and written atomically, so readers probe lock-free. When growth happens - at worst it misses a
// key inserted after the swap, indistinguishable from the Get racing the Set.
type hashSlots struct {
	base *atomic.Uint64 // first slot; the backing array stays alive through this interior pointer
	mask uint32         // slot count minus one; the count is a power of two
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
	slots := make([]atomic.Uint64, slotCount)
	return &hashSlots{base: &slots[0], mask: uint32(slotCount - 1)}
}

// shortHashOf folds a key hash into the slot prefilter tag, stepping over the empty and tombstone markers.
func shortHashOf(h uint64) uint32 {
	shortHash := uint32(h)
	if shortHash < minKeyShortHash {
		shortHash += minKeyShortHash
	}
	return shortHash
}

func packSlot(shortHash, idx uint32) uint64 { return uint64(shortHash)<<32 | uint64(idx) }

// slotHash is the packed slot's short hash half.
func slotHash(packed uint64) uint32 { return uint32(packed >> 32) }

// slotIdx is the packed slot's pool index half.
func slotIdx(packed uint64) uint32 { return uint32(packed) }

// home is the probe start for a key hash: the high hash bits, so the slot position stays uncorrelated with the
// low-bits short hash and the lowest bits picking the shard.
func (t *hashSlots) home(hash uint64) uint32 { return uint32(hash>>32) & t.mask }

// slot resolves a probe position (any uint32; wrapped by the mask).
func (t *hashSlots) slot(pos uint32) *atomic.Uint64 {
	return (*atomic.Uint64)(unsafe.Add(unsafe.Pointer(t.base), uintptr(pos&t.mask)*unsafe.Sizeof(atomic.Uint64{})))
}

func (t *hashSlots) slotCount() int { return int(t.mask) + 1 }

// insertFresh links idx into the first free slot; only for tombstone-free tables nobody reads yet (rebuilds).
func (t *hashSlots) insertFresh(keyHash uint64, idx uint32) {
	for pos := t.home(keyHash); ; pos++ {
		if slot := t.slot(pos); slot.Load() == 0 {
			slot.Store(packSlot(shortHashOf(keyHash), idx))
			return
		}
	}
}

// unlink tombs the slot pointing exactly at (key hash, pool index) and reports whether it was found; not finding it
// is normal - an item that died earlier already lost its slot.
func (t *hashSlots) unlink(keyHash uint64, idx uint32) bool {
	shortHash := shortHashOf(keyHash)
	for pos := t.home(keyHash); ; pos++ {
		packed := t.slot(pos).Load()
		if slotHash(packed) == emptyShortHash {
			return false
		}
		if slotHash(packed) == shortHash && slotIdx(packed) == idx {
			t.slot(pos).Store(tombSlot)
			return true
		}
	}
}

func (t *hashSlots) bytes() int64 {
	return int64(unsafe.Sizeof(*t)) + int64(t.slotCount())*int64(unsafe.Sizeof(atomic.Uint64{}))
}
