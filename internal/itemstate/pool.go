package itemstate

import "unsafe"

// poolChunk is a block of poolChunkSize state records allocated as a single object: memory for items is grabbed in
// large chunks rather than one object per Set. A record is a fixed 16 bytes regardless of the key/value types, so a
// chunk is exactly 2 KiB - a malloc size class with zero slack.
type poolChunk[K comparable, V any] struct {
	states [poolChunkSize]State[K, V]
}

// Pool is a shard's pool of reusable state records. It is a slab-style allocator: records live by value inside
// fixed-size chunks and evicted records return to a freelist instead of the GC, so on a steady-state cache the only
// per-Set allocation is the item's immutable Entry box (a chunk costs an amortized 1/poolChunkSize of an insert
// during growth). Records are addressed by a 32-bit index in the cache-wide Registry that all shard pools share: the
// pool allocates chunks, the registry assigns them their global index ranges and resolves indices back to records
// (lock-free, which is what the cache's Get path relies on). Claim/Release are the classic pool checkout/return pair.
// Not thread-safe: access must be guarded by the caller (the shard mutex).
type Pool[K comparable, V any] struct {
	reg      *Registry[K, V]
	tail     *poolChunk[K, V] // the current partially filled chunk
	tailBase uint32           // global index of tail's first record
	tailUsed int              // records handed out from tail
	free     []uint32
}

// NewPool creates a pool that registers its chunks in reg. Shard pools of one cache must share one registry - their
// indices live in the same table. The zero value is also usable: it lazily creates a private registry on first Claim
// (convenient for standalone use and tests).
func NewPool[K comparable, V any](reg *Registry[K, V]) Pool[K, V] { return Pool[K, V]{reg: reg} }

// At resolves a pool index into its state record via the shared registry.
func (p *Pool[K, V]) At(idx uint32) *State[K, V] { return p.reg.At(idx) }

// Claim hands out a state record for a key/value pair: from the freelist, or - when it is empty - the next untouched
// record (allocating and registering a new chunk when needed). The Entry box is published strictly before the meta
// word goes live, so a lock-free reader that observes the record alive always finds its box in place. The previous
// occupant's generation is bumped and the reference counter is reset to zero. Returns the record, its new generation
// and its global pool index.
func (p *Pool[K, V]) Claim(key K, value V, expireOff uint32) (*State[K, V], uint32, uint32) {
	var record *State[K, V]
	var idx uint32
	if freeCount := len(p.free); freeCount > 0 {
		idx = p.free[freeCount-1]
		p.free = p.free[:freeCount-1]
		record = p.reg.At(idx)
	} else {
		if p.tail == nil || p.tailUsed == poolChunkSize {
			if p.reg == nil {
				p.reg = &Registry[K, V]{}
			}
			p.tail = &poolChunk[K, V]{}
			p.tailBase = p.reg.register(p.tail)
			p.tailUsed = 0
		}
		record = &p.tail.states[p.tailUsed]
		idx = p.tailBase + uint32(p.tailUsed)
		p.tailUsed++
	}
	record.entry.Store(&Entry[K, V]{Key: key, Value: value})
	gen := uint32(record.meta.Load()) + 1
	record.meta.Store(uint64(expireOff)<<ExpireShift | uint64(gen))
	return record, gen, idx
}

// Release returns a state record to the freelist by its pool index. The Entry box reference is dropped so the pool
// does not keep the key and value alive from the garbage collector's point of view.
func (p *Pool[K, V]) Release(idx uint32) {
	p.reg.At(idx).entry.Store(nil)
	p.free = append(p.free, idx)
}

// Bytes returns the memory footprint of the pool's own bookkeeping - the freelist's backing array. The chunks
// themselves are accounted once by the shared Registry's Bytes. Not thread-safe: call under the shard mutex, same as
// every other Pool method.
func (p *Pool[K, V]) Bytes() int64 {
	return int64(cap(p.free)) * int64(unsafe.Sizeof(uint32(0)))
}
