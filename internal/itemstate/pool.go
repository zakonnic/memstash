package itemstate

import "unsafe"

// poolChunk is a block of poolChunkSize state records allocated as one object. Chunk size follows the record's
// inline Entry: 12 KiB for word-sized pairs, 24 KiB for string keys with slice values - exact malloc size classes.
type poolChunk[K comparable, V any] struct {
	states [poolChunkSize]State[K, V]
}

// Pool is a shard's slab allocator of state records: records live by value inside fixed-size chunks and evicted ones
// return to a freelist instead of the GC. Records are addressed by 32-bit indices in the cache-wide Registry that
// all shard pools share; the registry resolves them back to records lock-free. Not thread-safe: guard with the shard
// mutex.
type Pool[K comparable, V any] struct {
	reg      *Registry[K, V]
	tail     *poolChunk[K, V] // the current partially filled chunk
	tailBase uint32           // global index of tail's first record
	tailUsed int
	free     []uint32
}

// NewPool creates a pool that registers its chunks in reg; shard pools of one cache must share one registry. The
// zero value lazily creates a private registry on first Claim (handy standalone and in tests).
func NewPool[K comparable, V any](reg *Registry[K, V]) Pool[K, V] { return Pool[K, V]{reg: reg} }

// At resolves a pool index into its state record via the shared registry.
func (p *Pool[K, V]) At(idx uint32) *State[K, V] { return p.reg.At(idx) }

// Claim hands out a record for a key/value pair: from the freelist, or the next untouched record (registering a new
// chunk when needed). The Entry is written strictly before the meta word goes live, so a reader that sees the record
// alive always finds the pair in place. Returns the record, its new generation and its pool index.
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
	record.setEntry(Entry[K, V]{Key: key, Value: value})
	// Occupancies advance by two: odd generations are SetValue's write-in-progress marker.
	gen := (uint32(record.meta.Load()) + 2) &^ 1
	record.meta.Store(uint64(expireOff)<<ExpireShift | uint64(gen))
	return record, gen, idx
}

// Release returns a record to the freelist, clearing its Entry so the key and value do not outlive the item. The
// record must already be a tombstone (Kill) - the dead bit is what shields stale readers from the cleared pair.
func (p *Pool[K, V]) Release(idx uint32) {
	p.reg.At(idx).clearEntry()
	p.free = append(p.free, idx)
}

// Bytes returns the footprint of the pool's own bookkeeping (the freelist); the chunks are accounted once by the
// shared Registry.
func (p *Pool[K, V]) Bytes() int64 {
	return int64(cap(p.free)) * int64(unsafe.Sizeof(uint32(0)))
}
