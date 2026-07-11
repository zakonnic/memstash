package itemstate

import "unsafe"

// poolChunk is a block of poolChunkSize state records allocated as a single object: memory for items is grabbed in
// large chunks rather than one object per Set.
type poolChunk[K comparable] struct {
	states [poolChunkSize]State[K]
}

// Pool is a shard's pool of reusable state records. It is a slab-style allocator: records live by value inside
// fixed-size chunks and evicted records return to a freelist instead of the GC, so on a steady-state cache the Set
// path allocates nothing at all (a chunk costs an amortized 1/poolChunkSize of an insert during growth).
// Records are addressed by a 32-bit pool index (chunk number * poolChunkSize + offset), which is what the eviction
// queues store instead of pointers; a shard is therefore limited to 4G records. Claim/Release are the classic pool
// checkout/return pair. Not thread-safe: access must be guarded by the caller (the shard mutex).
type Pool[K comparable] struct {
	chunks []*poolChunk[K]
	free   []uint32
	next   int // number of records ever handed out
}

// At resolves a pool index into its state record. The chunks are append-only and records never move, so the returned
// pointer is stable for the record's whole lifetime.
func (p *Pool[K]) At(idx uint32) *State[K] {
	return &p.chunks[idx/poolChunkSize].states[idx%poolChunkSize]
}

// Claim hands out a state record for a key: from the freelist, or - when it is empty - the next untouched record
// (allocating a new chunk when needed). The previous occupant's generation is bumped and the reference counter is
// reset to zero. Returns the record, its new generation and its pool index.
func (p *Pool[K]) Claim(key K, expireOff uint32) (*State[K], uint32, uint32) {
	var idx uint32
	if freeCount := len(p.free); freeCount > 0 {
		idx = p.free[freeCount-1]
		p.free = p.free[:freeCount-1]
	} else {
		if p.next == len(p.chunks)*poolChunkSize {
			p.chunks = append(p.chunks, &poolChunk[K]{})
		}
		idx = uint32(p.next)
		p.next++
	}
	record := p.At(idx)
	gen := uint32(record.meta.Load()) + 1
	record.Key = key
	record.meta.Store(uint64(expireOff)<<ExpireShift | uint64(gen))
	return record, gen, idx
}

// Release returns a state record to the freelist by its pool index. The key is zeroed so the pool does not keep
// foreign data (strings, for example) alive from the garbage collector's point of view.
func (p *Pool[K]) Release(idx uint32) {
	var zero K
	p.At(idx).Key = zero
	p.free = append(p.free, idx)
}

// Bytes returns the memory footprint of the pool's allocated chunks (every record ever handed out, live or on the
// freelist - a chunk is never released back to the GC) plus the backing arrays of the chunk directory and the
// freelist. Not thread-safe: call under the shard mutex, same as every other Pool method.
func (p *Pool[K]) Bytes() int64 {
	chunkBytes := int64(len(p.chunks)) * int64(unsafe.Sizeof(poolChunk[K]{}))
	dirBytes := int64(cap(p.chunks)) * int64(unsafe.Sizeof((*poolChunk[K])(nil)))
	freeBytes := int64(cap(p.free)) * int64(unsafe.Sizeof(uint32(0)))
	return chunkBytes + dirBytes + freeBytes
}
