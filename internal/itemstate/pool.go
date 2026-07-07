package itemstate

import "unsafe"

// poolChunk is a block of chunkSize state records allocated as a single object: memory for items is grabbed in large
// chunks rather than one object per Set.
type poolChunk[K comparable] struct {
	states [chunkSize]State[K]
}

// Pool is a shard's pool of reusable state records. It is a slab-style allocator: records live by value inside
// fixed-size chunks and evicted records return to a freelist instead of the GC, so on a steady-state cache the Set
// path allocates nothing at all (a chunk costs an amortized 1/chunkSize of an insert during growth). Claim/Release
// are the classic pool checkout/return pair. Not thread-safe: access must be guarded by the caller (the shard mutex).
type Pool[K comparable] struct {
	chunks []*poolChunk[K]
	free   []*State[K]
	next   int // number of records ever handed out
}

// Claim hands out a state record for a key: from the freelist, or - when it is empty - the next untouched record
// (allocating a new chunk when needed). The previous occupant's generation is bumped and the reference counter is
// reset to zero.
func (p *Pool[K]) Claim(key K, expireOff uint32) (*State[K], uint32) {
	var record *State[K]
	if freeCount := len(p.free); freeCount > 0 {
		record = p.free[freeCount-1]
		p.free = p.free[:freeCount-1]
	} else {
		if p.next == len(p.chunks)*chunkSize {
			p.chunks = append(p.chunks, &poolChunk[K]{})
		}
		targetChunk := p.chunks[p.next/chunkSize]
		record = &targetChunk.states[p.next%chunkSize]
		p.next++
	}
	gen := uint32(record.meta.Load()) + 1
	record.Key = key
	record.meta.Store(uint64(expireOff)<<ExpireShift | uint64(gen))
	return record, gen
}

// Release returns a state record to the freelist. The key is zeroed so the pool does not keep foreign data (strings,
// for example) alive from the garbage collector's point of view.
func (p *Pool[K]) Release(record *State[K]) {
	var zero K
	record.Key = zero
	p.free = append(p.free, record)
}

// Bytes returns the memory footprint of the pool's allocated chunks (every record ever handed out, live or on the
// freelist - a chunk is never released back to the GC) plus the freelist slice's backing array. Not thread-safe:
// call under the shard mutex, same as every other Pool method.
func (p *Pool[K]) Bytes() int64 {
	chunkBytes := int64(len(p.chunks)) * int64(unsafe.Sizeof(poolChunk[K]{}))
	var statePtr *State[K]
	freeBytes := int64(cap(p.free)) * int64(unsafe.Sizeof(statePtr))
	return chunkBytes + freeBytes
}
