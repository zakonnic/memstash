package itemstate

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// maxChunks caps the global index space at 2^32 records (maxChunks * poolChunkSize)
const maxChunks = 1 << 25 // = 32 - 7 for 128 bytes poolChunkSize

// MaxRecords is the largest number of records a Registry can ever address: idx is a uint32 shared by every shard's
// Pool, so this is a cache-wide ceiling that adding shards cannot raise.
const MaxRecords = int64(maxChunks) * poolChunkSize

// Registry is the cache-wide directory of pool chunks: every chunk allocated by any shard's Pool is registered here
// and receives a global 32-bit index range. The directory is published as an atomic pointer to its first element, so
// At is lock-free and safe concurrently with registration - that is what lets the cache's Get path resolve a map
// entry's pool index into its state record without taking the shard mutex (and lets the map entry hold a 4-byte index
// instead of a pointer).
//
// The zero value is ready to use.
type Registry[K comparable] struct {
	mu     sync.Mutex
	chunks []*poolChunk[K]
	// base is the readers' view: &chunks[0], republished after every append. At indexes off it with raw pointer
	// arithmetic - no slice header load and no bounds check, which keeps the Get hot path at two dependent loads
	// (chunk pointer, then the record's meta word in the caller).
	//
	// Safety of the missing bounds check rests on the publication order: a chunk is registered (and base republished)
	// strictly before any index into it is handed out by Claim, and that index reaches a reader only through a map
	// entry stored later still - the entry load's acquire semantics therefore guarantee the reader observes both the
	// directory element and, if append relocated the directory, the new base. Old bases stay valid for stragglers:
	// a superseded array still holds correct chunk pointers for every index that was in range when it was current.
	base atomic.Pointer[*poolChunk[K]]
}

// register adds a chunk to the directory and returns the global index of its first record.
func (r *Registry[K]) register(chunk *poolChunk[K]) uint32 {
	r.mu.Lock()
	if len(r.chunks) == maxChunks {
		r.mu.Unlock()
		panic("memstash: pool index space exhausted (2^32 state records)")
	}
	r.chunks = append(r.chunks, chunk)
	r.base.Store(&r.chunks[0])
	base := uint32(len(r.chunks)-1) * poolChunkSize
	r.mu.Unlock()
	return base
}

// At resolves a global pool index into its state record.
// idx must have been handed out by a Claim (see base for why that makes the unchecked indexing sound).
func (r *Registry[K]) At(idx uint32) *State[K] {
	basePtr := unsafe.Pointer(r.base.Load()) // r.base.Load() gives atomic load with no locks
	chunk := *(**poolChunk[K])(unsafe.Add(basePtr, uintptr(idx/poolChunkSize)*unsafe.Sizeof(basePtr)))
	return &chunk.states[idx%poolChunkSize]
}

// Bytes returns the memory footprint of every registered chunk plus the directory's backing array.
func (r *Registry[K]) Bytes() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	chunkBytes := int64(len(r.chunks)) * int64(unsafe.Sizeof(poolChunk[K]{}))
	dirBytes := int64(cap(r.chunks)) * int64(unsafe.Sizeof((*poolChunk[K])(nil)))
	return chunkBytes + dirBytes
}
