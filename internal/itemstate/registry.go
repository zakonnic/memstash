package itemstate

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// maxChunks caps the global index space at 2^32 records (maxChunks * poolChunkSize).
const maxChunks = (1 << 32) / poolChunkSize

// MaxRecords is the largest number of records a Registry can address: idx is a uint32 shared by every shard's Pool,
// a cache-wide ceiling that adding shards cannot raise.
const MaxRecords = int64(maxChunks) * poolChunkSize

// Registry is the cache-wide directory of pool chunks: every chunk allocated by any shard's Pool is registered here
// and receives a global 32-bit index range. At is lock-free, which lets the Get path resolve a table slot's index
// without the shard mutex. The zero value is ready to use.
type Registry[K comparable, V any] struct {
	mu     sync.Mutex
	chunks []*poolChunk[K, V]
	// base is the readers' view: &chunks[0], republished after every append. At indexes off it with raw pointer
	// arithmetic - no slice header load, no bounds check.
	//
	// The missing bounds check is sound because of publication order: a chunk is registered (and base republished)
	// before any index into it leaves Claim, and that index reaches a reader only through a table slot stored later
	// still - so the reader observes both the directory element and, if append relocated the directory, the new base.
	// Old bases stay valid for stragglers: a superseded array still resolves every index that was in range for it.
	base atomic.Pointer[*poolChunk[K, V]]
}

// register adds a chunk to the directory and returns the global index of its first record.
func (r *Registry[K, V]) register(chunk *poolChunk[K, V]) uint32 {
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

// At resolves a global pool index into its state record. Lock-free; idx must have been handed out by a Claim (see
// base for why that makes the unchecked indexing sound).
func (r *Registry[K, V]) At(idx uint32) *State[K, V] {
	basePtr := unsafe.Pointer(r.base.Load())
	chunk := *(**poolChunk[K, V])(unsafe.Add(basePtr, uintptr(idx/poolChunkSize)*unsafe.Sizeof(basePtr)))
	return &chunk.states[idx%poolChunkSize]
}

// Bytes returns the footprint of every registered chunk plus the directory's backing array.
func (r *Registry[K, V]) Bytes() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	chunkBytes := int64(len(r.chunks)) * int64(unsafe.Sizeof(poolChunk[K, V]{}))
	dirBytes := int64(cap(r.chunks)) * int64(unsafe.Sizeof((*poolChunk[K, V])(nil)))
	return chunkBytes + dirBytes
}
