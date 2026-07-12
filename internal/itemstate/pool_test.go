package itemstate

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// TestPoolBytes verifies the byte accounting split between the pool and the registry: the registry owns the chunks
// (one per poolChunkSize claims) and its directory, the pool owns only its freelist's backing array.
func TestPoolBytes(t *testing.T) {
	var reg Registry[string]
	p := NewPool(&reg)
	assert.Zero(t, p.Bytes(), "an empty pool has no freelist")
	assert.Zero(t, reg.Bytes(), "an empty registry has no chunks")

	chunkBytes := int64(unsafe.Sizeof(poolChunk[string]{}))
	dirEntryBytes := int64(unsafe.Sizeof((*poolChunk[string])(nil)))
	idxBytes := int64(unsafe.Sizeof(uint32(0)))

	indices := make([]uint32, 0, poolChunkSize+1)
	for i := 0; i < poolChunkSize; i++ {
		_, _, idx := p.Claim("k", 0)
		indices = append(indices, idx)
	}
	dirBytes := int64(cap(reg.chunks)) * dirEntryBytes
	assert.Equal(t, chunkBytes+dirBytes, reg.Bytes(), "exactly one chunk after poolChunkSize claims")

	_, _, extra := p.Claim("k", 0)
	indices = append(indices, extra)
	dirBytes = int64(cap(reg.chunks)) * dirEntryBytes
	assert.Equal(t, 2*chunkBytes+dirBytes, reg.Bytes(), "a claim beyond poolChunkSize registers a second chunk")

	p.Release(indices[0])
	freeBytes := int64(cap(p.free)) * idxBytes
	assert.Positive(t, freeBytes)
	assert.Equal(t, freeBytes, p.Bytes(), "the pool itself accounts only its freelist")
}

// TestPoolClaimRelease verifies the index round trip: At resolves what Claim handed out, Release recycles the index
// and bumps the generation on reuse.
func TestPoolClaimRelease(t *testing.T) {
	var p Pool[string] // the zero value must self-host a private registry
	record, gen, idx := p.Claim("a", 0)
	assert.Same(t, record, p.At(idx), "At must resolve the claimed index to the same record")
	assert.Equal(t, "a", record.Key)

	p.Release(idx)
	assert.Empty(t, p.At(idx).Key, "Release must zero the key")

	record2, gen2, idx2 := p.Claim("b", 0)
	assert.Equal(t, idx, idx2, "the freelist must recycle the released index")
	assert.Same(t, record, record2)
	assert.Equal(t, gen+1, gen2, "reuse must bump the generation")
}

// TestPoolSharedRegistry verifies that pools sharing one registry hand out non-overlapping global indices, all
// resolvable through the same registry - the invariant the cache's per-shard pools rely on.
func TestPoolSharedRegistry(t *testing.T) {
	var reg Registry[int]
	a, b := NewPool(&reg), NewPool(&reg)

	seen := make(map[uint32]struct{})
	for i := 0; i < poolChunkSize+1; i++ {
		_, _, idxA := a.Claim(i, 0)
		_, _, idxB := b.Claim(-i-1, 0)
		for _, idx := range []uint32{idxA, idxB} {
			_, dup := seen[idx]
			assert.False(t, dup, "global index %d handed out twice", idx)
			seen[idx] = struct{}{}
		}
		assert.Equal(t, i, reg.At(idxA).Key)
		assert.Equal(t, -i-1, reg.At(idxB).Key)
	}
}
