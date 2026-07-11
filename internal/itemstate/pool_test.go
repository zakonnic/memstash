package itemstate

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// TestPoolBytes verifies the pool's byte accounting: chunk growth (one chunk per poolChunkSize claims), the chunk
// directory, and the freelist's own backing array, which grows independently as records are released.
func TestPoolBytes(t *testing.T) {
	var p Pool[string]
	assert.Zero(t, p.Bytes(), "an empty pool has no chunks or freelist")

	chunkBytes := int64(unsafe.Sizeof(poolChunk[string]{}))
	dirEntryBytes := int64(unsafe.Sizeof((*poolChunk[string])(nil)))
	idxBytes := int64(unsafe.Sizeof(uint32(0)))

	indices := make([]uint32, 0, poolChunkSize+1)
	for i := 0; i < poolChunkSize; i++ {
		_, _, idx := p.Claim("k", 0)
		indices = append(indices, idx)
	}
	dirBytes := int64(cap(p.chunks)) * dirEntryBytes
	assert.Equal(t, chunkBytes+dirBytes, p.Bytes(), "exactly one chunk after poolChunkSize claims")

	_, _, extra := p.Claim("k", 0)
	indices = append(indices, extra)
	dirBytes = int64(cap(p.chunks)) * dirEntryBytes
	assert.Equal(t, 2*chunkBytes+dirBytes, p.Bytes(), "a claim beyond poolChunkSize allocates a second chunk")

	p.Release(indices[0])
	freeBytes := int64(cap(p.free)) * idxBytes
	assert.Positive(t, freeBytes)
	assert.Equal(t, 2*chunkBytes+dirBytes+freeBytes, p.Bytes(), "a released record grows the freelist's backing array")
}

// TestPoolClaimRelease verifies the index round trip: At resolves what Claim handed out, Release recycles the index
// and bumps the generation on reuse.
func TestPoolClaimRelease(t *testing.T) {
	var p Pool[string]
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
