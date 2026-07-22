package itemstate

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// TestPoolBytes verifies the byte accounting split between the pool and the registry: the registry owns the chunks
// (one per poolChunkSize claims) and its directory, the pool owns only its freelist's backing array.
func TestPoolBytes(t *testing.T) {
	var reg Registry[string, string]
	p := NewPool(&reg)
	assert.Zero(t, p.Bytes(), "an empty pool has no freelist")
	assert.Zero(t, reg.Bytes(), "an empty registry has no chunks")

	chunkBytes := int64(unsafe.Sizeof(poolChunk[string, string]{}))
	dirEntryBytes := int64(unsafe.Sizeof((*poolChunk[string, string])(nil)))
	idxBytes := int64(unsafe.Sizeof(uint32(0)))

	indices := make([]uint32, 0, poolChunkSize+1)
	for i := 0; i < poolChunkSize; i++ {
		_, _, idx := p.Claim("k", "v", 0)
		indices = append(indices, idx)
	}
	dirBytes := int64(cap(reg.chunks)) * dirEntryBytes
	assert.Equal(t, chunkBytes+dirBytes, reg.Bytes(), "exactly one chunk after poolChunkSize claims")

	_, _, extra := p.Claim("k", "v", 0)
	indices = append(indices, extra)
	dirBytes = int64(cap(reg.chunks)) * dirEntryBytes
	assert.Equal(t, 2*chunkBytes+dirBytes, reg.Bytes(), "a claim beyond poolChunkSize registers a second chunk")

	p.Release(indices[0])
	freeBytes := int64(cap(p.free)) * idxBytes
	assert.Positive(t, freeBytes)
	assert.Equal(t, freeBytes, p.Bytes(), "the pool itself accounts only its freelist")
}

// TestPoolClaimRelease verifies the index round trip: At resolves what Claim handed out, Release recycles the index,
// clears the Entry and bumps the generation on reuse.
func TestPoolClaimRelease(t *testing.T) {
	var p Pool[string, string] // the zero value must self-host a private registry
	record, gen, idx := p.Claim("a", "va", 0)
	assert.Same(t, record, p.At(idx), "At must resolve the claimed index to the same record")
	assert.Equal(t, "a", record.Entry().Key)
	assert.Equal(t, "va", record.Entry().Value)

	record.Kill() // only tombstones go back to the freelist
	p.Release(idx)
	assert.Zero(t, *p.At(idx).Entry(), "Release must clear the Entry so the pair does not outlive the item")

	record2, gen2, idx2 := p.Claim("b", "vb", 0)
	assert.Equal(t, idx, idx2, "the freelist must recycle the released index")
	assert.Same(t, record, record2)
	assert.Equal(t, gen+2, gen2, "reuse must advance the generation (by two: odd marks a write)")
	assert.Equal(t, "b", record2.Entry().Key)
}

// TestPoolSharedRegistry verifies that pools sharing one registry hand out non-overlapping global indices, all
// resolvable through the same registry - the invariant the cache's per-shard pools rely on.
func TestPoolSharedRegistry(t *testing.T) {
	var reg Registry[int, int]
	a, b := NewPool(&reg), NewPool(&reg)

	seen := make(map[uint32]struct{})
	for i := 0; i < poolChunkSize+1; i++ {
		_, _, idxA := a.Claim(i, i, 0)
		_, _, idxB := b.Claim(-i-1, i, 0)
		for _, idx := range []uint32{idxA, idxB} {
			_, dup := seen[idx]
			assert.False(t, dup, "global index %d handed out twice", idx)
			seen[idx] = struct{}{}
		}
		assert.Equal(t, i, reg.At(idxA).Entry().Key)
		assert.Equal(t, -i-1, reg.At(idxB).Entry().Key)
	}
}

func TestStateSize(t *testing.T) {
	assert.Zero(t, unsafe.Sizeof(State[int, int]{})%8, "records must stay 8-aligned inside a chunk")
	assert.Zero(t, unsafe.Sizeof(State[string, [3]int64]{})%8)
	assert.EqualValues(t, poolChunkSize*unsafe.Sizeof(State[string, string]{}), unsafe.Sizeof(poolChunk[string, string]{}))
}
