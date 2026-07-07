package itemstate

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// TestPoolBytes verifies the pool's byte accounting: chunk growth (one chunk per chunkSize claims) and the
// freelist's own backing array, which grows independently as records are released.
func TestPoolBytes(t *testing.T) {
	var p Pool[string]
	assert.Zero(t, p.Bytes(), "an empty pool has no chunks or freelist")

	chunkBytes := int64(unsafe.Sizeof(poolChunk[string]{}))
	ptrBytes := int64(unsafe.Sizeof((*State[string])(nil)))

	states := make([]*State[string], 0, chunkSize+1)
	for i := 0; i < chunkSize; i++ {
		s, _ := p.Claim("k", 0)
		states = append(states, s)
	}
	assert.Equal(t, chunkBytes, p.Bytes(), "exactly one chunk after chunkSize claims")

	extra, _ := p.Claim("k", 0)
	states = append(states, extra)
	assert.Equal(t, 2*chunkBytes, p.Bytes(), "a claim beyond chunkSize allocates a second chunk")

	p.Release(states[0])
	assert.Equal(t, 2*chunkBytes+ptrBytes, p.Bytes(), "a released record grows the freelist's backing array")
}
