package eviction

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash/internal/itemstate"
)

// TestS3FIFOPolicyBytes verifies that Bytes is the sum of its parts (the fixed struct, small, main and the ghost
// table) by comparing it against those unexported fields directly.
func TestS3FIFOPolicyBytes(t *testing.T) {
	var pool itemstate.Pool[string, string]
	p := NewS3FIFO(&pool, 1000, 100)
	structBytes := int64(unsafe.Sizeof(*p))
	// The ghost table pre-allocates all of its slots at construction (unlike the queues, which grow chunks lazily),
	// so a freshly built policy already reports its ghost size, not zero.
	assert.Equal(t, structBytes+p.ghost.bytes(), p.Bytes(),
		"empty policy: only the struct itself and the pre-allocated ghost table count")

	for i := 0; i < 200; i++ {
		addFromPool(p, &pool, fmt.Sprintf("k%d", i))
	}
	for i := 0; i < 150; i++ {
		_, ok := p.Evict(0)
		require.True(t, ok, "eviction %d: small must still have items to evict", i)
	}

	assert.Equal(t, structBytes+p.small.Bytes()+p.main.Bytes()+p.ghost.bytes(), p.Bytes(),
		"S3FIFOPolicy.Bytes must be the sum of the struct, small, main and ghost")
	assert.Positive(t, p.Bytes())
}

// TestS3FIFOGhostPromotion verifies the ghost path end to end: a key evicted from small (untouched, so it becomes a
// victim) is remembered by ghost, and re-adding it sends the new node straight to main.
func TestS3FIFOGhostPromotion(t *testing.T) {
	var pool itemstate.Pool[string, string]
	p := NewS3FIFO(&pool, 10, 100)

	// Fill small past its capacity (10/10 = 1) and evict: the untouched head becomes a victim and goes to ghost.
	addFromPool(p, &pool, "victim")
	addFromPool(p, &pool, "other")
	idx, ok := p.Evict(0)
	require.True(t, ok)
	require.Equal(t, "victim", pool.At(idx).Entry().Key, "the untouched head of small must be the victim")
	pool.Release(idx)

	mainBefore := p.main.Len()
	addFromPool(p, &pool, "victim")
	assert.Equal(t, mainBefore+1, p.main.Len(), "a ghost hit must route the key straight to main")

	// The hit consumed the ghost entry: the next add of the same key goes to small again.
	smallBefore := p.small.Len()
	addFromPool(p, &pool, "victim")
	assert.Equal(t, smallBefore+1, p.small.Len(), "a ghost entry is removed on hit")
}

// TestGhostTable exercises the fingerprint table directly: push/hit round trip, remove-on-hit, aging past the
// window, and the zero-capacity no-op mode.
func TestGhostTable(t *testing.T) {
	g := newGhostTable[int](64)
	assert.False(t, g.hit(1), "an empty table hits nothing")

	g.push(1)
	assert.True(t, g.hit(1), "a pushed key must hit")
	assert.False(t, g.hit(1), "a hit consumes the entry")

	// Aging: after ageWindow pushes of other keys, the entry is treated as absent even if its slot survived.
	g.push(2)
	for i := 0; i < int(g.ageWindow); i++ {
		g.push(1000 + i)
	}
	assert.False(t, g.hit(2), "an entry older than the age window must not hit")

	disabled := newGhostTable[int](0)
	disabled.push(1) // must not panic
	assert.False(t, disabled.hit(1), "a zero-capacity table never hits")
	assert.Zero(t, disabled.bytes())
}
