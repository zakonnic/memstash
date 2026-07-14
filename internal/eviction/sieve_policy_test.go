package eviction

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash/internal/itemstate"
)

// touch sets one reference-counter bit on the record, as a lock-free reader would.
func touch(pool *itemstate.Pool[string, string], idx uint32) {
	state := pool.At(idx)
	state.TouchWith(state.Load())
}

// TestSieveEvictionOrder verifies the SIEVE scan: the hand starts at the oldest item, a visited item survives in
// place with its counter cleared, and the hand resumes where it stopped instead of rescanning from the tail.
func TestSieveEvictionOrder(t *testing.T) {
	var pool itemstate.Pool[string, string]
	p := NewSieve(&pool)

	idxA := addFromPool(p, &pool, "a") // oldest
	addFromPool(p, &pool, "b")
	addFromPool(p, &pool, "c") // newest
	touch(&pool, idxA)

	victims := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		idx, ok := p.Evict(0)
		require.True(t, ok, "eviction %d must find a victim", i)
		victims = append(victims, pool.At(idx).Entry().Key)
		pool.Release(idx)
	}
	// a is visited: the hand clears it and moves on to b, then c; the wrap-around finds a unvisited.
	assert.Equal(t, []string{"b", "c", "a"}, victims)
	assert.Zero(t, p.Len())

	_, ok := p.Evict(0)
	assert.False(t, ok, "an empty policy must report nothing to evict")
}

// TestSieveRetainedStayInPlace pins SIEVE's defining property: an item that survives a scan is not re-queued, so
// newer unvisited items are examined (and evicted) before it is looked at again.
func TestSieveRetainedStayInPlace(t *testing.T) {
	var pool itemstate.Pool[string, string]
	p := NewSieve(&pool)

	idxHot := addFromPool(p, &pool, "hot")
	addFromPool(p, &pool, "cold1")
	touch(&pool, idxHot)

	idx, ok := p.Evict(0)
	require.True(t, ok)
	assert.Equal(t, "cold1", pool.At(idx).Entry().Key, "the visited oldest item must survive the scan")
	pool.Release(idx)

	// A new item lands between the hand and the head: with the hot item retained in place, the newcomer is the
	// next unvisited candidate (after the wrap the hand passes hot only if it is unvisited again).
	touch(&pool, idxHot)
	addFromPool(p, &pool, "cold2")
	idx, ok = p.Evict(0)
	require.True(t, ok)
	assert.Equal(t, "cold2", pool.At(idx).Entry().Key, "a retained item must not be re-queued behind newcomers")
	pool.Release(idx)

	assert.Equal(t, 1, p.Len())
	live := 0
	p.Range(func(node itemstate.QNode) {
		assert.Equal(t, "hot", pool.At(node.Idx).Entry().Key)
		live++
	})
	assert.Equal(t, 1, live, "Range must visit each queued node exactly once")
}

// TestSieveSweep verifies dead nodes are released in bulk, live nodes and the hand survive, and the freed slots are
// reused by later adds.
func TestSieveSweep(t *testing.T) {
	var pool itemstate.Pool[string, string]
	p := NewSieve(&pool)

	indices := make([]uint32, 0, 6)
	for i := 0; i < 6; i++ {
		indices = append(indices, addFromPool(p, &pool, fmt.Sprintf("k%d", i)))
	}
	// Park the hand mid-list on a soon-to-be-dead node: k0 visited survives, the scan stops after evicting k1.
	touch(&pool, indices[0])
	idx, ok := p.Evict(0)
	require.True(t, ok)
	require.Equal(t, "k1", pool.At(idx).Entry().Key)
	pool.Release(idx)

	for _, i := range []int{2, 4} {
		pool.At(indices[i]).Kill()
	}
	released := map[string]bool{}
	p.Sweep(func(idx uint32) {
		released[pool.At(idx).Entry().Key] = true // Entry is still readable: Release happens in the callback's caller
		pool.Release(idx)
	})
	assert.Equal(t, map[string]bool{"k2": true, "k4": true}, released)
	assert.Equal(t, 3, p.Len())

	// The policy keeps evicting correctly after the sweep (the hand was on the killed k2).
	victims := map[string]bool{}
	for i := 0; i < 3; i++ {
		idx, ok := p.Evict(0)
		require.True(t, ok, "eviction %d after sweep", i)
		victims[pool.At(idx).Entry().Key] = true
		pool.Release(idx)
	}
	assert.Equal(t, map[string]bool{"k0": true, "k3": true, "k5": true}, victims)

	// Freed slots go back through the freelist: re-adding must not grow the node slice.
	capBefore := cap(p.nodes)
	for i := 0; i < 6; i++ {
		addFromPool(p, &pool, fmt.Sprintf("r%d", i))
	}
	assert.Equal(t, capBefore, cap(p.nodes), "freed node slots must be reused before the slice grows")
}

func TestSieveBytes(t *testing.T) {
	var pool itemstate.Pool[string, string]
	p := NewSieve(&pool)
	structBytes := int64(unsafe.Sizeof(*p))
	assert.Equal(t, structBytes, p.Bytes(), "an empty policy is just its struct")

	for i := 0; i < 100; i++ {
		addFromPool(p, &pool, fmt.Sprintf("k%d", i))
	}
	assert.Equal(t, structBytes+int64(cap(p.nodes))*int64(unsafe.Sizeof(sieveNode{})), p.Bytes())
}
