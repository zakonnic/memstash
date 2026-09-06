package eviction

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSieveEvictionOrder verifies the SIEVE scan: the hand starts at the oldest item, a visited item survives in
// place with its counter cleared, and the hand resumes where it stopped instead of rescanning from the tail.
func TestSieveEvictionOrder(t *testing.T) {
	pool := newArena()
	p := NewSieve(&pool.items)

	addFromPool(p, pool, "a") // oldest
	addFromPool(p, pool, "b")
	addFromPool(p, pool, "c") // newest
	touch(pool, "a")

	victims := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		idx, ok := p.Evict(0)
		require.True(t, ok, "eviction %d must find a victim", i)
		victims = append(victims, pool.keyAt(idx))
		pool.release(idx)
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
	pool := newArena()
	p := NewSieve(&pool.items)

	addFromPool(p, pool, "hot")
	addFromPool(p, pool, "cold1")
	touch(pool, "hot")

	idx, ok := p.Evict(0)
	require.True(t, ok)
	assert.Equal(t, "cold1", pool.keyAt(idx), "the visited oldest item must survive the scan")
	pool.release(idx)

	// A new item lands between the hand and the head: with the hot item retained in place, the newcomer is the
	// next unvisited candidate (after the wrap the hand passes hot only if it is unvisited again).
	touch(pool, "hot")
	addFromPool(p, pool, "cold2")
	idx, ok = p.Evict(0)
	require.True(t, ok)
	assert.Equal(t, "cold2", pool.keyAt(idx), "a retained item must not be re-queued behind newcomers")
	pool.release(idx)

	assert.Equal(t, 1, p.Len())
	assert.True(t, pool.alive("hot"))
}

// TestSieveSweep verifies stale nodes are dropped in slices, live nodes and the hand survive, and the freed slots
// are reused by later adds.
func TestSieveSweep(t *testing.T) {
	pool := newArena()
	p := NewSieve(&pool.items)

	for i := 0; i < 6; i++ {
		addFromPool(p, pool, fmt.Sprintf("k%d", i))
	}
	// Park the hand mid-list on a soon-to-be-dead node: k0 visited survives, the scan stops after evicting k1.
	touch(pool, "k0")
	idx, ok := p.Evict(0)
	require.True(t, ok)
	require.Equal(t, "k1", pool.keyAt(idx))
	pool.release(idx)

	pool.kill("k2")
	pool.kill("k4")
	// Two nodes per call: the pass over five nodes takes three calls.
	calls := 0
	for more := true; more; calls++ {
		more = p.Sweep(2, true)
	}
	assert.Equal(t, 3, calls)
	assert.Equal(t, 3, p.Len())

	// The policy keeps evicting correctly after the sweep (the hand was on the killed k2).
	victims := map[string]bool{}
	for i := 0; i < 3; i++ {
		idx, ok := p.Evict(0)
		require.True(t, ok, "eviction %d after sweep", i)
		victims[pool.keyAt(idx)] = true
		pool.release(idx)
	}
	assert.Equal(t, map[string]bool{"k0": true, "k3": true, "k5": true}, victims)

	// Freed slots go back through the freelist: re-adding must not grow the node slice.
	capBefore := cap(p.nodes)
	for i := 0; i < 6; i++ {
		addFromPool(p, pool, fmt.Sprintf("r%d", i))
	}
	assert.Equal(t, capBefore, cap(p.nodes), "freed node slots must be reused before the slice grows")
}

// TestSieveSweepSurvivesEvictionUnderCursor: a sweep pass interrupted by an eviction of the node it stopped at
// resumes on a valid node.
func TestSieveSweepSurvivesEvictionUnderCursor(t *testing.T) {
	pool := newArena()
	p := NewSieve(&pool.items)
	for i := 0; i < 4; i++ {
		addFromPool(p, pool, fmt.Sprintf("k%d", i))
	}
	require.True(t, p.Sweep(1, true), "one node examined, three to go") // the cursor now rests on k2 (head is k3)
	for i := 0; i < 3; i++ {                                            // evict from the tail: k0, k1, k2 - the cursor's node among them
		idx, ok := p.Evict(0)
		require.True(t, ok)
		pool.release(idx)
	}
	// The evictions consumed the rest of the pass, so this call opens the next one - and a pass carries the parity
	// the last one did not, or it would read the marks that one left as its own and drop the nodes it kept.
	assert.False(t, p.Sweep(10, false), "the pass finishes on what is left")
	assert.Equal(t, 1, p.Len())
	assert.True(t, pool.alive("k3"))
}

func TestSieveBytes(t *testing.T) {
	pool := newArena()
	p := NewSieve(&pool.items)
	structBytes := int64(unsafe.Sizeof(*p))
	assert.Equal(t, structBytes, p.Bytes(), "an empty policy is just its struct")

	for i := 0; i < 100; i++ {
		addFromPool(p, pool, fmt.Sprintf("k%d", i))
	}
	assert.Equal(t, structBytes+int64(cap(p.nodes))*int64(unsafe.Sizeof(sieveNode{})), p.Bytes())
}
