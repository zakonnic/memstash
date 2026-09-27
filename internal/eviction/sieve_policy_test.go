package eviction

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSieveEvictionOrder verifies the SIEVE scan: the hand starts at the oldest item, a visited item survives in
// place with its counter cleared, and the hand resumes where it stopped instead of rescanning from the front.
func TestSieveEvictionOrder(t *testing.T) {
	pool := newArena()
	p := NewSieve(&pool.items)
	for _, key := range []string{"a", "b", "c"} {
		pool.add(p, key)
	}
	pool.touch("a")

	var victims []string
	for range 3 {
		key, ok := pool.evict(p)
		require.True(t, ok)
		victims = append(victims, key)
	}
	// a is visited: the hand clears it and moves on to b, then c; the wrap-around finds a unvisited.
	assert.Equal(t, []string{"b", "c", "a"}, victims)
	_, ok := pool.evict(p)
	assert.False(t, ok, "an empty policy must report nothing to evict")
}

// TestSieveRetainedStayInPlace pins SIEVE's defining property: an item that survives a scan is not re-queued, so
// newer unvisited items are examined (and evicted) before it is looked at again.
func TestSieveRetainedStayInPlace(t *testing.T) {
	pool := newArena()
	p := NewSieve(&pool.items)
	pool.add(p, "hot")
	pool.add(p, "cold1")
	pool.touch("hot")

	key, ok := pool.evict(p)
	require.True(t, ok)
	assert.Equal(t, "cold1", key, "the visited oldest item must survive the scan")

	pool.touch("hot")
	pool.add(p, "cold2")
	key, ok = pool.evict(p)
	require.True(t, ok)
	assert.Equal(t, "cold2", key, "a retained item must not be re-queued behind newcomers")
	assert.Equal(t, []string{"hot"}, pool.queued(0))
}

// TestSieveHandSurvivesDeletes: deleting the item under the hand, and the compaction that follows, leave the scan
// where it would have gone next.
func TestSieveHandSurvivesDeletes(t *testing.T) {
	pool := newArena()
	p := NewSieve(&pool.items)
	for i := range 6 {
		pool.add(p, fmt.Sprintf("k%d", i))
	}
	pool.touch("k0")
	pool.touch("k2")
	key, _ := pool.evict(p) // clears k0, evicts k1; the hand rests on k2
	require.Equal(t, "k1", key)

	idx, _ := pool.slotOf("k2")
	pool.release(p, idx)
	key, _ = pool.evict(p)
	assert.Equal(t, "k3", key, "the hand must have stepped from the deleted k2 onto k3")
}
