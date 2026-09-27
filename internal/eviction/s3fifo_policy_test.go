package eviction

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash/internal/itemstore"
)

func TestS3FIFOPolicyBytes(t *testing.T) {
	pool := newArena()
	p := NewS3FIFO(&pool.items, 1000, 100)
	assert.Equal(t, int64(unsafe.Sizeof(*p))+p.ghost.bytes(), p.Bytes())
	assert.Positive(t, p.ghost.bytes(), "the ghost table is allocated up front")
}

// TestS3FIFOGhostPromotion verifies the ghost path end to end: a key evicted from small (untouched, so it becomes a
// victim) is remembered by ghost, and re-adding it sends it straight to main.
func TestS3FIFOGhostPromotion(t *testing.T) {
	pool := newArena()
	p := NewS3FIFO(&pool.items, 10, 100)

	// Fill small past its capacity (10/10 = 1) and evict: the untouched front becomes a victim and goes to ghost.
	pool.add(p, "victim")
	pool.add(p, "other")
	key, ok := pool.evict(p)
	require.True(t, ok)
	require.Equal(t, "victim", key, "the untouched front of small must be the victim")

	pool.add(p, "victim")
	assert.Equal(t, []string{"victim"}, pool.queued(mainQueue), "a ghost hit must route the key straight to main")
	assert.EqualValues(t, 1, p.weight, "an item sent to main never weighs on small")

	// The hit consumed the ghost entry: the next insert of the key goes to small again.
	victimIdx, _ := pool.slotOf("victim")
	pool.release(p, victimIdx)
	pool.add(p, "victim")
	assert.Equal(t, []string{"other", "victim"}, pool.queued(0), "a ghost entry is removed on hit")
}

// TestS3FIFOSmallWeight follows small's weight through every way an item leaves it: promotion, eviction, removal.
func TestS3FIFOSmallWeight(t *testing.T) {
	pool := newArena()
	p := NewS3FIFO(&pool.items, 50, 100) // smallCap = 5
	for i := range 8 {
		pool.add(p, fmt.Sprintf("k%d", i))
	}
	require.EqualValues(t, 8, p.weight)

	pool.touch("k0")
	key, ok := pool.evict(p)
	require.True(t, ok)
	assert.Equal(t, "k1", key, "the touched front is promoted, the next one evicted")
	assert.Equal(t, []string{"k0"}, pool.queued(mainQueue))
	assert.EqualValues(t, 6, p.weight)

	idx, _ := pool.slotOf("k0")
	p.Reweigh(idx, 3)
	assert.EqualValues(t, 6, p.weight, "reweighing a main item leaves small alone")
	idx, _ = pool.slotOf("k5")
	p.Reweigh(idx, 3)
	assert.EqualValues(t, 9, p.weight)
	p.Remove(itemstore.QNode{Idx: idx, Cost: 4}) // the cache hands over the weight it accounts the item with
	assert.EqualValues(t, 5, p.weight)
}

// TestGhostTable exercises the fingerprint table directly: push/hit round trip, remove-on-hit, aging past the
// window, and the zero-capacity no-op mode.
func TestGhostTable(t *testing.T) {
	g := newGhostTable(64)
	assert.False(t, g.hit(1), "an empty table hits nothing")

	g.push(1)
	assert.True(t, g.hit(1), "a pushed key must hit")
	assert.False(t, g.hit(1), "a hit consumes the entry")

	// Aging: after ageWindow pushes of other keys, the entry is treated as absent even if its slot survived.
	g.push(2)
	for i := 0; i < int(g.ageWindow); i++ {
		g.push(uint64(1000 + i))
	}
	assert.False(t, g.hit(2), "an entry older than the age window must not hit")

	disabled := newGhostTable(0)
	disabled.push(1) // must not panic
	assert.False(t, disabled.hit(1), "a zero-capacity table never hits")
	assert.Zero(t, disabled.bytes())
}
