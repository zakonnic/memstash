package itemstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func (m *model) queued(queue uint) []uint64 {
	var keys []uint64
	for idx, ok := m.t.Oldest(queue); ok; idx, ok = m.t.Newer(idx) {
		keys = append(keys, m.t.At(idx).Entry().Key)
	}
	return keys
}

func (m *model) slotOf(key uint64) uint32 {
	for idx := range uint32(m.t.Len()) {
		if item := m.t.At(idx); !Empty(item.Metadata()) && item.Entry().Key == key {
			return idx
		}
	}
	panic("key not in the table")
}

func (m *model) handKey() uint64 {
	idx, ok := m.t.Hand()
	if !ok {
		return 0
	}
	return m.t.At(idx).Entry().Key
}

func TestRequeueBetweenQueues(t *testing.T) {
	m := newModel(64, 1)
	for key := uint64(1); key <= 5; key++ {
		m.insert(key, key)
	}
	m.t.Requeue(1, m.slotOf(2))
	m.t.Requeue(1, m.slotOf(4))
	m.t.Requeue(0, m.slotOf(1))
	require.Equal(t, []uint64{3, 5, 1}, m.queued(0))
	require.Equal(t, []uint64{2, 4}, m.queued(1))
	for key := range m.live {
		inMain := m.t.At(m.slotOf(key)).Metadata()&InMainQueue != 0
		require.Equal(t, key == 2 || key == 4, inMain, "key %d: InMainQueue must follow the queue", key)
	}
	m.t.Requeue(0, m.slotOf(4))
	require.Zero(t, m.t.At(m.slotOf(4)).Metadata()&InMainQueue, "back in queue 0, the bit goes")
}

// TestHandFollowsItsItem: the hand starts at the front, wraps past the back, steps on when its item is removed and
// moves along when a compaction moves its item.
func TestHandFollowsItsItem(t *testing.T) {
	m := newModel(64, 2)
	var scratch []ShiftEntry
	for key := uint64(1); key <= 3; key++ {
		m.insert(key, key)
	}
	require.EqualValues(t, 1, m.handKey())
	m.t.AdvanceHand()
	m.t.AdvanceHand()
	require.EqualValues(t, 3, m.handKey())
	m.t.AdvanceHand()
	require.EqualValues(t, 1, m.handKey(), "past the back the hand wraps to the front")

	m.t.AdvanceHand()
	m.remove(2, &scratch)
	require.EqualValues(t, 3, m.handKey(), "the hand steps off a removed item onto the next one")

	// Park the hand on a key displaced by a neighbor homed at the same slot, then delete the neighbor: the key moves
	// home and the hand must move with it.
	var near, far uint64
	for key := uint64(100); far == 0; key++ {
		if home := m.t.Home(m.hash(key)); home > 40 && home < 50 && m.t.Home(m.hash(key+1<<32)) == home {
			near, far = key, key+1<<32
		}
	}
	m.insert(near, near)
	m.insert(far, far)
	for m.handKey() != far {
		m.t.AdvanceHand()
	}
	moved := m.slotOf(far)
	m.remove(near, &scratch)
	require.NotEqual(t, moved, m.slotOf(far), "the delete must have moved the displaced key home")
	require.Equal(t, far, m.handKey())
	m.check(t)
}

func TestGrowKeepsQueuesAndHand(t *testing.T) {
	m := newModel(64, 3)
	for key := uint64(1); key <= 40; key++ {
		m.insert(key, key)
	}
	for key := uint64(2); key <= 40; key += 3 {
		m.t.Requeue(1, m.slotOf(key))
	}
	for m.handKey() != 7 {
		m.t.AdvanceHand()
	}
	queues := [][]uint64{m.queued(0), m.queued(1)}

	m.t = m.t.Grow(256, true)
	require.Equal(t, queues, [][]uint64{m.queued(0), m.queued(1)})
	require.EqualValues(t, 7, m.handKey())
	for key := range m.live {
		_, _, ok := m.lookup(key)
		require.True(t, ok, "key %d lost in the growth", key)
	}
}

// TestGrowOutOfOrderKeepsMembership: a growth that does not keep the order still keeps every item in its queue.
func TestGrowOutOfOrderKeepsMembership(t *testing.T) {
	m := newModel(64, 4)
	for key := uint64(1); key <= 40; key++ {
		m.insert(key, key)
	}
	for key := uint64(2); key <= 40; key += 3 {
		m.t.Requeue(1, m.slotOf(key))
	}
	queues := [][]uint64{m.queued(0), m.queued(1)}

	m.t = m.t.Grow(256, false)
	require.ElementsMatch(t, queues[0], m.queued(0))
	require.ElementsMatch(t, queues[1], m.queued(1))
	for _, key := range queues[1] {
		require.NotZero(t, m.t.At(m.slotOf(key)).Metadata()&InMainQueue, "key %d", key)
	}
}
