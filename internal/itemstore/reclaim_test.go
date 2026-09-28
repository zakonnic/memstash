package itemstore

import (
	"hash/maphash"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// model drives a FlatHashMap the way the cache does - the same insert walk, the same removal, kill and Reclaim - and
// keeps a plain map and the insertion order as the oracle.
type model struct {
	t     *FlatHashMap[uint64, uint64]
	h     maphash.Seed
	live  map[uint64]uint64
	order []uint64
	rng   *rand.Rand
	moves atomic.Uint32
}

func newModel(slots int, seed uint64) *model {
	h := maphash.MakeSeed()
	return &model{t: NewFlatHashMap[uint64, uint64](slots, h), h: h, live: map[uint64]uint64{},
		rng: rand.New(rand.NewPCG(seed, seed^0x9E37))}
}

func (m *model) hash(key uint64) uint64 { return maphash.Comparable(m.h, key) }

// insert mirrors Cache.setMemory for a key known to be absent.
func (m *model) insert(key, value uint64) uint32 {
	hash := m.hash(key)
	home := m.t.Home(hash)
	for pos := home; ; pos++ {
		item := m.t.At(pos)
		if Empty(item.Metadata()) {
			item.Publish(Entry[uint64, uint64]{Key: key, Value: value}, TagOf(hash), 0, m.t.Dist(home, m.t.Wrap(pos)))
			m.t.NoteDisplaced(hash, pos)
			m.t.MarkChain(home, m.t.Wrap(pos))
			m.t.Enqueue(0, m.t.Wrap(pos))
			m.live[key] = value
			m.order = append(m.order, key)
			return m.t.Wrap(pos)
		}
	}
}

// lookup mirrors the lock-free reader's walk: tag compare, key compare, group-edge and HasNext exits.
func (m *model) lookup(key uint64) (uint64, int, bool) {
	hash := m.hash(key)
	tagged := TagOf(hash)
	home := m.t.Home(hash)
	groupEnd := (home | 7) + 1
	walk := 0
	for pos := home; ; pos++ {
		if pos == groupEnd && !m.t.OverflowedTag(home, OverflowBucket(hash)) {
			return 0, walk, false
		}
		item := m.t.At(pos)
		metaWord := item.Metadata()
		walk++
		if metaWord&DeadOrTag == tagged && item.Entry().Key == key {
			return item.Entry().Value, walk, true
		}
		if metaWord&HasNext == 0 {
			return 0, walk, false
		}
	}
}

// remove mirrors Cache.killAt: dequeue, kill, forget the overflow count, reclaim the slot.
func (m *model) remove(key uint64, scratch *[]ShiftEntry) {
	hash := m.hash(key)
	tagged := TagOf(hash)
	for pos := m.t.Home(hash); ; pos++ {
		item := m.t.At(pos)
		metaWord := item.Metadata()
		if metaWord&DeadOrTag == tagged && item.Entry().Key == key {
			m.t.Remove(m.t.Wrap(pos))
			item.Kill()
			m.t.ForgetDisplaced(hash, pos)
			m.t.Reclaim(m.t.Wrap(pos), scratch, &m.moves)
			delete(m.live, key)
			m.order = slices.DeleteFunc(m.order, func(k uint64) bool { return k == key })
			return
		}
		if metaWord&HasNext == 0 {
			panic("remove: key not found")
		}
	}
}

// check verifies the table against the oracle: every live key is found by a reader walk, queue 0 holds the live keys
// in insertion order with consistent links, no slot is dead, chain bits are never missing where a chain runs,
// overflow counters never under-count, and the occupied slot set is exactly what inserting the live keys into a
// fresh table produces (linear probing's occupied set is order-independent, which is what backward-shift deletion
// preserves).
func (m *model) check(t *testing.T) {
	t.Helper()
	size := uint32(m.t.Len())
	occupied := make([]bool, size)
	liveCount := 0
	for pos := uint32(0); pos < size; pos++ {
		metaWord := m.t.At(pos).Metadata()
		require.Zero(t, metaWord&Dead, "slot %d is dead after the reclaim", pos)
		if !Empty(metaWord) {
			occupied[pos] = true
			liveCount++
		}
	}
	require.Equal(t, len(m.live), liveCount, "live slot count")

	for key, want := range m.live {
		got, _, ok := m.lookup(key)
		require.True(t, ok, "key %d is live but the probe missed it", key)
		require.Equal(t, want, got, "key %d", key)
	}
	var listed []uint64
	for idx, ok := m.t.Oldest(0); ok; idx, ok = m.t.Newer(idx) {
		require.Equal(t, idx, m.t.queueLinkAt(m.t.queueLinkAt(idx).next).prev, "slot %d: its successor does not point back", idx)
		listed = append(listed, m.t.At(idx).Entry().Key)
	}
	require.True(t, slices.Equal(m.order, listed), "queue 0 is %v, want the insertion order %v", listed, m.order)

	// Chain bits: a slot some live key's chain runs through must carry HasNext. Overflow counters: at least the
	// number of keys displaced past their group, per tag bucket, saturating at overflowMax.
	needBit := make([]bool, size)
	displaced := map[[2]uint32]uint32{}
	for pos := uint32(0); pos < size; pos++ {
		if !occupied[pos] {
			continue
		}
		hash := m.hash(m.t.At(pos).Entry().Key)
		home := m.t.Home(hash)
		steps := m.t.Dist(home, pos)
		// The meta word carries that same distance, which is what lets a compaction move an item without its hash.
		require.Equal(t, min(steps, DispMax), DispOf(m.t.At(pos).Metadata()),
			"slot %d: the stored displacement disagrees with the key's home", pos)
		require.Equal(t, OverflowBucket(hash), BucketOf(m.t.At(pos).Metadata()),
			"slot %d: the overflow bucket is not recoverable from the tag", pos)
		require.Equal(t, uint64(home)<<32|TagOf(hash)>>TagShift, m.t.Fingerprint(pos), "slot %d: fingerprint", pos)
		for d := uint32(0); d < steps; d++ {
			needBit[m.t.Wrap(home+d)] = true
		}
		if home>>groupShift != pos>>groupShift {
			displaced[[2]uint32{home >> groupShift, OverflowBucket(hash)}]++
		}
	}
	for pos := uint32(0); pos < size; pos++ {
		if needBit[pos] {
			require.NotZero(t, m.t.At(pos).Metadata()&HasNext, "slot %d lies on a live chain but its bit is clear", pos)
		}
	}
	for gb, n := range displaced {
		got := m.t.overflowAt(gb[0]<<groupShift).Load() >> (gb[1] * overflowWidth) & overflowMax
		require.GreaterOrEqual(t, got, min(n, overflowMax), "group %d bucket %d under-counts its displaced keys", gb[0], gb[1])
	}

	// The occupied set of a fresh table holding the same keys.
	fresh := NewFlatHashMap[uint64, uint64](int(size), m.h)
	for key, value := range m.live {
		hash := m.hash(key)
		for pos := fresh.Home(hash); ; pos++ {
			if item := fresh.At(pos); Empty(item.Metadata()) {
				item.Publish(Entry[uint64, uint64]{Key: key, Value: value}, TagOf(hash), 0, fresh.Dist(fresh.Home(hash), fresh.Wrap(pos)))
				break
			}
		}
	}
	for pos := uint32(0); pos < size; pos++ {
		require.Equal(t, occupied[pos], !Empty(fresh.At(pos).Metadata()),
			"slot %d: the reclaimed table and a fresh one holding the same keys disagree", pos)
	}
}

// TestReclaimMatchesFreshTable churns a small table at high load through random inserts and deletes and checks the
// full state after every operation, wraps included (a 64-slot table at 0.8 wraps constantly).
func TestReclaimMatchesFreshTable(t *testing.T) {
	for _, slots := range []int{64, 256} {
		m := newModel(slots, uint64(slots))
		var scratch []ShiftEntry
		target := slots * 4 / 5
		next := uint64(1)
		for step := 0; step < 4000; step++ {
			if len(m.live) < target || m.rng.IntN(3) == 0 && len(m.live) > 0 {
				if len(m.live) < target && m.rng.IntN(2) == 0 || len(m.live) == 0 {
					m.insert(next, next*7)
					next++
				} else {
					for key := range m.live {
						m.remove(key, &scratch)
						break
					}
				}
			} else {
				for key := range m.live {
					m.remove(key, &scratch)
					break
				}
			}
			m.check(t)
		}
	}
}

// TestReclaimAcrossChunkBoundary drives runs that cross the seam between two 64Ki-slot chunks: keys homed just before
// the boundary are pushed past it, then deleted in every order.
func TestReclaimAcrossChunkBoundary(t *testing.T) {
	m := newModel(2*chunkSlots, 7)
	var scratch []ShiftEntry
	// Keys homed in the last group before the seam, found by rejection sampling.
	var keys []uint64
	for key := uint64(1); len(keys) < 12; key++ {
		if home := m.t.Home(m.hash(key)); home >= chunkSlots-8 && home < chunkSlots {
			keys = append(keys, key)
		}
	}
	for _, key := range keys {
		m.insert(key, key)
	}
	m.check(t)
	for len(m.live) > 0 {
		pick := keys[m.rng.IntN(len(keys))]
		if _, ok := m.live[pick]; !ok {
			continue
		}
		m.remove(pick, &scratch)
		m.check(t)
	}
}

// TestReclaimUncoveredHoleIsCheap pins the common case: a key sitting at its home with nothing chained past it
// leaves an empty slot behind without touching its neighbors.
func TestReclaimUncoveredHoleIsCheap(t *testing.T) {
	m := newModel(64, 3)
	var scratch []ShiftEntry
	idx := m.insert(1, 1)
	require.Zero(t, m.t.At(idx).Metadata()&HasNext)
	m.remove(1, &scratch)
	require.True(t, Empty(m.t.At(idx).Metadata()))
	require.Empty(t, scratch, "an uncovered hole needs no run scan")
	m.check(t)
}

// TestClearAdvancesGeneration: a slot cleared and refilled must not validate a snapshot of its previous occupant,
// even when the new one carries the same tag.
func TestClearAdvancesGeneration(t *testing.T) {
	item := NewFlatHashMap[uint64, uint64](64, maphash.MakeSeed()).At(0)
	item.Publish(Entry[uint64, uint64]{Key: 1, Value: 1}, TagOfZero, 0, 0)
	before := item.Metadata()
	item.Kill()
	item.Clear()
	require.True(t, Empty(item.Metadata()))
	item.Publish(Entry[uint64, uint64]{Key: 2, Value: 2}, TagOfZero, 0, 0)
	_, ok := item.Snapshot(before)
	require.False(t, ok, "a snapshot of the old occupant validated against the new one")
}
