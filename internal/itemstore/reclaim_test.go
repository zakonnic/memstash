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
	next  uint64 // the last key homed handed out
	// The last reclaim: its slot and how many slots its walk took in, the hole included. check wants exact chain
	// bits there.
	hole, walked uint32
}

func newModel(slots int, seed uint64) *model {
	h := maphash.MakeSeed()
	return &model{t: NewFlatHashMap[uint64, uint64](slots, h), h: h, live: map[uint64]uint64{},
		rng: rand.New(rand.NewPCG(seed, seed^0x9E37))}
}

func (m *model) hash(key uint64) uint64 { return maphash.Comparable(m.h, key) }

// homed returns a key it has not handed out yet whose home is the given slot.
func (m *model) homed(home uint32) uint64 {
	for {
		if m.next++; m.t.Home(m.hash(m.next)) == home {
			return m.next
		}
	}
}

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
func (m *model) remove(key uint64, scratch *[]int64) {
	hash := m.hash(key)
	tagged := TagOf(hash)
	for pos := m.t.Home(hash); ; pos++ {
		item := m.t.At(pos)
		metaWord := item.Metadata()
		if metaWord&DeadOrTag == tagged && item.Entry().Key == key {
			m.t.Remove(m.t.Wrap(pos))
			item.Kill()
			m.t.ForgetDisplaced(hash, pos)
			m.hole, m.walked = m.t.Wrap(pos), 1
			for metaWord := item.Metadata(); metaWord&HasNext != 0; m.walked++ {
				if metaWord = m.t.At(m.hole + m.walked).Metadata(); Empty(metaWord) {
					break
				}
			}
			m.t.Reclaim(m.hole, scratch, &m.moves)
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
// in insertion order with consistent links, no slot is dead, chain bits are never missing where a chain runs and
// exact where the last reclaim walked, overflow counters never under-count, and the occupied slot set is exactly what
// inserting the live keys into a fresh table produces (linear probing's occupied set is order-independent, which is
// what backward-shift deletion preserves).
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
		if home>>groupShift != pos>>groupShift || pos < home { // past the group's edge on the key's walk
			displaced[[2]uint32{home >> groupShift, OverflowBucket(hash)}]++
		}
	}
	for pos := uint32(0); pos < size; pos++ {
		if needBit[pos] {
			require.NotZero(t, m.t.At(pos).Metadata()&HasNext, "slot %d lies on a live chain but its bit is clear", pos)
		}
	}
	// A reclaim leaves no stale bit on the slots it walked, nor on the one behind its hole. Inserts keep that.
	for d := uint32(0); d <= m.walked; d++ {
		pos := m.t.Wrap(m.t.back(m.hole, 1) + d)
		require.Equal(t, needBit[pos], m.t.At(pos).Metadata()&HasNext != 0,
			"slot %d, %d from the reclaimed slot %d: a stale chain bit", pos, int(d)-1, m.hole)
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
		var scratch []int64
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
	var scratch []int64
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
	var scratch []int64
	idx := m.insert(1, 1)
	require.Zero(t, m.t.At(idx).Metadata()&HasNext)
	m.remove(1, &scratch)
	require.True(t, Empty(m.t.At(idx).Metadata()))
	require.Zero(t, cap(scratch), "an uncovered hole needs no run scan")
	m.check(t)
}

// TestReclaimClearsTheBitBehindTheHole: the dead key may have been the last one chained past the slot before it.
func TestReclaimClearsTheBitBehindTheHole(t *testing.T) {
	m := newModel(64, 4)
	var scratch []int64
	a, b := m.homed(8), m.homed(8) // slots 8 and 9
	m.insert(a, a)
	m.insert(b, b)
	require.NotZero(t, m.t.At(8).Metadata()&HasNext)
	m.remove(b, &scratch)
	require.Zero(t, m.t.At(8).Metadata()&HasNext, "nothing is chained past slot 8 any more")
	m.check(t)
}

// TestReclaimClearsStaleChainBits: a chain bit no key earns only makes the reclaim walk further. The hole still ends
// in the first slot nothing is chained past, and the bits the walk crossed come out clear. Three shapes: the walk
// ends on a live slot whose bit is clear, on an empty slot right after the last hole, and on an empty slot after a
// live one that keeps its place.
func TestReclaimClearsStaleChainBits(t *testing.T) {
	m := newModel(64, 5)
	var scratch []int64
	chained := func(slots ...uint32) (bits []bool) {
		for _, slot := range slots {
			bits = append(bits, m.t.At(slot).Metadata()&HasNext != 0)
		}
		return bits
	}

	a, b := m.homed(8), m.homed(8) // slots 8 and 9
	m.insert(a, a)
	m.insert(b, b)
	for _, home := range []uint32{10, 11} {
		key := m.homed(home)
		m.insert(key, key)
	}
	m.t.At(9).MarkHasNext()
	m.t.At(10).MarkHasNext()
	m.remove(a, &scratch)
	require.Equal(t, b, m.t.At(8).Entry().Key, "the key chained past the hole must move in")
	require.Equal(t, []bool{false, false, false, false}, chained(8, 9, 10, 11))
	m.check(t)

	e, f := m.homed(20), m.homed(20) // slots 20 and 21, slot 22 stays empty
	m.insert(e, e)
	m.insert(f, f)
	m.t.At(21).MarkHasNext()
	m.remove(e, &scratch)
	require.Equal(t, f, m.t.At(20).Entry().Key, "the key chained past the hole must move in")
	require.Equal(t, []bool{false, false}, chained(20, 21))
	m.check(t)

	g, h := m.homed(30), m.homed(30) // slots 30 and 31
	m.insert(g, g)
	m.insert(h, h)
	stays := m.homed(32) // slot 32, slot 33 stays empty
	m.insert(stays, stays)
	m.t.At(31).MarkHasNext()
	m.t.At(32).MarkHasNext()
	m.remove(g, &scratch)
	require.Equal(t, h, m.t.At(30).Entry().Key, "the key chained past the hole must move in")
	require.Equal(t, stays, m.t.At(32).Entry().Key)
	require.Equal(t, []bool{false, false, false}, chained(30, 31, 32))
	m.check(t)
}

// TestWrappedKeyCountsAsDisplaced: in a table with one free slot a key can walk all the way around and settle in its
// home group, before its home. A probe leaves that group at its edge, so the overflow counter has to count the key,
// or the probe stops there and misses it. The count follows the key through a compaction and goes when it does.
func TestWrappedKeyCountsAsDisplaced(t *testing.T) {
	m := newModel(16, 9)
	var scratch []int64
	var atHome [16]uint64
	for home := range uint32(16) {
		if home != 4 {
			atHome[home] = m.homed(home)
			m.insert(atHome[home], atHome[home])
		}
	}
	wrapped := m.homed(5)
	require.EqualValues(t, 4, m.insert(wrapped, wrapped), "the only free slot lies behind the key's home, in its group")
	m.check(t)

	m.remove(atHome[0], &scratch)
	require.Equal(t, wrapped, m.t.At(0).Entry().Key, "the wrapped key is the only one chained past slot 0")
	m.check(t)
	m.remove(wrapped, &scratch)
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
