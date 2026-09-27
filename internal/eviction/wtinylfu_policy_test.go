package eviction

import (
	"fmt"
	"slices"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash/internal/itemstore"
)

// TestFreqSketch exercises the Count-Min sketch directly: saturation, the minimum estimate, aging and the disabled
// zero-size mode.
func TestFreqSketch(t *testing.T) {
	s := newFreqSketch(1024)
	const hash = 0xdeadbeefcafebabe

	assert.Zero(t, s.estimate(hash), "a fresh sketch estimates 0")
	for i := 1; i <= 5; i++ {
		s.increment(hash)
		assert.EqualValues(t, i, s.estimate(hash), "the estimate must follow the increments")
	}
	for i := 0; i < 50; i++ {
		s.increment(hash)
	}
	assert.EqualValues(t, counterMax, s.estimate(hash), "counters must saturate at 15")

	s.reset()
	assert.EqualValues(t, counterMax/2, s.estimate(hash), "aging must halve the counters")

	disabled := newFreqSketch(0)
	disabled.increment(hash) // must not panic
	assert.Zero(t, disabled.estimate(hash))
	assert.Zero(t, disabled.bytes())
}

// TestFreqSketchAgingBySampleWindow verifies the automatic reset: once the sample window fills, the counters halve.
func TestFreqSketchAgingBySampleWindow(t *testing.T) {
	s := newFreqSketch(1024)
	const hot = 0x1234567890abcdef

	for i := 0; i < 10; i++ {
		s.increment(hot)
	}
	require.EqualValues(t, 10, s.estimate(hot))
	// Drive unrelated keys until additions drops - the signature of a reset having fired.
	prev := uint32(0)
	for i := uint64(0); s.additions >= prev && i < 1<<22; i++ {
		prev = s.additions
		s.increment((i + 1) * 0x9e3779b97f4a7c15)
	}
	assert.Less(t, s.additions, prev, "the sample window must have rolled over")
	assert.Less(t, s.estimate(hot), uint32(10), "the hot key's counters must have been halved by aging")
}

// TestWTinyLFUAdmission verifies the filter end to end: a low-frequency window candidate is rejected (and becomes
// the victim) instead of displacing a higher-frequency main resident.
func TestWTinyLFUAdmission(t *testing.T) {
	pool := newArena()
	p := NewWTinyLFU(&pool.items, 300, 1024) // windowCap = 3

	pool.add(p, "resident")
	pool.touch("resident") // reused in the window: will be admitted to main outright
	for i := range 4 {
		pool.add(p, fmt.Sprintf("one-hit-%d", i))
	}

	// windowWeight 5 > 3: Evict promotes the touched resident to main (recording its reuse in the sketch), then the
	// next candidate "one-hit-0" (frequency 1) loses the admission duel against the resident (frequency 2).
	key, ok := pool.evict(p)
	require.True(t, ok)
	assert.Equal(t, "one-hit-0", key, "the rejected candidate must be the victim")
	assert.Equal(t, []string{"resident"}, pool.queued(mainQueue), "main must hold exactly the admitted resident")
	assert.EqualValues(t, 3, p.weight)
}

// TestWTinyLFUAdmitsFrequentKey verifies the other side of the filter: a candidate whose key was seen more often
// than main's next victim is admitted.
func TestWTinyLFUAdmitsFrequentKey(t *testing.T) {
	pool := newArena()
	p := NewWTinyLFU(&pool.items, 300, 1024) // windowCap = 3

	// Seat a low-frequency resident in main: the first window overflow admits it (main is empty), and the untouched
	// filler behind it loses the 1-vs-1 duel and is evicted.
	pool.add(p, "seat")
	for i := range 4 { // enough that the window stays over budget after the seat's own promotion
		pool.add(p, fmt.Sprintf("filler-%d", i))
	}
	key, ok := pool.evict(p)
	require.True(t, ok)
	assert.Equal(t, "filler-0", key, "the 1-vs-1 admission duel must keep the seated resident")
	require.Equal(t, []string{"seat"}, pool.queued(mainQueue))

	// Pump the hot key's frequency and push it through the window under sustained pressure: low-frequency chaff is
	// rejected at admission, the hot key must pass into main.
	hot := pool.claim("hot")
	for range 4 {
		p.sketch.increment(p.keyHash(hot))
	}
	p.Add(itemstore.QNode{Idx: hot, Cost: 1})
	for i := 0; i < 20 && !slices.Contains(pool.queued(mainQueue), "hot"); i++ {
		pool.add(p, fmt.Sprintf("chaff-%d", i)) // keeps the window over budget
		_, ok := pool.evict(p)
		require.True(t, ok, "pressure eviction %d", i)
	}
	assert.Contains(t, pool.queued(mainQueue), "hot", "the frequent candidate must be admitted to main")
}

func TestWTinyLFUBytes(t *testing.T) {
	pool := newArena()
	p := NewWTinyLFU(&pool.items, 1000, 1024)
	assert.Equal(t, int64(unsafe.Sizeof(*p))+p.sketch.bytes(), p.Bytes())
	assert.Positive(t, p.sketch.bytes(), "the sketch is allocated up front")
}
