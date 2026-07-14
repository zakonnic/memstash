package eviction

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash/internal/itemstate"
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
	var pool itemstate.Pool[string, string]
	p := NewWTinyLFU(&pool, 300, 1024) // windowCap = 3

	residentIdx := addFromPool(p, &pool, "resident")
	touch(&pool, residentIdx) // reused in the window: will be admitted to main outright
	for i := 0; i < 4; i++ {
		addFromPool(p, &pool, fmt.Sprintf("one-hit-%d", i))
	}

	// windowWeight 5 > 3: Evict promotes the touched resident to main (recording its reuse in the sketch), then the
	// next candidate "one-hit-0" (frequency 1) loses the admission duel against the resident (frequency 2).
	idx, ok := p.Evict(0)
	require.True(t, ok)
	assert.Equal(t, "one-hit-0", pool.At(idx).Entry().Key, "the rejected candidate must be the victim")
	pool.Release(idx)

	assert.Equal(t, 1, p.main.Len(), "main must hold exactly the admitted resident")
	victim, ok := p.main.Peek()
	require.True(t, ok)
	assert.Equal(t, "resident", pool.At(victim.Idx).Entry().Key)
	assert.Zero(t, pool.At(residentIdx).Load()&itemstate.Dead, "the protected resident must stay alive")
}

// TestWTinyLFUAdmitsFrequentKey verifies the other side of the filter: a candidate whose key was seen more often
// than main's next victim is admitted.
func TestWTinyLFUAdmitsFrequentKey(t *testing.T) {
	var pool itemstate.Pool[string, string]
	p := NewWTinyLFU(&pool, 300, 1024) // windowCap = 3

	// Seat a low-frequency resident in main: the first window overflow admits it (main is empty), and the untouched
	// filler behind it loses the 1-vs-1 duel and is evicted.
	addFromPool(p, &pool, "seat")
	for i := 0; i < 4; i++ { // enough that the window stays over budget after the seat's own pop
		addFromPool(p, &pool, fmt.Sprintf("filler-%d", i))
	}
	idx, ok := p.Evict(0)
	require.True(t, ok)
	assert.Equal(t, "filler-0", pool.At(idx).Entry().Key, "the 1-vs-1 admission duel must keep the seated resident")
	pool.Release(idx)
	require.Equal(t, 1, p.main.Len())

	// Pump the hot key's frequency and push it through the window under sustained pressure: low-frequency chaff is
	// rejected at admission, the hot key must pass into main.
	for i := 0; i < 4; i++ {
		p.sketch.increment(p.keyHash("hot"))
	}
	hotIdx := addFromPool(p, &pool, "hot")
	inMain := func(key string) bool {
		found := false
		p.main.Range(func(node itemstate.QNode) { found = found || pool.At(node.Idx).Entry().Key == key })
		return found
	}
	for i := 0; i < 20 && !inMain("hot"); i++ {
		addFromPool(p, &pool, fmt.Sprintf("chaff-%d", i)) // keeps the window over budget
		idx, ok := p.Evict(0)
		require.True(t, ok, "pressure eviction %d", i)
		pool.Release(idx)
	}
	assert.True(t, inMain("hot"), "the frequent candidate must be admitted to main")
	assert.Zero(t, pool.At(hotIdx).Load()&itemstate.Dead, "the admitted key must stay alive")
}

// TestWTinyLFUBytes verifies Bytes is the sum of its parts, sketch included.
func TestWTinyLFUBytes(t *testing.T) {
	var pool itemstate.Pool[string, string]
	p := NewWTinyLFU(&pool, 1000, 1024)
	structBytes := int64(unsafe.Sizeof(*p))
	// The sketch pre-allocates at construction, like the S3-FIFO ghost table.
	assert.Equal(t, structBytes+p.sketch.bytes(), p.Bytes())
	assert.Positive(t, p.sketch.bytes())

	for i := 0; i < 200; i++ {
		addFromPool(p, &pool, fmt.Sprintf("k%d", i))
	}
	assert.Equal(t, structBytes+p.window.Bytes()+p.main.Bytes()+p.sketch.bytes(), p.Bytes())
}
