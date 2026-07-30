package itemstore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSetAgainstMap cross-checks Set against a plain map on a mix of present and absent keys, at sizes that force
// several rehashes past the preallocated capacity.
func TestSetAgainstMap(t *testing.T) {
	for _, count := range []int{0, 1, 7, 8, 9, 1000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			set := NewSet[string](count)
			want := make(map[string]struct{}, count)
			for i := range count {
				key := fmt.Sprintf("k%05d", i)
				set.Add(key)
				want[key] = struct{}{}
			}
			for key := range want {
				assert.True(t, set.Exists(key), "added key %q reported absent", key)
			}
			for i := count; i < count+100; i++ {
				assert.False(t, set.Exists(fmt.Sprintf("k%05d", i)), "never-added key reported present")
			}
		})
	}
}

// TestSetZeroValue covers the empty table: Exists must answer instead of indexing into nil slots, and Add must grow
// into one.
func TestSetZeroValue(t *testing.T) {
	var set Set[int]
	assert.False(t, set.Exists(1))

	set.Add(1)
	assert.True(t, set.Exists(1))
	assert.False(t, set.Exists(2))
}

// TestSetAddIsIdempotent checks that repeats do not consume slots - a table that keeps its preallocated size proves
// they collapsed rather than piling up until a rehash.
func TestSetAddIsIdempotent(t *testing.T) {
	set := NewSet[int](4)
	slots := len(set.slots)
	require.NotZero(t, slots)

	for range 1000 {
		set.Add(42)
	}
	assert.True(t, set.Exists(42))
	assert.Equal(t, slots, len(set.slots), "duplicate Adds grew the table")
}

// TestSetGrowKeepsEverything hammers the smallest possible table so that growth runs many times over, re-adding keys
// that already moved once.
func TestSetGrowKeepsEverything(t *testing.T) {
	set := NewSet[int](1)
	for i := range 10_000 {
		set.Add(i)
	}
	for i := range 10_000 {
		require.True(t, set.Exists(i), "key %d lost across rehashes", i)
	}
	assert.False(t, set.Exists(10_000))
}

// TestSetLenCountsDistinct pins Len to distinct keys - batchLoad sizes a slice off it, so repeats must not inflate it.
func TestSetLenCountsDistinct(t *testing.T) {
	set := NewSet[string](8)
	assert.Zero(t, set.Len())
	for range 5 {
		set.Add("a")
		set.Add("b")
	}
	assert.Equal(t, 2, set.Len())
}

// TestNewSetHoldsCapacity is NewSet's documented promise: capacity keys go in without a rehash. A probe-length bound
// used to break this above ~2k keys while the table was still half empty.
func TestNewSetHoldsCapacity(t *testing.T) {
	for _, capacity := range []int{8, 128, 2048, 32768} {
		set := NewSet[string](capacity)
		sized := len(set.slots)
		for i := range capacity {
			set.Add(fmt.Sprintf("user:profile:%019d", i))
		}
		assert.Equal(t, sized, len(set.slots), "capacity=%d rehashed although it was sized for the keys", capacity)
		assert.Equal(t, capacity, set.Len(), "capacity=%d lost or double-counted keys", capacity)
	}
}

// TestNewSetFromZeroAlloc is the whole point of the buffer constructor: a fill-then-query cycle that stays within the
// stack buffer must not touch the heap.
func TestNewSetFromZeroAlloc(t *testing.T) {
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	allocs := testing.AllocsPerRun(100, func() {
		var buf [64]SetSlot[string]
		set := NewSetFrom(buf[:])
		for _, key := range keys {
			set.Add(key)
		}
		for _, key := range keys {
			if !set.Exists(key) {
				panic("added key reported absent")
			}
		}
	})
	assert.Zero(t, allocs, "membership within the buffer must not allocate")
}

// TestNewSetFromSpill drives an undersized buffer past its capacity: growth must take over and lose nothing.
func TestNewSetFromSpill(t *testing.T) {
	var buf [8]SetSlot[int]
	set := NewSetFrom(buf[:])
	for i := range 1000 {
		set.Add(i)
	}
	for i := range 1000 {
		require.True(t, set.Exists(i), "key %d lost after spilling to the heap", i)
	}
	assert.False(t, set.Exists(1000))
}

// TestNewSetFromNonPowerOfTwo checks the prefix rounding: a length-100 buffer is used at its 64-slot prefix and still
// answers correctly. A nil buffer degrades to an empty, growable Set.
func TestNewSetFromNonPowerOfTwo(t *testing.T) {
	set := NewSetFrom(make([]SetSlot[int], 100))
	for i := range 30 {
		set.Add(i)
	}
	for i := range 30 {
		assert.True(t, set.Exists(i))
	}
	assert.False(t, set.Exists(30))

	empty := NewSetFrom[int](nil)
	assert.False(t, empty.Exists(1))
	empty.Add(1)
	assert.True(t, empty.Exists(1))
}
