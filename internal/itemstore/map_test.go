package itemstore

import (
	"hash/maphash"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFlatHashMapChunkedSlots pins the chunked addressing: a table spanning several chunks still hands out one
// distinct slot per index, and positions past the last slot wrap to the front.
func TestFlatHashMapChunkedSlots(t *testing.T) {
	const slots = 2 * chunkSlots
	table := NewFlatHashMap[uint32, uint32](slots, maphash.MakeSeed())
	require.Len(t, table.chunks, 2)
	require.Empty(t, table.short, "a chunked table keeps nothing in the flat run")

	for i := range uint32(slots) {
		table.At(i).Publish(Entry[uint32, uint32]{Key: i, Value: i}, 0, 0, 0)
	}
	for i := range uint32(slots) {
		if got := table.At(i).Entry().Key; got != i {
			t.Fatalf("slot %d holds key %d: chunks overlap or the split is off", i, got)
		}
	}

	assert.Same(t, table.At(0), table.At(slots), "a position past the last slot wraps to the front")
	assert.Same(t, table.At(chunkSlots), table.At(slots+chunkSlots), "the wrap lands in the right chunk")
}

// TestFlatHashMapShortTable covers a table too small for a chunk: one flat run of exactly that many slots.
func TestFlatHashMapShortTable(t *testing.T) {
	const slots = 64
	table := NewFlatHashMap[uint32, uint32](slots, maphash.MakeSeed())
	require.Empty(t, table.chunks, "a table this size stays one flat run")
	require.Len(t, table.short, slots)

	for i := range uint32(slots) {
		table.At(i).Publish(Entry[uint32, uint32]{Key: i, Value: i}, 0, 0, 0)
	}
	for i := range uint32(slots) {
		require.EqualValues(t, i, table.At(i).Entry().Key)
	}
	assert.Same(t, table.At(1), table.At(slots+1))
}

// TestOverflowTagBuckets pins the split overflow word: counters are independent per tag bucket, they come back down,
// and a saturated one sticks non-zero rather than wrapping into its neighbor.
func TestOverflowTagBuckets(t *testing.T) {
	const slots = 64
	table := NewFlatHashMap[uint32, uint32](slots, maphash.MakeSeed())

	// Two hashes homed in group 0 but landing in different tag buckets, plus a position outside that group.
	var a, b uint64 = 1 << 14, 2 << 14
	require.EqualValues(t, 0, table.Home(a))
	require.EqualValues(t, 0, table.Home(b))
	require.NotEqual(t, OverflowBucket(a), OverflowBucket(b))
	const outside uint32 = 8

	table.NoteDisplaced(a, outside)
	assert.True(t, table.OverflowedTag(0, OverflowBucket(a)))
	assert.False(t, table.OverflowedTag(0, OverflowBucket(b)), "one tag's overflow must not answer for another")

	table.ForgetDisplaced(a, outside)
	assert.False(t, table.OverflowedTag(0, OverflowBucket(a)), "the counter must come back down")

	table.NoteDisplaced(a, 1)
	assert.False(t, table.OverflowedTag(0, OverflowBucket(a)), "a key inside its home group is not displaced")

	for range overflowMax + 4 {
		table.NoteDisplaced(a, outside)
	}
	for range overflowMax + 4 {
		table.ForgetDisplaced(a, outside)
	}
	assert.True(t, table.OverflowedTag(0, OverflowBucket(a)), "a saturated counter sticks: over-walk, never a miss")
	assert.False(t, table.OverflowedTag(0, OverflowBucket(b)), "saturation must not spill into the next bucket")
}
