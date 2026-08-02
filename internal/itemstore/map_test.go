package itemstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFlatHashMapChunkedSlots pins the chunked addressing: a table spanning several chunks still hands out one
// distinct slot per index, and positions past the last slot wrap to the front.
func TestFlatHashMapChunkedSlots(t *testing.T) {
	const slots = 2 * chunkSlots
	table := NewFlatHashMap[uint32, uint32](slots)
	require.Len(t, table.chunks, 2)
	require.Empty(t, table.short, "a chunked table keeps nothing in the flat run")

	for i := range uint32(slots) {
		table.At(i).Publish(Entry[uint32, uint32]{Key: i, Value: i}, 0, 0)
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
	table := NewFlatHashMap[uint32, uint32](slots)
	require.Empty(t, table.chunks, "a table this size stays one flat run")
	require.Len(t, table.short, slots)

	for i := range uint32(slots) {
		table.At(i).Publish(Entry[uint32, uint32]{Key: i, Value: i}, 0, 0)
	}
	for i := range uint32(slots) {
		require.EqualValues(t, i, table.At(i).Entry().Key)
	}
	assert.Same(t, table.At(1), table.At(slots+1))
}
