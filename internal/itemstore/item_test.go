package itemstore

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// TestItemSize pins the item footprints: the meta word plus the Entry and nothing else.
func TestItemSize(t *testing.T) {
	assert.EqualValues(t, 24, unsafe.Sizeof(Item[uint64, uint64]{}))
	assert.EqualValues(t, 48, unsafe.Sizeof(Item[string, []byte]{}))
}

// TestSnapshotSeqlock: a multi-word overwrite must invalidate a snapshot taken against the pre-overwrite meta word,
// a single-word one must not bump the generation at all.
func TestSnapshotSeqlock(t *testing.T) {
	item := NewFlatHashMap[string, string](64).At(0)
	item.Publish(Entry[string, string]{Key: "k", Value: "old"}, 0, 0)
	before := item.Metadata()

	item.SetValue("new")
	assert.Equal(t, before+2, item.Metadata(), "a multi-word overwrite must advance the generation twice")
	_, ok := item.Snapshot(before)
	assert.False(t, ok, "a snapshot against the pre-overwrite meta word must fail validation")
	entry, ok := item.Snapshot(item.Metadata())
	assert.True(t, ok)
	assert.Equal(t, "new", entry.Value)

	word := NewFlatHashMap[uint64, uint64](64).At(0)
	word.Publish(Entry[uint64, uint64]{Key: 1, Value: 10}, 0, 0)
	wordBefore := word.Metadata()
	word.SetValue(20)
	assert.Equal(t, wordBefore, word.Metadata(), "a single-word overwrite must not disturb the meta word")
	wordEntry, ok := word.Snapshot(wordBefore)
	assert.True(t, ok)
	assert.EqualValues(t, 20, wordEntry.Value)
}

// TestSnapshotRejectsWriteInProgress covers the reader that lives entirely inside one write window: an odd meta word
// must fail validation even though it never changed.
func TestSnapshotRejectsWriteInProgress(t *testing.T) {
	item := NewFlatHashMap[string, string](64).At(0)
	item.Publish(Entry[string, string]{Key: "k", Value: "v"}, 0, 0)

	item.beginWrite()
	mid := item.Metadata()
	assert.NotZero(t, mid&1, "beginWrite must leave the generation odd")
	_, ok := item.Snapshot(mid)
	assert.False(t, ok, "a snapshot inside a write window must fail even with a stable meta word")
	item.endWrite()
	assert.Zero(t, item.Metadata()&1, "endWrite must settle the generation even")
}

// TestGenWrapNeverLooksEmpty covers the generation wrapping on a item that carries nothing else: no TTL and a zero
// tag leave gen as the only non-zero field, so landing on 0 would make a live item read as a never-occupied slot.
func TestGenWrapNeverLooksEmpty(t *testing.T) {
	lastEven := GenMask - 1

	item := NewFlatHashMap[string, string](64).At(0)
	item.meta.Store(lastEven)
	item.Publish(Entry[string, string]{Key: "k", Value: "v"}, 0, 0)
	assert.NotZero(t, item.Metadata(), "a wrapped occupancy must not publish an empty-slot meta word")

	item.meta.Store(lastEven)
	item.SetValue("wrapped")
	assert.NotZero(t, item.Metadata(), "a wrapped in-place write must not leave an empty-slot meta word")
	assert.Zero(t, item.Metadata()&1, "endWrite must settle the generation even")
}
