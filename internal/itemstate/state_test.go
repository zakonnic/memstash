package itemstate

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// TestRecordSize pins the record footprints: the meta word plus the Entry and nothing else, so chunks land on exact
// malloc size classes.
func TestRecordSize(t *testing.T) {
	assert.EqualValues(t, 24, unsafe.Sizeof(State[uint64, uint64]{}))
	assert.EqualValues(t, 48, unsafe.Sizeof(State[string, []byte]{}))
	assert.EqualValues(t, 12*1024, unsafe.Sizeof(poolChunk[uint64, uint64]{}))
	assert.EqualValues(t, 24*1024, unsafe.Sizeof(poolChunk[string, []byte]{}))
}

// TestSnapshotSeqlock: a multi-word overwrite must invalidate a snapshot taken against the pre-overwrite meta word,
// a single-word one must not bump the generation at all.
func TestSnapshotSeqlock(t *testing.T) {
	var p Pool[string, string]
	record, _, _ := p.Claim("k", "old", 0)
	before := record.Load()

	record.SetValue("new")
	assert.Equal(t, before+2, record.Load(), "a multi-word overwrite must advance the generation twice")
	_, ok := record.Snapshot(before)
	assert.False(t, ok, "a snapshot against the pre-overwrite meta word must fail validation")
	entry, ok := record.Snapshot(record.Load())
	assert.True(t, ok)
	assert.Equal(t, "new", entry.Value)

	var pw Pool[uint64, uint64]
	word, _, _ := pw.Claim(1, 10, 0)
	wordBefore := word.Load()
	word.SetValue(20)
	assert.Equal(t, wordBefore, word.Load(), "a single-word overwrite must not disturb the meta word")
	wordEntry, ok := word.Snapshot(wordBefore)
	assert.True(t, ok)
	assert.EqualValues(t, 20, wordEntry.Value)
}

// TestSnapshotRejectsWriteInProgress covers the reader that lives entirely inside one write window: an odd meta word
// must fail validation even though it never changed.
func TestSnapshotRejectsWriteInProgress(t *testing.T) {
	var p Pool[string, string]
	record, _, _ := p.Claim("k", "v", 0)

	record.beginWrite()
	mid := record.Load()
	assert.NotZero(t, mid&1, "beginWrite must leave the generation odd")
	_, ok := record.Snapshot(mid)
	assert.False(t, ok, "a snapshot inside a write window must fail even with a stable meta word")
	record.endWrite()
	assert.Zero(t, record.Load()&1, "endWrite must settle the generation even")
}
