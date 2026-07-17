//go:build !race && amd64

package itemstate

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// TestInlineRecordSize pins the record footprints: the meta word plus the Entry and nothing else, landing chunks on
// exact malloc size classes.
func TestInlineRecordSize(t *testing.T) {
	assert.EqualValues(t, 24, unsafe.Sizeof(State[uint64, uint64]{}))
	assert.EqualValues(t, 48, unsafe.Sizeof(State[string, []byte]{}))
	assert.EqualValues(t, 3072, unsafe.Sizeof(poolChunk[uint64, uint64]{}))
	assert.EqualValues(t, 6144, unsafe.Sizeof(poolChunk[string, []byte]{}))
}

// TestInlineSnapshotSeqlock exercises the seqlock edges: a multi-word overwrite must invalidate a snapshot taken
// against the pre-overwrite meta word, while a single-word overwrite needs no generation bump at all.
func TestInlineSnapshotSeqlock(t *testing.T) {
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
