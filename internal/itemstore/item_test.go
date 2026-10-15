package itemstore

import (
	"hash/maphash"
	"math/rand"
	"reflect"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestItemSize pins the item footprints: the meta word plus the Entry and nothing else. The meta word aligns the item
// to 8 bytes on 32-bit platforms too, so a string and a slice of five 4-byte words round up to 32.
func TestItemSize(t *testing.T) {
	assert.EqualValues(t, 24, unsafe.Sizeof(Item[uint64, uint64]{}))
	assert.EqualValues(t, (8+5*unsafe.Sizeof(uintptr(0))+7)&^7, unsafe.Sizeof(Item[string, []byte]{}))
}

// TestSnapshotSeqlock: SetValue must invalidate a snapshot taken against the pre-overwrite meta word, SetScalar must
// not touch the meta word at all.
func TestSnapshotSeqlock(t *testing.T) {
	item := NewFlatHashMap[string, string](64, maphash.MakeSeed()).At(0)
	item.Publish(Entry[string, string]{Key: "k", Value: "old"}, 0, 0, 0)
	before := item.Metadata()

	item.SetValue("new")
	assert.Equal(t, before+2, item.Metadata(), "a multi-word overwrite must advance the generation twice")
	_, ok := item.Snapshot(before)
	assert.False(t, ok, "a snapshot against the pre-overwrite meta word must fail validation")
	entry, ok := item.Snapshot(item.Metadata())
	assert.True(t, ok)
	assert.Equal(t, "new", entry.Value)

	word := NewFlatHashMap[uint64, int](64, maphash.MakeSeed()).At(0)
	word.Publish(Entry[uint64, int]{Key: 1, Value: 10}, 0, 0, 0)
	wordBefore := word.Metadata()
	word.SetScalar(20)
	assert.Equal(t, wordBefore, word.Metadata(), "a single-store overwrite must not disturb the meta word")
	wordEntry, ok := word.Snapshot(wordBefore)
	assert.True(t, ok)
	assert.EqualValues(t, 20, wordEntry.Value)
	word.SetValue(30)
	assert.Equal(t, wordBefore+2, word.Metadata(), "SetValue brackets whatever V is")
}

// TestOneStore pins which values may skip the seqlock bracket. A false "true" lets a reader validate a torn value,
// as an 8-byte string on a 32-bit platform did up to 0.9.8; a false "false" only costs the bracket.
func TestOneStore(t *testing.T) {
	type halves struct{ A, B uint32 }
	type paddedHalves struct {
		_    [0]uint64 // word-sized and word-aligned, still two stores
		A, B uint32
	}
	type wrapped struct {
		_ [0]func()
		P *int
	}
	words64 := unsafe.Sizeof(uintptr(0)) == 8
	for _, tc := range []struct {
		t    reflect.Type
		want bool
	}{
		{reflect.TypeFor[int](), true},
		{reflect.TypeFor[uint32](), true},
		{reflect.TypeFor[bool](), true},
		{reflect.TypeFor[*int](), true},
		{reflect.TypeFor[map[int]int](), true},
		{reflect.TypeFor[func()](), true},
		{reflect.TypeFor[chan int](), true},
		{reflect.TypeFor[unsafe.Pointer](), true},
		{reflect.TypeFor[[1]uintptr](), true},
		{reflect.TypeFor[wrapped](), true},
		{reflect.TypeFor[struct{}](), true},
		{reflect.TypeFor[uint64](), words64},
		{reflect.TypeFor[float64](), words64},
		{reflect.TypeFor[string](), false},
		{reflect.TypeFor[[]byte](), false},
		{reflect.TypeFor[any](), false},
		{reflect.TypeFor[complex64](), false},
		{reflect.TypeFor[[2]uint16](), false},
		{reflect.TypeFor[halves](), false},
		{reflect.TypeFor[paddedHalves](), false},
		{reflect.TypeFor[[16]byte](), false},
	} {
		assert.Equal(t, tc.want, OneStore(tc.t), "%v", tc.t)
	}
}

// TestSnapshotRejectsWriteInProgress covers the reader that lives entirely inside one write window: an odd meta word
// must fail validation even though it never changed.
func TestSnapshotRejectsWriteInProgress(t *testing.T) {
	item := NewFlatHashMap[string, string](64, maphash.MakeSeed()).At(0)
	item.Publish(Entry[string, string]{Key: "k", Value: "v"}, 0, 0, 0)

	item.beginWrite()
	mid := item.Metadata()
	assert.NotZero(t, mid&1, "beginWrite must leave the generation odd")
	_, ok := item.Snapshot(mid)
	assert.False(t, ok, "a snapshot inside a write window must fail even with a stable meta word")
	item.endWrite()
	assert.Zero(t, item.Metadata()&1, "endWrite must settle the generation even")
}

// TestGenWrapNeverLooksEmpty covers the generation wrapping on an item that carries nothing else: no TTL and a zero
// tag leave gen as the only non-zero field, so landing on 0 would make a live item read as a never-occupied slot.
func TestGenWrapNeverLooksEmpty(t *testing.T) {
	lastEven := GenMask - 1

	item := NewFlatHashMap[string, string](64, maphash.MakeSeed()).At(0)
	item.meta.Store(lastEven)
	item.Publish(Entry[string, string]{Key: "k", Value: "v"}, 0, 0, 0)
	assert.NotZero(t, item.Metadata(), "a wrapped occupancy must not publish an empty-slot meta word")

	item.meta.Store(lastEven)
	item.SetValue("wrapped")
	assert.NotZero(t, item.Metadata(), "a wrapped in-place write must not leave an empty-slot meta word")
	assert.Zero(t, item.Metadata()&1, "endWrite must settle the generation even")
}

// TestTagNeverMatchesEmptySlot pins what lets probes drop their meta == 0 test: a never-occupied slot must never
// look like a candidate. Its meta word is 0, so the candidate compare metaWord&DeadOrTag == tag can only hold if
// some hash yields tag 0 - and then a probe for the zero-valued key would snapshot the slot's zeroed Entry, match
// it, and report a hit on a key the cache never held.
func TestTagNeverMatchesEmptySlot(t *testing.T) {
	var empty Item[uint64, uint64]
	require.Zero(t, empty.Metadata(), "a fresh slot must read as a zero meta word")

	check := func(hash uint64) {
		t.Helper()
		tag := TagOf(hash)
		require.NotZero(t, tag, "hash %#016x yields a zero tag", hash)
		require.Zero(t, tag&^TagMask, "hash %#016x puts bits outside the tag field", hash)
		require.NotEqual(t, empty.Metadata()&DeadOrTag, tag, "hash %#016x matches an empty slot", hash)
		require.Equal(t, OverflowBucket(hash), BucketOf(tag), "hash %#016x: the tag loses the overflow bucket", hash)
		if raw := hash >> 14 & (TagMask >> TagShift); raw != 0 {
			require.Equal(t, raw<<TagShift, tag, "hash %#016x: a non-zero tag must carry all 13 hash bits", hash)
		}
	}
	check(0)
	for shift := range 64 { // every single-bit hash, including those with no bits in the tag field at all
		check(1 << uint(shift))
	}
	rng := rand.New(rand.NewSource(1))
	for range 200_000 {
		check(rng.Uint64())
	}
}
