package tests

import (
	"context"
	"errors"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

func TestBatchSetGet(t *testing.T) {
	ctx := context.Background()
	l2 := newL2Stub()
	writer := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100, L2Cache: l2, WritePolicy: memstash.WriteThrough,
	})

	items := memstash.List[string, string]{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}, {Key: "c", Value: "3"}}
	require.NoError(t, writer.BatchSet(ctx, items))

	// Write-through: everything is in L2 in one batch write.
	for _, item := range items {
		v, ok := l2.snapshot(item.Key)
		require.True(t, ok, "key %s missing in L2 after BatchSet", item.Key)
		assert.Equal(t, item.Value, v)
	}

	// A fresh cache over the same L2: BatchGet mixes an L1 hit with L2 hits and promotes the latter.
	reader := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100, L2Cache: l2, WritePolicy: memstash.WriteThrough,
	})
	require.NoError(t, reader.Set(ctx, "mem-only", "m")) // an L1 resident
	got, err := reader.BatchGet(ctx, []string{"a", "b", "c", "mem-only", "absent"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1", "b": "2", "c": "3", "mem-only": "m"}, got.ToMap())

	// The L2 hits must have been promoted into the reader's memory.
	for _, item := range items {
		_, ok := reader.GetFromMemory(item.Key)
		assert.True(t, ok, "key %s was not promoted to L1 by BatchGet", item.Key)
	}
}

func TestBatchSetWriteBackFlushedByWait(t *testing.T) {
	ctx := context.Background()
	l2 := newL2Stub()
	c, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(100),
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteBack),
	)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.BatchSet(ctx, memstash.List[string, string]{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}))
	c.Wait()
	for _, key := range []string{"a", "b"} {
		_, ok := l2.snapshot(key)
		assert.True(t, ok, "batch write-back write %s is not in L2 after Wait", key)
	}
}

func TestBatchGetOrLoad(t *testing.T) {
	ctx := context.Background()
	l2 := newL2Stub()
	require.NoError(t, l2.Set(ctx, "from-l2", "l2-value", 0))

	c := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100, L2Cache: l2, WritePolicy: memstash.WriteThrough,
	})
	require.NoError(t, c.Set(ctx, "from-mem", "mem-value"))

	var calls atomic.Int32
	var askedKeys []string
	loader := func(_ context.Context, keys []string) (memstash.List[string, string], error) {
		calls.Add(1)
		askedKeys = append([]string(nil), keys...)
		return memstash.List[string, string]{{Key: "loaded", Value: "loader-value"}}, nil // "absent" is deliberately omitted
	}

	got, err := c.BatchGetOrLoad(ctx, []string{"from-mem", "from-l2", "loaded", "absent"}, loader)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"from-mem": "mem-value",
		"from-l2":  "l2-value",
		"loaded":   "loader-value",
	}, got.ToMap(), "a key omitted by the loader must be absent from the result")

	// The loader must have been asked exactly for the keys missing in both levels.
	require.EqualValues(t, 1, calls.Load())
	sort.Strings(askedKeys)
	assert.Equal(t, []string{"absent", "loaded"}, askedKeys)

	// Everything resolved is cached: the second call does not touch the loader.
	got, err = c.BatchGetOrLoad(ctx, []string{"from-mem", "from-l2", "loaded"}, loader)
	require.NoError(t, err)
	assert.Len(t, got, 3)
	assert.EqualValues(t, 1, calls.Load(), "the loader was called again for cached keys")

	// The write policy applies to loader results: "loaded" must have reached L2 (write-through).
	v, ok := l2.snapshot("loaded")
	require.True(t, ok, "the loaded value was not written through to L2")
	assert.Equal(t, "loader-value", v)
}

func TestBatchGetOrLoadErrorReturnsPartial(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, c.Set(ctx, "cached", "v"))

	boom := errors.New("boom")
	got, err := c.BatchGetOrLoad(ctx, []string{"cached", "missing"},
		func(context.Context, []string) (memstash.List[string, string], error) { return nil, boom })
	require.ErrorIs(t, err, boom)
	assert.Equal(t, map[string]string{"cached": "v"}, got.ToMap(), "the resolved part must be returned alongside the error")

	_, err = c.BatchGetOrLoad(ctx, []string{"x"}, nil)
	require.ErrorIs(t, err, memstash.ErrNilLoader)
}

func TestLoadableCacheBatch(t *testing.T) {
	ctx := context.Background()
	var calls atomic.Int32
	lc, err := memstash.NewLoadable(
		func(_ context.Context, key int) (string, error) {
			calls.Add(1)
			return "v", nil
		},
		memstash.WithMemoryCapacity(100))
	require.NoError(t, err)
	defer lc.Close()

	got, err := lc.BatchGetOrLoad(ctx, []int{1, 2, 3})
	require.NoError(t, err)
	assert.Len(t, got, 3)
	assert.EqualValues(t, 3, calls.Load(), "the synthesized batch loader calls the single loader per key")

	got, err = lc.BatchGet(ctx, []int{1, 2, 3, 4})
	require.NoError(t, err)
	assert.Len(t, got, 3, "BatchGet must not invoke the loader")

	require.NoError(t, lc.BatchSet(ctx, memstash.List[int, string]{{Key: 5, Value: "v5"}}))
	v, ok := lc.Cache().GetFromMemory(5)
	require.True(t, ok)
	assert.Equal(t, "v5", v)
}

func TestBatchGetFromMemory(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 1000})

	keys := make([]string, 0, 50)
	for i := 0; i < 50; i++ {
		key := "k" + string(rune('A'+i%26)) + string(rune('0'+i/26))
		keys = append(keys, key)
		if i%3 != 0 { // every third key stays absent
			require.NoError(t, c.Set(ctx, key, "v-"+key))
		}
	}
	require.NoError(t, c.Delete(ctx, keys[1])) // present, then deleted: the chain walks past a tombstone
	require.NoError(t, c.Set(ctx, keys[2], "overwritten"))

	dst := make(memstash.List[string, string], 0, len(keys))
	dst = c.BatchGetFromMemory(keys, dst[:0])

	want := map[string]string{}
	for i, key := range keys {
		if i%3 != 0 && i != 1 {
			want[key] = "v-" + key
		}
	}
	want[keys[2]] = "overwritten"
	assert.Equal(t, want, dst.ToMap())

	// Reuse: a second call into the same backing array must not allocate.
	base := &dst[:1][0]
	dst = c.BatchGetFromMemory(keys, dst[:0])
	assert.Same(t, base, &dst[0], "a reused dst must keep its backing array")
	assert.Equal(t, want, dst.ToMap())

	assert.Empty(t, c.BatchGetFromMemory(nil, nil))
}
