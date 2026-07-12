package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

// TestOversizedOverwriteDropsStaleValue: when a Set brings a value that does not fit the cache at all, the write is
// dropped - but an older value stored under the same key must be dropped with it. Serving the pre-overwrite value
// after a successful-looking Set would hand the caller stale data.
func TestOversizedOverwriteDropsStaleValue(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 1000,
		CostFunc:       func(_ string, v string) uint32 { return uint32(len(v)) },
		Shards:         1, // the exact "does not fit" boundary equals the whole capacity
	})

	require.NoError(t, c.Set(ctx, "a", "0123456789"))
	_, ok := c.GetFromMemory("a")
	require.True(t, ok, "sanity: the small value must be stored")

	// The oversized overwrite cannot be stored - but it must invalidate the old value.
	require.NoError(t, c.Set(ctx, "a", string(make([]byte, 2000))))
	_, ok = c.GetFromMemory("a")
	assert.False(t, ok, "stale value served after an oversized overwrite")
	assert.EqualValues(t, 0, c.Weight(), "weight of the dropped value was not subtracted")

	// The cache must stay fully functional.
	require.NoError(t, c.Set(ctx, "a", "fresh"))
	v, ok := c.GetFromMemory("a")
	require.True(t, ok)
	assert.Equal(t, "fresh", v)
}

// TestBatchGetOrLoadLoaderReturnsExtraKey: a loader that returns a key nobody asked for (a prefetching or simply
// buggy loader) must not crash the cache. The extra value may be cached, but the result contains only requested keys.
func TestBatchGetOrLoadLoaderReturnsExtraKey(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})

	loader := func(_ context.Context, keys []string) (memstash.List[string, string], error) {
		out := make(memstash.List[string, string], 0, len(keys)+1)
		for _, key := range keys {
			out = append(out, memstash.KeyVal[string, string]{Key: key, Value: "v:" + key})
		}
		return append(out, memstash.KeyVal[string, string]{Key: "extra", Value: "surprise"}), nil
	}

	got, err := c.BatchGetOrLoad(ctx, []string{"want"}, loader)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"want": "v:want"}, got.ToMap(),
		"the result must contain exactly the requested keys")
}

// TestGetOrLoadPanicDoesNotWedgeKey: a panicking loader must not leave the key's singleflight slot occupied forever.
// The panic propagates to the caller, but the next GetOrLoad for the same key must run its loader normally instead of
// waiting on a flight that will never finish.
func TestGetOrLoadPanicDoesNotWedgeKey(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})

	func() {
		defer func() {
			require.NotNil(t, recover(), "the loader's panic must propagate to the GetOrLoad caller")
		}()
		_, _ = c.GetOrLoad(ctx, "k", func(context.Context, string) (string, error) {
			panic("loader exploded")
		})
	}()

	// The key must be loadable again; guard with a timeout so a wedged flight fails the test instead of hanging it.
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	v, err := c.GetOrLoad(timeoutCtx, "k", func(context.Context, string) (string, error) {
		return "recovered", nil
	})
	require.NoError(t, err, "the key is wedged: the flight of the panicked loader was never resolved")
	assert.Equal(t, "recovered", v)
}

// TestBatchGetOrLoadPanicDoesNotWedgeKeys is the batch counterpart of TestGetOrLoadPanicDoesNotWedgeKey.
func TestBatchGetOrLoadPanicDoesNotWedgeKeys(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})

	func() {
		defer func() {
			require.NotNil(t, recover(), "the loader's panic must propagate to the BatchGetOrLoad caller")
		}()
		_, _ = c.BatchGetOrLoad(ctx, []string{"a", "b"}, func(context.Context, []string) (memstash.List[string, string], error) {
			panic("batch loader exploded")
		})
	}()

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	got, err := c.BatchGetOrLoad(timeoutCtx, []string{"a", "b"},
		func(_ context.Context, keys []string) (memstash.List[string, string], error) {
			out := make(memstash.List[string, string], 0, len(keys))
			for _, key := range keys {
				out = append(out, memstash.KeyVal[string, string]{Key: key, Value: "v:" + key})
			}
			return out, nil
		})
	require.NoError(t, err, "keys are wedged: the flights of the panicked loader were never resolved")
	assert.Equal(t, map[string]string{"a": "v:a", "b": "v:b"}, got.ToMap())
}

// TestNegativeTTLRejected: a negative TTL is a misconfiguration, not a request for uint32 wraparound arithmetic.
func TestNegativeTTLRejected(t *testing.T) {
	_, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(10),
		memstash.WithTTL(-time.Second),
	)
	require.ErrorIs(t, err, memstash.ErrBadTTL)
}

// TestWriteBackSetAfterCloseWritesSync: with the write-back worker gone, a Set must fall back to a synchronous L2
// write (as enqueueWriteBack documents) instead of parking the value in a channel nobody drains.
func TestWriteBackSetAfterCloseWritesSync(t *testing.T) {
	ctx := context.Background()
	l2 := newL2Stub()
	c, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(10),
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteBack),
	)
	require.NoError(t, err)

	c.Close()
	require.NoError(t, c.Set(ctx, "k", "v"))
	_, ok := l2.snapshot("k")
	assert.True(t, ok, "a write-back Set after Close was silently lost instead of being written synchronously")
}
