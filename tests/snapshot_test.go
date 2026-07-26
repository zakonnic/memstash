package tests

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
)

func TestSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	src := newCache(t, memstash.Config[string, string]{MemoryCapacity: 1000})

	want := map[string]string{}
	for i := range 200 {
		key := fmt.Sprintf("k%03d", i)
		want[key] = "v" + key
		require.NoError(t, src.Set(ctx, key, want[key]))
	}
	require.NoError(t, src.Delete(ctx, "k000"))
	delete(want, "k000")

	var buf bytes.Buffer
	require.NoError(t, src.SaveTo(&buf, l2.StringCodec(), l2.StringCodec()))

	dst := newCache(t, memstash.Config[string, string]{MemoryCapacity: 1000})
	require.NoError(t, dst.LoadFrom(ctx, &buf, l2.StringCodec(), l2.StringCodec()))

	assert.Equal(t, want, maps.Collect(dst.Iterator()))
	assert.Equal(t, len(want), dst.Len())
}

func TestSnapshotEmptyCache(t *testing.T) {
	ctx := context.Background()
	src := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	var buf bytes.Buffer
	require.NoError(t, src.SaveTo(&buf, l2.StringCodec(), l2.StringCodec()))

	dst := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, dst.LoadFrom(ctx, &buf, l2.StringCodec(), l2.StringCodec()))
	assert.Zero(t, dst.Len())
}

// TestSnapshotEmptyKeyAndValue covers the length bias that keeps 0 free as the end-of-stream marker: an empty key is
// a legal key and must not end the stream.
func TestSnapshotEmptyKeyAndValue(t *testing.T) {
	ctx := context.Background()
	src := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, src.Set(ctx, "", ""))
	require.NoError(t, src.Set(ctx, "after", "v"))

	var buf bytes.Buffer
	require.NoError(t, src.SaveTo(&buf, l2.StringCodec(), l2.StringCodec()))

	dst := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, dst.LoadFrom(ctx, &buf, l2.StringCodec(), l2.StringCodec()))
	assert.Equal(t, map[string]string{"": "", "after": "v"}, maps.Collect(dst.Iterator()))
}

// TestSnapshotLoadMerges pins that LoadFrom adds to what is already cached and lets the snapshot win on a clash.
func TestSnapshotLoadMerges(t *testing.T) {
	ctx := context.Background()
	src := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, src.Set(ctx, "shared", "from-snapshot"))
	require.NoError(t, src.Set(ctx, "only-in-snapshot", "v"))

	var buf bytes.Buffer
	require.NoError(t, src.SaveTo(&buf, l2.StringCodec(), l2.StringCodec()))

	dst := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, dst.Set(ctx, "shared", "old"))
	require.NoError(t, dst.Set(ctx, "only-in-cache", "v"))
	require.NoError(t, dst.LoadFrom(ctx, &buf, l2.StringCodec(), l2.StringCodec()))

	assert.Equal(t, map[string]string{
		"shared":           "from-snapshot",
		"only-in-snapshot": "v",
		"only-in-cache":    "v",
	}, maps.Collect(dst.Iterator()))
}

// TestSnapshotLoadRespectsCapacity pins that loading goes through the normal write path: a snapshot bigger than the
// cache does not blow past the capacity.
func TestSnapshotLoadRespectsCapacity(t *testing.T) {
	ctx := context.Background()
	src := newCache(t, memstash.Config[string, string]{MemoryCapacity: 500, Shards: 1})
	for i := range 500 {
		require.NoError(t, src.Set(ctx, fmt.Sprintf("k%03d", i), "v"))
	}

	var buf bytes.Buffer
	require.NoError(t, src.SaveTo(&buf, l2.StringCodec(), l2.StringCodec()))

	dst := newCache(t, memstash.Config[string, string]{MemoryCapacity: 50, Shards: 1})
	require.NoError(t, dst.LoadFrom(ctx, &buf, l2.StringCodec(), l2.StringCodec()))
	assert.LessOrEqual(t, dst.Len(), 50, "loading must not exceed the capacity")
	assert.Positive(t, dst.Len())
}

func TestSnapshotRejectsGarbage(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})

	for name, data := range map[string][]byte{
		"empty":          {},
		"bad magic":      []byte("not-a-snapshot-at-all"),
		"header only":    []byte("memstash"),
		"short header":   append([]byte("memstash"), 2, 0, 0),
		"bad version":    append([]byte("memstash"), 99, 0, 0, 0, 0, 0, 0, 0, 0),
		"truncated body": append([]byte("memstash"), 2, 0, 0, 0, 0, 0, 0, 0, 0, 5, 'a', 'b'),
	} {
		t.Run(name, func(t *testing.T) {
			err := c.LoadFrom(ctx, bytes.NewReader(data), l2.StringCodec(), l2.StringCodec())
			assert.ErrorIs(t, err, memstash.ErrBadSnapshot)
		})
	}
}

// TestSnapshotTruncatedStreamIsRejected covers the end-of-stream marker: a snapshot cut short mid-way must not pass
// for a complete one, however valid the records before the cut are.
func TestSnapshotTruncatedStreamIsRejected(t *testing.T) {
	ctx := context.Background()
	src := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	for i := range 20 {
		require.NoError(t, src.Set(ctx, fmt.Sprintf("k%02d", i), "value"))
	}
	var buf bytes.Buffer
	require.NoError(t, src.SaveTo(&buf, l2.StringCodec(), l2.StringCodec()))

	full := buf.Bytes()
	dst := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	err := dst.LoadFrom(ctx, bytes.NewReader(full[:len(full)-10]), l2.StringCodec(), l2.StringCodec())
	require.ErrorIs(t, err, memstash.ErrBadSnapshot)
	assert.Positive(t, dst.Len(), "the items read before the cut stay in the cache")
}

func TestSnapshotNilCodec(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	var buf bytes.Buffer

	assert.ErrorIs(t, c.SaveTo(&buf, nil, l2.StringCodec()), memstash.ErrNilCodec)
	assert.ErrorIs(t, c.SaveTo(&buf, l2.StringCodec(), nil), memstash.ErrNilCodec)
	assert.ErrorIs(t, c.LoadFrom(ctx, &buf, nil, l2.StringCodec()), memstash.ErrNilCodec)
	assert.ErrorIs(t, c.LoadFrom(ctx, &buf, l2.StringCodec(), nil), memstash.ErrNilCodec)
}

type failingWriter struct{ after int }

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.after <= 0 {
		return 0, errWriteFailed
	}
	w.after -= len(p)
	return len(p), nil
}

var errWriteFailed = errors.New("writer is out of space")

// TestSnapshotWriterErrorPropagates covers both places a write can fail: the final flush of a snapshot that fit in
// the buffer, and a flush in the middle of a bigger one.
func TestSnapshotWriterErrorPropagates(t *testing.T) {
	ctx := context.Background()
	small := newCache(t, memstash.Config[string, string]{MemoryCapacity: 10_000})
	require.NoError(t, small.Set(ctx, "k", "v"))
	assert.ErrorIs(t, small.SaveTo(&failingWriter{}, l2.StringCodec(), l2.StringCodec()), errWriteFailed)

	big := newCache(t, memstash.Config[string, string]{MemoryCapacity: 10_000})
	for i := range 2000 {
		require.NoError(t, big.Set(ctx, fmt.Sprintf("key-%05d", i), "a value long enough to fill the buffer"))
	}
	assert.ErrorIs(t, big.SaveTo(&failingWriter{after: 4096}, l2.StringCodec(), l2.StringCodec()), errWriteFailed)
}

type brokenCodec struct{}

func (brokenCodec) Marshal(string) ([]byte, error)   { return nil, errCodecFailed }
func (brokenCodec) Unmarshal([]byte) (string, error) { return "", errCodecFailed }

var errCodecFailed = errors.New("codec refuses")

func TestSnapshotCodecErrorPropagates(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, c.Set(ctx, "k", "v"))

	var buf bytes.Buffer
	assert.ErrorIs(t, c.SaveTo(&buf, brokenCodec{}, l2.StringCodec()), errCodecFailed)

	buf.Reset()
	require.NoError(t, c.SaveTo(&buf, l2.StringCodec(), l2.StringCodec()))
	dst := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	assert.ErrorIs(t, dst.LoadFrom(ctx, &buf, brokenCodec{}, l2.StringCodec()), errCodecFailed)
}

// TestSnapshotJSONValues covers a non-string value type through the codec the adapters use.
func TestSnapshotJSONValues(t *testing.T) {
	type user struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	ctx := context.Background()
	src, err := memstash.New[string, user](memstash.WithMemoryCapacity(100))
	require.NoError(t, err)
	t.Cleanup(src.Close)
	require.NoError(t, src.Set(ctx, "u1", user{Name: "Ada", Age: 36}))

	var buf bytes.Buffer
	require.NoError(t, src.SaveTo(&buf, l2.StringCodec(), l2.JSONCodec[user]()))

	dst, err := memstash.New[string, user](memstash.WithMemoryCapacity(100))
	require.NoError(t, err)
	t.Cleanup(dst.Close)
	require.NoError(t, dst.LoadFrom(ctx, &buf, l2.StringCodec(), l2.JSONCodec[user]()))

	got, ok := dst.GetFromMemory("u1")
	require.True(t, ok)
	assert.Equal(t, user{Name: "Ada", Age: 36}, got)
}

// TestSnapshotLoadWithTTLKeepsSchedule pins LoadWithTTL: an item keeps the moment it was going to expire at, so the
// wait before loading eats into its remaining life.
func TestSnapshotLoadWithTTLKeepsSchedule(t *testing.T) {
	ctx := context.Background()
	src := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100, TTL: 4 * time.Second})
	require.NoError(t, src.Set(ctx, "k", "v"))

	var buf bytes.Buffer
	require.NoError(t, src.SaveTo(&buf, l2.StringCodec(), l2.StringCodec()))

	dst := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100, TTL: 4 * time.Second})
	require.NoError(t, dst.LoadFrom(ctx, &buf, l2.StringCodec(), l2.StringCodec(), memstash.LoadWithTTL))
	_, ok := dst.GetFromMemory("k")
	assert.True(t, ok, "the item still had life left")

	// Past the original deadline the restored item must be gone, even though it was loaded moments ago.
	time.Sleep(6 * time.Second)
	_, ok = dst.GetFromMemory("k")
	assert.False(t, ok, "LoadWithTTL must not hand out a fresh lifetime")
}

// TestSnapshotLoadWithTTLSkipsExpired pins that an item whose deadline passed while the snapshot sat around is
// dropped instead of being resurrected.
func TestSnapshotLoadWithTTLSkipsExpired(t *testing.T) {
	ctx := context.Background()
	src := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100, TTL: time.Second})
	require.NoError(t, src.Set(ctx, "k", "v"))

	var buf bytes.Buffer
	require.NoError(t, src.SaveTo(&buf, l2.StringCodec(), l2.StringCodec()))
	time.Sleep(3 * time.Second) // the snapshot outlives the item it holds

	dst := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100, TTL: time.Second})
	require.NoError(t, dst.LoadFrom(ctx, &buf, l2.StringCodec(), l2.StringCodec(), memstash.LoadWithTTL))
	assert.Zero(t, dst.Len(), "an item past its deadline must be skipped")
}

// TestSnapshotLoadWithCurrentTTL pins the default: the same stale snapshot loads fine and gets a full lifetime.
func TestSnapshotLoadWithCurrentTTL(t *testing.T) {
	ctx := context.Background()
	src := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100, TTL: time.Second})
	require.NoError(t, src.Set(ctx, "k", "v"))

	var buf bytes.Buffer
	require.NoError(t, src.SaveTo(&buf, l2.StringCodec(), l2.StringCodec()))
	time.Sleep(3 * time.Second)

	dst := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100, TTL: time.Minute})
	require.NoError(t, dst.LoadFrom(ctx, &buf, l2.StringCodec(), l2.StringCodec(), memstash.LoadWithCurrentTTL))
	_, ok := dst.GetFromMemory("k")
	assert.True(t, ok, "LoadWithCurrentTTL must give the item a fresh lifetime")
}

// TestSnapshotLoadWithTTLIntoCacheWithoutTTL covers the documented fallback: with no clock to expire against, the
// saved deadlines are dropped rather than the items.
func TestSnapshotLoadWithTTLIntoCacheWithoutTTL(t *testing.T) {
	ctx := context.Background()
	src := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100, TTL: time.Minute})
	require.NoError(t, src.Set(ctx, "k", "v"))

	var buf bytes.Buffer
	require.NoError(t, src.SaveTo(&buf, l2.StringCodec(), l2.StringCodec()))

	dst := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100}) // no TTL
	require.NoError(t, dst.LoadFrom(ctx, &buf, l2.StringCodec(), l2.StringCodec(), memstash.LoadWithTTL))
	_, ok := dst.GetFromMemory("k")
	assert.True(t, ok)
}

func TestSnapshotLoadToL2(t *testing.T) {
	ctx := context.Background()
	src := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, src.Set(ctx, "a", "1"))
	require.NoError(t, src.Set(ctx, "b", "2"))

	var buf bytes.Buffer
	require.NoError(t, src.SaveTo(&buf, l2.StringCodec(), l2.StringCodec()))

	target := newL2Stub()
	dst := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100, L2Cache: target, WritePolicy: memstash.WriteThrough,
	})
	require.NoError(t, dst.LoadFrom(ctx, &buf, l2.StringCodec(), l2.StringCodec(), memstash.LoadToL2))

	for key, want := range map[string]string{"a": "1", "b": "2"} {
		got, ok := target.snapshot(key)
		require.True(t, ok, "key %s was not written to L2", key)
		assert.Equal(t, want, got)
	}
}

// TestSnapshotLoadWithoutL2Option pins that L2 stays untouched by default.
func TestSnapshotLoadWithoutL2Option(t *testing.T) {
	ctx := context.Background()
	src := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, src.Set(ctx, "a", "1"))

	var buf bytes.Buffer
	require.NoError(t, src.SaveTo(&buf, l2.StringCodec(), l2.StringCodec()))

	target := newL2Stub()
	dst := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100, L2Cache: target, WritePolicy: memstash.WriteThrough,
	})
	require.NoError(t, dst.LoadFrom(ctx, &buf, l2.StringCodec(), l2.StringCodec()))

	_, ok := target.snapshot("a")
	assert.False(t, ok, "LoadFrom must not write to L2 unless asked")
}

func TestSnapshotConflictingLoadOptions(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	var buf bytes.Buffer
	require.NoError(t, c.SaveTo(&buf, l2.StringCodec(), l2.StringCodec()))

	err := c.LoadFrom(ctx, &buf, l2.StringCodec(), l2.StringCodec(),
		memstash.LoadWithTTL, memstash.LoadWithCurrentTTL)
	assert.ErrorIs(t, err, memstash.ErrConflictingLoadOptions)
}
