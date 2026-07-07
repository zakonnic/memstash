package l2_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
)

func TestResolveKeyFunc(t *testing.T) {
	t.Run("string keys", func(t *testing.T) {
		cases := []struct {
			name    string
			keyFunc func(string) string
			in      string
			want    string
		}{
			{name: "nil means identity", keyFunc: nil, in: "k", want: "k"},
			{name: "custom function is used as is", keyFunc: func(string) string { return "custom" }, in: "k", want: "custom"},
			{name: "PrefixedString namespaces the key", keyFunc: l2.PrefixedString("app:"), in: "k", want: "app:k"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				resolved, err := l2.ResolveKeyFunc(tc.keyFunc)
				require.NoError(t, err)
				assert.Equal(t, tc.want, resolved(tc.in))
			})
		}
	})

	t.Run("non-string keys", func(t *testing.T) {
		t.Run("custom function is required", func(t *testing.T) {
			_, err := l2.ResolveKeyFunc[int](nil)
			require.ErrorIs(t, err, l2.ErrKeyFuncRequired)
		})
		t.Run("custom function is used as is", func(t *testing.T) {
			resolved, err := l2.ResolveKeyFunc(strconv.Itoa)
			require.NoError(t, err)
			assert.Equal(t, "7", resolved(7))
		})
	})
}

func TestMemcacheExpiration(t *testing.T) {
	cases := []struct {
		name string
		ttl  time.Duration
		// want is the exact expected value; for the unix-timestamp branch use check instead.
		want  int32
		check func(t *testing.T, got int32)
	}{
		{name: "zero keeps no-expiration", ttl: 0, want: 0},
		{name: "negative is treated as no-expiration", ttl: -time.Second, want: 0},
		{name: "sub-second rounds up to one second", ttl: 300 * time.Millisecond, want: 1},
		{name: "whole seconds pass through", ttl: 90 * time.Second, want: 90},
		{name: "exactly 30 days stays relative", ttl: 30 * 24 * time.Hour, want: 30 * 24 * 60 * 60},
		{
			name: "beyond 30 days becomes an absolute unix timestamp",
			ttl:  31 * 24 * time.Hour,
			check: func(t *testing.T, got int32) {
				assert.GreaterOrEqual(t, int64(got), time.Now().Unix(), "expected a unix timestamp, not a relative count")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := l2.MemcacheExpiration(tc.ttl)
			if tc.check != nil {
				tc.check(t, got)
				return
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestRedisMillis(t *testing.T) {
	cases := []struct {
		name string
		ttl  time.Duration
		want int64
	}{
		{name: "sub-millisecond rounds up to one", ttl: time.Microsecond, want: 1},
		{name: "whole milliseconds pass through", ttl: 2500 * time.Millisecond, want: 2500},
		{name: "fractional milliseconds round up", ttl: 1500*time.Millisecond + time.Microsecond, want: 1501},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, l2.RedisMillis(tc.ttl))
		})
	}
}

func TestCodecs(t *testing.T) {
	t.Run("json round-trip", func(t *testing.T) {
		type payload struct{ N int }
		codec := l2.JSONCodec[payload]()

		data, err := codec.Marshal(payload{N: 42})
		require.NoError(t, err)
		back, err := codec.Unmarshal(data)
		require.NoError(t, err)
		assert.Equal(t, payload{N: 42}, back)
	})

	t.Run("bytes pass-through", func(t *testing.T) {
		codec := l2.BytesCodec()

		data, err := codec.Marshal([]byte("v"))
		require.NoError(t, err)
		assert.Equal(t, []byte("v"), data)
		back, err := codec.Unmarshal([]byte("v"))
		require.NoError(t, err)
		assert.Equal(t, []byte("v"), back)
	})

	t.Run("string round-trip", func(t *testing.T) {
		codec := l2.StringCodec()

		data, err := codec.Marshal("v")
		require.NoError(t, err)
		assert.Equal(t, []byte("v"), data)
		back, err := codec.Unmarshal([]byte("v"))
		require.NoError(t, err)
		assert.Equal(t, "v", back)
	})
}

// TestWithKeyFuncAndResolveOptions covers the adapter-constructor option plumbing: WithKeyFunc is a regular
// memstash.Option that ResolveOptions picks out of a mixed option list, falling back to ResolveKeyFunc's own rules
// when it is absent, and reporting ErrOptionMismatch when it was built for a different key type.
func TestWithKeyFuncAndResolveOptions(t *testing.T) {
	t.Run("WithKeyFunc overrides the default mapping", func(t *testing.T) {
		opts := []memstash.Option{l2.WithKeyFunc[int](strconv.Itoa)}
		resolved, err := l2.ResolveOptions[int](opts)
		require.NoError(t, err)
		assert.Equal(t, "7", resolved(7))
	})

	t.Run("no WithKeyFunc falls back to ResolveKeyFunc's rules", func(t *testing.T) {
		resolved, err := l2.ResolveOptions[string](nil)
		require.NoError(t, err)
		assert.Equal(t, "k", resolved("k"))

		_, err = l2.ResolveOptions[int](nil)
		require.ErrorIs(t, err, l2.ErrKeyFuncRequired)
	})

	t.Run("foreign options (with no ApplyTyped) are ignored", func(t *testing.T) {
		opts := []memstash.Option{memstash.WithMemoryCapacity(10)}
		resolved, err := l2.ResolveOptions[string](opts)
		require.NoError(t, err)
		assert.Equal(t, "k", resolved("k"))
	})

	t.Run("a key-func option built for a different key type is a mismatch", func(t *testing.T) {
		opts := []memstash.Option{l2.WithKeyFunc[int](strconv.Itoa)}
		_, err := l2.ResolveOptions[string](opts)
		require.ErrorIs(t, err, memstash.ErrOptionMismatch)
	})
}

// sequentialStub is a minimal L2Cache used to verify the sequential batch fallbacks: BatchGet/BatchSet just call
// back into l2.BatchGetSequential/BatchSetSequential, so this is also representative of how an adapter without a
// native multi-get/multi-set wires them up.
type sequentialStub struct {
	mu       sync.Mutex
	m        map[string]string
	getCalls int
	setCalls int
	failKey  string // if set, Get on this key returns errBoom
}

var errBoom = errors.New("boom")

func newSequentialStub() *sequentialStub { return &sequentialStub{m: map[string]string{}} }

func (s *sequentialStub) Get(_ context.Context, key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getCalls++
	if s.failKey != "" && key == s.failKey {
		return "", false, errBoom
	}
	v, ok := s.m[key]
	return v, ok, nil
}

func (s *sequentialStub) Set(_ context.Context, key, value string, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setCalls++
	s.m[key] = value
	return nil
}

func (s *sequentialStub) BatchGet(ctx context.Context, keys []string) (memstash.List[string, string], error) {
	return l2.BatchGetSequential(ctx, s, keys)
}

func (s *sequentialStub) BatchSet(ctx context.Context, items memstash.List[string, string], ttl time.Duration) error {
	return l2.BatchSetSequential(ctx, s, items, ttl)
}

func (s *sequentialStub) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
	return nil
}

func TestBatchSequentialFallbacks(t *testing.T) {
	ctx := context.Background()
	store := newSequentialStub()

	require.NoError(t, store.BatchSet(ctx, memstash.List[string, string]{
		{Key: "a", Value: "1"}, {Key: "b", Value: "2"},
	}, time.Minute))
	assert.Equal(t, 2, store.setCalls, "BatchSetSequential must call Set once per item")

	found, err := store.BatchGet(ctx, []string{"a", "b", "missing"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1", "b": "2"}, found.ToMap())
	assert.Equal(t, 3, store.getCalls, "BatchGetSequential must call Get once per key")

	t.Run("stops at the first error, keeping the partial result gathered so far", func(t *testing.T) {
		store := newSequentialStub()
		require.NoError(t, store.Set(ctx, "a", "1", 0))
		store.failKey = "b"

		found, err := store.BatchGet(ctx, []string{"a", "b", "c"})
		require.ErrorIs(t, err, errBoom)
		assert.Equal(t, map[string]string{"a": "1"}, found.ToMap(),
			"the result gathered before the error must still be returned")
	})
}
