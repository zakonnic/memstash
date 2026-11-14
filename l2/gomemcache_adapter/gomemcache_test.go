package gomemcache_adapter_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	gomemcache_adapter "github.com/zakonnic/memstash/l2/gomemcache_adapter"
)

// TestConstructors instantiates the generic constructor chain; no memcached server is needed - constructors do not
// dial.
func TestConstructors(t *testing.T) {
	client := memcache.New("localhost:11211")

	cases := []struct {
		name    string
		build   func() (any, error)
		wantErr error // nil means the constructor must succeed
	}{
		{
			name:  "NewBytes with string keys",
			build: func() (any, error) { return gomemcache_adapter.NewBytes[string](client, 0) },
		},
		{
			name:  "NewJSON with string keys",
			build: func() (any, error) { return gomemcache_adapter.NewJSON[string, int](client, 0) },
		},
		{
			name: "NewJSON with a key function for non-string keys",
			build: func() (any, error) {
				return gomemcache_adapter.NewJSON[int, string](client, 0, l2.WithKeyFunc(strconv.Itoa))
			},
		},
		{
			name:    "non-string keys without a key function",
			build:   func() (any, error) { return gomemcache_adapter.NewBytes[int](client, 0) },
			wantErr: l2.ErrKeyFuncRequired,
		},
		{
			name:    "nil client",
			build:   func() (any, error) { return gomemcache_adapter.New[string, string](nil, l2.StringCodec(), 0) },
			wantErr: l2.ErrNilClient,
		},
		{
			name:    "nil codec",
			build:   func() (any, error) { return gomemcache_adapter.New[string, string](client, nil, 0) },
			wantErr: l2.ErrNilCodec,
		},
		{
			name: "NewCache with a key function of a mismatched key type",
			build: func() (any, error) {
				return gomemcache_adapter.NewCache[int, string](client, l2.StringCodec(), 0,
					l2.WithKeyFunc(func(key string) string { return key }))
			},
			wantErr: memstash.ErrOptionMismatch,
		},
		{
			name:    "negative wbConcurrentCalls",
			build:   func() (any, error) { return gomemcache_adapter.NewBytes[string](client, -1) },
			wantErr: l2.ErrBadConcurrentCalls,
		},
		{
			name: "NewCache with WithWriteBackWorkers, which wbConcurrentCalls replaces",
			build: func() (any, error) {
				return gomemcache_adapter.NewStringCache[string](client, 8, memstash.WithWriteBackWorkers(8))
			},
			wantErr: memstash.ErrWorkersSetByL2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			built, err := tc.build()
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, built)
		})
	}
}

// TestNewCache verifies that one flat option list feeds both the cache (memstash.With*) and the adapter
// (l2.WithKeyFunc). No memcached server is needed: WriteDisabled keeps Set away from L2 and the memory level answers
// the reads.
func TestNewCache(t *testing.T) {
	client := memcache.New("localhost:11211")

	c, err := gomemcache_adapter.NewCache[int, string](client, l2.StringCodec(), 0,
		memstash.WithMemoryCapacity(100),
		memstash.WithWritePolicy(memstash.WriteDisabled),
		l2.WithKeyFunc(strconv.Itoa),
	)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Set(context.Background(), 7, "v"))
	v, ok := c.GetFromMemory(7)
	require.True(t, ok, "GetFromMemory after Set")
	assert.Equal(t, "v", v)

	// The codec-fixed cache constructors share the same plumbing - a smoke instantiation is enough.
	cacheJSON, err := gomemcache_adapter.NewJSONCache[string, int](client, 0, memstash.WithMemoryCapacity(10))
	require.NoError(t, err)
	cacheJSON.Close()
	cacheBytes, err := gomemcache_adapter.NewBytesCache[string](client, 0, memstash.WithMemoryCapacity(10))
	require.NoError(t, err)
	cacheBytes.Close()
}

// TestConcurrentCalls: wbConcurrentCalls is reported as given. Without it write-back takes half the idle connections
// and leaves reads the rest, but never fewer than DefaultWriteBackWorkers calls, the default of 2 idle connections
// included.
func TestConcurrentCalls(t *testing.T) {
	for _, tc := range []struct{ maxIdle, wbCalls, calls int }{
		{0, 0, 4}, {2, 0, 4}, {32, 0, 16}, {300, 0, 150}, {300, 7, 7}, {2, 100, 100},
	} {
		client := memcache.New("localhost:11211")
		client.MaxIdleConns = tc.maxIdle
		adapter, err := gomemcache_adapter.NewBytes[string](client, tc.wbCalls)
		require.NoError(t, err)
		assert.Equal(t, tc.calls, adapter.ConcurrentCalls(),
			"MaxIdleConns %d, wbConcurrentCalls %d", tc.maxIdle, tc.wbCalls)
	}
}
