// Package mc_adapter adapts the memcachier binary-protocol client (github.com/memcachier/mc/v3) to the
// memstash.L2Cache contract.
//
// The client does not support context: the ctx arguments are ignored, calls can be neither canceled nor given a
// per-call deadline. Bound the latency with mc.Config timeouts instead.
//
// Give it a client of one server. The client's server hasher (mc v3.0.3) shares one FNV state between concurrent
// calls, so with several servers a call can pick the wrong one: a false miss on a read, a stale value left behind by
// a write or a delete. For a pool of memcached servers use gomemcache_adapter.
package mc_adapter

import (
	"context"
	"errors"
	"time"

	mclib "github.com/memcachier/mc/v3"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
)

// Cache is an L2 adapter over the memcachier client of a single server (see the package doc); its lifecycle stays
// with the caller.
type Cache[K comparable, V any] struct {
	client  *mclib.Client
	codec   memstash.Codec[V]
	keyFunc func(K) string
}

var _ memstash.L2Cache[string, string] = (*Cache[string, string])(nil)

// New creates the adapter with an explicit value codec. By default keys must be strings (identity mapping); for other
// key types pass l2.WithKeyFunc. A key memcached rejects is stored under a stand-in (see l2.MemcacheKey).
func New[K comparable, V any](client *mclib.Client, codec memstash.Codec[V], opts ...memstash.Option) (*Cache[K, V], error) {
	if client == nil {
		return nil, l2.ErrNilClient
	}
	if codec == nil {
		return nil, l2.ErrNilCodec
	}
	keyFunc, err := l2.ExtractKeyFunc[K](opts)
	if err != nil {
		return nil, err
	}
	storageKey := keyFunc
	keyFunc = func(key K) string { return l2.MemcacheKey(storageKey(key)) }
	return &Cache[K, V]{client: client, codec: codec, keyFunc: keyFunc}, nil
}

// NewJSON creates the adapter that marshals values with encoding/json.
func NewJSON[K comparable, V any](client *mclib.Client, opts ...memstash.Option) (*Cache[K, V], error) {
	return New[K, V](client, l2.JSONCodec[V](), opts...)
}

// NewBytes creates the adapter that passes []byte values through unchanged.
func NewBytes[K comparable](client *mclib.Client, opts ...memstash.Option) (*Cache[K, []byte], error) {
	return New[K, []byte](client, l2.BytesCodec(), opts...)
}

// NewCache builds a two-level cache in one call: a new memstash.Cache backed by this adapter as its L2. The single
// option list is shared: the memstash.With* options configure the cache, l2.WithKeyFunc configures the adapter.
func NewCache[K comparable, V any](client *mclib.Client, codec memstash.Codec[V], opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	adapter, err := New[K, V](client, codec, opts...)
	if err != nil {
		return nil, err
	}
	cacheOpts := make([]memstash.Option, 0, len(opts)+1)
	cacheOpts = append(cacheOpts, opts...)
	cacheOpts = append(cacheOpts, memstash.WithL2Cache[K, V](adapter))
	return memstash.New[K, V](cacheOpts...)
}

// NewJSONCache builds a two-level cache with the JSON value codec (see NewCache).
func NewJSONCache[K comparable, V any](client *mclib.Client, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	return NewCache[K, V](client, l2.JSONCodec[V](), opts...)
}

// NewBytesCache builds a two-level cache that passes []byte values through unchanged (see NewCache).
func NewBytesCache[K comparable](client *mclib.Client, opts ...memstash.Option) (*memstash.Cache[K, []byte], error) {
	return NewCache[K, []byte](client, l2.BytesCodec(), opts...)
}

// NewStringCache builds a two-level cache that passes string values through unchanged (see NewCache).
func NewStringCache[K comparable](client *mclib.Client, opts ...memstash.Option) (*memstash.Cache[K, string], error) {
	return NewCache[K, string](client, l2.StringCodec(), opts...)
}

// Get returns the value; a missing key is (zero, false, nil). The context is ignored: the client has no context
// support.
func (c *Cache[K, V]) Get(_ context.Context, key K) (V, bool, error) {
	var zero V
	data, _, _, err := c.client.Get(c.keyFunc(key))
	if err != nil {
		if errors.Is(err, mclib.ErrNotFound) {
			return zero, false, nil
		}
		return zero, false, err
	}
	value, err := c.codec.Unmarshal([]byte(data))
	if err != nil {
		return zero, false, err
	}
	return value, true, nil
}

// Set stores the value; ttl == 0 means "no expiration". The context is ignored: the client has no context support.
func (c *Cache[K, V]) Set(_ context.Context, key K, value V, ttl time.Duration) error {
	data, err := c.codec.Marshal(value)
	if err != nil {
		return err
	}
	_, err = c.client.Set(c.keyFunc(key), string(data), 0, uint32(l2.MemcacheExpiration(ttl)), 0)
	return err
}

// Delete removes the key; a missing key is not an error. The context is ignored: the client has no context support.
func (c *Cache[K, V]) Delete(_ context.Context, key K) error {
	if err := c.client.Del(c.keyFunc(key)); err != nil && !errors.Is(err, mclib.ErrNotFound) {
		return err
	}
	return nil
}

// BatchWorkers is the number of goroutines the concurrent batch fallbacks run. Raise mc.Config.PoolSize (the
// default is 1) towards this value, or the goroutines serialize on the connection pool.
const BatchWorkers = 8

// BatchGet fetches the keys with concurrent Gets: the client exposes no multi-get. The context is ignored: the
// client has no context support.
func (c *Cache[K, V]) BatchGet(ctx context.Context, keys []K) (memstash.List[K, V], error) {
	return l2.BatchGetConcurrent(ctx, c, keys, BatchWorkers)
}

// BatchSet stores the items with concurrent Sets: the client exposes no multi-set. The context is ignored: the
// client has no context support.
func (c *Cache[K, V]) BatchSet(ctx context.Context, items memstash.List[K, V], ttl time.Duration) error {
	return l2.BatchSetConcurrent(ctx, c, items, ttl, BatchWorkers)
}

// BatchDelete removes the keys with concurrent Deletes: the client exposes no multi-delete. The context is ignored:
// the client has no context support.
func (c *Cache[K, V]) BatchDelete(ctx context.Context, keys []K) error {
	return l2.BatchDeleteConcurrent(ctx, c, keys, BatchWorkers)
}

// memstash.SingleCallL2, spelled out so the module still builds against a core without it.
var _ interface{ ConcurrentCalls() int } = (*Cache[string, []byte])(nil)

// ConcurrentCalls is DefaultWriteBackWorkers. The client's pool is a channel of Config.PoolSize connections it does
// not expose, and a call past it waits in the same queue as the reads, so each write-back worker takes a turn from
// them. Set memstash.WithWriteBackWorkers to about half of PoolSize. It makes the adapter a memstash.SingleCallL2.
func (c *Cache[K, V]) ConcurrentCalls() int { return memstash.DefaultWriteBackWorkers }
