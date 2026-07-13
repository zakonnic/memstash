// Package valyala_adapter adapts the valyala/ybc memcached client (github.com/valyala/ybc/libs/go/memcache) to the
// memstash.L2Cache contract.
//
// The client does not support context: the ctx arguments are ignored, calls can be neither canceled nor given a
// per-call deadline. Bound the latency with the client's ReadTimeout/WriteTimeout instead.
//
// Note: building this adapter requires a C toolchain. The ybc memcache package bundles a cgo-backed CachingClient
// alongside the pure-Go network client, so the whole package compiles only with cgo enabled.
package valyala_adapter

import (
	"context"
	"errors"
	"time"

	"github.com/valyala/ybc/libs/go/memcache"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
)

// Cache is an L2 adapter over the valyala/ybc memcached client. The client is safe for concurrent use. Its lifecycle
// stays with the caller: the client must be Start()-ed before use and Stop()-ed by the owner.
type Cache[K comparable, V any] struct {
	client  *memcache.Client
	codec   memstash.Codec[V]
	keyFunc func(K) string
}

var _ memstash.L2Cache[string, string] = (*Cache[string, string])(nil)

// New creates the adapter with an explicit value codec. By default keys must be strings (identity mapping, 250 bytes
// maximum); for other key types pass l2.WithKeyFunc.
func New[K comparable, V any](client *memcache.Client, codec memstash.Codec[V], opts ...memstash.Option) (*Cache[K, V], error) {
	if client == nil {
		return nil, l2.ErrNilClient
	}
	if codec == nil {
		return nil, l2.ErrNilCodec
	}
	keyFunc, err := l2.ResolveOptions[K](opts)
	if err != nil {
		return nil, err
	}
	return &Cache[K, V]{client: client, codec: codec, keyFunc: keyFunc}, nil
}

// NewJSON creates the adapter that marshals values with encoding/json.
func NewJSON[K comparable, V any](client *memcache.Client, opts ...memstash.Option) (*Cache[K, V], error) {
	return New[K, V](client, l2.JSONCodec[V](), opts...)
}

// NewBytes creates the adapter that passes []byte values through unchanged.
func NewBytes[K comparable](client *memcache.Client, opts ...memstash.Option) (*Cache[K, []byte], error) {
	return New[K, []byte](client, l2.BytesCodec(), opts...)
}

// NewCache builds a two-level cache in one call: a new memstash.Cache backed by this adapter as its L2. The single
// option list is shared: the memstash.With* options configure the cache, l2.WithKeyFunc configures the adapter.
func NewCache[K comparable, V any](client *memcache.Client, codec memstash.Codec[V], opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	adapter, err := New[K, V](client, codec, opts...)
	if err != nil {
		return nil, err
	}
	cacheOpts := make([]memstash.Option, 0, len(opts)+1)
	cacheOpts = append(cacheOpts, opts...)
	cacheOpts = append(cacheOpts, memstash.WithL2Cache[K, V](adapter))
	return memstash.New[K, V](cacheOpts...)
}

// NewCacheJSON builds a two-level cache with the JSON value codec (see NewCache).
func NewCacheJSON[K comparable, V any](client *memcache.Client, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	return NewCache[K, V](client, l2.JSONCodec[V](), opts...)
}

// NewCacheBytes builds a two-level cache that passes []byte values through unchanged (see NewCache).
func NewCacheBytes[K comparable](client *memcache.Client, opts ...memstash.Option) (*memstash.Cache[K, []byte], error) {
	return NewCache[K, []byte](client, l2.BytesCodec(), opts...)
}

// Get returns the value; a missing key is (zero, false, nil). The context is ignored: the client has no context
// support.
func (c *Cache[K, V]) Get(_ context.Context, key K) (V, bool, error) {
	var zero V
	item := memcache.Item{Key: []byte(c.keyFunc(key))}
	if err := c.client.Get(&item); err != nil {
		if errors.Is(err, memcache.ErrCacheMiss) {
			return zero, false, nil
		}
		return zero, false, err
	}
	value, err := c.codec.Unmarshal(item.Value)
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
	if ttl > 0 && ttl < time.Second {
		ttl = time.Second // memcached expiration resolution is one second - do not let it truncate to "never"
	}
	return c.client.Set(&memcache.Item{
		Key:        []byte(c.keyFunc(key)),
		Value:      data,
		Expiration: ttl,
	})
}

// Delete removes the key; a missing key is not an error. The context is ignored: the client has no context support.
func (c *Cache[K, V]) Delete(_ context.Context, key K) error {
	if err := c.client.Delete([]byte(c.keyFunc(key))); err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
		return err
	}
	return nil
}

// BatchGet fetches all keys in one native GetMulti round trip. The context is ignored: the client has no context
// support.
func (c *Cache[K, V]) BatchGet(ctx context.Context, keys []K) (memstash.List[K, V], error) {
	length := len(keys)
	found := make(memstash.List[K, V], 0, length)
	if length == 0 {
		return found, nil
	}
	items := make([]memcache.Item, length)
	for i, key := range keys {
		items[i].Key = []byte(c.keyFunc(key))
	}
	if err := c.client.GetMulti(items); err != nil {
		return found, err
	}
	for i := range items {
		if items[i].Value == nil {
			continue // GetMulti does not modify Value for items missing on the server
		}
		value, err := c.codec.Unmarshal(items[i].Value)
		if err != nil {
			return found, err
		}
		found = append(found, memstash.KeyVal[K, V]{Key: keys[i], Value: value})
	}
	return found, nil
}

// BatchWorkers is the number of goroutines the concurrent batch fallback runs; the ybc client pipelines concurrent
// requests natively, so no connection tuning is needed.
const BatchWorkers = 8

// BatchSet stores the items with concurrent Sets: the protocol has no multi-set. The context is ignored: the client
// has no context support.
func (c *Cache[K, V]) BatchSet(ctx context.Context, items memstash.List[K, V], ttl time.Duration) error {
	return l2.BatchSetConcurrent(ctx, c, items, ttl, BatchWorkers)
}
