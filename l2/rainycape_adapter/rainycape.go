// Package rainycape_adapter adapts the rainycape memcached client (github.com/rainycape/memcache) to the
// memstash.L2Cache contract.
//
// The client does not support context: the ctx arguments are ignored, calls can be neither canceled nor given a
// per-call deadline. Bound the latency with memcache.Client.SetTimeout instead.
package rainycape_adapter

import (
	"context"
	"errors"
	"time"

	"github.com/rainycape/memcache"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
)

// Cache is an L2 adapter over the rainycape memcached client. The client is safe for concurrent use; its lifecycle
// stays with the caller.
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

// NewJSONCache builds a two-level cache with the JSON value codec (see NewCache).
func NewJSONCache[K comparable, V any](client *memcache.Client, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	return NewCache[K, V](client, l2.JSONCodec[V](), opts...)
}

// NewBytesCache builds a two-level cache that passes []byte values through unchanged (see NewCache).
func NewBytesCache[K comparable](client *memcache.Client, opts ...memstash.Option) (*memstash.Cache[K, []byte], error) {
	return NewCache[K, []byte](client, l2.BytesCodec(), opts...)
}

// NewStringCache builds a two-level cache that passes string values through unchanged (see NewCache).
func NewStringCache[K comparable](client *memcache.Client, opts ...memstash.Option) (*memstash.Cache[K, string], error) {
	return NewCache[K, string](client, l2.StringCodec(), opts...)
}

// Get returns the value; a missing key is (zero, false, nil). The context is ignored: the client has no context
// support.
func (c *Cache[K, V]) Get(_ context.Context, key K) (V, bool, error) {
	var zero V
	item, err := c.client.Get(c.keyFunc(key))
	if err != nil {
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
	return c.client.Set(&memcache.Item{
		Key:        c.keyFunc(key),
		Value:      data,
		Expiration: l2.MemcacheExpiration(ttl),
	})
}

// Delete removes the key; a missing key is not an error. The context is ignored: the client has no context support.
func (c *Cache[K, V]) Delete(_ context.Context, key K) error {
	if err := c.client.Delete(c.keyFunc(key)); err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
		return err
	}
	return nil
}

// BatchGet fetches all keys in one native GetMulti round trip. Raise the connection limit via
// Client.SetMaxIdleConnsPerAddr when large batches run concurrently. The context is ignored: the client has no
// context support.
func (c *Cache[K, V]) BatchGet(ctx context.Context, keys []K) (memstash.List[K, V], error) {
	length := len(keys)
	found := make(memstash.List[K, V], 0, length)
	if length == 0 {
		return found, nil
	}
	storageKeys := make([]string, length)
	byStorageKey := make(map[string]K, length)
	for i, key := range keys {
		storageKeys[i] = c.keyFunc(key)
		byStorageKey[storageKeys[i]] = key
	}
	items, err := c.client.GetMulti(storageKeys)
	if err != nil {
		return found, err
	}
	for storageKey, item := range items {
		value, err := c.codec.Unmarshal(item.Value)
		if err != nil {
			return found, err
		}
		found = append(found, memstash.KeyVal[K, V]{
			Key:   byStorageKey[storageKey],
			Value: value,
		})
	}
	return found, nil
}

// BatchWorkers is the number of goroutines the concurrent batch fallback runs. Raise the connection limit via
// Client.SetMaxIdleConnsPerAddr to at least this value, or each batch churns through fresh connections.
const BatchWorkers = 8

// BatchSet stores the items with concurrent Sets: the protocol has no multi-set. The context is ignored: the client
// has no context support.
func (c *Cache[K, V]) BatchSet(ctx context.Context, items memstash.List[K, V], ttl time.Duration) error {
	return l2.BatchSetConcurrent(ctx, c, items, ttl, BatchWorkers)
}

// BatchDelete removes the keys with concurrent Deletes: the protocol has no multi-delete. The context is ignored:
// the client has no context support.
func (c *Cache[K, V]) BatchDelete(ctx context.Context, keys []K) error {
	return l2.BatchDeleteConcurrent(ctx, c, keys, BatchWorkers)
}
