// Package goredis_adapter adapts a go-redis client (github.com/redis/go-redis/v9) to the memstash.L2Cache contract.
package goredis_adapter

import (
	"context"
	"errors"
	"time"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"

	"github.com/redis/go-redis/v9"
)

// Cache is an L2 adapter over go-redis. Any go-redis client (single node, cluster, ring) implements redis.Cmdable and
// is safe for concurrent use. The client's lifecycle stays with the caller.
type Cache[K comparable, V any] struct {
	client  redis.Cmdable
	codec   memstash.Codec[V]
	keyFunc func(K) string
}

var _ memstash.L2Cache[string, string] = (*Cache[string, string])(nil)

// New creates the adapter with an explicit value codec. By default keys must be strings (identity mapping); for other
// key types pass l2.WithKeyFunc.
func New[K comparable, V any](client redis.Cmdable, codec memstash.Codec[V], opts ...memstash.Option) (*Cache[K, V], error) {
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
func NewJSON[K comparable, V any](client redis.Cmdable, opts ...memstash.Option) (*Cache[K, V], error) {
	return New[K, V](client, l2.JSONCodec[V](), opts...)
}

// NewBytes creates the adapter that passes []byte values through unchanged.
func NewBytes[K comparable](client redis.Cmdable, opts ...memstash.Option) (*Cache[K, []byte], error) {
	return New[K, []byte](client, l2.BytesCodec(), opts...)
}

// NewCache builds a two-level cache in one call: a new memstash.Cache backed by this adapter as its L2. The single
// option list is shared: the memstash.With* options configure the cache, l2.WithKeyFunc configures the adapter.
func NewCache[K comparable, V any](client redis.Cmdable, codec memstash.Codec[V], opts ...memstash.Option) (*memstash.Cache[K, V], error) {
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
func NewCacheJSON[K comparable, V any](client redis.Cmdable, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	return NewCache[K, V](client, l2.JSONCodec[V](), opts...)
}

// NewCacheBytes builds a two-level cache that passes []byte values through unchanged (see NewCache).
func NewCacheBytes[K comparable](client redis.Cmdable, opts ...memstash.Option) (*memstash.Cache[K, []byte], error) {
	return NewCache[K, []byte](client, l2.BytesCodec(), opts...)
}

// Get returns the value; a missing key is (zero, false, nil).
func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	data, err := c.client.Get(ctx, c.keyFunc(key)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return zero, false, nil
		}
		return zero, false, err
	}
	value, err := c.codec.Unmarshal(data)
	if err != nil {
		return zero, false, err
	}
	return value, true, nil
}

// Set stores the value; ttl == 0 means "no expiration".
func (c *Cache[K, V]) Set(ctx context.Context, key K, value V, ttl time.Duration) error {
	data, err := c.codec.Marshal(value)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, c.keyFunc(key), data, ttl).Err()
}

// Delete removes the key; a missing key is not an error.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	return c.client.Del(ctx, c.keyFunc(key)).Err()
}

// BatchGet fetches all keys in one pipelined round trip (the pipeline is routed per node on a cluster client).
func (c *Cache[K, V]) BatchGet(ctx context.Context, keys []K) (memstash.List[K, V], error) {
	length := len(keys)
	found := make(memstash.List[K, V], 0, length)
	if length == 0 {
		return found, nil
	}
	cmds := make([]*redis.StringCmd, length)
	if _, err := c.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i, key := range keys {
			cmds[i] = pipe.Get(ctx, c.keyFunc(key))
		}
		return nil
	}); err != nil && !errors.Is(err, redis.Nil) { // Pipelined reports redis.Nil for ordinary misses
		return found, err
	}
	for i, cmd := range cmds {
		data, err := cmd.Bytes()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return found, err
		}
		value, err := c.codec.Unmarshal(data)
		if err != nil {
			return found, err
		}
		found = append(found, memstash.KeyVal[K, V]{Key: keys[i], Value: value})
	}
	return found, nil
}

// BatchSet stores all items in one pipelined round trip; ttl == 0 means "no expiration".
func (c *Cache[K, V]) BatchSet(ctx context.Context, items memstash.List[K, V], ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}
	_, err := c.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, item := range items {
			data, err := c.codec.Marshal(item.Value)
			if err != nil {
				return err
			}
			pipe.Set(ctx, c.keyFunc(item.Key), data, ttl)
		}
		return nil
	})
	return err
}
