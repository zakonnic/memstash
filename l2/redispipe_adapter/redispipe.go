// Package redispipe_adapter adapts a redispipe sender (github.com/joomcode/redispipe) to the memstash.L2Cache
// contract.
package redispipe_adapter

import (
	"context"
	"fmt"
	"time"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"

	redispiperedis "github.com/joomcode/redispipe/redis"
)

// Cache is an L2 adapter over a redispipe Sender (redisconn or rediscluster). The sender is safe for concurrent use;
// its lifecycle stays with the caller. Note the redispipe semantics: a context canceled after the request was sent
// does not undo the command on the server.
type Cache[K comparable, V any] struct {
	sync    redispiperedis.SyncCtx
	codec   memstash.Codec[V]
	keyFunc func(K) string
}

var _ memstash.L2Cache[string, string] = (*Cache[string, string])(nil)

// New creates the adapter with an explicit value codec. By default keys must be strings (identity mapping); for other
// key types pass l2.WithKeyFunc.
func New[K comparable, V any](sender redispiperedis.Sender, codec memstash.Codec[V], opts ...memstash.Option) (*Cache[K, V], error) {
	if sender == nil {
		return nil, l2.ErrNilClient
	}
	if codec == nil {
		return nil, l2.ErrNilCodec
	}
	keyFunc, err := l2.ResolveOptions[K](opts)
	if err != nil {
		return nil, err
	}
	return &Cache[K, V]{sync: redispiperedis.SyncCtx{S: sender}, codec: codec, keyFunc: keyFunc}, nil
}

// NewJSON creates the adapter that marshals values with encoding/json.
func NewJSON[K comparable, V any](sender redispiperedis.Sender, opts ...memstash.Option) (*Cache[K, V], error) {
	return New[K, V](sender, l2.JSONCodec[V](), opts...)
}

// NewBytes creates the adapter that passes []byte values through unchanged.
func NewBytes[K comparable](sender redispiperedis.Sender, opts ...memstash.Option) (*Cache[K, []byte], error) {
	return New[K, []byte](sender, l2.BytesCodec(), opts...)
}

// NewCache builds a two-level cache in one call: a new memstash.Cache backed by this adapter as its L2. The single
// option list is shared: the memstash.With* options configure the cache, l2.WithKeyFunc configures the adapter.
func NewCache[K comparable, V any](sender redispiperedis.Sender, codec memstash.Codec[V], opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	adapter, err := New[K, V](sender, codec, opts...)
	if err != nil {
		return nil, err
	}
	cacheOpts := make([]memstash.Option, 0, len(opts)+1)
	cacheOpts = append(cacheOpts, opts...)
	cacheOpts = append(cacheOpts, memstash.WithL2Cache[K, V](adapter))
	return memstash.New[K, V](cacheOpts...)
}

// NewCacheJSON builds a two-level cache with the JSON value codec (see NewCache).
func NewCacheJSON[K comparable, V any](sender redispiperedis.Sender, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	return NewCache[K, V](sender, l2.JSONCodec[V](), opts...)
}

// NewCacheBytes builds a two-level cache that passes []byte values through unchanged (see NewCache).
func NewCacheBytes[K comparable](sender redispiperedis.Sender, opts ...memstash.Option) (*memstash.Cache[K, []byte], error) {
	return NewCache[K, []byte](sender, l2.BytesCodec(), opts...)
}

// Get returns the value; a missing key is (zero, false, nil).
func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	result := c.sync.Do(ctx, "GET", c.keyFunc(key))
	if err := redispiperedis.AsError(result); err != nil {
		return zero, false, err
	}
	if result == nil {
		return zero, false, nil
	}
	data, ok := result.([]byte)
	if !ok {
		return zero, false, fmt.Errorf("memstash/l2/redispipe: unexpected GET reply type %T", result)
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
	var result any
	if ttl > 0 {
		result = c.sync.Do(ctx, "SET", c.keyFunc(key), data, "PX", l2.RedisMillis(ttl))
	} else {
		result = c.sync.Do(ctx, "SET", c.keyFunc(key), data)
	}
	return redispiperedis.AsError(result)
}

// Delete removes the key; a missing key is not an error.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	return redispiperedis.AsError(c.sync.Do(ctx, "DEL", c.keyFunc(key)))
}

// BatchGet fetches all keys in one SendMany batch (redispipe pipelines it over its single connection).
func (c *Cache[K, V]) BatchGet(ctx context.Context, keys []K) (memstash.List[K, V], error) {
	length := len(keys)
	found := make(memstash.List[K, V], 0, length)
	if length == 0 {
		return found, nil
	}
	requests := make([]redispiperedis.Request, length)
	for i, key := range keys {
		requests[i] = redispiperedis.Req("GET", c.keyFunc(key))
	}
	for i, result := range c.sync.SendMany(ctx, requests) {
		if err := redispiperedis.AsError(result); err != nil {
			return found, err
		}
		if result == nil {
			continue
		}
		data, ok := result.([]byte)
		if !ok {
			return found, fmt.Errorf("memstash/l2/redispipe: unexpected GET reply type %T", result)
		}
		value, err := c.codec.Unmarshal(data)
		if err != nil {
			return found, err
		}
		found = append(found, memstash.KeyVal[K, V]{Key: keys[i], Value: value})
	}
	return found, nil
}

// BatchSet stores all items in one SendMany batch; ttl == 0 means "no expiration".
func (c *Cache[K, V]) BatchSet(ctx context.Context, items memstash.List[K, V], ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}
	requests := make([]redispiperedis.Request, 0, len(items))
	for _, item := range items {
		data, err := c.codec.Marshal(item.Value)
		if err != nil {
			return err
		}
		if ttl > 0 {
			requests = append(requests, redispiperedis.Req("SET", c.keyFunc(item.Key), data, "PX", l2.RedisMillis(ttl)))
		} else {
			requests = append(requests, redispiperedis.Req("SET", c.keyFunc(item.Key), data))
		}
	}
	for _, result := range c.sync.SendMany(ctx, requests) {
		if err := redispiperedis.AsError(result); err != nil {
			return err
		}
	}
	return nil
}
