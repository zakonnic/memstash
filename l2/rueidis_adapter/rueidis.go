// Package rueidis_adapter adapts a rueidis client (github.com/redis/rueidis) to the memstash.L2Cache contract.
package rueidis_adapter

import (
	"context"
	"time"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"

	rueidislib "github.com/redis/rueidis"
)

// Cache is an L2 adapter over rueidis. The client is safe for concurrent use; its lifecycle stays with the caller.
type Cache[K comparable, V any] struct {
	client  rueidislib.Client
	codec   memstash.Codec[V]
	keyFunc func(K) string
}

var _ memstash.L2Cache[string, string] = (*Cache[string, string])(nil)

// Multi-key commands (MGET/MSET) beat a per-key pipeline while they stay small.
// But one huge command is worse than a stream of small ones, so we set this limit.
const multiKeyBudget = 16 * 1024

// argWireOverhead approximates the RESP framing bytes added per argument ($<len>\r\n...\r\n).
const argWireOverhead = 16

// New creates the adapter with an explicit value codec. By default keys must be strings (identity mapping); for other
// key types pass l2.WithKeyFunc.
func New[K comparable, V any](client rueidislib.Client, codec memstash.Codec[V], opts ...memstash.Option) (*Cache[K, V], error) {
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
func NewJSON[K comparable, V any](client rueidislib.Client, opts ...memstash.Option) (*Cache[K, V], error) {
	return New[K, V](client, l2.JSONCodec[V](), opts...)
}

// NewBytes creates the adapter that passes []byte values through unchanged.
func NewBytes[K comparable](client rueidislib.Client, opts ...memstash.Option) (*Cache[K, []byte], error) {
	return New[K, []byte](client, l2.BytesCodec(), opts...)
}

// NewCache builds a two-level cache in one call: a new memstash.Cache backed by this adapter as its L2. The single
// option list is shared: the memstash.With* options configure the cache, l2.WithKeyFunc configures the adapter.
func NewCache[K comparable, V any](client rueidislib.Client, codec memstash.Codec[V], opts ...memstash.Option) (*memstash.Cache[K, V], error) {
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
func NewCacheJSON[K comparable, V any](client rueidislib.Client, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	return NewCache[K, V](client, l2.JSONCodec[V](), opts...)
}

// NewCacheBytes builds a two-level cache that passes []byte values through unchanged (see NewCache).
func NewCacheBytes[K comparable](client rueidislib.Client, opts ...memstash.Option) (*memstash.Cache[K, []byte], error) {
	return NewCache[K, []byte](client, l2.BytesCodec(), opts...)
}

// Get returns the value; a missing key is (zero, false, nil).
func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	cmd := c.client.B().Get().Key(c.keyFunc(key)).Build()
	data, err := c.client.Do(ctx, cmd).AsBytes()
	if err != nil {
		if rueidislib.IsRedisNil(err) {
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
	// The command builder is single-use, so each branch builds its own command from scratch.
	var cmd rueidislib.Completed
	if ttl > 0 {
		cmd = c.client.B().Set().Key(c.keyFunc(key)).Value(rueidislib.BinaryString(data)).
			PxMilliseconds(l2.RedisMillis(ttl)).Build()
	} else {
		cmd = c.client.B().Set().Key(c.keyFunc(key)).Value(rueidislib.BinaryString(data)).Build()
	}
	return c.client.Do(ctx, cmd).Error()
}

// Delete removes the key; a missing key is not an error.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	cmd := c.client.B().Del().Key(c.keyFunc(key)).Build()
	return c.client.Do(ctx, cmd).Error()
}

// BatchGet fetches all keys in one round trip: the rueidis MGet helper within multiKeyBudget, a DoMulti pipeline of
// GETs above it.
func (c *Cache[K, V]) BatchGet(ctx context.Context, keys []K) (memstash.List[K, V], error) {
	length := len(keys)
	found := make(memstash.List[K, V], 0, length)
	if length == 0 {
		return found, nil
	}
	storageKeys := make([]string, length)
	size := 0
	for i, key := range keys {
		storageKeys[i] = c.keyFunc(key)
		size += len(storageKeys[i]) + argWireOverhead
	}
	if size <= multiKeyBudget {
		replies, err := rueidislib.MGet(c.client, ctx, storageKeys)
		if err != nil {
			return found, err
		}
		for i, key := range keys {
			msg, ok := replies[storageKeys[i]]
			if !ok || msg.IsNil() {
				continue
			}
			data, err := msg.AsBytes() // a zero-copy view into the reply
			if err != nil {
				return found, err
			}
			value, err := c.codec.Unmarshal(data)
			if err != nil {
				return found, err
			}
			found = append(found, memstash.KeyVal[K, V]{Key: key, Value: value})
		}
		return found, nil
	}
	cmds := make([]rueidislib.Completed, length)
	for i, storageKey := range storageKeys {
		cmds[i] = c.client.B().Get().Key(storageKey).Build()
	}
	for i, resp := range c.client.DoMulti(ctx, cmds...) {
		data, err := resp.AsBytes()
		if err != nil {
			if rueidislib.IsRedisNil(err) {
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

// BatchSet stores all items in one round trip; ttl == 0 means "no expiration". A no-TTL batch within multiKeyBudget
// uses the rueidis MSet helper, anything else a DoMulti pipeline of SETs.
func (c *Cache[K, V]) BatchSet(ctx context.Context, items memstash.List[K, V], ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}
	if ttl <= 0 {
		kvs := make(map[string]string, len(items))
		size := 0
		for _, item := range items {
			data, err := c.codec.Marshal(item.Value)
			if err != nil {
				return err
			}
			storageKey := c.keyFunc(item.Key)
			kvs[storageKey] = rueidislib.BinaryString(data)
			size += len(storageKey) + len(data) + 2*argWireOverhead
		}
		if size <= multiKeyBudget {
			for _, err := range rueidislib.MSet(c.client, ctx, kvs) {
				if err != nil {
					return err
				}
			}
			return nil
		}
		cmds := make([]rueidislib.Completed, 0, len(kvs))
		for storageKey, data := range kvs {
			cmds = append(cmds, c.client.B().Set().Key(storageKey).Value(data).Build())
		}
		return doMultiErr(c.client.DoMulti(ctx, cmds...))
	}
	cmds := make([]rueidislib.Completed, 0, len(items))
	for _, item := range items {
		data, err := c.codec.Marshal(item.Value)
		if err != nil {
			return err
		}
		cmds = append(cmds, c.client.B().Set().Key(c.keyFunc(item.Key)).Value(rueidislib.BinaryString(data)).
			PxMilliseconds(l2.RedisMillis(ttl)).Build())
	}
	return doMultiErr(c.client.DoMulti(ctx, cmds...))
}

// doMultiErr returns the first error of a DoMulti result set.
func doMultiErr(resps []rueidislib.RedisResult) error {
	for _, resp := range resps {
		if err := resp.Error(); err != nil {
			return err
		}
	}
	return nil
}
