// Package redispipe_adapter adapts a redispipe sender (github.com/joomcode/redispipe) to the memstash.L2Cache
// contract.
package redispipe_adapter

import (
	"context"
	"fmt"
	"time"

	redispiperedis "github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/redisconn"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
)

// Cache is an L2 adapter over a redispipe Sender (redisconn or rediscluster). The sender is safe for concurrent use;
// its lifecycle stays with the caller. Note the redispipe semantics: a context canceled after the request was sent
// does not undo the command on the server.
type Cache[K comparable, V any] struct {
	sync    redispiperedis.SyncCtx
	codec   memstash.Codec[V]
	keyFunc func(K) string
	// singleNode marks a plain *redisconn.Connection sender, whose commands always reach one server, so batches may
	// use MGET/MSET - other senders (rediscluster, wrappers) keep the per-key SendMany.
	singleNode bool
	// orderedMget marks a sender whose MGET replies come back in key order. Same probe as singleNode by default, but
	// l2.WithOrderedMget can override it on its own.
	orderedMget bool
}

var _ memstash.L2Cache[string, string] = (*Cache[string, string])(nil)

// IsOrderedMgetAvailable reports whether MGET on this sender returns its replies in key order. It holds for a plain
// *redisconn.Connection; a rediscluster sender splits the keys by slot and the order is lost.
func IsOrderedMgetAvailable(sender redispiperedis.Sender) bool {
	_, ok := sender.(*redisconn.Connection)
	return ok
}

// Multi-key commands (MGET/MSET) beat a per-key batch while they stay small.
// But one huge command is worse than a stream of small ones, so we set this limit.
const multiKeyBudget = 12 * 1024

// argWireOverhead approximates the RESP framing bytes added per argument ($<len>\r\n...\r\n).
const argWireOverhead = 16

// MaxMsetItems is the longest batch that can still fit multiKeyBudget. Past it the framing alone busts the budget
// whatever the values weigh, so BatchSet builds the SendMany requests directly and never boxes the flat MSET args.
const MaxMsetItems = multiKeyBudget / (2 * argWireOverhead)

// New creates the adapter with an explicit value codec. By default keys must be strings (identity mapping); for other
// key types pass l2.WithKeyFunc.
func New[K comparable, V any](sender redispiperedis.Sender, codec memstash.Codec[V], opts ...memstash.Option) (*Cache[K, V], error) {
	if sender == nil {
		return nil, l2.ErrNilClient
	}
	if codec == nil {
		return nil, l2.ErrNilCodec
	}
	keyFunc, err := l2.ExtractKeyFunc[K](opts)
	if err != nil {
		return nil, err
	}
	mgetMode, err := l2.ExtractOrderedMget(opts)
	if err != nil {
		return nil, err
	}
	_, singleNode := sender.(*redisconn.Connection)
	return &Cache[K, V]{
		sync:        redispiperedis.SyncCtx{S: sender},
		codec:       codec,
		keyFunc:     keyFunc,
		singleNode:  singleNode,
		orderedMget: l2.ResolveOrderedMget(mgetMode, func() bool { return IsOrderedMgetAvailable(sender) }),
	}, nil
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

// NewJSONCache builds a two-level cache with the JSON value codec (see NewCache).
func NewJSONCache[K comparable, V any](sender redispiperedis.Sender, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	return NewCache[K, V](sender, l2.JSONCodec[V](), opts...)
}

// NewBytesCache builds a two-level cache that passes []byte values through unchanged (see NewCache).
func NewBytesCache[K comparable](sender redispiperedis.Sender, opts ...memstash.Option) (*memstash.Cache[K, []byte], error) {
	return NewCache[K, []byte](sender, l2.BytesCodec(), opts...)
}

// NewStringCache builds a two-level cache that passes string values through unchanged (see NewCache).
func NewStringCache[K comparable](sender redispiperedis.Sender, opts ...memstash.Option) (*memstash.Cache[K, string], error) {
	return NewCache[K, string](sender, l2.StringCodec(), opts...)
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

// BatchDelete removes all keys in one round trip: a single DEL request on a plain connection when the batch fits
// multiKeyBudget, otherwise a SendMany batch of DELs.
func (c *Cache[K, V]) BatchDelete(ctx context.Context, keys []K) error {
	if len(keys) == 0 {
		return nil
	}
	storageKeys := make([]string, len(keys))
	size := 0
	for i, key := range keys {
		storageKeys[i] = c.keyFunc(key)
		size += len(storageKeys[i]) + argWireOverhead
	}
	if c.singleNode && size <= multiKeyBudget {
		// Boxing happens here only: the SendMany path below takes the keys as they are.
		args := make([]interface{}, len(storageKeys))
		for i, storageKey := range storageKeys {
			args[i] = storageKey
		}
		return redispiperedis.AsError(c.sync.Do(ctx, "DEL", args...))
	}
	requests := make([]redispiperedis.Request, len(storageKeys))
	for i, storageKey := range storageKeys {
		requests[i] = redispiperedis.Req("DEL", storageKey)
	}
	return sendManyErr(c.sync.SendMany(ctx, requests))
}

// BatchGet fetches all keys in one round trip: a single MGET request when the sender returns ordered replies (see
// l2.WithOrderedMget) and the batch fits multiKeyBudget, otherwise a SendMany batch of GETs.
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
	if c.orderedMget && size <= multiKeyBudget {
		// Boxing happens here only: the SendMany path below takes the keys as they are.
		args := make([]interface{}, length)
		for i, storageKey := range storageKeys {
			args[i] = storageKey
		}
		result := c.sync.Do(ctx, "MGET", args...)
		if err := redispiperedis.AsError(result); err != nil {
			return found, err
		}
		replies, ok := result.([]interface{})
		if !ok {
			return found, fmt.Errorf("memstash/l2/redispipe: unexpected MGET reply type %T", result)
		}
		for i, reply := range replies {
			if reply == nil { // a miss
				continue
			}
			data, ok := reply.([]byte)
			if !ok {
				return found, fmt.Errorf("memstash/l2/redispipe: unexpected MGET element type %T", reply)
			}
			value, err := c.codec.Unmarshal(data)
			if err != nil {
				return found, err
			}
			found = append(found, memstash.KeyVal[K, V]{Key: keys[i], Value: value})
		}
		return found, nil
	}
	return c.batchGetSendMany(ctx, keys, storageKeys)
}

// batchGetSendMany is the per-key BatchGet path: a SendMany batch of GETs. storageKeys is the caller's already
// mapped keys, positionally matching keys.
func (c *Cache[K, V]) batchGetSendMany(ctx context.Context, keys []K, storageKeys []string) (memstash.List[K, V], error) {
	found := make(memstash.List[K, V], 0, len(keys))
	requests := make([]redispiperedis.Request, len(storageKeys))
	for i, storageKey := range storageKeys {
		requests[i] = redispiperedis.Req("GET", storageKey)
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

// BatchSet stores all items in one round trip; ttl == 0 means "no expiration". A no-TTL batch on a plain connection
// that fits multiKeyBudget goes out as a single MSET request; anything else as a SendMany batch of SETs.
func (c *Cache[K, V]) BatchSet(ctx context.Context, items memstash.List[K, V], ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}
	if c.singleNode && ttl <= 0 && len(items) <= MaxMsetItems {
		args := make([]interface{}, 0, 2*len(items))
		size := 0
		for _, item := range items {
			data, err := c.codec.Marshal(item.Value)
			if err != nil {
				return err
			}
			storageKey := c.keyFunc(item.Key)
			args = append(args, storageKey, data)
			size += len(storageKey) + len(data) + 2*argWireOverhead
		}
		if size <= multiKeyBudget {
			return redispiperedis.AsError(c.sync.Do(ctx, "MSET", args...))
		}
		requests := make([]redispiperedis.Request, 0, len(items))
		for i := 0; i < len(args); i += 2 {
			requests = append(requests, redispiperedis.Req("SET", args[i], args[i+1]))
		}
		return sendManyErr(c.sync.SendMany(ctx, requests))
	}
	var millis int64
	if ttl > 0 {
		millis = l2.RedisMillis(ttl)
	}
	requests := make([]redispiperedis.Request, 0, len(items))
	for _, item := range items {
		data, err := c.codec.Marshal(item.Value)
		if err != nil {
			return err
		}
		if ttl > 0 {
			requests = append(requests, redispiperedis.Req("SET", c.keyFunc(item.Key), data, "PX", millis))
		} else {
			requests = append(requests, redispiperedis.Req("SET", c.keyFunc(item.Key), data))
		}
	}
	return sendManyErr(c.sync.SendMany(ctx, requests))
}

// sendManyErr returns the first error of a SendMany result set.
func sendManyErr(results []interface{}) error {
	for _, result := range results {
		if err := redispiperedis.AsError(result); err != nil {
			return err
		}
	}
	return nil
}
