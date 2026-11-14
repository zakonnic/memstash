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
	wbCalls int
}

var _ memstash.L2Cache[string, string] = (*Cache[string, string])(nil)

// New creates the adapter with an explicit value codec. By default keys must be strings (identity mapping); for other
// key types pass l2.WithKeyFunc. A key memcached rejects is stored under a stand-in (see l2.MemcacheKey).
//
// wbConcurrentCalls is how many write-back calls the client gets at once (see ConcurrentCalls), and with that the
// cache's WriteBack pool: memstash.WithWriteBackWorkers is rejected with this adapter. 0 means half the client's
// MaxIdleConnsPerAddr, at least memstash.DefaultWriteBackWorkers, so reads keep the other half.
func New[K comparable, V any](client *memcache.Client, codec memstash.Codec[V], wbConcurrentCalls int, opts ...memstash.Option) (*Cache[K, V], error) {
	if client == nil {
		return nil, l2.ErrNilClient
	}
	if codec == nil {
		return nil, l2.ErrNilCodec
	}
	if wbConcurrentCalls < 0 {
		return nil, l2.ErrBadConcurrentCalls
	}
	keyFunc, err := l2.ExtractKeyFunc[K](opts)
	if err != nil {
		return nil, err
	}
	storageKey := keyFunc
	keyFunc = func(key K) string { return l2.MemcacheKey(storageKey(key)) }
	return &Cache[K, V]{client: client, codec: codec, keyFunc: keyFunc, wbCalls: wbConcurrentCalls}, nil
}

// NewJSON creates the adapter that marshals values with encoding/json (wbConcurrentCalls: see New).
func NewJSON[K comparable, V any](client *memcache.Client, wbConcurrentCalls int, opts ...memstash.Option) (*Cache[K, V], error) {
	return New[K, V](client, l2.JSONCodec[V](), wbConcurrentCalls, opts...)
}

// NewBytes creates the adapter that passes []byte values through unchanged (wbConcurrentCalls: see New).
func NewBytes[K comparable](client *memcache.Client, wbConcurrentCalls int, opts ...memstash.Option) (*Cache[K, []byte], error) {
	return New[K, []byte](client, l2.BytesCodec(), wbConcurrentCalls, opts...)
}

// NewCache builds a two-level cache in one call: a new memstash.Cache backed by this adapter as its L2. The single
// option list is shared: the memstash.With* options configure the cache, l2.WithKeyFunc configures the adapter.
// wbConcurrentCalls sizes the cache's WriteBack pool (see New).
func NewCache[K comparable, V any](client *memcache.Client, codec memstash.Codec[V], wbConcurrentCalls int, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	adapter, err := New[K, V](client, codec, wbConcurrentCalls, opts...)
	if err != nil {
		return nil, err
	}
	cacheOpts := make([]memstash.Option, 0, len(opts)+1)
	cacheOpts = append(cacheOpts, opts...)
	cacheOpts = append(cacheOpts, memstash.WithL2Cache[K, V](adapter))
	return memstash.New[K, V](cacheOpts...)
}

// NewJSONCache builds a two-level cache with the JSON value codec (see NewCache).
func NewJSONCache[K comparable, V any](client *memcache.Client, wbConcurrentCalls int, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	return NewCache[K, V](client, l2.JSONCodec[V](), wbConcurrentCalls, opts...)
}

// NewBytesCache builds a two-level cache that passes []byte values through unchanged (see NewCache).
func NewBytesCache[K comparable](client *memcache.Client, wbConcurrentCalls int, opts ...memstash.Option) (*memstash.Cache[K, []byte], error) {
	return NewCache[K, []byte](client, l2.BytesCodec(), wbConcurrentCalls, opts...)
}

// NewStringCache builds a two-level cache that passes string values through unchanged (see NewCache).
func NewStringCache[K comparable](client *memcache.Client, wbConcurrentCalls int, opts ...memstash.Option) (*memstash.Cache[K, string], error) {
	return NewCache[K, string](client, l2.StringCodec(), wbConcurrentCalls, opts...)
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

// BatchGet fetches the keys in one GetMulti round trip and asks again, with Get, for every key missing from the
// answer. GetMulti drops its errors - an unreachable server, a read past the connection's deadline - and answers with
// fewer items, so a key it leaves out is a miss or a failure, and Get tells which. A batch of misses therefore costs
// the concurrent Gets on top. The context is ignored: the client has no context support.
func (c *Cache[K, V]) BatchGet(ctx context.Context, keys []K) (memstash.List[K, V], error) {
	if len(keys) <= 1 {
		return l2.BatchGetSequential(ctx, c, keys)
	}
	storageKeys := make([]string, len(keys))
	for i, key := range keys {
		storageKeys[i] = c.keyFunc(key)
	}
	items, err := c.client.GetMulti(storageKeys)
	if err != nil {
		return nil, err
	}
	found := make(memstash.List[K, V], 0, len(keys))
	var absent []K
	var errs []error
	for i, key := range keys {
		item, ok := items[storageKeys[i]]
		if !ok {
			absent = append(absent, key)
			continue
		}
		value, err := c.codec.Unmarshal(item.Value)
		if err != nil {
			errs = append(errs, l2.KeyErr(key, err))
			continue
		}
		found = append(found, memstash.KeyVal[K, V]{Key: key, Value: value})
	}
	if len(absent) == 0 {
		return found, errors.Join(errs...)
	}
	reread, err := l2.BatchGetConcurrent(ctx, c, absent, BatchWorkers)
	if err != nil {
		// The failure concerns the keys Get did not find or did not reach, never the ones GetMulti returned: a miss
		// read before the failure is not told apart. reread keeps absent's order.
		answered := 0
		for _, key := range absent {
			if answered < len(reread) && reread[answered].Key == key {
				answered++
				continue
			}
			errs = append(errs, l2.KeyErr(key, err))
		}
	}
	return append(found, reread...), errors.Join(errs...)
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

// memstash.SingleCallL2, spelled out so the module still builds against a core without it.
var _ interface{ ConcurrentCalls() int } = (*Cache[string, []byte])(nil)

// ConcurrentCalls is the constructor's wbConcurrentCalls. Without one it is half the client's MaxIdleConnsPerAddr, so
// reads keep the other half; at least DefaultWriteBackWorkers. Past the idle connections memcache.Client dials one for
// a call and closes it after. It makes the adapter a memstash.SingleCallL2.
func (c *Cache[K, V]) ConcurrentCalls() int {
	if c.wbCalls > 0 {
		return c.wbCalls
	}
	return max(c.client.MaxIdleConnsPerAddr()/2, memstash.DefaultWriteBackWorkers)
}
