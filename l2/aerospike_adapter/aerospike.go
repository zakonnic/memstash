// Package aerospike_adapter adapts an Aerospike cluster (github.com/aerospike/aerospike-client-go/v7) to the
// memstash.L2Cache contract. Aerospike is a fast key/value store with native per-record TTL, well suited as an L2
// tier for real-time workloads.
//
// The constructor takes the client's ClientIfc interface, satisfied by *aerospike.Client, keeping the adapter
// independent of a concrete client. Records are addressed by (namespace, set, key); the value lives in a single
// "value" bin and expiration is handled by Aerospike itself.
package aerospike_adapter

import (
	"context"
	"time"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"

	as "github.com/aerospike/aerospike-client-go/v7"
	astypes "github.com/aerospike/aerospike-client-go/v7/types"
)

// binName holds the encoded value in every record.
const binName = "value"

// Cache is an L2 adapter over Aerospike. The client's lifecycle stays with the caller.
type Cache[K comparable, V any] struct {
	client    as.ClientIfc
	namespace string
	set       string
	codec     memstash.Codec[V]
	keyFunc   func(K) string
}

var _ memstash.L2Cache[string, string] = (*Cache[string, string])(nil)

// New creates the adapter with an explicit value codec. namespace is required; set may be empty. By default keys must
// be strings (identity mapping); for other key types pass l2.WithKeyFunc.
func New[K comparable, V any](client as.ClientIfc, codec memstash.Codec[V], namespace, set string, opts ...memstash.Option) (*Cache[K, V], error) {
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
	return &Cache[K, V]{client: client, namespace: namespace, set: set, codec: codec, keyFunc: keyFunc}, nil
}

// NewJSON creates the adapter that marshals values with encoding/json.
func NewJSON[K comparable, V any](client as.ClientIfc, namespace, set string, opts ...memstash.Option) (*Cache[K, V], error) {
	return New[K, V](client, l2.JSONCodec[V](), namespace, set, opts...)
}

// NewBytes creates the adapter that passes []byte values through unchanged.
func NewBytes[K comparable](client as.ClientIfc, namespace, set string, opts ...memstash.Option) (*Cache[K, []byte], error) {
	return New[K, []byte](client, l2.BytesCodec(), namespace, set, opts...)
}

// NewCache builds a two-level cache in one call: a new memstash.Cache backed by this adapter as its L2. The single
// option list is shared: the memstash.With* options configure the cache, l2.WithKeyFunc configures the adapter.
func NewCache[K comparable, V any](client as.ClientIfc, codec memstash.Codec[V], namespace, set string, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	adapter, err := New[K, V](client, codec, namespace, set, opts...)
	if err != nil {
		return nil, err
	}
	cacheOpts := make([]memstash.Option, 0, len(opts)+1)
	cacheOpts = append(cacheOpts, opts...)
	cacheOpts = append(cacheOpts, memstash.WithL2Cache[K, V](adapter))
	return memstash.New[K, V](cacheOpts...)
}

// NewCacheJSON builds a two-level cache with the JSON value codec (see NewCache).
func NewCacheJSON[K comparable, V any](client as.ClientIfc, namespace, set string, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	return NewCache[K, V](client, l2.JSONCodec[V](), namespace, set, opts...)
}

// NewCacheBytes builds a two-level cache that passes []byte values through unchanged (see NewCache).
func NewCacheBytes[K comparable](client as.ClientIfc, namespace, set string, opts ...memstash.Option) (*memstash.Cache[K, []byte], error) {
	return NewCache[K, []byte](client, l2.BytesCodec(), namespace, set, opts...)
}

// Get returns the value; a missing (or expired) key is (zero, false, nil).
func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	asKey, aerr := c.newKey(key)
	if aerr != nil {
		return zero, false, aerr
	}
	rec, aerr := c.client.Get(nil, asKey, binName)
	if aerr != nil {
		if aerr.Matches(astypes.KEY_NOT_FOUND_ERROR) {
			return zero, false, nil
		}
		return zero, false, aerr
	}
	return c.decode(rec)
}

// Set stores the value; ttl == 0 means "no expiration".
func (c *Cache[K, V]) Set(ctx context.Context, key K, value V, ttl time.Duration) error {
	data, err := c.codec.Marshal(value)
	if err != nil {
		return err
	}
	asKey, aerr := c.newKey(key)
	if aerr != nil {
		return aerr
	}
	if aerr := c.client.Put(as.NewWritePolicy(0, expiration(ttl)), asKey, as.BinMap{binName: data}); aerr != nil {
		return aerr
	}
	return nil
}

// Delete removes the key; a missing key is not an error.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	asKey, aerr := c.newKey(key)
	if aerr != nil {
		return aerr
	}
	if _, aerr := c.client.Delete(nil, asKey); aerr != nil {
		return aerr
	}
	return nil
}

// BatchGet fetches all keys in one BatchGet round trip; the returned records align with the requested keys, nil for
// misses.
func (c *Cache[K, V]) BatchGet(ctx context.Context, keys []K) (memstash.List[K, V], error) {
	found := make(memstash.List[K, V], 0, len(keys))
	if len(keys) == 0 {
		return found, nil
	}
	asKeys := make([]*as.Key, len(keys))
	for i, key := range keys {
		asKey, aerr := c.newKey(key)
		if aerr != nil {
			return found, aerr
		}
		asKeys[i] = asKey
	}
	records, aerr := c.client.BatchGet(nil, asKeys, binName)
	if aerr != nil {
		return found, aerr
	}
	for i, rec := range records {
		if rec == nil {
			continue
		}
		value, ok, err := c.decode(rec)
		if err != nil {
			return found, err
		}
		if ok {
			found = append(found, memstash.KeyVal[K, V]{Key: keys[i], Value: value})
		}
	}
	return found, nil
}

// BatchSet stores the items one by one: the client's write path is per-record. The context is honored by neither the
// per-op nor the batch APIs used here.
func (c *Cache[K, V]) BatchSet(ctx context.Context, items memstash.List[K, V], ttl time.Duration) error {
	return l2.BatchSetSequential(ctx, c, items, ttl)
}

func (c *Cache[K, V]) newKey(key K) (*as.Key, as.Error) {
	return as.NewKey(c.namespace, c.set, c.keyFunc(key))
}

// decode extracts the value bin from a record.
func (c *Cache[K, V]) decode(rec *as.Record) (V, bool, error) {
	var zero V
	if rec == nil {
		return zero, false, nil
	}
	raw, ok := rec.Bins[binName].([]byte)
	if !ok {
		return zero, false, nil
	}
	value, err := c.codec.Unmarshal(raw)
	if err != nil {
		return zero, false, err
	}
	return value, true, nil
}

// expiration maps a TTL to the Aerospike write-policy expiration: 0 means "never expire", otherwise whole seconds
// rounded up.
func expiration(ttl time.Duration) uint32 {
	if ttl <= 0 {
		return as.TTLDontExpire
	}
	return uint32((ttl + time.Second - 1) / time.Second)
}
