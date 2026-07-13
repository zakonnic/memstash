// Package tarantool_adapter adapts a Tarantool instance (github.com/tarantool/go-tarantool/v2) to the
// memstash.L2Cache contract. Tarantool is an in-memory database frequently used as a fast, persistent cache tier.
//
// The constructor takes the tarantool.Doer interface, satisfied by *tarantool.Connection and by connection pools, so
// the adapter does not depend on how connections are managed.
//
// Storage: a space (default "github.com/zakonnic/memstash_cache") whose tuples are [key string, value binary, expire_at unsigned], with
// the primary index on the key. Create it once, for example:
//
//	box.schema.space.create('memstash_cache', {if_not_exists = true})
//	box.space.memstash_cache:format({{name='key',type='string'}, {name='value',type='varbinary'}, {name='expire_at',type='unsigned'}})
//	box.space.memstash_cache:create_index('primary', {parts={'key'}, if_not_exists = true})
//
// TTL: Tarantool has no protocol-level expiration. expire_at holds a unix-second deadline (0 = never); the adapter
// hides expired items on read. Reclaim them with the server-side 'expirationd' module or a periodic sweep.
package tarantool_adapter

import (
	"context"
	"time"

	tarantool "github.com/tarantool/go-tarantool/v2"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
)

// DefaultSpace is the space name used when the caller passes an empty space.
const DefaultSpace = "github.com/zakonnic/memstash_cache"

// primaryIndex is the index scanned by key; the primary index is always id 0.
const primaryIndex = 0

// row mirrors a cache item decoded as a msgpack array.
type row struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint:unused // controls msgpack array encoding
	Key      string
	Value    []byte
	ExpireAt int64
}

// Cache is an L2 adapter over a Tarantool space. The Doer's lifecycle stays with the caller.
type Cache[K comparable, V any] struct {
	doer    tarantool.Doer
	space   string
	codec   memstash.Codec[V]
	keyFunc func(K) string
}

var _ memstash.L2Cache[string, string] = (*Cache[string, string])(nil)

// New creates the adapter with an explicit value codec. space defaults to DefaultSpace when empty. By default keys
// must be strings (identity mapping); for other key types pass l2.WithKeyFunc.
func New[K comparable, V any](doer tarantool.Doer, codec memstash.Codec[V], space string, opts ...memstash.Option) (*Cache[K, V], error) {
	if doer == nil {
		return nil, l2.ErrNilClient
	}
	if codec == nil {
		return nil, l2.ErrNilCodec
	}
	if space == "" {
		space = DefaultSpace
	}
	keyFunc, err := l2.ResolveOptions[K](opts)
	if err != nil {
		return nil, err
	}
	return &Cache[K, V]{doer: doer, space: space, codec: codec, keyFunc: keyFunc}, nil
}

// NewJSON creates the adapter that marshals values with encoding/json.
func NewJSON[K comparable, V any](doer tarantool.Doer, space string, opts ...memstash.Option) (*Cache[K, V], error) {
	return New[K, V](doer, l2.JSONCodec[V](), space, opts...)
}

// NewBytes creates the adapter that passes []byte values through unchanged.
func NewBytes[K comparable](doer tarantool.Doer, space string, opts ...memstash.Option) (*Cache[K, []byte], error) {
	return New[K, []byte](doer, l2.BytesCodec(), space, opts...)
}

// NewCache builds a two-level cache in one call: a new memstash.Cache backed by this adapter as its L2. The single
// option list is shared: the memstash.With* options configure the cache, l2.WithKeyFunc configures the adapter.
func NewCache[K comparable, V any](doer tarantool.Doer, codec memstash.Codec[V], space string, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	adapter, err := New[K, V](doer, codec, space, opts...)
	if err != nil {
		return nil, err
	}
	cacheOpts := make([]memstash.Option, 0, len(opts)+1)
	cacheOpts = append(cacheOpts, opts...)
	cacheOpts = append(cacheOpts, memstash.WithL2Cache[K, V](adapter))
	return memstash.New[K, V](cacheOpts...)
}

// NewJSONCache builds a two-level cache with the JSON value codec (see NewCache).
func NewJSONCache[K comparable, V any](doer tarantool.Doer, space string, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	return NewCache[K, V](doer, l2.JSONCodec[V](), space, opts...)
}

// NewBytesCache builds a two-level cache that passes []byte values through unchanged (see NewCache).
func NewBytesCache[K comparable](doer tarantool.Doer, space string, opts ...memstash.Option) (*memstash.Cache[K, []byte], error) {
	return NewCache[K, []byte](doer, l2.BytesCodec(), space, opts...)
}

// Get returns the value; a missing (or expired) key is (zero, false, nil).
func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	var rows []row
	req := tarantool.NewSelectRequest(c.space).Context(ctx).Index(primaryIndex).Limit(1).Key([]any{c.keyFunc(key)})
	if err := c.doer.Do(req).GetTyped(&rows); err != nil {
		return zero, false, err
	}
	if len(rows) == 0 || expired(rows[0]) {
		return zero, false, nil
	}
	value, err := c.codec.Unmarshal(rows[0].Value)
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
	req := tarantool.NewReplaceRequest(c.space).Context(ctx).Tuple([]any{c.keyFunc(key), data, expiresAt(ttl)})
	_, err = c.doer.Do(req).Get()
	return err
}

// expiresAt converts a TTL to the stored unix-second deadline.
func expiresAt(ttl time.Duration) uint64 {
	if ttl <= 0 {
		return 0
	}
	return uint64(time.Now().Add(ttl).Unix())
}

// Delete removes the key; a missing key is not an error.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	req := tarantool.NewDeleteRequest(c.space).Context(ctx).Index(primaryIndex).Key([]any{c.keyFunc(key)})
	_, err := c.doer.Do(req).Get()
	return err
}

// BatchGet issues all selects up front (async futures) and then resolves them, pipelining the round trip.
func (c *Cache[K, V]) BatchGet(ctx context.Context, keys []K) (memstash.List[K, V], error) {
	found := make(memstash.List[K, V], 0, len(keys))
	if len(keys) == 0 {
		return found, nil
	}
	futures := make([]*tarantool.Future, len(keys))
	for i, key := range keys {
		req := tarantool.NewSelectRequest(c.space).Context(ctx).Index(primaryIndex).Limit(1).Key([]any{c.keyFunc(key)})
		futures[i] = c.doer.Do(req)
	}
	for i, future := range futures {
		var rows []row
		if err := future.GetTyped(&rows); err != nil {
			return found, err
		}
		if len(rows) == 0 || expired(rows[0]) {
			continue
		}
		value, err := c.codec.Unmarshal(rows[0].Value)
		if err != nil {
			return found, err
		}
		found = append(found, memstash.KeyVal[K, V]{Key: keys[i], Value: value})
	}
	return found, nil
}

// BatchSet issues all replaces up front (async futures) and then resolves them; ttl == 0 means "no expiration".
func (c *Cache[K, V]) BatchSet(ctx context.Context, items memstash.List[K, V], ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}
	deadline := expiresAt(ttl)
	futures := make([]*tarantool.Future, 0, len(items))
	for _, item := range items {
		data, err := c.codec.Marshal(item.Value)
		if err != nil {
			return err
		}
		req := tarantool.NewReplaceRequest(c.space).Context(ctx).Tuple([]any{c.keyFunc(item.Key), data, deadline})
		futures = append(futures, c.doer.Do(req))
	}
	for _, future := range futures {
		if _, err := future.Get(); err != nil {
			return err
		}
	}
	return nil
}

func expired(r row) bool {
	return r.ExpireAt != 0 && r.ExpireAt <= time.Now().Unix()
}
