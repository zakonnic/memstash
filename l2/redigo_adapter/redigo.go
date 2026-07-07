// Package redigo_adapter adapts a redigo connection pool (github.com/gomodule/redigo) to the memstash.L2Cache
// contract.
package redigo_adapter

import (
	"context"
	"errors"
	"time"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"

	redigolib "github.com/gomodule/redigo/redis"
)

// Cache is an L2 adapter over a redigo *Pool. The pool is safe for concurrent use; its lifecycle stays with the
// caller.
type Cache[K comparable, V any] struct {
	pool    *redigolib.Pool
	codec   memstash.Codec[V]
	keyFunc func(K) string
}

var _ memstash.L2Cache[string, string] = (*Cache[string, string])(nil)

// New creates the adapter with an explicit value codec. By default keys must be strings (identity mapping); for other
// key types pass l2.WithKeyFunc.
func New[K comparable, V any](pool *redigolib.Pool, codec memstash.Codec[V], opts ...memstash.Option) (*Cache[K, V], error) {
	if pool == nil {
		return nil, l2.ErrNilClient
	}
	if codec == nil {
		return nil, l2.ErrNilCodec
	}
	keyFunc, err := l2.ResolveOptions[K](opts)
	if err != nil {
		return nil, err
	}
	return &Cache[K, V]{pool: pool, codec: codec, keyFunc: keyFunc}, nil
}

// NewJSON creates the adapter that marshals values with encoding/json.
func NewJSON[K comparable, V any](pool *redigolib.Pool, opts ...memstash.Option) (*Cache[K, V], error) {
	return New[K, V](pool, l2.JSONCodec[V](), opts...)
}

// NewBytes creates the adapter that passes []byte values through unchanged.
func NewBytes[K comparable](pool *redigolib.Pool, opts ...memstash.Option) (*Cache[K, []byte], error) {
	return New[K, []byte](pool, l2.BytesCodec(), opts...)
}

// NewCache builds a two-level cache in one call: a new memstash.Cache backed by this adapter as its L2. The single
// option list is shared: the memstash.With* options configure the cache, l2.WithKeyFunc configures the adapter.
func NewCache[K comparable, V any](pool *redigolib.Pool, codec memstash.Codec[V], opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	adapter, err := New[K, V](pool, codec, opts...)
	if err != nil {
		return nil, err
	}
	cacheOpts := make([]memstash.Option, 0, len(opts)+1)
	cacheOpts = append(cacheOpts, opts...)
	cacheOpts = append(cacheOpts, memstash.WithL2Cache[K, V](adapter))
	return memstash.New[K, V](cacheOpts...)
}

// NewCacheJSON builds a two-level cache with the JSON value codec (see NewCache).
func NewCacheJSON[K comparable, V any](pool *redigolib.Pool, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	return NewCache[K, V](pool, l2.JSONCodec[V](), opts...)
}

// NewCacheBytes builds a two-level cache that passes []byte values through unchanged (see NewCache).
func NewCacheBytes[K comparable](pool *redigolib.Pool, opts ...memstash.Option) (*memstash.Cache[K, []byte], error) {
	return NewCache[K, []byte](pool, l2.BytesCodec(), opts...)
}

// do borrows a connection, runs one command and returns the connection to the pool. The context cancels the wait for
// a free connection always; it cancels the command itself only when the pool dials connections with context support
// (Pool.DialContext) - otherwise the command falls back to the plain Do.
func (c *Cache[K, V]) do(ctx context.Context, cmd string, args ...any) (any, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if connWithCtx, ok := conn.(redigolib.ConnWithContext); ok {
		return connWithCtx.DoContext(ctx, cmd, args...)
	}
	return conn.Do(cmd, args...)
}

// Get returns the value; a missing key is (zero, false, nil).
func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	data, err := redigolib.Bytes(c.do(ctx, "GET", c.keyFunc(key)))
	if err != nil {
		if errors.Is(err, redigolib.ErrNil) {
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
	if ttl > 0 {
		_, err = c.do(ctx, "SET", c.keyFunc(key), data, "PX", l2.RedisMillis(ttl))
	} else {
		_, err = c.do(ctx, "SET", c.keyFunc(key), data)
	}
	return err
}

// Delete removes the key; a missing key is not an error.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	_, err := c.do(ctx, "DEL", c.keyFunc(key))
	return err
}

// BatchGet fetches all keys in one pipelined round trip (Send/Flush/Receive) over a single pooled connection. As with
// do, the context always cancels the wait for a connection; the commands themselves only with Pool.DialContext.
func (c *Cache[K, V]) BatchGet(ctx context.Context, keys []K) (memstash.List[K, V], error) {
	length := len(keys)
	found := make(memstash.List[K, V], 0, length)
	if length == 0 {
		return found, nil
	}
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return found, err
	}
	defer conn.Close()

	for _, key := range keys {
		if err := conn.Send("GET", c.keyFunc(key)); err != nil {
			return found, err
		}
	}
	if err := conn.Flush(); err != nil {
		return found, err
	}
	for _, key := range keys {
		data, err := redigolib.Bytes(conn.Receive())
		if err != nil {
			if errors.Is(err, redigolib.ErrNil) {
				continue
			}
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

// BatchSet stores all items in one pipelined round trip over a single pooled connection; ttl == 0 means "no
// expiration".
func (c *Cache[K, V]) BatchSet(ctx context.Context, items memstash.List[K, V], ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	for _, item := range items {
		data, err := c.codec.Marshal(item.Value)
		if err != nil {
			return err
		}
		if ttl > 0 {
			err = conn.Send("SET", c.keyFunc(item.Key), data, "PX", l2.RedisMillis(ttl))
		} else {
			err = conn.Send("SET", c.keyFunc(item.Key), data)
		}
		if err != nil {
			return err
		}
	}
	if err := conn.Flush(); err != nil {
		return err
	}
	for range items {
		if _, err := conn.Receive(); err != nil {
			return err
		}
	}
	return nil
}
