// Package redigo_adapter adapts a redigo connection pool (github.com/gomodule/redigo) to the memstash.L2Cache
// contract.
package redigo_adapter

import (
	"context"
	"errors"
	"time"

	redigolib "github.com/gomodule/redigo/redis"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
)

// Cache is an L2 adapter over a redigo *Pool. The pool is safe for concurrent use; its lifecycle stays with the
// caller.
type Cache[K comparable, V any] struct {
	pool    *redigolib.Pool
	codec   memstash.Codec[V]
	keyFunc func(K) string
}

var _ memstash.L2Cache[string, string] = (*Cache[string, string])(nil)

// Multi-key commands (MGET/MSET) beat a per-key pipeline while they stay small.
// But one huge command is worse than a stream of small ones, so we set this limit.
// And a command that does not fit redigo's fixed 4 KiB write buffer goes out in several write calls.
const multiKeyBudget = 3500

// argWireOverhead approximates the RESP framing bytes added per argument ($<len>\r\n...\r\n).
const argWireOverhead = 16

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

// NewJSONCache builds a two-level cache with the JSON value codec (see NewCache).
func NewJSONCache[K comparable, V any](pool *redigolib.Pool, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	return NewCache[K, V](pool, l2.JSONCodec[V](), opts...)
}

// NewBytesCache builds a two-level cache that passes []byte values through unchanged (see NewCache).
func NewBytesCache[K comparable](pool *redigolib.Pool, opts ...memstash.Option) (*memstash.Cache[K, []byte], error) {
	return NewCache[K, []byte](pool, l2.BytesCodec(), opts...)
}

// NewStringCache builds a two-level cache that passes string values through unchanged (see NewCache).
func NewStringCache[K comparable](pool *redigolib.Pool, opts ...memstash.Option) (*memstash.Cache[K, string], error) {
	return NewCache[K, string](pool, l2.StringCodec(), opts...)
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

// BatchGet fetches all keys in one round trip: one MGET command when the batch fits multiKeyBudget, otherwise a
// pipeline of GETs (Send/Flush/Receive). As with do, the context cancels the commands only with Pool.DialContext.
func (c *Cache[K, V]) BatchGet(ctx context.Context, keys []K) (memstash.List[K, V], error) {
	length := len(keys)
	found := make(memstash.List[K, V], 0, length)
	if length == 0 {
		return found, nil
	}
	args := make([]any, length)
	size := 0
	for i, key := range keys {
		storageKey := c.keyFunc(key)
		args[i] = storageKey
		size += len(storageKey) + argWireOverhead
	}
	if size <= multiKeyBudget {
		replies, err := redigolib.Values(c.do(ctx, "MGET", args...))
		if err != nil {
			return found, err
		}
		for i, reply := range replies {
			if reply == nil { // a miss
				continue
			}
			data, err := redigolib.Bytes(reply, nil)
			if err != nil {
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
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return found, err
	}
	defer conn.Close()

	for _, arg := range args {
		if err := conn.Send("GET", arg); err != nil {
			return found, err
		}
	}
	if err := conn.Flush(); err != nil {
		return found, err
	}
	for i := range keys {
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
		found = append(found, memstash.KeyVal[K, V]{Key: keys[i], Value: value})
	}
	return found, nil
}

// BatchSet stores all items in one round trip; ttl == 0 means "no expiration". A no-TTL batch within multiKeyBudget
// goes out as a single MSET command; larger batches and any batch with a TTL are pipelined SETs.
func (c *Cache[K, V]) BatchSet(ctx context.Context, items memstash.List[K, V], ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}
	args := make([]any, 0, 2*len(items))
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
	if ttl <= 0 && size <= multiKeyBudget {
		_, err := c.do(ctx, "MSET", args...)
		return err
	}
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	for i := 0; i < len(args); i += 2 {
		if ttl > 0 {
			err = conn.Send("SET", args[i], args[i+1], "PX", l2.RedisMillis(ttl))
		} else {
			err = conn.Send("SET", args[i], args[i+1])
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
