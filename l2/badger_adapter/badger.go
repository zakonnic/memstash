// Package badger_adapter adapts an embedded BadgerDB v4 store (github.com/dgraph-io/badger/v4) to the
// memstash.L2Cache contract. Badger is a pure-Go LSM key/value store with native per-key TTL, which makes it a good
// L2 when you want persistence and cross-restart survival without running a separate cache server.
//
// The constructor takes a *badger.DB. Users of badgerhold (github.com/timshannon/badgerhold), which is a query layer
// on top of Badger, can pass their store's underlying handle via store.Badger().
//
// TTL is handled by Badger itself: entries written WithTTL expire on their own and are reclaimed during value-log GC.
package badger_adapter

import (
	"context"
	"errors"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
)

// Cache is an L2 adapter over BadgerDB. The *badger.DB lifecycle stays with the caller.
type Cache[K comparable, V any] struct {
	db      *badger.DB
	codec   memstash.Codec[V]
	keyFunc func(K) string
}

var _ memstash.L2Cache[string, string] = (*Cache[string, string])(nil)

// New creates the adapter with an explicit value codec. By default keys must be strings (identity mapping); for other
// key types pass l2.WithKeyFunc.
func New[K comparable, V any](db *badger.DB, codec memstash.Codec[V], opts ...memstash.Option) (*Cache[K, V], error) {
	if db == nil {
		return nil, l2.ErrNilClient
	}
	if codec == nil {
		return nil, l2.ErrNilCodec
	}
	keyFunc, err := l2.ResolveOptions[K](opts)
	if err != nil {
		return nil, err
	}
	return &Cache[K, V]{db: db, codec: codec, keyFunc: keyFunc}, nil
}

// NewJSON creates the adapter that marshals values with encoding/json.
func NewJSON[K comparable, V any](db *badger.DB, opts ...memstash.Option) (*Cache[K, V], error) {
	return New[K, V](db, l2.JSONCodec[V](), opts...)
}

// NewBytes creates the adapter that passes []byte values through unchanged.
func NewBytes[K comparable](db *badger.DB, opts ...memstash.Option) (*Cache[K, []byte], error) {
	return New[K, []byte](db, l2.BytesCodec(), opts...)
}

// NewCache builds a two-level cache in one call: a new memstash.Cache backed by this adapter as its L2. The single
// option list is shared: the memstash.With* options configure the cache, l2.WithKeyFunc configures the adapter.
func NewCache[K comparable, V any](db *badger.DB, codec memstash.Codec[V], opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	adapter, err := New[K, V](db, codec, opts...)
	if err != nil {
		return nil, err
	}
	cacheOpts := make([]memstash.Option, 0, len(opts)+1)
	cacheOpts = append(cacheOpts, opts...)
	cacheOpts = append(cacheOpts, memstash.WithL2Cache[K, V](adapter))
	return memstash.New[K, V](cacheOpts...)
}

// NewJSONCache builds a two-level cache with the JSON value codec (see NewCache).
func NewJSONCache[K comparable, V any](db *badger.DB, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	return NewCache[K, V](db, l2.JSONCodec[V](), opts...)
}

// NewBytesCache builds a two-level cache that passes []byte values through unchanged (see NewCache).
func NewBytesCache[K comparable](db *badger.DB, opts ...memstash.Option) (*memstash.Cache[K, []byte], error) {
	return NewCache[K, []byte](db, l2.BytesCodec(), opts...)
}

// NewStringCache builds a two-level cache that passes string values through unchanged (see NewCache).
func NewStringCache[K comparable](db *badger.DB, opts ...memstash.Option) (*memstash.Cache[K, string], error) {
	return NewCache[K, string](db, l2.StringCodec(), opts...)
}

// Get returns the value; a missing (or expired) key is (zero, false, nil).
func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var (
		zero  V
		value V
		found bool
	)
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(c.keyFunc(key)))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}
		return item.Value(func(data []byte) error {
			v, decErr := c.codec.Unmarshal(data)
			if decErr != nil {
				return decErr
			}
			value, found = v, true
			return nil
		})
	})
	if err != nil {
		return zero, false, err
	}
	return value, found, nil
}

// Set stores the value; ttl == 0 means "no expiration".
func (c *Cache[K, V]) Set(ctx context.Context, key K, value V, ttl time.Duration) error {
	data, err := c.codec.Marshal(value)
	if err != nil {
		return err
	}
	return c.db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(newEntry(c.keyFunc(key), data, ttl))
	})
}

// Delete removes the key; a missing key is not an error.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	return c.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(c.keyFunc(key)))
	})
}

// BatchGet fetches all keys within a single read transaction.
func (c *Cache[K, V]) BatchGet(ctx context.Context, keys []K) (memstash.List[K, V], error) {
	found := make(memstash.List[K, V], 0, len(keys))
	if len(keys) == 0 {
		return found, nil
	}
	err := c.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get([]byte(c.keyFunc(key)))
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					continue
				}
				return err
			}
			data, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			value, err := c.codec.Unmarshal(data)
			if err != nil {
				return err
			}
			found = append(found, memstash.KeyVal[K, V]{Key: key, Value: value})
		}
		return nil
	})
	if err != nil {
		return found[:0], err
	}
	return found, nil
}

// BatchSet stores all items through a single WriteBatch (Badger's bulk-write path); ttl == 0 means "no expiration".
func (c *Cache[K, V]) BatchSet(ctx context.Context, items memstash.List[K, V], ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}
	wb := c.db.NewWriteBatch()
	defer wb.Cancel()
	for _, item := range items {
		data, err := c.codec.Marshal(item.Value)
		if err != nil {
			return err
		}
		if err := wb.SetEntry(newEntry(c.keyFunc(item.Key), data, ttl)); err != nil {
			return err
		}
	}
	return wb.Flush()
}

// newEntry builds a Badger entry, attaching a TTL only when one is requested.
func newEntry(key string, data []byte, ttl time.Duration) *badger.Entry {
	e := badger.NewEntry([]byte(key), data)
	if ttl > 0 {
		e = e.WithTTL(ttl)
	}
	return e
}
