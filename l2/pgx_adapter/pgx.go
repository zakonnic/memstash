// Package pgx_adapter adapts a native pgx v5 connection (github.com/jackc/pgx/v5) to the memstash.L2Cache contract,
// using a single PostgreSQL table as a key/value store with an expiry column. Compared with the database/sql-based
// sql_adapter, this one speaks pgx's binary protocol and uses pgx.Batch for real request pipelining.
//
// The constructor takes the small DB interface below, satisfied by *pgxpool.Pool, *pgx.Conn and pgx.Tx, so the
// adapter is independent of how you manage connections.
//
// TTL note: PostgreSQL has no server-side expiration. Expired rows are filtered out on read; call DeleteExpired
// periodically (a reaper) to purge them.
package pgx_adapter

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
)

// DB is the subset of pgx used by the adapter; *pgxpool.Pool, *pgx.Conn and pgx.Tx all satisfy it.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// DefaultTable is the table name used when the caller passes an empty table.
const DefaultTable = "github.com/zakonnic/memstash_cache"

// CreateTableSQL returns the DDL for the cache table.
func CreateTableSQL(table string) string {
	if table == "" {
		table = DefaultTable
	}
	return "CREATE TABLE IF NOT EXISTS " + table +
		" (cache_key TEXT PRIMARY KEY, value BYTEA, expires_at BIGINT NOT NULL DEFAULT 0)"
}

// Cache is an L2 adapter over a PostgreSQL table accessed through pgx. The DB's lifecycle stays with the caller.
type Cache[K comparable, V any] struct {
	db      DB
	codec   memstash.Codec[V]
	keyFunc func(K) string

	getQuery      string
	setQuery      string
	deleteQuery   string
	reapQuery     string
	batchGetQuery string
	batchSetQuery string
}

var _ memstash.L2Cache[string, string] = (*Cache[string, string])(nil)

// New creates the adapter with an explicit value codec. table defaults to DefaultTable when empty. By default keys
// must be strings (identity mapping); for other key types pass l2.WithKeyFunc.
func New[K comparable, V any](db DB, codec memstash.Codec[V], table string, opts ...memstash.Option) (*Cache[K, V], error) {
	if db == nil {
		return nil, l2.ErrNilClient
	}
	if codec == nil {
		return nil, l2.ErrNilCodec
	}
	if table == "" {
		table = DefaultTable
	}
	keyFunc, err := l2.ResolveOptions[K](opts)
	if err != nil {
		return nil, err
	}
	return &Cache[K, V]{
		db:      db,
		codec:   codec,
		keyFunc: keyFunc,
		getQuery: "SELECT value FROM " + table +
			" WHERE cache_key = $1 AND (expires_at = 0 OR expires_at > $2)",
		setQuery: "INSERT INTO " + table + " (cache_key, value, expires_at) VALUES ($1, $2, $3)" +
			" ON CONFLICT (cache_key) DO UPDATE SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at",
		deleteQuery: "DELETE FROM " + table + " WHERE cache_key = $1",
		reapQuery:   "DELETE FROM " + table + " WHERE expires_at <> 0 AND expires_at <= $1",
		batchGetQuery: "SELECT cache_key, value FROM " + table +
			" WHERE cache_key = ANY($1) AND (expires_at = 0 OR expires_at > $2)",
		batchSetQuery: "INSERT INTO " + table + " (cache_key, value, expires_at)" +
			" SELECT * FROM unnest($1::text[], $2::bytea[], $3::bigint[])" +
			" ON CONFLICT (cache_key) DO UPDATE SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at",
	}, nil
}

// NewJSON creates the adapter that marshals values with encoding/json.
func NewJSON[K comparable, V any](db DB, table string, opts ...memstash.Option) (*Cache[K, V], error) {
	return New[K, V](db, l2.JSONCodec[V](), table, opts...)
}

// NewBytes creates the adapter that passes []byte values through unchanged.
func NewBytes[K comparable](db DB, table string, opts ...memstash.Option) (*Cache[K, []byte], error) {
	return New[K, []byte](db, l2.BytesCodec(), table, opts...)
}

// NewCache builds a two-level cache in one call: a new memstash.Cache backed by this adapter as its L2. The single
// option list is shared: the memstash.With* options configure the cache, l2.WithKeyFunc configures the adapter.
func NewCache[K comparable, V any](db DB, codec memstash.Codec[V], table string, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	adapter, err := New[K, V](db, codec, table, opts...)
	if err != nil {
		return nil, err
	}
	cacheOpts := make([]memstash.Option, 0, len(opts)+1)
	cacheOpts = append(cacheOpts, opts...)
	cacheOpts = append(cacheOpts, memstash.WithL2Cache[K, V](adapter))
	return memstash.New[K, V](cacheOpts...)
}

// NewJSONCache builds a two-level cache with the JSON value codec (see NewCache).
func NewJSONCache[K comparable, V any](db DB, table string, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	return NewCache[K, V](db, l2.JSONCodec[V](), table, opts...)
}

// NewBytesCache builds a two-level cache that passes []byte values through unchanged (see NewCache).
func NewBytesCache[K comparable](db DB, table string, opts ...memstash.Option) (*memstash.Cache[K, []byte], error) {
	return NewCache[K, []byte](db, l2.BytesCodec(), table, opts...)
}

// NewStringCache builds a two-level cache that passes string values through unchanged (see NewCache).
func NewStringCache[K comparable](db DB, table string, opts ...memstash.Option) (*memstash.Cache[K, string], error) {
	return NewCache[K, string](db, l2.StringCodec(), table, opts...)
}

// Get returns the value; a missing (or expired) key is (zero, false, nil).
func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	var data []byte
	err := c.db.QueryRow(ctx, c.getQuery, c.keyFunc(key), time.Now().Unix()).Scan(&data)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
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
	_, err = c.db.Exec(ctx, c.setQuery, c.keyFunc(key), data, expiresAt(ttl))
	return err
}

// Delete removes the key; a missing key is not an error.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	_, err := c.db.Exec(ctx, c.deleteQuery, c.keyFunc(key))
	return err
}

// BatchGet fetches all keys in one "WHERE cache_key = ANY($1)" statement, the keys passed as one text[] parameter.
func (c *Cache[K, V]) BatchGet(ctx context.Context, keys []K) (memstash.List[K, V], error) {
	found := make(memstash.List[K, V], 0, len(keys))
	if len(keys) == 0 {
		return found, nil
	}
	byStorageKey := make(map[string]K, len(keys))
	storageKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		storageKey := c.keyFunc(key)
		if _, seen := byStorageKey[storageKey]; !seen {
			storageKeys = append(storageKeys, storageKey)
		}
		byStorageKey[storageKey] = key
	}
	rows, err := c.db.Query(ctx, c.batchGetQuery, storageKeys, time.Now().Unix())
	if err != nil {
		return found, err
	}
	defer rows.Close()
	for rows.Next() {
		var storageKey string
		var data []byte
		if err := rows.Scan(&storageKey, &data); err != nil {
			return found, err
		}
		key, ok := byStorageKey[storageKey]
		if !ok {
			continue
		}
		value, err := c.codec.Unmarshal(data)
		if err != nil {
			return found, err
		}
		found = append(found, memstash.KeyVal[K, V]{Key: key, Value: value})
	}
	return found, rows.Err()
}

// BatchSet stores all items in one "INSERT ... SELECT FROM unnest(...)" upsert with three array parameters;
// ttl == 0 means "no expiration". Duplicate keys collapse to the last value - PostgreSQL rejects the same key twice
// in one upsert statement.
func (c *Cache[K, V]) BatchSet(ctx context.Context, items memstash.List[K, V], ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}
	storageKeys := make([]string, 0, len(items))
	values := make([][]byte, 0, len(items))
	rowIndex := make(map[string]int, len(items))
	for _, item := range items {
		data, err := c.codec.Marshal(item.Value)
		if err != nil {
			return err
		}
		storageKey := c.keyFunc(item.Key)
		if i, seen := rowIndex[storageKey]; seen {
			values[i] = data
			continue
		}
		rowIndex[storageKey] = len(storageKeys)
		storageKeys = append(storageKeys, storageKey)
		values = append(values, data)
	}
	deadlines := make([]int64, len(storageKeys))
	deadline := expiresAt(ttl)
	for i := range deadlines {
		deadlines[i] = deadline
	}
	_, err := c.db.Exec(ctx, c.batchSetQuery, storageKeys, values, deadlines)
	return err
}

// DeleteExpired purges rows whose TTL has elapsed. Call it periodically: expired rows are hidden from Get but are not
// removed automatically.
func (c *Cache[K, V]) DeleteExpired(ctx context.Context) (int64, error) {
	tag, err := c.db.Exec(ctx, c.reapQuery, time.Now().Unix())
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func expiresAt(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	return time.Now().Add(ttl).Unix()
}
