// Package sql_adapter adapts any database/sql-compatible store (SQLite, PostgreSQL, MySQL, ...) to the
// memstash.L2Cache contract, using a single table as a key/value store with an expiry column.
//
// It depends only on the standard library: the constructor takes the small DB interface below, which is satisfied by
// *sql.DB, *sql.Tx, *sql.Conn, and by pgx through its database/sql shim (github.com/jackc/pgx/v5/stdlib). For a
// PostgreSQL deployment that wants the binary protocol and real pipelining, prefer the pgx_adapter.
//
// TTL note: SQL has no server-side expiration. Expired rows are filtered out on read (WHERE expires_at ...), but they
// stay in the table until deleted. Call DeleteExpired periodically (a reaper) or partition the table by time.
package sql_adapter

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
)

// DB is the subset of *sql.DB the adapter needs; *sql.DB, *sql.Tx and *sql.Conn all satisfy it, which keeps the
// adapter independent of any particular driver or database/sql version.
type DB interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// DefaultTable is the table name used when the caller passes an empty table.
const DefaultTable = "github.com/zakonnic/memstash_cache"

// Dialect captures the two fragments of the KV SQL that differ between databases: how a positional parameter is
// rendered and how an upsert is spelled. The predefined dialects cover the common engines; supply your own for
// anything else.
type Dialect struct {
	// Placeholder renders the n-th positional parameter (1-based).
	Placeholder func(n int) string
	// Upsert is the conflict clause appended to "INSERT INTO t (cache_key, value, expires_at) VALUES (...)".
	Upsert string
}

func qmark(int) string    { return "?" }
func dollar(n int) string { return "$" + strconv.Itoa(n) }

var (
	// SQLite targets SQLite (also works for any engine using "?" placeholders and ON CONFLICT).
	SQLite = Dialect{Placeholder: qmark, Upsert: "ON CONFLICT(cache_key) DO UPDATE SET value = excluded.value, expires_at = excluded.expires_at"}
	// Postgres targets PostgreSQL ($n placeholders, EXCLUDED upsert).
	Postgres = Dialect{Placeholder: dollar, Upsert: "ON CONFLICT (cache_key) DO UPDATE SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at"}
	// MySQL targets MySQL/MariaDB (? placeholders, ON DUPLICATE KEY upsert).
	MySQL = Dialect{Placeholder: qmark, Upsert: "ON DUPLICATE KEY UPDATE value = VALUES(value), expires_at = VALUES(expires_at)"}
)

// CreateTableSQL returns a portable DDL statement for the cache table. On MySQL replace TEXT with VARCHAR(255) for the
// key so it can be a primary key.
func CreateTableSQL(table string) string {
	if table == "" {
		table = DefaultTable
	}
	return "CREATE TABLE IF NOT EXISTS " + table +
		" (cache_key TEXT PRIMARY KEY, value BLOB, expires_at BIGINT NOT NULL DEFAULT 0)"
}

// Cache is an L2 adapter over a database/sql table. The DB's lifecycle stays with the caller.
type Cache[K comparable, V any] struct {
	db      DB
	codec   memstash.Codec[V]
	keyFunc func(K) string

	// Precomputed statements (table and dialect are fixed at construction).
	getQuery    string
	setQuery    string
	deleteQuery string
	reapQuery   string
}

var _ memstash.L2Cache[string, string] = (*Cache[string, string])(nil)

// New creates the adapter with an explicit value codec. table defaults to DefaultTable when empty; dialect selects the
// placeholder and upsert syntax (SQLite, Postgres, MySQL, or your own). By default keys must be strings (identity
// mapping); for other key types pass l2.WithKeyFunc.
func New[K comparable, V any](db DB, codec memstash.Codec[V], table string, dialect Dialect, opts ...memstash.Option) (*Cache[K, V], error) {
	if db == nil {
		return nil, l2.ErrNilClient
	}
	if codec == nil {
		return nil, l2.ErrNilCodec
	}
	if dialect.Placeholder == nil {
		return nil, fmt.Errorf("memstash/l2/sql_adapter: dialect has no Placeholder")
	}
	if table == "" {
		table = DefaultTable
	}
	keyFunc, err := l2.ResolveOptions[K](opts)
	if err != nil {
		return nil, err
	}
	ph := dialect.Placeholder
	return &Cache[K, V]{
		db:      db,
		codec:   codec,
		keyFunc: keyFunc,
		getQuery: "SELECT value FROM " + table + " WHERE cache_key = " + ph(1) +
			" AND (expires_at = 0 OR expires_at > " + ph(2) + ")",
		setQuery: "INSERT INTO " + table + " (cache_key, value, expires_at) VALUES (" +
			ph(1) + ", " + ph(2) + ", " + ph(3) + ") " + dialect.Upsert,
		deleteQuery: "DELETE FROM " + table + " WHERE cache_key = " + ph(1),
		reapQuery:   "DELETE FROM " + table + " WHERE expires_at <> 0 AND expires_at <= " + ph(1),
	}, nil
}

// NewJSON creates the adapter that marshals values with encoding/json.
func NewJSON[K comparable, V any](db DB, table string, dialect Dialect, opts ...memstash.Option) (*Cache[K, V], error) {
	return New[K, V](db, l2.JSONCodec[V](), table, dialect, opts...)
}

// NewBytes creates the adapter that passes []byte values through unchanged.
func NewBytes[K comparable](db DB, table string, dialect Dialect, opts ...memstash.Option) (*Cache[K, []byte], error) {
	return New[K, []byte](db, l2.BytesCodec(), table, dialect, opts...)
}

// NewCache builds a two-level cache in one call: a new memstash.Cache backed by this adapter as its L2. The single
// option list is shared: the memstash.With* options configure the cache, l2.WithKeyFunc configures the adapter.
func NewCache[K comparable, V any](db DB, codec memstash.Codec[V], table string, dialect Dialect, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	adapter, err := New[K, V](db, codec, table, dialect, opts...)
	if err != nil {
		return nil, err
	}
	cacheOpts := make([]memstash.Option, 0, len(opts)+1)
	cacheOpts = append(cacheOpts, opts...)
	cacheOpts = append(cacheOpts, memstash.WithL2Cache[K, V](adapter))
	return memstash.New[K, V](cacheOpts...)
}

// NewCacheJSON builds a two-level cache with the JSON value codec (see NewCache).
func NewCacheJSON[K comparable, V any](db DB, table string, dialect Dialect, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	return NewCache[K, V](db, l2.JSONCodec[V](), table, dialect, opts...)
}

// NewCacheBytes builds a two-level cache that passes []byte values through unchanged (see NewCache).
func NewCacheBytes[K comparable](db DB, table string, dialect Dialect, opts ...memstash.Option) (*memstash.Cache[K, []byte], error) {
	return NewCache[K, []byte](db, l2.BytesCodec(), table, dialect, opts...)
}

// Get returns the value; a missing (or expired) key is (zero, false, nil).
func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	rows, err := c.db.QueryContext(ctx, c.getQuery, c.keyFunc(key), time.Now().Unix())
	if err != nil {
		return zero, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return zero, false, rows.Err()
	}
	var data []byte
	if err := rows.Scan(&data); err != nil {
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
	_, err = c.db.ExecContext(ctx, c.setQuery, c.keyFunc(key), data, expiresAt(ttl))
	return err
}

// Delete removes the key; a missing key is not an error.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	_, err := c.db.ExecContext(ctx, c.deleteQuery, c.keyFunc(key))
	return err
}

// BatchGet and BatchSet run sequentially: batching in SQL means dialect-specific IN/UNNEST expansion, which would
// break the driver-agnostic design. On a single connection these still share the same round-trip pipeline the driver
// provides.
func (c *Cache[K, V]) BatchGet(ctx context.Context, keys []K) (memstash.List[K, V], error) {
	return l2.BatchGetSequential(ctx, c, keys)
}

func (c *Cache[K, V]) BatchSet(ctx context.Context, items memstash.List[K, V], ttl time.Duration) error {
	return l2.BatchSetSequential(ctx, c, items, ttl)
}

// DeleteExpired purges rows whose TTL has elapsed. Call it periodically: expired rows are hidden from Get but are not
// removed automatically.
func (c *Cache[K, V]) DeleteExpired(ctx context.Context) (int64, error) {
	res, err := c.db.ExecContext(ctx, c.reapQuery, time.Now().Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// expiresAt converts a TTL to the stored unix-second deadline; 0 means "no expiration".
func expiresAt(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	return time.Now().Add(ttl).Unix()
}
