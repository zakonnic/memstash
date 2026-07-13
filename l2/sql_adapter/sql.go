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
	"strings"
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

// Dialect captures the fragments of the KV SQL that differ between databases: how a positional parameter is
// rendered and how an upsert is spelled. The predefined dialects cover the common engines; supply your own for
// anything else.
type Dialect struct {
	// Placeholder renders the n-th positional parameter (1-based).
	Placeholder func(n int) string
	// Upsert is the conflict clause appended to "INSERT INTO t (cache_key, value, expires_at) VALUES (...)".
	Upsert string
	// MultiRowUpsert marks that the engine accepts a multi-row "VALUES (...), (...), ...".
	MultiRowUpsert bool
}

func qmark(int) string    { return "?" }
func dollar(n int) string { return "$" + strconv.Itoa(n) }

var (
	// SQLite targets SQLite (also works for any engine using "?" placeholders and ON CONFLICT).
	SQLite = Dialect{Placeholder: qmark, Upsert: "ON CONFLICT(cache_key) DO UPDATE SET value = excluded.value, expires_at = excluded.expires_at", MultiRowUpsert: true}
	// Postgres targets PostgreSQL ($n placeholders, EXCLUDED upsert).
	Postgres = Dialect{Placeholder: dollar, Upsert: "ON CONFLICT (cache_key) DO UPDATE SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at", MultiRowUpsert: true}
	// MySQL targets MySQL/MariaDB (? placeholders, ON DUPLICATE KEY upsert).
	MySQL = Dialect{Placeholder: qmark, Upsert: "ON DUPLICATE KEY UPDATE value = VALUES(value), expires_at = VALUES(expires_at)", MultiRowUpsert: true}
)

// maxStatementParams caps the positional parameters per batch statement: 900 stays below the tightest common engine
// limits (SQLite's historical 999, SQL Server's 2100).
const maxStatementParams = 900

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
	table   string
	dialect Dialect

	// Precomputed statements (table and dialect are fixed at construction); the batch statements depend on the
	// batch size and are built per call.
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
		table:   table,
		dialect: dialect,
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

// NewJSONCache builds a two-level cache with the JSON value codec (see NewCache).
func NewJSONCache[K comparable, V any](db DB, table string, dialect Dialect, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	return NewCache[K, V](db, l2.JSONCodec[V](), table, dialect, opts...)
}

// NewBytesCache builds a two-level cache that passes []byte values through unchanged (see NewCache).
func NewBytesCache[K comparable](db DB, table string, dialect Dialect, opts ...memstash.Option) (*memstash.Cache[K, []byte], error) {
	return NewCache[K, []byte](db, l2.BytesCodec(), table, dialect, opts...)
}

// NewStringCache builds a two-level cache that passes string values through unchanged (see NewCache).
func NewStringCache[K comparable](db DB, table string, dialect Dialect, opts ...memstash.Option) (*memstash.Cache[K, string], error) {
	return NewCache[K, string](db, l2.StringCodec(), table, dialect, opts...)
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

// BatchGet fetches all keys with "SELECT ... WHERE cache_key IN (...)", split into chunks of maxStatementParams.
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
	now := time.Now().Unix()
	const chunkKeys = maxStatementParams - 1 // one parameter is the expiry deadline
	for start := 0; start < len(storageKeys); start += chunkKeys {
		chunk := storageKeys[start:min(start+chunkKeys, len(storageKeys))]
		if err := c.batchGetChunk(ctx, chunk, now, byStorageKey, &found); err != nil {
			return found, err
		}
	}
	return found, nil
}

// batchGetChunk runs one IN statement and appends the hits to found.
func (c *Cache[K, V]) batchGetChunk(ctx context.Context, chunk []string, now int64, byStorageKey map[string]K, found *memstash.List[K, V]) error {
	ph := c.dialect.Placeholder
	var query strings.Builder
	query.WriteString("SELECT cache_key, value FROM ")
	query.WriteString(c.table)
	query.WriteString(" WHERE cache_key IN (")
	args := make([]any, 0, len(chunk)+1)
	for i, storageKey := range chunk {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString(ph(i + 1))
		args = append(args, storageKey)
	}
	query.WriteString(") AND (expires_at = 0 OR expires_at > ")
	query.WriteString(ph(len(chunk) + 1))
	query.WriteString(")")
	args = append(args, now)

	rows, err := c.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var storageKey string
		var data []byte
		if err := rows.Scan(&storageKey, &data); err != nil {
			return err
		}
		key, ok := byStorageKey[storageKey]
		if !ok {
			continue
		}
		value, err := c.codec.Unmarshal(data)
		if err != nil {
			return err
		}
		*found = append(*found, memstash.KeyVal[K, V]{Key: key, Value: value})
	}
	return rows.Err()
}

// BatchSet stores all items with multi-row "INSERT ... VALUES (...), (...) <upsert>" statements when
// Dialect.MultiRowUpsert allows, per-item statements otherwise; ttl == 0 means "no expiration". Duplicate keys
// collapse to the last value.
func (c *Cache[K, V]) BatchSet(ctx context.Context, items memstash.List[K, V], ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}
	if !c.dialect.MultiRowUpsert {
		return l2.BatchSetSequential(ctx, c, items, ttl)
	}
	// Collapse duplicate storage keys (last wins): PostgreSQL rejects the same key twice in one upsert statement.
	type kvRow struct {
		storageKey string
		data       []byte
	}
	rows := make([]kvRow, 0, len(items))
	rowIndex := make(map[string]int, len(items))
	for _, item := range items {
		data, err := c.codec.Marshal(item.Value)
		if err != nil {
			return err
		}
		storageKey := c.keyFunc(item.Key)
		if i, seen := rowIndex[storageKey]; seen {
			rows[i].data = data
			continue
		}
		rowIndex[storageKey] = len(rows)
		rows = append(rows, kvRow{storageKey: storageKey, data: data})
	}
	deadline := expiresAt(ttl)
	const chunkRows = maxStatementParams / 3 // three parameters per row
	ph := c.dialect.Placeholder
	for start := 0; start < len(rows); start += chunkRows {
		chunk := rows[start:min(start+chunkRows, len(rows))]
		var query strings.Builder
		query.WriteString("INSERT INTO ")
		query.WriteString(c.table)
		query.WriteString(" (cache_key, value, expires_at) VALUES ")
		args := make([]any, 0, 3*len(chunk))
		for i, row := range chunk {
			if i > 0 {
				query.WriteString(", ")
			}
			query.WriteString("(")
			query.WriteString(ph(3*i + 1))
			query.WriteString(", ")
			query.WriteString(ph(3*i + 2))
			query.WriteString(", ")
			query.WriteString(ph(3*i + 3))
			query.WriteString(")")
			args = append(args, row.storageKey, row.data, deadline)
		}
		query.WriteString(" ")
		query.WriteString(c.dialect.Upsert)
		if _, err := c.db.ExecContext(ctx, query.String(), args...); err != nil {
			return err
		}
	}
	return nil
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
