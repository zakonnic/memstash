package integration

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	sql_adapter "github.com/zakonnic/memstash/l2/sql_adapter"
)

// runSQLSuite opens the database, creates the cache table with engine-specific DDL and runs the shared suite
// through the sql_adapter with the given dialect.
func runSQLSuite(t *testing.T, driver, dsn, ddl, table string, dialect sql_adapter.Dialect) {
	db, err := sql.Open(driver, dsn)
	require.NoError(t, err, "sql.Open")
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(ddl)
	require.NoError(t, err, "create table")

	runSuite(t, func(t *testing.T, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
		opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
		c, err := sql_adapter.NewCache[string, string](db, l2.StringCodec(), table, dialect, opts...)
		require.NoError(t, err, "NewCache")
		t.Cleanup(c.Close)
		return c
	})
}

func TestSQLAdapterPostgres(t *testing.T) {
	requireServer(t, postgresAddr())
	runSQLSuite(t, "pgx", postgresDSN(),
		"CREATE TABLE IF NOT EXISTS memstash_sql (cache_key TEXT PRIMARY KEY, value BYTEA, expires_at BIGINT NOT NULL DEFAULT 0)",
		"memstash_sql", sql_adapter.Postgres)
}

func TestSQLAdapterMySQL(t *testing.T) {
	requireServer(t, mysqlAddr())
	// interpolateParams collapses the driver's prepare/execute/close into one command - one round trip instead of three.
	runSQLSuite(t, "mysql", "memstash:memstash@tcp("+mysqlAddr()+")/memstash?interpolateParams=true",
		"CREATE TABLE IF NOT EXISTS memstash_sql (cache_key VARCHAR(255) PRIMARY KEY, value BLOB, expires_at BIGINT NOT NULL DEFAULT 0)",
		"memstash_sql", sql_adapter.MySQL)
}
