package integration

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	pgx_adapter "github.com/zakonnic/memstash/l2/pgx_adapter"
)

func postgresDSN() string {
	return "postgres://memstash:memstash@" + postgresAddr() + "/memstash"
}

func TestPgxAdapter(t *testing.T) {
	requireServer(t, postgresAddr())
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, postgresDSN())
	require.NoError(t, err, "pgxpool.New")
	t.Cleanup(pool.Close)
	_, err = pool.Exec(ctx, pgx_adapter.CreateTableSQL("memstash_pgx"))
	require.NoError(t, err, "create table")

	runSuite(t, func(t *testing.T, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
		opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
		c, err := pgx_adapter.NewCache[string, string](pool, l2.StringCodec(), "memstash_pgx", opts...)
		require.NoError(t, err, "NewCache")
		t.Cleanup(c.Close)
		return c
	})
}
