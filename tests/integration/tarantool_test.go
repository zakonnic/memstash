package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tarantool "github.com/tarantool/go-tarantool/v2"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	tarantool_adapter "github.com/zakonnic/memstash/l2/tarantool_adapter"
)

func TestTarantoolAdapter(t *testing.T) {
	requireServer(t, tarantoolAddr())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The user and the space come from docker/tarantool/init.lua.
	conn, err := tarantool.Connect(ctx, tarantool.NetDialer{
		Address:  tarantoolAddr(),
		User:     "memstash",
		Password: "memstash",
	}, tarantool.Opts{Timeout: 5 * time.Second})
	require.NoError(t, err, "tarantool.Connect")
	t.Cleanup(func() { _ = conn.Close() })

	runSuite(t, func(t *testing.T, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
		opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
		c, err := tarantool_adapter.NewCache[string, string](conn, l2.StringCodec(), "memstash_cache", opts...)
		require.NoError(t, err, "NewCache")
		t.Cleanup(c.Close)
		return c
	})
}
