package integration

import (
	"context"
	"testing"

	redigolib "github.com/gomodule/redigo/redis"

	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	redigo_adapter "github.com/zakonnic/memstash/l2/redigo_adapter"
)

func TestRedigoAdapter(t *testing.T) {
	requireServer(t, redisAddr())
	pool := &redigolib.Pool{
		// DialContext (rather than Dial) makes the pool's connections context-aware, so the adapter can cancel
		// commands, not just the wait for a free connection.
		DialContext: func(ctx context.Context) (redigolib.Conn, error) {
			return redigolib.DialContext(ctx, "tcp", redisAddr())
		},
		MaxIdle: 8,
	}
	t.Cleanup(func() { _ = pool.Close() })

	runSuite(t, func(t *testing.T, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
		opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
		c, err := redigo_adapter.NewCache[string, string](pool, l2.StringCodec(), opts...)
		require.NoError(t, err, "NewCache")
		t.Cleanup(c.Close)
		return c
	})
}
