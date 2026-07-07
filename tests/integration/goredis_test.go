package integration

import (
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	goredis_adapter "github.com/zakonnic/memstash/l2/goredis_adapter"
)

func TestGoRedisAdapter(t *testing.T) {
	requireServer(t, redisAddr())
	client := redis.NewClient(&redis.Options{Addr: redisAddr()})
	t.Cleanup(func() { _ = client.Close() })

	runSuite(t, func(t *testing.T, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
		opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
		c, err := goredis_adapter.NewCache[string, string](client, l2.StringCodec(), opts...)
		require.NoError(t, err, "NewCache")
		t.Cleanup(c.Close)
		return c
	})
}
