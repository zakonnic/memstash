// Cluster variants of the redis adapter tests: the same suite against the 3-master cluster from
// docker/docker-compose.yml, exercising the adapters' multi-node batch paths.
// redigo has no cluster support and stays single-node only.
package integration

import (
	"context"
	"testing"

	"github.com/joomcode/redispipe/rediscluster"
	goredislib "github.com/redis/go-redis/v9"
	rueidislib "github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	goredis_adapter "github.com/zakonnic/memstash/l2/goredis_adapter"
	redispipe_adapter "github.com/zakonnic/memstash/l2/redispipe_adapter"
	rueidis_adapter "github.com/zakonnic/memstash/l2/rueidis_adapter"
)

func TestRueidisAdapterCluster(t *testing.T) {
	addrs := redisClusterAddrs()
	requireServer(t, addrs[0])
	client, err := rueidislib.NewClient(rueidislib.ClientOption{InitAddress: addrs})
	require.NoError(t, err, "rueidis.NewClient")
	t.Cleanup(client.Close)

	runSuite(t, func(t *testing.T, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
		opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
		c, err := rueidis_adapter.NewCache[string, string](client, l2.StringCodec(), opts...)
		require.NoError(t, err, "NewCache")
		t.Cleanup(c.Close)
		return c
	})
}

func TestGoRedisAdapterCluster(t *testing.T) {
	addrs := redisClusterAddrs()
	requireServer(t, addrs[0])
	client := goredislib.NewClusterClient(&goredislib.ClusterOptions{Addrs: addrs})
	t.Cleanup(func() { _ = client.Close() })

	runSuite(t, func(t *testing.T, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
		opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
		c, err := goredis_adapter.NewCache[string, string](client, l2.StringCodec(), opts...)
		require.NoError(t, err, "NewCache")
		t.Cleanup(c.Close)
		return c
	})
}

func TestRedispipeAdapterCluster(t *testing.T) {
	addrs := redisClusterAddrs()
	requireServer(t, addrs[0])
	cluster, err := rediscluster.NewCluster(context.Background(), addrs, rediscluster.Opts{})
	require.NoError(t, err, "rediscluster.NewCluster")
	t.Cleanup(cluster.Close)

	runSuite(t, func(t *testing.T, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
		opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
		c, err := redispipe_adapter.NewCache[string, string](cluster, l2.StringCodec(), opts...)
		require.NoError(t, err, "NewCache")
		t.Cleanup(c.Close)
		return c
	})
}
