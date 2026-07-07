package integration

import (
	"testing"

	"github.com/bradfitz/gomemcache/memcache"

	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	gomemcache_adapter "github.com/zakonnic/memstash/l2/gomemcache_adapter"
)

func TestGomemcacheAdapter(t *testing.T) {
	requireServer(t, memcachedAddr())
	client := memcache.New(memcachedAddr())
	client.MaxIdleConns = 32 // the default is 2: too few for concurrent batch (GetMulti) traffic
	t.Cleanup(func() { _ = client.Close() })

	runSuite(t, func(t *testing.T, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
		opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
		c, err := gomemcache_adapter.NewCache[string, string](client, l2.StringCodec(), opts...)
		require.NoError(t, err, "NewCache")
		t.Cleanup(c.Close)
		return c
	})
}
