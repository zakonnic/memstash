package integration

import (
	"testing"

	"github.com/rainycape/memcache"

	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	rainycape_adapter "github.com/zakonnic/memstash/l2/rainycape_adapter"
)

func TestRainycapeAdapter(t *testing.T) {
	requireServer(t, memcachedAddr())
	client, err := memcache.New(memcachedAddr())
	require.NoError(t, err, "memcache.New")
	client.SetMaxIdleConnsPerAddr(32) // more room for concurrent batch (GetMulti) traffic
	t.Cleanup(func() { _ = client.Close() })

	runSuite(t, func(t *testing.T, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
		opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
		c, err := rainycape_adapter.NewCache[string, string](client, l2.StringCodec(), opts...)
		require.NoError(t, err, "NewCache")
		t.Cleanup(c.Close)
		return c
	})
}
