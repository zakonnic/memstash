package integration

import (
	"testing"

	mclib "github.com/memcachier/mc/v3"

	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	mc_adapter "github.com/zakonnic/memstash/l2/mc_adapter"
)

func TestMcAdapter(t *testing.T) {
	requireServer(t, memcachedAddr())
	// Plain binary protocol, no SASL: empty credentials against the stock memcached container.
	client := mclib.NewMC(memcachedAddr(), "", "")
	t.Cleanup(client.Quit)

	runSuite(t, func(t *testing.T, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
		opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
		c, err := mc_adapter.NewCache[string, string](client, l2.StringCodec(), opts...)
		require.NoError(t, err, "NewCache")
		t.Cleanup(c.Close)
		return c
	})
}
