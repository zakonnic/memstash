package integration

import (
	"testing"

	valkeylib "github.com/valkey-io/valkey-go"

	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	valkey_adapter "github.com/zakonnic/memstash/l2/valkey_adapter"
)

func TestValkeyAdapter(t *testing.T) {
	requireServer(t, valkeyAddr())
	client, err := valkeylib.NewClient(valkeylib.ClientOption{InitAddress: []string{valkeyAddr()}})
	require.NoError(t, err, "valkey.NewClient")
	t.Cleanup(client.Close)

	runSuite(t, func(t *testing.T, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
		opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
		c, err := valkey_adapter.NewCache[string, string](client, l2.StringCodec(), opts...)
		require.NoError(t, err, "NewCache")
		t.Cleanup(c.Close)
		return c
	})
}
