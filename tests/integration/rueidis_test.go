package integration

import (
	"testing"

	rueidislib "github.com/redis/rueidis"

	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	rueidis_adapter "github.com/zakonnic/memstash/l2/rueidis_adapter"
)

func TestRueidisAdapter(t *testing.T) {
	requireServer(t, redisAddr())
	client, err := rueidislib.NewClient(rueidislib.ClientOption{InitAddress: []string{redisAddr()}})
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
