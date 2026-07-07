package integration

import (
	"context"
	"testing"

	"github.com/joomcode/redispipe/redisconn"

	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	redispipe_adapter "github.com/zakonnic/memstash/l2/redispipe_adapter"
)

func TestRedispipeAdapter(t *testing.T) {
	requireServer(t, redisAddr())
	sender, err := redisconn.Connect(context.Background(), redisAddr(), redisconn.Opts{})
	require.NoError(t, err, "redisconn.Connect")
	t.Cleanup(sender.Close)

	runSuite(t, func(t *testing.T, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
		opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
		c, err := redispipe_adapter.NewCache[string, string](sender, l2.StringCodec(), opts...)
		require.NoError(t, err, "NewCache")
		t.Cleanup(c.Close)
		return c
	})
}
