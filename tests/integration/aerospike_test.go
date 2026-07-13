package integration

import (
	"net"
	"strconv"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	aerospike_adapter "github.com/zakonnic/memstash/l2/aerospike_adapter"
)

func TestAerospikeAdapter(t *testing.T) {
	requireServer(t, aerospikeAddr())
	host, portStr, err := net.SplitHostPort(aerospikeAddr())
	require.NoError(t, err, "split address")
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err, "parse port")

	client, aerr := as.NewClient(host, port)
	require.NoError(t, aerr, "aerospike.NewClient")
	t.Cleanup(client.Close)

	runSuite(t, func(t *testing.T, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
		opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
		// The "memstash" namespace comes from docker/aerospike/aerospike.conf.
		c, err := aerospike_adapter.NewCache[string, string](client, l2.StringCodec(), "memstash", "cache", opts...)
		require.NoError(t, err, "NewCache")
		t.Cleanup(c.Close)
		return c
	})
}
