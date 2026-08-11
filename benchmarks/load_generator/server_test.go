package load_generator

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
)

// The addresses of docker/docker-compose.yml; a server that is not up skips its case.
const (
	testRedisAddr     = "127.0.0.1:43210"
	testMemcachedAddr = "127.0.0.1:43214"
	testValkeyAddr    = "127.0.0.1:43221"
)

func testClusterAddrs() []string {
	return []string{"127.0.0.1:43211", "127.0.0.1:43212", "127.0.0.1:43213"}
}

func requireListening(t *testing.T, addr string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
	if err != nil {
		t.Skipf("no server on %s: %v", addr, err)
	}
	conn.Close()
}

// TestServerTypesRoundTrip dials every ServerType the way a scenario does and checks a value written through one
// cache comes back through another - the only proof that the address reached the right client and adapter.
func TestServerTypesRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		server ServerType
		addrs  []string
	}{
		{"rueidis", Rueidis, []string{testRedisAddr}},
		{"valkey", Valkey, []string{testValkeyAddr}},
		{"goredis", GoRedis, []string{testRedisAddr}},
		{"redigo", Redigo, []string{testRedisAddr}},
		{"redispipe", Redispipe, []string{testRedisAddr}},
		{"gomemcache", Gomemcache, []string{testMemcachedAddr}},
		{"rainycape", Rainycape, []string{testMemcachedAddr}},
		{"mc", Mc, []string{testMemcachedAddr}},
		{"rueidis cluster", Rueidis, testClusterAddrs()},
		{"valkey cluster", Valkey, testClusterAddrs()},
		{"goredis cluster", GoRedis, testClusterAddrs()},
		{"redispipe cluster", Redispipe, testClusterAddrs()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireListening(t, tc.addrs[0])
			ctx := context.Background()
			prefix := fmt.Sprintf("lg|%s|%d|", tc.name, time.Now().UnixNano())

			open := func() (*memstash.Cache[string, []byte], func()) {
				cache, closeL2, err := openL2[string, []byte](tc.server, tc.addrs, l2.BytesCodec(),
					[]memstash.Option{
						memstash.WithMemoryCapacity(64),
						memstash.WithWritePolicy(memstash.WriteThrough), // L2 must hold it by the time Set returns
						l2.WithKeyFunc(l2.PrefixedString(prefix)),
					})
				require.NoError(t, err)
				return cache, func() { cache.Close(); closeL2() }
			}

			writer, closeWriter := open()
			defer closeWriter()
			require.NoError(t, writer.Set(ctx, "k", []byte("v")))

			reader, closeReader := open()
			defer closeReader()
			got, ok, err := reader.Get(ctx, "k") // nothing in this L1, so a hit can only come from L2
			require.NoError(t, err)
			require.True(t, ok, "the value never reached L2")
			assert.Equal(t, []byte("v"), got)
		})
	}
}
