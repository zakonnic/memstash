//go:build cgo

// The valyala/ybc memcache package bundles cgo-backed code, so this test compiles only with a C toolchain available
// (see the valyala_adapter package documentation). Without cgo the file is silently excluded from the build.
package integration

import (
	"testing"

	"github.com/valyala/ybc/libs/go/memcache"

	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	valyala_adapter "github.com/zakonnic/memstash/l2/valyala_adapter"
)

func TestValyalaAdapter(t *testing.T) {
	requireServer(t, memcachedAddr())
	client := &memcache.Client{ServerAddr: memcachedAddr()}
	client.Start()
	t.Cleanup(client.Stop)

	runSuite(t, func(t *testing.T, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
		opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
		c, err := valyala_adapter.NewCache[string, string](client, l2.StringCodec(), opts...)
		require.NoError(t, err, "NewCache")
		t.Cleanup(c.Close)
		return c
	})
}
