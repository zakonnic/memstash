package integration

import (
	"context"
	"strings"
	"testing"

	gomemcache "github.com/bradfitz/gomemcache/memcache"
	mclib "github.com/memcachier/mc/v3"
	rainycape "github.com/rainycape/memcache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	gomemcache_adapter "github.com/zakonnic/memstash/l2/gomemcache_adapter"
	mc_adapter "github.com/zakonnic/memstash/l2/mc_adapter"
	rainycape_adapter "github.com/zakonnic/memstash/l2/rainycape_adapter"
)

// Keys memcached rejects as they are - a space, a control character, more than 250 bytes - must work like any other,
// alone and inside a batch with ordinary keys, through every memcached adapter.
func TestMemcachedAwkwardKeys(t *testing.T) {
	requireServer(t, memcachedAddr())
	rc, err := rainycape.New(memcachedAddr())
	require.NoError(t, err)
	mc := mclib.NewMC(memcachedAddr(), "", "")
	t.Cleanup(mc.Quit)
	adapters := map[string]func(opts ...memstash.Option) (*memstash.Cache[string, string], error){
		"gomemcache": func(opts ...memstash.Option) (*memstash.Cache[string, string], error) {
			return gomemcache_adapter.NewCache[string, string](gomemcache.New(memcachedAddr()), l2.StringCodec(), 0, opts...)
		},
		"mc": func(opts ...memstash.Option) (*memstash.Cache[string, string], error) {
			return mc_adapter.NewCache[string, string](mc, l2.StringCodec(), 0, opts...)
		},
		"rainycape": func(opts ...memstash.Option) (*memstash.Cache[string, string], error) {
			return rainycape_adapter.NewCache[string, string](rc, l2.StringCodec(), 0, opts...)
		},
	}
	for name, newCache := range adapters {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			prefix := uniquePrefix(t)
			keys := []string{"plain", "with space", "tab" + string(rune(9)), strings.Repeat("x", 300)}
			writer, err := newCache(l2.WithKeyFunc(l2.PrefixedString(prefix)), memstash.WithWritePolicy(memstash.WriteThrough))
			require.NoError(t, err)
			t.Cleanup(writer.Close)
			for _, key := range keys {
				require.NoError(t, writer.Set(ctx, key, "v:"+key), "Set %q", key)
			}

			reader, err := newCache(l2.WithKeyFunc(l2.PrefixedString(prefix)))
			require.NoError(t, err)
			t.Cleanup(reader.Close)
			v, ok, err := reader.Get(ctx, keys[1])
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, "v:"+keys[1], v)

			found, err := reader.BatchGet(ctx, keys)
			require.NoError(t, err, "one awkward key must not fail the batch")
			assert.Len(t, found, len(keys))

			require.NoError(t, writer.BatchDelete(ctx, keys))
			fresh, err := newCache(l2.WithKeyFunc(l2.PrefixedString(prefix)))
			require.NoError(t, err)
			t.Cleanup(fresh.Close)
			found, err = fresh.BatchGet(ctx, keys)
			require.NoError(t, err)
			assert.Empty(t, found, "deleted keys came back")
		})
	}
}
