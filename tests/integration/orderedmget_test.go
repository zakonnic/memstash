package integration

import (
	"context"
	"fmt"
	"testing"

	goredislib "github.com/redis/go-redis/v9"
	rueidislib "github.com/redis/rueidis"
	valkeylib "github.com/valkey-io/valkey-go"

	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	goredis_adapter "github.com/zakonnic/memstash/l2/goredis_adapter"
	rueidis_adapter "github.com/zakonnic/memstash/l2/rueidis_adapter"
	valkey_adapter "github.com/zakonnic/memstash/l2/valkey_adapter"
)

// orderedMgetModes covers both BatchGet reply paths: the positional MGET array and the key-to-reply map.
var orderedMgetModes = []struct {
	name string
	mode memstash.DetectMode
}{
	{"AutoDetect", memstash.AutoDetect},
	{"Enabled", memstash.Enabled},
	{"Disabled", memstash.Disabled},
}

// requireBatchGetPairing stores every other key, then reads the whole range back: the gaps make a positional reply
// walk that lost its alignment pair values with the wrong keys instead of silently passing.
func requireBatchGetPairing(t *testing.T, store memstash.L2Cache[string, string]) {
	t.Helper()
	ctx := context.Background()
	const n = 64
	keys := make([]string, 0, n)
	stored := make(memstash.List[string, string], 0, n/2)
	want := make(map[string]string, n/2)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%02d", i)
		keys = append(keys, key)
		if i%2 == 0 {
			value := fmt.Sprintf("value-of-%02d", i)
			stored = append(stored, memstash.KeyVal[string, string]{Key: key, Value: value})
			want[key] = value
		}
	}
	require.NoError(t, store.BatchDelete(ctx, keys))
	require.NoError(t, store.BatchSet(ctx, stored, 0))

	found, err := store.BatchGet(ctx, keys)
	require.NoError(t, err)
	require.Len(t, found, len(want), "only the stored half must come back")
	for _, kv := range found {
		require.Equal(t, want[kv.Key], kv.Value, "value paired with key %s", kv.Key)
	}
	require.NoError(t, store.BatchDelete(ctx, keys))
}

func TestOrderedMgetRueidis(t *testing.T) {
	requireServer(t, redisAddr())
	client, err := rueidislib.NewClient(rueidislib.ClientOption{InitAddress: []string{redisAddr()}})
	require.NoError(t, err, "rueidis.NewClient")
	t.Cleanup(client.Close)
	require.True(t, rueidis_adapter.IsOrderedMgetAvailable(client), "a standalone node must probe as ordered")

	for _, tc := range orderedMgetModes {
		t.Run(tc.name, func(t *testing.T) {
			store, err := rueidis_adapter.New[string, string](client, l2.StringCodec(),
				l2.WithKeyFunc(l2.PrefixedString("omget-rueidis-"+tc.name+"|")), l2.WithOrderedMget(tc.mode))
			require.NoError(t, err, "New")
			requireBatchGetPairing(t, store)
		})
	}
}

func TestOrderedMgetValkey(t *testing.T) {
	requireServer(t, valkeyAddr())
	client, err := valkeylib.NewClient(valkeylib.ClientOption{InitAddress: []string{valkeyAddr()}})
	require.NoError(t, err, "valkey.NewClient")
	t.Cleanup(client.Close)
	require.True(t, valkey_adapter.IsOrderedMgetAvailable(client), "a standalone node must probe as ordered")

	for _, tc := range orderedMgetModes {
		t.Run(tc.name, func(t *testing.T) {
			store, err := valkey_adapter.New[string, string](client, l2.StringCodec(),
				l2.WithKeyFunc(l2.PrefixedString("omget-valkey-"+tc.name+"|")), l2.WithOrderedMget(tc.mode))
			require.NoError(t, err, "New")
			requireBatchGetPairing(t, store)
		})
	}
}

func TestOrderedMgetGoRedis(t *testing.T) {
	requireServer(t, redisAddr())
	client := goredislib.NewClient(&goredislib.Options{Addr: redisAddr()})
	t.Cleanup(func() { _ = client.Close() })
	require.True(t, goredis_adapter.IsOrderedMgetAvailable(client), "a standalone node must probe as ordered")

	for _, tc := range orderedMgetModes {
		t.Run(tc.name, func(t *testing.T) {
			store, err := goredis_adapter.New[string, string](client, l2.StringCodec(),
				l2.WithKeyFunc(l2.PrefixedString("omget-goredis-"+tc.name+"|")), l2.WithOrderedMget(tc.mode))
			require.NoError(t, err, "New")
			requireBatchGetPairing(t, store)
		})
	}
}

// TestOrderedMgetClusterProbe pins the AutoDetect contract that keeps a cluster correct: a client spread over several
// nodes must not be taken for an ordered one, so BatchGet stays on the regrouping helper.
func TestOrderedMgetClusterProbe(t *testing.T) {
	addrs := redisClusterAddrs()
	requireServer(t, addrs[0])

	rueidisCl, err := rueidislib.NewClient(rueidislib.ClientOption{InitAddress: addrs})
	require.NoError(t, err, "rueidis.NewClient")
	t.Cleanup(rueidisCl.Close)
	require.False(t, rueidis_adapter.IsOrderedMgetAvailable(rueidisCl), "a 3-master cluster must not probe as ordered")

	valkeyCl, err := valkeylib.NewClient(valkeylib.ClientOption{InitAddress: addrs})
	require.NoError(t, err, "valkey.NewClient")
	t.Cleanup(valkeyCl.Close)
	require.False(t, valkey_adapter.IsOrderedMgetAvailable(valkeyCl), "a 3-master cluster must not probe as ordered")

	goredisCl := goredislib.NewClusterClient(&goredislib.ClusterOptions{Addrs: addrs})
	t.Cleanup(func() { _ = goredisCl.Close() })
	require.False(t, goredis_adapter.IsOrderedMgetAvailable(goredisCl), "a cluster client must not probe as ordered")

	store, err := valkey_adapter.New[string, string](valkeyCl, l2.StringCodec(),
		l2.WithKeyFunc(l2.PrefixedString("omget-cluster|")))
	require.NoError(t, err, "New")
	requireBatchGetPairing(t, store)
}
