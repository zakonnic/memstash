package integration

// Benchmarks for the raw adapter batch paths against live Redis: measures L2Cache.BatchGet/BatchSet directly,
// bypassing L1. The three size profiles exercise both sides of the adapters' multiKeyBudget: small batches take the
// multi-key MGET/MSET fast path, large ones the per-key pipeline fallback.

import (
	"context"
	"fmt"
	"testing"
	"time"

	redigolib "github.com/gomodule/redigo/redis"
	"github.com/joomcode/redispipe/redisconn"
	goredislib "github.com/redis/go-redis/v9"
	rueidislib "github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	goredis_adapter "github.com/zakonnic/memstash/l2/goredis_adapter"
	redigo_adapter "github.com/zakonnic/memstash/l2/redigo_adapter"
	redispipe_adapter "github.com/zakonnic/memstash/l2/redispipe_adapter"
	rueidis_adapter "github.com/zakonnic/memstash/l2/rueidis_adapter"
)

func benchAdapterBatch(b *testing.B, store memstash.L2Cache[string, string]) {
	ctx := context.Background()
	makeBatch := func(n, valSize int) (memstash.List[string, string], []string) {
		val := make([]byte, valSize)
		for i := range val {
			val[i] = 'x'
		}
		items := make(memstash.List[string, string], 0, n)
		keys := make([]string, 0, n)
		for i := 0; i < n; i++ {
			key := fmt.Sprintf("bench-batch-k%d", i)
			items = append(items, memstash.KeyVal[string, string]{Key: key, Value: string(val)})
			keys = append(keys, key)
		}
		return items, keys
	}

	for _, tc := range []struct {
		name    string
		n       int
		valSize int
	}{
		{"100x20B", 100, 20},   // fits every budget -> multi-key command
		{"500x200B", 500, 200}, // ~110KB -> per-key pipeline everywhere
		{"100x2KB", 100, 2048}, // ~210KB values -> pipeline for sets, MGET for gets in the big-budget clients
	} {
		items, keys := makeBatch(tc.n, tc.valSize)
		require.NoError(b, store.BatchSet(ctx, items, 0))
		b.Run("BatchGet"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				found, err := store.BatchGet(ctx, keys)
				if err != nil || len(found) != tc.n {
					b.Fatalf("BatchGet: %v, found %d", err, len(found))
				}
			}
		})
		b.Run("BatchSet"+tc.name+"NoTTL", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := store.BatchSet(ctx, items, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("BatchSet"+tc.name+"TTL", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := store.BatchSet(ctx, items, time.Minute); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkAdapterBatchRueidis(b *testing.B) {
	requireServer(b, redisAddr())
	client, err := rueidislib.NewClient(rueidislib.ClientOption{InitAddress: []string{redisAddr()}})
	require.NoError(b, err)
	b.Cleanup(client.Close)
	store, err := rueidis_adapter.New[string, string](client, l2.StringCodec())
	require.NoError(b, err)
	benchAdapterBatch(b, store)
}

func BenchmarkAdapterBatchGoRedis(b *testing.B) {
	requireServer(b, redisAddr())
	client := goredislib.NewClient(&goredislib.Options{Addr: redisAddr()})
	b.Cleanup(func() { _ = client.Close() })
	store, err := goredis_adapter.New[string, string](client, l2.StringCodec())
	require.NoError(b, err)
	benchAdapterBatch(b, store)
}

func BenchmarkAdapterBatchRedigo(b *testing.B) {
	requireServer(b, redisAddr())
	pool := &redigolib.Pool{
		MaxIdle: 4,
		Dial:    func() (redigolib.Conn, error) { return redigolib.Dial("tcp", redisAddr()) },
	}
	b.Cleanup(func() { _ = pool.Close() })
	store, err := redigo_adapter.New[string, string](pool, l2.StringCodec())
	require.NoError(b, err)
	benchAdapterBatch(b, store)
}

func BenchmarkAdapterBatchRedispipe(b *testing.B) {
	requireServer(b, redisAddr())
	conn, err := redisconn.Connect(context.Background(), redisAddr(), redisconn.Opts{})
	require.NoError(b, err)
	b.Cleanup(conn.Close)
	store, err := redispipe_adapter.New[string, string](conn, l2.StringCodec())
	require.NoError(b, err)
	benchAdapterBatch(b, store)
}
