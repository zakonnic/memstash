package benchmarks

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/maypok86/otter/v2"
	"github.com/zakonnic/memstash"
)

// The singleflight machinery only runs on a miss, so these keep every call missing: an erroring loader is never
// cached, so every iteration claims, loads and releases the flights for real.

var (
	errFlight = errors.New("flight")
)

func BenchmarkFlightMemstash(b *testing.B) {
	ctx := context.Background()
	c, err := memstash.New[string, string](memstash.WithMemoryCapacity(10_000))
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	load := func(context.Context, string) (string, error) { return "", errFlight }

	b.ReportAllocs()
	for b.Loop() {
		_, _ = c.GetOrLoad(ctx, "key-0", load)
	}
}

func BenchmarkFlightOtter(b *testing.B) {
	ctx := context.Background()
	c := otter.Must(&otter.Options[string, string]{MaximumSize: 10_000})
	loader := otter.LoaderFunc[string, string](func(context.Context, string) (string, error) {
		return "", errFlight
	})

	b.ReportAllocs()
	for b.Loop() {
		_, _ = c.Get(ctx, "key-0", loader)
	}
}

func BenchmarkFlightBatchMemstash(b *testing.B) {
	ctx := context.Background()
	load := func(context.Context, []string) (memstash.List[string, string], error) { return nil, errFlight }
	for _, n := range []int{2, 8, 32} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			c, err := memstash.New[string, string](memstash.WithMemoryCapacity(10_000))
			if err != nil {
				b.Fatal(err)
			}
			defer c.Close()
			keys := make([]string, n)
			for i := range keys {
				keys[i] = fmt.Sprintf("key-%d", i)
			}
			b.ReportAllocs()
			for b.Loop() {
				_, _ = c.BatchGetOrLoad(ctx, keys, load)
			}
		})
	}
}

func BenchmarkFlightBatchOtter(b *testing.B) {
	ctx := context.Background()
	bulk := otter.BulkLoaderFunc[string, string](func(_ context.Context, keys []string) (map[string]string, error) {
		return nil, errFlight
	})
	for _, n := range []int{2, 8, 32} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			c := otter.Must(&otter.Options[string, string]{MaximumSize: 10_000})
			keys := make([]string, n)
			for i := range keys {
				keys[i] = fmt.Sprintf("key-%d", i)
			}
			b.ReportAllocs()
			for b.Loop() {
				_, _ = c.BulkGet(ctx, keys, bulk)
			}
		})
	}
}

func benchFlightCache(b *testing.B) *memstash.Cache[string, string] {
	b.Helper()
	c, err := memstash.New[string, string](memstash.WithMemoryCapacity(10_000))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(c.Close)
	return c
}

func benchKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	return keys
}

func BenchmarkGetOrLoadMiss(b *testing.B) {
	ctx := context.Background()
	c := benchFlightCache(b)
	load := func(context.Context, string) (string, error) { return "", errFlight }

	b.ReportAllocs()
	for b.Loop() {
		_, _ = c.GetOrLoad(ctx, "key-0", load)
	}
}

// BenchmarkGetOrLoadMissSerial is the baseline BatchGetOrLoad must beat: the same keys resolved one at a time.
func BenchmarkGetOrLoadMissSerial(b *testing.B) {
	ctx := context.Background()
	load := func(context.Context, string) (string, error) { return "", errFlight }
	for _, n := range []int{2, 8, 32} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			c := benchFlightCache(b)
			keys := benchKeys(n)
			b.ReportAllocs()
			for b.Loop() {
				for _, key := range keys {
					_, _ = c.GetOrLoad(ctx, key, load)
				}
			}
		})
	}
}

func BenchmarkBatchGetOrLoadMiss(b *testing.B) {
	ctx := context.Background()
	load := func(context.Context, []string) (memstash.List[string, string], error) { return nil, errFlight }
	for _, n := range []int{1, 2, 8, 32} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			c := benchFlightCache(b)
			keys := benchKeys(n)
			b.ReportAllocs()
			for b.Loop() {
				_, _ = c.BatchGetOrLoad(ctx, keys, load)
			}
		})
	}
}

// The flight registry is 100% write traffic (insert+delete pairs), so these measure it under real parallelism:
// distinct keys per goroutine = pure registry throughput, one shared key = contention plus coalescing.
func BenchmarkGetOrLoadMissParallel(b *testing.B) {
	ctx := context.Background()
	c := benchFlightCache(b)
	load := func(context.Context, string) (string, error) { return "", errFlight }

	var seq atomic.Int64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		base := fmt.Sprintf("w%d-", seq.Add(1))
		i := 0
		for pb.Next() {
			i++
			_, _ = c.GetOrLoad(ctx, base+strconv.Itoa(i&63), load)
		}
	})
}

func BenchmarkGetOrLoadMissParallelSharedKey(b *testing.B) {
	ctx := context.Background()
	c := benchFlightCache(b)
	load := func(context.Context, string) (string, error) { return "", errFlight }

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = c.GetOrLoad(ctx, "hot", load)
		}
	})
}

func BenchmarkBatchGetOrLoadMissParallel(b *testing.B) {
	ctx := context.Background()
	c := benchFlightCache(b)
	load := func(context.Context, []string) (memstash.List[string, string], error) { return nil, errFlight }

	var seq atomic.Int64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		base := fmt.Sprintf("w%d-", seq.Add(1))
		keys := make([]string, 8)
		for i := range keys {
			keys[i] = base + strconv.Itoa(i)
		}
		for pb.Next() {
			_, _ = c.BatchGetOrLoad(ctx, keys, load)
		}
	})
}
