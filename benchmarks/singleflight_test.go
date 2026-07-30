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

// batchLoader answers every key it is asked for, so the loaded values stay cached - the steady state the all-miss
// benchmarks above never reach.
func batchLoader(_ context.Context, keys []string) (memstash.List[string, string], error) {
	out := make(memstash.List[string, string], 0, len(keys))
	for _, key := range keys {
		out = append(out, memstash.KeyVal[string, string]{Key: key, Value: key})
	}
	return out, nil
}

// BenchmarkBatchGetOrLoadHit is the common case: a warm cache where no key reaches the loader.
func BenchmarkBatchGetOrLoadHit(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{2, 8, 32} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			c := benchFlightCache(b)
			keys := benchKeys(n)
			if _, err := c.BatchGetOrLoad(ctx, keys, batchLoader); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				_, _ = c.BatchGetOrLoad(ctx, keys, batchLoader)
			}
		})
	}
}

// BenchmarkBatchGetOrLoadHalfHit mixes the two: half the batch is cached, half claims a flight.
func BenchmarkBatchGetOrLoadHalfHit(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{8, 32} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			c := benchFlightCache(b)
			keys := benchKeys(n)
			for i := 0; i < n; i += 2 {
				if err := c.Set(ctx, keys[i], keys[i]); err != nil {
					b.Fatal(err)
				}
			}
			// A loader that finds nothing keeps the odd half missing, so the mix stays 50/50 without any extra work
			// inside the timed loop.
			empty := func(context.Context, []string) (memstash.List[string, string], error) { return nil, nil }
			b.ReportAllocs()
			for b.Loop() {
				_, _ = c.BatchGetOrLoad(ctx, keys, empty)
			}
		})
	}
}

func BenchmarkGetAndRefresh(b *testing.B) {
	ctx := context.Background()
	c := benchFlightCache(b)
	if err := c.Set(ctx, "key-0", "v"); err != nil {
		b.Fatal(err)
	}
	// A nil loader isolates the read half; with one, every call would spawn a goroutine and measure the scheduler.
	b.ReportAllocs()
	for b.Loop() {
		_, _ = c.GetAndRefresh(ctx, "key-0", nil)
	}
}

func BenchmarkBatchGetAndRefresh(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{8, 32} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			c := benchFlightCache(b)
			keys := benchKeys(n)
			for _, key := range keys {
				if err := c.Set(ctx, key, key); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				_ = c.BatchGetAndRefresh(ctx, keys, nil)
			}
		})
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
