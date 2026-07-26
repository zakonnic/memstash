package tests

import (
	"context"
	"testing"

	"github.com/zakonnic/memstash"
)

func benchCache(b *testing.B, capacity int64) *memstash.Cache[int, int] {
	b.Helper()
	c, err := memstash.New[int, int](memstash.WithMemoryCapacity(capacity))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(c.Close)
	return c
}

// BenchmarkSetOverwrite lands every Set on a live key: the in-place replacement path.
func BenchmarkSetOverwrite(b *testing.B) {
	ctx := context.Background()
	c := benchCache(b, 4096)
	for i := range 1024 {
		_ = c.Set(ctx, i, i)
	}
	i := 0
	for b.Loop() {
		_ = c.Set(ctx, i&1023, i)
		i++
	}
}

// BenchmarkSetEvict keeps the shard over capacity, so every Set evicts a victim.
func BenchmarkSetEvict(b *testing.B) {
	ctx := context.Background()
	c := benchCache(b, 1024)
	for i := range 2048 {
		_ = c.Set(ctx, i, i)
	}
	i := 2048
	for b.Loop() {
		_ = c.Set(ctx, i, i)
		i++
	}
}

// BenchmarkSetDelete pairs each insert with a delete that always finds a live key.
func BenchmarkSetDelete(b *testing.B) {
	ctx := context.Background()
	c := benchCache(b, 4096)
	i := 0
	for b.Loop() {
		_ = c.Set(ctx, i, i)
		_ = c.Delete(ctx, i)
		i++
	}
}

func benchWatchedCache(b *testing.B, capacity int64) *memstash.Cache[int, int] {
	b.Helper()
	sink := 0
	c, err := memstash.New[int, int](
		memstash.WithMemoryCapacity(capacity),
		memstash.WithOnDeletion(func(key int, value int, cause memstash.DeletionCause) { sink += value }),
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(c.Close)
	return c
}

// BenchmarkSetOverwriteWatched is BenchmarkSetOverwrite with a handler attached: the cost of the feature in use.
func BenchmarkSetOverwriteWatched(b *testing.B) {
	ctx := context.Background()
	c := benchWatchedCache(b, 4096)
	for i := range 1024 {
		_ = c.Set(ctx, i, i)
	}
	i := 0
	for b.Loop() {
		_ = c.Set(ctx, i&1023, i)
		i++
	}
}

// BenchmarkSetEvictWatched is BenchmarkSetEvict with a handler attached.
func BenchmarkSetEvictWatched(b *testing.B) {
	ctx := context.Background()
	c := benchWatchedCache(b, 1024)
	for i := range 2048 {
		_ = c.Set(ctx, i, i)
	}
	i := 2048
	for b.Loop() {
		_ = c.Set(ctx, i, i)
		i++
	}
}
