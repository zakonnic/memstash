package tests

import (
	"context"
	"fmt"
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

func TestIterator(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 1000})

	want := map[string]string{}
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("k%03d", i)
		want[key] = "v" + key
		require.NoError(t, c.Set(ctx, key, "v"+key))
	}
	require.NoError(t, c.Delete(ctx, "k000"))
	delete(want, "k000")

	assert.Equal(t, want, maps.Collect(c.Iterator()))
}

func TestIteratorEarlyBreak(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	for i := 0; i < 50; i++ {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("k%d", i), "v"))
	}

	seen := 0
	for range c.Iterator() {
		seen++
		if seen == 10 {
			break
		}
	}
	assert.Equal(t, 10, seen)
}

func TestIteratorOverwriteYieldsLatestValue(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, c.Set(ctx, "k", "old"))
	require.NoError(t, c.Set(ctx, "k", "new"))

	assert.Equal(t, map[string]string{"k": "new"}, maps.Collect(c.Iterator()))
}
