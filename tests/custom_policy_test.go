package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

// fifoPolicy is a minimal custom eviction policy built purely on the public contract: plain FIFO, no second chances.
// It doubles as the compile-time proof that memstash.EvictionPolicy is implementable outside the module.
type fifoPolicy[K comparable, V any] struct {
	items memstash.Items[K, V]
	nodes []memstash.QNode
	head  int
}

var _ memstash.EvictionPolicy[string, int] = (*fifoPolicy[string, int])(nil)

func (p *fifoPolicy[K, V]) Add(node memstash.QNode) { p.nodes = append(p.nodes, node) }

func (p *fifoPolicy[K, V]) Len() int { return len(p.nodes) - p.head }

func (p *fifoPolicy[K, V]) Bytes() int64 { return int64(cap(p.nodes) * 8) }

func (p *fifoPolicy[K, V]) Evict(nowOff uint32) (uint32, bool) {
	if p.head < len(p.nodes) {
		node := p.nodes[p.head]
		p.head++
		return node.Idx, true // the cache kills and accounts the victim
	}
	return 0, false
}

func (p *fifoPolicy[K, V]) Sweep(release func(idx uint32)) {
	kept := p.nodes[:0]
	for _, node := range p.nodes[p.head:] {
		if p.items.At(node.Idx).Load()&memstash.ItemDead != 0 {
			release(node.Idx)
		} else {
			kept = append(kept, node)
		}
	}
	p.nodes, p.head = kept, 0
}

func (p *fifoPolicy[K, V]) Rebuild(remap func(oldIdx uint32) (uint32, bool)) {
	kept := p.nodes[:0]
	for _, node := range p.nodes[p.head:] {
		if newIdx, live := remap(node.Idx); live {
			node.Idx = newIdx
			kept = append(kept, node)
		}
	}
	p.nodes, p.head = kept, 0
}

func newFIFOPolicy[K comparable, V any](items memstash.Items[K, V], _ int64) memstash.EvictionPolicy[K, V] {
	return &fifoPolicy[K, V]{items: items}
}

// TestCustomEvictionPolicy runs the cache on the FIFO policy above: strict FIFO order with one shard, capacity
// respected, deletes swept through the custom Sweep, and the shard tables staying consistent.
func TestCustomEvictionPolicy(t *testing.T) {
	ctx := context.Background()
	c, err := memstash.New[string, int](
		memstash.WithMemoryCapacity(8),
		memstash.WithShards(1),
		memstash.WithCustomEvictionPolicy(newFIFOPolicy[string, int]),
	)
	require.NoError(t, err)
	defer c.Close()

	for i := 0; i < 16; i++ {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("k%d", i), i))
	}
	assert.LessOrEqual(t, c.Len(), 8, "capacity must be respected")
	for i := 0; i < 8; i++ {
		_, ok := c.GetFromMemory(fmt.Sprintf("k%d", i))
		assert.False(t, ok, "FIFO must have evicted the oldest key k%d", i)
	}
	for i := 8; i < 16; i++ {
		v, ok := c.GetFromMemory(fmt.Sprintf("k%d", i))
		require.True(t, ok, "the newest keys must survive under FIFO (k%d lost)", i)
		assert.Equal(t, i, v)
	}

	// Delete-heavy churn below capacity exercises the custom Sweep through the cache's tombstone reclamation.
	for i := 0; i < 10_000; i++ {
		key := fmt.Sprintf("churn-%d", i)
		require.NoError(t, c.Set(ctx, key, i))
		require.NoError(t, c.Delete(ctx, key))
	}
	require.NoError(t, c.Set(ctx, "after-churn", 42))
	v, ok := c.GetFromMemory("after-churn")
	require.True(t, ok)
	assert.Equal(t, 42, v)
}

func TestCustomEvictionPolicyErrors(t *testing.T) {
	t.Run("a nil factory result is rejected", func(t *testing.T) {
		_, err := memstash.New[string, int](
			memstash.WithMemoryCapacity(8),
			memstash.WithCustomEvictionPolicy(
				func(memstash.Items[string, int], int64) memstash.EvictionPolicy[string, int] { return nil }),
		)
		require.ErrorIs(t, err, memstash.ErrNilCustomPolicy)
	})

	t.Run("a factory built for different key/value types is a mismatch", func(t *testing.T) {
		_, err := memstash.New[string, string](
			memstash.WithMemoryCapacity(8),
			memstash.WithCustomEvictionPolicy(newFIFOPolicy[string, int]),
		)
		require.ErrorIs(t, err, memstash.ErrOptionMismatch)
	})

	t.Run("the custom factory takes precedence over WithPolicy", func(t *testing.T) {
		c, err := memstash.New[string, int](
			memstash.WithMemoryCapacity(8),
			memstash.WithPolicy(memstash.PolicyS3FIFO),
			memstash.WithCustomEvictionPolicy(newFIFOPolicy[string, int]),
		)
		require.NoError(t, err)
		defer c.Close()
		ctx := context.Background()
		// S3-FIFO would protect the touched key; plain FIFO must evict it first regardless.
		require.NoError(t, c.Set(ctx, "first", 1))
		c.GetFromMemory("first")
		for i := 0; i < 8; i++ {
			require.NoError(t, c.Set(ctx, fmt.Sprintf("k%d", i), i))
		}
		_, ok := c.GetFromMemory("first")
		assert.False(t, ok, "the FIFO custom policy must have evicted the oldest key")
	})
}
