package memstash

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShardLayout pins what keeps a reader's cache line off the writers: the table pointer and the move sequence
// share the shard's first line, the mutex starts on the next one, and the shard is a whole number of lines so the
// array keeps every shard aligned the same way.
func TestShardLayout(t *testing.T) {
	type sh = shard[uint64, uint64]
	require.EqualValues(t, shardBytes, unsafe.Sizeof(sh{}), "shard size drifted: re-check the padding")
	assert.Zero(t, unsafe.Sizeof(sh{})%cacheLine, "a shard must span whole cache lines")
	assert.Zero(t, unsafe.Offsetof(sh{}.items), "the table pointer must open the read line")
	assert.Less(t, unsafe.Offsetof(sh{}.moveSeq), uintptr(cacheLine), "the move sequence must share the read line")
	assert.GreaterOrEqual(t, unsafe.Offsetof(sh{}.mu), uintptr(cacheLine), "the mutex must not share the read line")

	// The array itself starts wherever the allocator puts it - a shard slice carries pointers and is past the size
	// that gives it an allocation header, so the base is 8 bytes into a line. What has to hold is the separation,
	// and a whole-line stride carries it to every shard whatever the base.
	c, err := New[uint64, uint64](WithMemoryCapacity(1024), WithShardsCount(8))
	require.NoError(t, err)
	defer c.Close()
	for i := range c.shards {
		s := &c.shards[i]
		read := uintptr(unsafe.Pointer(&s.items)) / cacheLine
		assert.Equal(t, read, uintptr(unsafe.Pointer(&s.moveSeq))/cacheLine,
			"shard %d: the table pointer and the move sequence must share one line", i)
		for j := range c.shards {
			assert.NotEqual(t, read, uintptr(unsafe.Pointer(&c.shards[j].mu))/cacheLine,
				"shard %d: its read line carries shard %d's mutex", i, j)
		}
	}
}
