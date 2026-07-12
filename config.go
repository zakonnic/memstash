package memstash

import (
	"errors"
	"math/bits"
	"runtime"
	"time"
)

var (
	ErrBadCapacity      = errors.New("memstash: MemoryCapacity must be positive")
	ErrCapacityTooLarge = errors.New("memstash: MemoryCapacity exceeds the addressable pool index space (2^32 records)")
	ErrUnknownPolicy    = errors.New("memstash: unknown eviction policy")
	ErrNilLoader        = errors.New("memstash: loader must not be nil")
	ErrBadTTL           = errors.New("memstash: TTL must not be negative")
	// ErrLoaderPanic resolves the singleflight of a loader that panicked: the panic itself propagates to the caller
	// that ran the loader, while every waiter joined on that flight receives this error instead of hanging forever.
	ErrLoaderPanic = errors.New("memstash: loader panicked")
)

// Config holds the cache configuration. Pass it to NewWithConfig directly, or configure the cache field by field
// with the With* options of New.
type Config[K comparable, V any] struct {
	// MemoryCapacity is the first-level capacity in weight units. When CostFunc == nil every item weighs 1 and the capacity
	// means the number of items. If CostFunc != nil it is required field, must be > 0.
	MemoryCapacity int64

	// CostFunc is the item weight function. It must be deterministic (the weight is recomputed during eviction) and the
	// values must be immutable. A result of 0 is treated as 1. nil means weight 1 for every item.
	CostFunc func(key K, value V) uint32

	// TTL is the lifetime of first-level items with one-second resolution. 0 means no TTL. The same TTL is passed to L2Cache
	// on writes.
	TTL time.Duration

	// Policy is the eviction policy. Defaults to PolicyClock.
	Policy Policy

	// Shards is the number of shards the eviction state (queues, state pool, weight) is split into. It is rounded up to a
	// power of two. 0 means automatic: GOMAXPROCS, but no more than 128 and such that each shard gets at least 64 weight
	// units. Capacity and ghost are divided evenly between shards; eviction operates within a single shard. Shards: 1
	// yields a globally deterministic eviction order (useful in tests).
	Shards int

	// L2Cache is the optional second level.
	L2Cache L2Cache[K, V]

	// WritePolicy is the L2Cache write policy. Defaults to WriteBack (asynchronous): use WriteThrough when the caller
	// must observe the value in L2 right after Set returns.
	WritePolicy WritePolicy

	// GhostSize is the total capacity of the S3-FIFO ghost queues (in keys). 0 means choose automatically:
	// MemoryCapacity (but no more than 1<<20) when CostFunc == nil, otherwise 8192.
	GhostSize int

	// WriteBackBuffer is the buffer size of the background WriteBack worker. 0 means 1024. On buffer overflow the write
	// is performed synchronously (no data is lost).
	WriteBackBuffer int

	// OnL2Error is an optional handler for L2Cache errors on paths where the error cannot be returned to the caller
	// (write-back, the write after a load in GetOrLoad, the L2Cache read inside GetOrLoad before the loader runs).
	OnL2Error func(key K, err error)
}

// isMemstashConfig marks every Config instantiation for the typed-option dispatch protocol (see Option).
func (c *Config[K, V]) isMemstashConfig() {}

// shardCount computes the final number of shards: a power of two that does not split the cache into pointlessly tiny
// pieces.
func (c *Config[K, V]) shardCount() int {
	count := c.Shards
	if count <= 0 {
		count = runtime.GOMAXPROCS(0)
	}
	count = min(count, 128)
	count = pow2Ceil(count)
	for count > 1 && c.MemoryCapacity/int64(count) < 64 {
		count >>= 1
	}
	return count
}

func pow2Ceil(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}

func (c *Config[K, V]) ghostSize() int {
	if c.GhostSize > 0 {
		return c.GhostSize
	}
	if c.CostFunc == nil {
		return int(min(c.MemoryCapacity, 1<<20))
	}
	return 8192
}

func (c *Config[K, V]) writeBackBuffer() int {
	if c.WriteBackBuffer > 0 {
		return c.WriteBackBuffer
	}
	return 1024
}
