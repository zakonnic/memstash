package memstash

import (
	"errors"
	"fmt"
	"math/bits"
	"runtime"
	"time"
)

// Errors reported by New and NewWithConfig for a configuration that cannot produce a working cache.
var (
	// Error is the base sentinel error for the package.
	Error = errors.New("memstash err")

	ErrBadCapacity         = fmt.Errorf("%w: MemoryCapacity must be positive", Error)
	ErrCapacityTooLarge    = fmt.Errorf("%w: MemoryCapacity exceeds the addressable table index space (2^32 items)", Error)
	ErrBadBudget           = fmt.Errorf("%w: MemoryBudget must be positive", Error)
	ErrBudgetAndCapacity   = fmt.Errorf("%w: MemoryBudget and MemoryCapacity are mutually exclusive", Error)
	ErrBudgetNeedsCostFunc = fmt.Errorf("%w: MemoryBudget cannot estimate the byte size of this type - set CostFunc explicitly", Error)
	ErrUnknownPolicy       = fmt.Errorf("%w: unknown eviction policy", Error)
	ErrNilCustomPolicy     = fmt.Errorf("%w: the custom eviction policy factory returned nil", Error)
	ErrNilLoader           = fmt.Errorf("%w: loader must not be nil", Error)
	ErrBadTTL              = fmt.Errorf("%w: TTL must not be negative", Error)
	ErrPanic               = fmt.Errorf("%w: panic recovered", Error)
	ErrLoaderPanic         = fmt.Errorf("%w: loader panicked", ErrPanic)
)

// PanicHandler is called with whatever recover() returned when the cache swallows a panic. Runs on the goroutine that
// recovered. handled says the panic was also passed on: to OnL2Error, or to the caller as an ErrLoaderPanic error.
type PanicHandler func(recovered any, handled bool)

// Config holds the cache configuration. Pass it to NewWithConfig directly, or configure the cache field by field
// with the With* options of New.
type Config[K comparable, V any] struct {
	// MemoryCapacity is the first-level capacity in weight units. When CostFunc == nil every item weighs 1 and the capacity
	// means the number of items. If CostFunc != nil it is required field, must be > 0. Mutually exclusive with MemoryBudget.
	MemoryCapacity int64

	// MemoryBudget bounds the cached elements size in bytes. When CostFunc == nil a cost function is derived automatically.
	// Require CostFunc for complex types.
	MemoryBudget int64

	// CostFunc is the item weight function. It must be deterministic (the weight is recomputed during eviction) and the
	// values must be immutable. A result of 0 is treated as 1. nil means weight 1 for every item.
	CostFunc func(key K, value V) uint32

	// TTL is the lifetime of first-level items with one-second resolution. 0 means no TTL. The same TTL is passed to L2Cache
	// on writes.
	TTL time.Duration

	// RefreshTTLOnGet makes every first-level hit extend the item's lifetime by a full TTL (sliding expiration).
	RefreshTTLOnGet bool

	// Policy is the eviction policy. Defaults to PolicyS3FIFO.
	Policy Policy

	// CustomPolicy is an optional factory for a user-supplied eviction policy, called once per shard. When set it
	// takes precedence over Policy. The factory must not return nil.
	CustomPolicy EvictionPolicyFactory[K, V]

	// Shards is the number of shards the eviction state (queues, state pool, weight) is split into. It is rounded up to
	// a power of two and capped at 128; shards are also halved until each holds at least 64 weight units. 0 means
	// automatic: GOMAXPROCS. Capacity and ghost are divided evenly between shards; eviction operates within a single
	// shard. Shards: 1 yields a globally deterministic eviction order (useful in tests).
	Shards int

	// L2Cache is the optional second level.
	L2Cache L2Cache[K, V]

	// WritePolicy is the L2Cache write policy. Defaults to WriteBack (asynchronous): use WriteThrough when the caller
	// must observe the value in L2 right after Set returns.
	WritePolicy WritePolicy

	// GhostSize is the total capacity of the S3-FIFO ghost queues and of the W-TinyLFU frequency sketch (in keys).
	// 0 means choose automatically: MemoryCapacity (but no more than 1<<20) when CostFunc == nil, otherwise 8192.
	GhostSize int

	// WriteBackBufferSize is the buffer size of the background WriteBack worker. 0 means 1024.
	// On buffer overflow the write is performed synchronously.
	WriteBackBufferSize int

	// WriteBackBatching is how the WriteBack worker drains its buffer. Defaults to BatchingFull.
	WriteBackBatching WriteBackBatching

	// OnL2Error is an optional handler for L2Cache errors on paths where the error cannot be returned to the caller
	// (write-back, the write after a load in GetOrLoad, the L2Cache read inside GetOrLoad before the loader runs).
	OnL2Error func(key K, err error)

	// OnDeletion is an optional handler called once for every item that leaves the first level, whatever the cause,
	// with the value the item held. It runs after the shard mutex is released, in the order the items died, on the
	// goroutine whose operation removed them - so a slow handler slows that caller down, not the whole shard.
	// Filter with DeletionCause.Automatic to see only the removals the cache decided on its own.
	OnDeletion func(key K, value V, cause DeletionCause)

	// OnPanic is an optional handler for the panics the cache recovers instead of letting them end the process:
	// on its own goroutines (the TTL clock, the write-back worker) and around a loader running in the background.
	// A loader that panics under a synchronous GetOrLoad is not recovered at all - that panic reaches the caller.
	OnPanic PanicHandler

	// StatsEnabled turns on the operation counters returned by Stats(). Off by default - adds 0.6 ns overhead.
	StatsEnabled bool
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
	if c.WriteBackBufferSize > 0 {
		return c.WriteBackBufferSize
	}
	return 1024
}
