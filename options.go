package memstash

import (
	"errors"
	"time"
)

// ErrOptionMismatch is returned by a constructor when an option carrying key/value types (WithCostFunc, WithL2Cache,
// WithOnL2Error, the adapters' WithKeyFunc) was built for key/value types different from the cache's.
var ErrOptionMismatch = errors.New("memstash: option type arguments do not match the cache's key/value types")

// Option configures a cache under construction. Build one with the With* helpers.
//
// Option has no type parameters, so K and V are named once, on New. Options that depend on K and V are checked against
// the cache when it is built and fail with ErrOptionMismatch.
//
// The fields are exported so that other packages, such as the L2 adapters, can define options of the same type and
// accept one flat option list. ApplyTyped receives arbitrary targets. It returns nil for a target it does not know,
// fills a target that matches exactly, and returns ErrOptionMismatch for a known target whose key/value types differ.
type Option struct {
	// ApplyField applies a plain-value per-field option.
	ApplyField func(f *FieldOverrides)
	// ApplyTyped applies a per-field option that mentions K/V to a target (for example *Config[K, V]).
	ApplyTyped func(target any) error
}

// FieldOverrides accumulates the plain-value per-field options; a nil pointer means "keep the base value". It exists
// so those options do not have to be generic: they cannot write into Config[K, V] directly without knowing K and V.
type FieldOverrides struct {
	memoryCapacity       *int64
	memoryBudget         *int64
	ttl                  *time.Duration
	refreshTTLOnGet      *bool
	policy               *Policy
	preallocateTable     *bool
	preallocationRatio   *float64
	growthThreshold      *float64
	shards               *int
	writePolicy          *WritePolicy
	ghostSize            *int
	writeBackBufferSize  *int
	writeBackWorkers     *int
	writeBackBatching    *WriteBackBatching
	getBatchedBufferSize *int
	getBatchedWorkers    *int
	onPanic              *PanicHandler
	statsEnabled         *bool
}

// configTarget is the marker implemented by every Config instantiation: it lets a typed option distinguish "a Config
// of foreign key/value types" (an error) from "not a Config at all" (some other package's target - not ours, skip).
type configTarget interface{ isMemstashConfig() }

// buildConfig assembles the final configuration by applying the options in the order they are given.
func buildConfig[K comparable, V any](opts []Option) (*Config[K, V], error) {
	var cfg Config[K, V]
	var fields FieldOverrides
	for _, opt := range opts {
		if opt.ApplyField != nil {
			opt.ApplyField(&fields)
		}
		if opt.ApplyTyped != nil {
			if err := opt.ApplyTyped(&cfg); err != nil {
				return &cfg, err
			}
		}
	}
	if fields.memoryCapacity != nil {
		cfg.MemoryCapacity = *fields.memoryCapacity
	}
	if fields.memoryBudget != nil {
		cfg.MemoryBudget = *fields.memoryBudget
	}
	if fields.ttl != nil {
		cfg.TTL = *fields.ttl
	}
	if fields.refreshTTLOnGet != nil {
		cfg.RefreshTTLOnGet = *fields.refreshTTLOnGet
	}
	if fields.policy != nil {
		cfg.Policy = *fields.policy
	}
	if fields.preallocateTable != nil {
		cfg.PreallocateMap = *fields.preallocateTable
	}
	if fields.preallocationRatio != nil {
		cfg.PreallocationRatio = *fields.preallocationRatio
	}
	if fields.growthThreshold != nil {
		cfg.GrowthThreshold = *fields.growthThreshold
	}
	if fields.shards != nil {
		cfg.ShardsCount = *fields.shards
	}
	if fields.writePolicy != nil {
		cfg.WritePolicy = *fields.writePolicy
	}
	if fields.ghostSize != nil {
		cfg.GhostSize = *fields.ghostSize
	}
	if fields.writeBackBufferSize != nil {
		cfg.WriteBackBufferSize = *fields.writeBackBufferSize
	}
	if fields.writeBackWorkers != nil {
		cfg.WriteBackWorkers = *fields.writeBackWorkers
	}
	if fields.writeBackBatching != nil {
		cfg.WriteBackBatching = *fields.writeBackBatching
	}
	if fields.getBatchedBufferSize != nil {
		cfg.GetBatchedBufferSize = *fields.getBatchedBufferSize
	}
	if fields.getBatchedWorkers != nil {
		cfg.GetBatchedWorkers = *fields.getBatchedWorkers
	}
	if fields.onPanic != nil {
		cfg.OnPanic = *fields.onPanic
	}
	if fields.statsEnabled != nil {
		cfg.StatsEnabled = *fields.statsEnabled
	}
	return &cfg, nil
}

// applyToConfig implements the typed-option dispatch protocol for *Config[K, V] targets.
func applyToConfig[K comparable, V any](target any, fill func(*Config[K, V])) error {
	typed, ok := target.(*Config[K, V])
	if !ok {
		if _, isConfig := target.(configTarget); isConfig {
			return ErrOptionMismatch // a Config, but of different key/value types
		}
		return nil // some other package's target - not ours to fill
	}
	fill(typed)
	return nil
}

// WithMemoryCapacity sets Config.MemoryCapacity: the first-level capacity in weight units.
func WithMemoryCapacity(capacity int64) Option {
	return Option{ApplyField: func(f *FieldOverrides) { f.memoryCapacity = &capacity }}
}

// WithMemoryBudget sets Config.MemoryBudget: the first-level bound in data bytes (keys and values). Mutually exclusive
// with WithMemoryCapacity; see Config.MemoryBudget for the key/value types the automatic size estimator supports.
func WithMemoryBudget(bytes int64) Option {
	return Option{ApplyField: func(f *FieldOverrides) { f.memoryBudget = &bytes }}
}

// WithCostFunc sets Config.CostFunc: the item weight function.
func WithCostFunc[K comparable, V any](costFunc func(key K, value V) uint32) Option {
	return Option{ApplyTyped: func(target any) error {
		return applyToConfig(target, func(cfg *Config[K, V]) { cfg.CostFunc = costFunc })
	}}
}

// WithTTL sets Config.TTL: the lifetime of first-level items.
func WithTTL(ttl time.Duration) Option {
	return Option{ApplyField: func(f *FieldOverrides) { f.ttl = &ttl }}
}

// WithRefreshTTLOnGet sets Config.RefreshTTLOnGet (sliding expiration).
func WithRefreshTTLOnGet() Option {
	return Option{ApplyField: func(f *FieldOverrides) {
		on := true
		f.refreshTTLOnGet = &on
	}}
}

// WithPolicy sets Config.Policy: the eviction policy.
func WithPolicy(policy Policy) Option {
	return Option{ApplyField: func(f *FieldOverrides) { f.policy = &policy }}
}

// WithCustomEvictionPolicy sets Config.CustomPolicy: a factory for a user-supplied eviction policy, called once per
// shard. It takes precedence over WithPolicy.
func WithCustomEvictionPolicy[K comparable, V any](factory EvictionPolicyFactory[K, V]) Option {
	return Option{ApplyTyped: func(target any) error {
		return applyToConfig(target, func(cfg *Config[K, V]) { cfg.CustomPolicy = factory })
	}}
}

// WithPreallocatedSize sets Config.PreallocateMap: every shard allocates its whole share of the capacity up front, so
// the cache never allocates a table after New. WithPreallocationRatio sets how full that table gets; without it the
// fill lands at 0.75 of the slots. It is rejected together with WithCostFunc or WithMemoryBudget: the table is sized
// in items, and a weighted capacity does not count items.
func WithPreallocatedSize() Option {
	return Option{ApplyField: func(f *FieldOverrides) {
		on := true
		f.preallocateTable = &on
	}}
}

// WithPreallocationRatio sets Config.PreallocationRatio: how full a preallocated shard sits at capacity, in (0, 1).
// It turns preallocation on by itself, so WithPreallocatedSize is not needed alongside it. Anything outside (0, 1) is
// rejected with ErrBadPreallocationRatio, except 0, which selects the default of 0.75 - the same table
// WithPreallocatedSize alone gives.
//
// Denser packs the table tighter and walks it further: with r the ratio, a hit walks (1 + 1/(1-r))/2 slots on
// average - 2.5 at 0.75, 5.5 at 0.9 - and a miss up to (1 + 1/(1-r)^2)/2 before the group filter cuts it short. The
// table never holds anything but live items, so those are the walks the cache keeps, however long it churns.
func WithPreallocationRatio(ratio float64) Option {
	return Option{ApplyField: func(f *FieldOverrides) {
		on := true
		f.preallocateTable, f.preallocationRatio = &on, &ratio
	}}
}

// WithGrowthThreshold sets Config.GrowthThreshold: the occupancy - live items over slots - a shard table grows at, in
// (0, 1). Anything outside it is rejected with ErrBadGrowthThreshold, except 0, which selects DefaultGrowthThreshold
// (5/6). Higher means fewer growths and less memory, at longer probe walks. A preallocated table never grows, so
// the option means nothing there.
func WithGrowthThreshold(threshold float64) Option {
	return Option{ApplyField: func(f *FieldOverrides) { f.growthThreshold = &threshold }}
}

// WithShardsCount sets Config.ShardsCount: the number of shards the first level is split into.
func WithShardsCount(shards int) Option {
	return Option{ApplyField: func(f *FieldOverrides) { f.shards = &shards }}
}

// WithL2Cache sets Config.L2Cache: the optional second level.
func WithL2Cache[K comparable, V any](l2 L2Cache[K, V]) Option {
	return Option{ApplyTyped: func(target any) error {
		return applyToConfig(target, func(cfg *Config[K, V]) { cfg.L2Cache = l2 })
	}}
}

// WithWritePolicy sets Config.WritePolicy: the L2Cache write policy.
func WithWritePolicy(writePolicy WritePolicy) Option {
	return Option{ApplyField: func(f *FieldOverrides) { f.writePolicy = &writePolicy }}
}

// WithGhostSize sets Config.GhostSize: the total capacity of the S3-FIFO ghost queues and of the W-TinyLFU
// frequency sketch (in keys).
func WithGhostSize(ghostSize int) Option {
	return Option{ApplyField: func(f *FieldOverrides) { f.ghostSize = &ghostSize }}
}

// WithWriteBackBuffer sets Config.WriteBackBufferSize: the buffer size of each WriteBack worker.
func WithWriteBackBuffer(size int) Option {
	return Option{ApplyField: func(f *FieldOverrides) { f.writeBackBufferSize = &size }}
}

// WithWriteBackWorkers sets Config.WriteBackWorkers: how many goroutines drain the WriteBack buffer.
func WithWriteBackWorkers(workers int) Option {
	return Option{ApplyField: func(f *FieldOverrides) { f.writeBackWorkers = &workers }}
}

// withWriteBackBatching builds the option for one WriteBackBatching mode.
func withWriteBackBatching(mode WriteBackBatching) Option {
	return Option{ApplyField: func(f *FieldOverrides) { f.writeBackBatching = &mode }}
}

// WithBatchingForWriteBack makes a WriteBack worker drain its buffer share in BatchSet batches (the default).
func WithBatchingForWriteBack() Option { return withWriteBackBatching(BatchingFull) }

// WithNoBatchingForWriteBack makes a WriteBack worker send one Set per write.
func WithNoBatchingForWriteBack() Option { return withWriteBackBatching(BatchingNone) }

// WithAdaptiveBatchingForWriteBack sends per-item Sets while the worker's buffer share is at most half full, BatchSet
// batches above.
func WithAdaptiveBatchingForWriteBack() Option { return withWriteBackBatching(BatchingAdaptive) }

// WithGetBatchedBuffer sets Config.GetBatchedBufferSize: the queue GetBatched coalesces reads through.
func WithGetBatchedBuffer(size int) Option {
	return Option{ApplyField: func(f *FieldOverrides) { f.getBatchedBufferSize = &size }}
}

// WithGetBatchedWorkers sets Config.GetBatchedWorkers: how many goroutines resolve the reads GetBatched coalesces.
func WithGetBatchedWorkers(workers int) Option {
	return Option{ApplyField: func(f *FieldOverrides) { f.getBatchedWorkers = &workers }}
}

// WithOnL2Error sets Config.OnL2Error: the handler for L2Cache errors that cannot be returned to the caller.
func WithOnL2Error[K comparable, V any](handler func(key K, err error)) Option {
	return Option{ApplyTyped: func(target any) error {
		return applyToConfig(target, func(cfg *Config[K, V]) { cfg.OnL2Error = handler })
	}}
}

// WithOnDeletion sets Config.OnDeletion: the handler for every item leaving the first level.
func WithOnDeletion[K comparable, V any](handler func(key K, value V, cause DeletionCause)) Option {
	return Option{ApplyTyped: func(target any) error {
		return applyToConfig(target, func(cfg *Config[K, V]) { cfg.OnDeletion = handler })
	}}
}

// WithPanicHandler sets Config.OnPanic: the handler for the panics the cache recovers on its own goroutines and
// around a background loader.
func WithPanicHandler(handler PanicHandler) Option {
	return Option{ApplyField: func(f *FieldOverrides) { f.onPanic = &handler }}
}

// WithStats sets Config.StatsEnabled: turns on the operation counters returned by Cache.Stats(). Off by default.
func WithStats() Option {
	return Option{ApplyField: func(f *FieldOverrides) {
		on := true
		f.statsEnabled = &on
	}}
}

// DetectMode says how an adapter decides whether an optional backend capability is there.
type DetectMode uint8

const (
	// AutoDetect probes the client and takes the answer from it. Default.
	AutoDetect DetectMode = iota
	// Disabled disables the capability.
	Disabled
	// Enabled asserts the capability without probing.
	Enabled
)
