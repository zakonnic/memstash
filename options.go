package memstash

import (
	"errors"
	"time"
)

// ErrOptionMismatch is returned by a constructor when an option carrying key/value types (WithCostFunc, WithL2Cache,
// WithOnL2Error, the adapters' WithKeyFunc) was built for key/value types different from the cache's.
var ErrOptionMismatch = errors.New("memstash: option type arguments do not match the cache's key/value types")

// Option configures a cache under construction; build one with the With* helpers. Option deliberately carries no type
// parameters, so the key/value types are written once on the constructor (New[K, V](...)) instead of on every option;
// the price is that options which do mention K and V are matched against the cache at run time (ErrOptionMismatch).
//
// The fields are exported so that satellite packages (the L2 adapters) can define their own options of the same type
// and accept one flat option list. An ApplyTyped implementation is handed arbitrary targets and must follow the
// dispatch protocol: silently ignore a target kind it does not recognize (return nil), fill the target when it
// matches exactly, and return ErrOptionMismatch when the target kind is recognized but its key/value types differ.
type Option struct {
	// ApplyField applies a plain-value per-field option.
	ApplyField func(f *FieldOverrides)
	// ApplyTyped applies a per-field option that mentions K/V to a target (for example *Config[K, V]).
	ApplyTyped func(target any) error
}

// FieldOverrides accumulates the plain-value per-field options; a nil pointer means "keep the base value". It exists
// so those options do not have to be generic: they cannot write into Config[K, V] directly without knowing K and V.
type FieldOverrides struct {
	memoryCapacity  *int64
	ttl             *time.Duration
	policy          *Policy
	shards          *int
	writePolicy     *WritePolicy
	ghostSize       *int
	writeBackBuffer *int
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
	if fields.ttl != nil {
		cfg.TTL = *fields.ttl
	}
	if fields.policy != nil {
		cfg.Policy = *fields.policy
	}
	if fields.shards != nil {
		cfg.Shards = *fields.shards
	}
	if fields.writePolicy != nil {
		cfg.WritePolicy = *fields.writePolicy
	}
	if fields.ghostSize != nil {
		cfg.GhostSize = *fields.ghostSize
	}
	if fields.writeBackBuffer != nil {
		cfg.WriteBackBuffer = *fields.writeBackBuffer
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

// WithPolicy sets Config.Policy: the eviction policy.
func WithPolicy(policy Policy) Option {
	return Option{ApplyField: func(f *FieldOverrides) { f.policy = &policy }}
}

// WithShards sets Config.Shards: the number of shards the eviction state is split into.
func WithShards(shards int) Option {
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

// WithGhostSize sets Config.GhostSize: the total capacity of the S3-FIFO ghost queues (in keys).
func WithGhostSize(ghostSize int) Option {
	return Option{ApplyField: func(f *FieldOverrides) { f.ghostSize = &ghostSize }}
}

// WithWriteBackBuffer sets Config.WriteBackBuffer: the buffer size of the background WriteBack worker.
func WithWriteBackBuffer(size int) Option {
	return Option{ApplyField: func(f *FieldOverrides) { f.writeBackBuffer = &size }}
}

// WithOnL2Error sets Config.OnL2Error: the handler for L2Cache errors that cannot be returned to the caller.
func WithOnL2Error[K comparable, V any](handler func(key K, err error)) Option {
	return Option{ApplyTyped: func(target any) error {
		return applyToConfig(target, func(cfg *Config[K, V]) { cfg.OnL2Error = handler })
	}}
}
