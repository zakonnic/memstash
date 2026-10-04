// Package l2 holds the shared building blocks for the L2Cache adapters that live in the memstash/l2/* submodules:
// ready-made value codecs, cache-key to storage-key mapping, and TTL conversions for the Redis and memcached wire
// formats. It has no third-party dependencies - the client libraries are pulled in only by the adapter submodule you
// actually import.
package l2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zakonnic/memstash"
)

var (
	// ErrKeyFuncRequired is returned by adapter constructors when the key type is not string and no key function was
	// provided: networked stores address values by string keys, so non-string keys need an explicit mapping.
	ErrKeyFuncRequired = fmt.Errorf("%w: key type is not string - provide a key function", memstash.Error)
	// ErrNilClient is returned by adapter constructors when the client is nil.
	ErrNilClient = fmt.Errorf("%w: client must not be nil", memstash.Error)
	// ErrNilCodec is returned by adapter constructors when the codec is nil.
	ErrNilCodec = fmt.Errorf("%w: codec must not be nil", memstash.Error)
)

// ResolveKeyFunc returns the cache-key to storage-key mapping for an adapter: the custom function when given, the
// identity for string keys, ErrKeyFuncRequired otherwise.
func ResolveKeyFunc[K comparable](custom func(K) string) (func(K) string, error) {
	if custom != nil {
		return custom, nil
	}
	var zero K
	if _, ok := any(zero).(string); !ok {
		return nil, ErrKeyFuncRequired
	}
	return func(key K) string { return any(key).(string) }, nil
}

// PrefixedString returns a key function for string keys that namespaces every key with the prefix.
func PrefixedString(prefix string) func(string) string {
	return func(key string) string { return prefix + key }
}

// --- adapter constructor options ---

// keyFuncTarget is the option target the adapters fill via ExtractKeyFunc. keyFuncMarker distinguishes "a key-func
// target of foreign key type" (an error) from "not a key-func target at all" (for example the cache Config - skip),
// as required by the memstash.Option dispatch protocol.
type keyFuncTarget[K comparable] struct {
	keyFunc func(K) string
}

func (*keyFuncTarget[K]) isKeyFuncTarget() {}

type keyFuncMarker interface{ isKeyFuncTarget() }

// WithKeyFunc sets the cache-key to storage-key mapping used by an adapter. Without it keys must be strings (identity
// mapping); for any other key type the option is required. It is a regular memstash.Option, so it can be passed to
// the adapters' NewCache next to the cache options; the cache constructor itself ignores it.
func WithKeyFunc[K comparable](keyFunc func(K) string) memstash.Option {
	return memstash.Option{ApplyTyped: func(target any) error {
		typed, ok := target.(*keyFuncTarget[K])
		if !ok {
			if _, isKeyFunc := target.(keyFuncMarker); isKeyFunc {
				return memstash.ErrOptionMismatch // a key-func target, but of a different key type
			}
			return nil // some other package's target (for example the cache Config) - not ours to fill
		}
		typed.keyFunc = keyFunc
		return nil
	}}
}

// ExtractKeyFunc extracts the WithKeyFunc option from the option list (foreign options are ignored) and resolves the
// key function (see ResolveKeyFunc).
func ExtractKeyFunc[K comparable](opts []memstash.Option) (func(K) string, error) {
	var target keyFuncTarget[K]
	for _, opt := range opts {
		if opt.ApplyTyped == nil {
			continue
		}
		if err := opt.ApplyTyped(&target); err != nil {
			return nil, err
		}
	}
	return ResolveKeyFunc(target.keyFunc)
}

// orderedMgetTarget is the option target the adapters fill via ExtractOrderedMget.
type orderedMgetTarget struct {
	mode memstash.DetectMode
}

// WithOrderedMget tells a Redis-family adapter that MGET hands its replies back in the order the keys were sent, so
// BatchGet can read the reply array positionally instead of going through the key-to-reply map its client's portable
// multi-get helper builds. Needs a single-shard setup. Adapters resolve that through their own IsOrderedMgetAvailable.
func WithOrderedMget(mode memstash.DetectMode) memstash.Option {
	return memstash.Option{ApplyTyped: func(target any) error {
		if typed, ok := target.(*orderedMgetTarget); ok {
			typed.mode = mode
		}
		return nil // some other package's target (for example the cache Config) - not ours to fill
	}}
}

// ExtractOrderedMget extracts the WithOrderedMget option from the option list (foreign options are ignored),
// defaulting to memstash.AutoDetect.
func ExtractOrderedMget(opts []memstash.Option) (memstash.DetectMode, error) {
	var target orderedMgetTarget
	for _, opt := range opts {
		if opt.ApplyTyped == nil {
			continue
		}
		if err := opt.ApplyTyped(&target); err != nil {
			return target.mode, err
		}
	}
	return target.mode, nil
}

// ResolveOrderedMget turns the mode into the flag an adapter stores, calling probe only for memstash.AutoDetect.
func ResolveOrderedMget(mode memstash.DetectMode, probe func() bool) bool {
	switch mode {
	case memstash.Enabled:
		return true
	case memstash.Disabled:
		return false
	default:
		return probe()
	}
}

// --- sequential batch fallbacks ---

// BatchGetSequential implements L2Cache.BatchGet for backends without a native multi-get by looping over Get. The
// partial result gathered so far is returned alongside the first error.
func BatchGetSequential[K comparable, V any](ctx context.Context, store memstash.L2Cache[K, V], keys []K) (memstash.List[K, V], error) {
	found := make(memstash.List[K, V], 0, len(keys))
	for _, key := range keys {
		value, ok, err := store.Get(ctx, key)
		if err != nil {
			return found, err
		}
		if ok {
			found = append(found, memstash.KeyVal[K, V]{Key: key, Value: value})
		}
	}
	return found, nil
}

// BatchSetSequential implements L2Cache.BatchSet for backends without a native multi-set by looping over Set. A
// failed item does not stop the rest, which belong to unrelated keys; the failures come back joined.
func BatchSetSequential[K comparable, V any](ctx context.Context, store memstash.L2Cache[K, V], items memstash.List[K, V], ttl time.Duration) error {
	return runSequential(ctx, len(items), func(i int) error {
		return store.Set(ctx, items[i].Key, items[i].Value, ttl)
	})
}

// BatchDeleteSequential implements L2Cache.BatchDelete for backends without a native multi-delete by looping over
// Delete. A failed key does not stop the rest; the failures come back joined.
func BatchDeleteSequential[K comparable, V any](ctx context.Context, store memstash.L2Cache[K, V], keys []K) error {
	return runSequential(ctx, len(keys), func(i int) error {
		return store.Delete(ctx, keys[i])
	})
}

// runSequential runs op for the indexes 0..n-1 in order and joins what failed; a canceled ctx ends the run.
func runSequential(ctx context.Context, n int, op func(i int) error) error {
	var errs []error
	for i := range n {
		if err := op(i); err != nil {
			errs = append(errs, err)
			if ctx.Err() != nil {
				break
			}
		}
	}
	return errors.Join(errs...)
}

// --- concurrent batch fallbacks ---

// BatchGetConcurrent implements L2Cache.BatchGet by running Get on up to workers goroutines. Found items keep the
// order of keys; the hits gathered so far are returned alongside the first error.
func BatchGetConcurrent[K comparable, V any](ctx context.Context, store memstash.L2Cache[K, V], keys []K, workers int) (memstash.List[K, V], error) {
	if len(keys) <= 1 || workers <= 1 {
		return BatchGetSequential(ctx, store, keys)
	}
	type slot struct {
		value V
		ok    bool
	}
	results := make([]slot, len(keys))
	firstErr := runConcurrent(ctx, len(keys), workers, true, func(i int) error {
		value, ok, err := store.Get(ctx, keys[i])
		if err != nil {
			return err
		}
		results[i] = slot{value: value, ok: ok}
		return nil
	})
	found := make(memstash.List[K, V], 0, len(keys))
	for i, result := range results {
		if result.ok {
			found = append(found, memstash.KeyVal[K, V]{Key: keys[i], Value: result.value})
		}
	}
	return found, firstErr
}

// BatchSetConcurrent implements L2Cache.BatchSet by running Set on up to workers goroutines. A repeated key is
// written once, with its last value (see LastValues). A failed item does not stop the rest; the failures come back
// joined.
func BatchSetConcurrent[K comparable, V any](ctx context.Context, store memstash.L2Cache[K, V], items memstash.List[K, V], ttl time.Duration, workers int) error {
	items = LastValues(items)
	if len(items) <= 1 || workers <= 1 {
		return BatchSetSequential(ctx, store, items, ttl)
	}
	return runConcurrent(ctx, len(items), workers, false, func(i int) error {
		return store.Set(ctx, items[i].Key, items[i].Value, ttl)
	})
}

// BatchDeleteConcurrent implements L2Cache.BatchDelete by running Delete on up to workers goroutines. A failed key
// does not stop the rest; the failures come back joined.
func BatchDeleteConcurrent[K comparable, V any](ctx context.Context, store memstash.L2Cache[K, V], keys []K, workers int) error {
	if len(keys) <= 1 || workers <= 1 {
		return BatchDeleteSequential(ctx, store, keys)
	}
	return runConcurrent(ctx, len(keys), workers, false, func(i int) error {
		return store.Delete(ctx, keys[i])
	})
}

// LastValues collapses every repeated key of a batch into its first item, carrying the last value. A backend that
// writes a batch's items in no fixed order needs it to end with each key's last value, as L2Cache.BatchSet requires.
func LastValues[K comparable, V any](items memstash.List[K, V]) memstash.List[K, V] {
	if len(items) <= 1 {
		return items
	}
	at := make(map[K]int, len(items))
	var deduped memstash.List[K, V] // nil until the first repeat: most batches have none
	for n, item := range items {
		i, seen := at[item.Key]
		switch {
		case seen && deduped == nil:
			deduped = append(make(memstash.List[K, V], 0, len(items)), items[:n]...)
			fallthrough
		case seen:
			deduped[i].Value = item.Value
		case deduped != nil:
			at[item.Key] = len(deduped)
			deduped = append(deduped, item)
		default:
			at[item.Key] = n
		}
	}
	if deduped == nil {
		return items
	}
	return deduped
}

// runConcurrent executes op for the indexes 0..n-1 on up to workers goroutines. With firstErr it stops starting new
// indexes after the first error and returns it; otherwise every index runs and the failures come back joined, a
// canceled ctx alone ending the run early. A panicking op counts as a failed one.
func runConcurrent(ctx context.Context, n, workers int, firstErr bool, op func(i int) error) error {
	var (
		next    atomic.Int64
		stopped atomic.Bool
		mu      sync.Mutex
		errs    []error
		wg      sync.WaitGroup
	)
	wg.Add(min(workers, n))
	for range min(workers, n) {
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= n || stopped.Load() {
					return
				}
				if err := runGuarded(op, i); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					if firstErr || ctx.Err() != nil {
						stopped.Store(true)
					}
				}
			}
		}()
	}
	wg.Wait()
	if firstErr && len(errs) > 0 {
		return errs[0]
	}
	return errors.Join(errs...)
}

// runGuarded runs op(i), turning a panic into an ErrPanic error.
func runGuarded(op func(i int) error, i int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", memstash.ErrPanic, r)
		}
	}()
	return op(i)
}

// --- codecs ---

// JSONCodec returns a Codec that marshals values with encoding/json.
func JSONCodec[V any]() memstash.Codec[V] { return jsonCodec[V]{} }

type jsonCodec[V any] struct{}

func (jsonCodec[V]) Marshal(value V) ([]byte, error) { return json.Marshal(value) }

func (jsonCodec[V]) Unmarshal(data []byte) (V, error) {
	var value V
	err := json.Unmarshal(data, &value)
	return value, err
}

// BytesCodec returns a pass-through Codec for []byte values.
func BytesCodec() memstash.Codec[[]byte] { return bytesCodec{} }

type bytesCodec struct{}

func (bytesCodec) Marshal(value []byte) ([]byte, error)  { return value, nil }
func (bytesCodec) Unmarshal(data []byte) ([]byte, error) { return data, nil }

// StringCodec returns a Codec for string values.
func StringCodec() memstash.Codec[string] { return stringCodec{} }

type stringCodec struct{}

func (stringCodec) Marshal(value string) ([]byte, error)  { return []byte(value), nil }
func (stringCodec) Unmarshal(data []byte) (string, error) { return string(data), nil }

// --- TTL conversions ---

// memcachedRelativeMax is the largest expiration value memcached treats as a relative number of seconds (30 days);
// anything larger is interpreted by the server as an absolute unix timestamp.
const memcachedRelativeMax = 30 * 24 * 60 * 60

// MemcacheExpiration converts a TTL to the memcached expiration field: 0 stays "no expiration", sub-second TTLs are
// rounded up to one second (the protocol resolution), and TTLs beyond 30 days are encoded as an absolute unix
// timestamp as the protocol requires (clamped to the int32 horizon, year 2038).
func MemcacheExpiration(ttl time.Duration) int32 {
	if ttl <= 0 {
		return 0
	}
	secs := int64(ttl / time.Second) // rounded up below; ttl+time.Second-1 would overflow near math.MaxInt64
	if ttl%time.Second > 0 {
		secs++
	}
	if secs <= memcachedRelativeMax {
		return int32(secs)
	}
	deadline := time.Now().Unix() + secs
	if deadline > math.MaxInt32 {
		deadline = math.MaxInt32
	}
	return int32(deadline)
}

// MemcacheKey is key if memcached accepts it - at most 250 bytes, no space, control character or DEL - and otherwise
// a stand-in that it does, derived from key's SHA-256. The memcached adapters store every key through it, so they
// agree on the stand-in and any key reads back.
func MemcacheKey(key string) string {
	if len(key) <= memcachedKeyMax && !strings.ContainsFunc(key, func(r rune) bool { return r <= ' ' || r == 0x7f }) {
		return key
	}
	sum := sha256.Sum256([]byte(key))
	return memcachedHashedPrefix + hex.EncodeToString(sum[:])
}

// memcachedKeyMax is the longest key memcached accepts; a stand-in is prefixed so it never reads as a raw key.
const (
	memcachedKeyMax       = 250
	memcachedHashedPrefix = "memstash:sha256:"
)

// ExpiresAt is the unix second an item written now with ttl expires at, for stores that keep a deadline in whole
// seconds; 0 for ttl <= 0, "no expiration". Rounded up: an item never expires before its ttl is out.
func ExpiresAt(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	deadline := time.Now().Add(ttl)
	if deadline.Nanosecond() > 0 {
		return deadline.Unix() + 1
	}
	return deadline.Unix()
}

// RedisMillis converts a TTL to whole milliseconds for the PX argument of SET, rounding sub-millisecond TTLs up to 1
// so a tiny but non-zero TTL is never mistaken for "no expiration".
func RedisMillis(ttl time.Duration) int64 {
	millis := int64(ttl / time.Millisecond) // rounded up below: ttl+time.Millisecond-1 could overflow
	if ttl%time.Millisecond > 0 {
		millis++
	}
	return max(millis, 1)
}
