// Package l2 holds the shared building blocks for the L2Cache adapters that live in the memstash/l2/* submodules:
// ready-made value codecs, cache-key to storage-key mapping, and TTL conversions for the Redis and memcached wire
// formats. It has no third-party dependencies - the client libraries are pulled in only by the adapter submodule you
// actually import.
package l2

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/zakonnic/memstash"
)

var (
	// ErrKeyFuncRequired is returned by adapter constructors when the key type is not string and no key function was
	// provided: networked stores address values by string keys, so non-string keys need an explicit mapping.
	ErrKeyFuncRequired = errors.New("memstash/l2: key type is not string - provide a key function")
	// ErrNilClient is returned by adapter constructors when the client is nil.
	ErrNilClient = errors.New("memstash/l2: client must not be nil")
	// ErrNilCodec is returned by adapter constructors when the codec is nil.
	ErrNilCodec = errors.New("memstash/l2: codec must not be nil")
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

// keyFuncTarget is the option target the adapters fill via ResolveOptions. keyFuncMarker distinguishes "a key-func
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

// ResolveOptions extracts the WithKeyFunc option from the option list (foreign options are ignored) and resolves the
// key function (see ResolveKeyFunc).
func ResolveOptions[K comparable](opts []memstash.Option) (func(K) string, error) {
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

// BatchSetSequential implements L2Cache.BatchSet for backends without a native multi-set by looping over Set.
func BatchSetSequential[K comparable, V any](ctx context.Context, store memstash.L2Cache[K, V], items memstash.List[K, V], ttl time.Duration) error {
	for _, item := range items {
		if err := store.Set(ctx, item.Key, item.Value, ttl); err != nil {
			return err
		}
	}
	return nil
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
	secs := int64((ttl + time.Second - 1) / time.Second)
	if secs <= memcachedRelativeMax {
		return int32(secs)
	}
	deadline := time.Now().Unix() + secs
	if deadline > math.MaxInt32 {
		deadline = math.MaxInt32
	}
	return int32(deadline)
}

// RedisMillis converts a TTL to whole milliseconds for the PX argument of SET, rounding sub-millisecond TTLs up to 1
// so a tiny but non-zero TTL is never mistaken for "no expiration".
func RedisMillis(ttl time.Duration) int64 {
	millis := int64((ttl + time.Millisecond - 1) / time.Millisecond)
	if millis < 1 {
		return 1
	}
	return millis
}
