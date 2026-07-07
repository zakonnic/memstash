package memstash

import (
	"context"
	"time"
)

// L2Cache is the contract for a second-level adapter (Redis, memcached, a database, or a custom one). Implementations
// must be thread-safe.
type L2Cache[K comparable, V any] interface {
	// Get returns the value and a presence flag. A missing key is (zero, false, nil), not an error.
	Get(ctx context.Context, key K) (value V, found bool, err error)
	// BatchGet returns the found subset of keys in one round trip where the backend supports it. A missing key is
	// simply absent from the result, not an error.
	BatchGet(ctx context.Context, keys []K) (List[K, V], error)
	// Set stores the value; ttl == 0 means "no expiration".
	Set(ctx context.Context, key K, value V, ttl time.Duration) error
	// BatchSet stores all items in one round trip where the backend supports it; ttl == 0 means "no expiration".
	BatchSet(ctx context.Context, items List[K, V], ttl time.Duration) error
	// Delete removes the key; a missing key is not treated as an error.
	Delete(ctx context.Context, key K) error
}

// Codec is the value serialization contract for networked L2 adapters.
type Codec[V any] interface {
	Marshal(value V) ([]byte, error)
	Unmarshal(data []byte) (V, error)
}

// WritePolicy defines how writes reach L2.
type WritePolicy uint8

const (
	// WriteBack writes to L2 asynchronously via a background worker (the default). Close() waits for the buffer to drain. Errors are
	// delivered to Config.OnL2Error.
	WriteBack WritePolicy = iota
	// WriteThrough writes to L2 synchronously on Set.
	WriteThrough
	// WriteDisabled uses L2 for reads only.
	WriteDisabled
)

// l2Write is a task for the background write-back worker. A non-nil flush marks a Wait checkpoint instead of a write:
// the channel is FIFO, so by the time the worker reaches the marker every write enqueued before it has been handed to
// L2, and closing flush releases the waiter.
type l2Write[K comparable, V any] struct {
	key   K
	value V
	flush chan<- struct{}
}
