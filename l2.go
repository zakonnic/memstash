package memstash

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// L2Cache is the contract for a second-level adapter (Redis, memcached, a database, or a custom one). Implementations
// must be safe for concurrent use.
type L2Cache[K comparable, V any] interface {
	// Get returns the value and a presence flag. A missing key is (zero, false, nil), not an error.
	Get(ctx context.Context, key K) (value V, found bool, err error)
	// BatchGet returns the found subset of keys in one round trip where the backend supports it: only keys asked for,
	// each at most once. A missing key is absent from the result, not an error. Values returned alongside an
	// error are used: return only keys that were read. The batch methods report a failure confined to some keys as
	// KeyError values, joined, and must not modify the slice they are handed.
	BatchGet(ctx context.Context, keys []K) (List[K, V], error)
	// Set stores the value; ttl == 0 means "no expiration".
	Set(ctx context.Context, key K, value V, ttl time.Duration) error
	// BatchSet stores all items in one round trip where the backend supports it; ttl == 0 means "no expiration". A
	// key may repeat: the backend must end with its last value.
	BatchSet(ctx context.Context, items List[K, V], ttl time.Duration) error
	// Delete removes the key; a missing key is not treated as an error.
	Delete(ctx context.Context, key K) error
	// BatchDelete removes all keys in one round trip where the backend supports it; missing keys are not errors.
	BatchDelete(ctx context.Context, keys []K) error
}

// KeyError is an L2 failure confined to one key of a batch, such as a value the codec rejects. The cache reports and
// fails only the keys a batch error names this way; an error that names no key concerns them all. Key must be the K
// value the batch was called with.
type KeyError struct {
	Key any
	Err error
}

func (e *KeyError) Error() string { return fmt.Sprintf("memstash: key %v: %v", e.Key, e.Err) }

func (e *KeyError) Unwrap() error { return e.Err }

// keyErr is the part of a batch error that concerns key: the KeyError naming it and whatever names no key.
func keyErr[K comparable](err error, key K) error {
	switch e := err.(type) { //nolint:errorlint // only a bare KeyError names a key: errors.As would stop at the first

	case *KeyError:
		if e.Key != any(key) {
			return nil
		}
	case interface{ Unwrap() []error }:
		var parts []error
		for _, part := range e.Unwrap() {
			if part = keyErr(part, key); part != nil {
				parts = append(parts, part)
			}
		}
		if len(parts) == 1 {
			return parts[0]
		}
		return errors.Join(parts...)
	}
	return err
}

// Codec is the value serialization contract for networked L2 adapters.
type Codec[V any] interface {
	Marshal(value V) ([]byte, error)
	Unmarshal(data []byte) (V, error)
}

// WritePolicy defines how writes reach L2.
type WritePolicy uint8

const (
	// WriteBack writes to L2 asynchronously via a pool of background workers (the default). Close() waits for the
	// buffer to drain. Errors are delivered to Config.OnL2Error.
	WriteBack WritePolicy = iota
	// WriteThrough writes to L2 synchronously on Set.
	WriteThrough
	// WriteDisabled uses L2 for reads only.
	WriteDisabled
)

// WriteBackBatching defines how a write-back worker drains its queue into L2. A batch carries one TTL: writes with
// different TTLs (SetWithTTL, LoadFrom with LoadWithTTL) break a batch where their TTL changes.
type WriteBackBatching uint8

const (
	// BatchingFull coalesces the queued writes into BatchSet batches (the default).
	BatchingFull WriteBackBatching = iota
	// BatchingNone sends every write as its own Set.
	BatchingNone
	// BatchingAdaptive sends individual Sets until the worker's queue is half full, then switches to BatchSet.
	BatchingAdaptive
)

// l2Write is a task for a background write-back worker. A non-nil flush marks a Wait checkpoint instead of a write:
// a queue is FIFO, so by the time its worker reaches the marker every write enqueued into that queue before it has
// been handed to L2, and closing flush releases the waiter. del marks a delete (value and ttl are unused).
type l2Write[K comparable, V any] struct {
	key   K
	value V
	ttl   time.Duration // the cache's TTL, or SetWithTTL's
	flush chan<- struct{}
	del   bool
}
