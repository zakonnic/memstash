package memstash

import (
	"context"
	"hash/maphash"
	"sync"
)

// flightCall is one singleflight flight. Its channel is created only once somebody actually waits, so an uncontended
// load - the common case - allocates none: val, err and ok are published strictly before finish.
type flightCall[V any] struct {
	mu   sync.Mutex
	done chan struct{}
	val  V
	err  error
	ok   bool
	over bool
}

func (f *flightCall[V]) finish() {
	f.mu.Lock()
	f.over = true
	done := f.done
	f.mu.Unlock()
	if done != nil {
		close(done)
	}
}

func (f *flightCall[V]) wait(ctx context.Context) error {
	f.mu.Lock()
	if f.over {
		f.mu.Unlock()
		return nil
	}
	if f.done == nil {
		f.done = make(chan struct{})
	}
	done := f.done
	f.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// flightShard is one stripe of the in-flight registry. A plain map beats a concurrent one for this traffic: entries
// arrive and leave in pairs, and a map that has grown once serves them without allocating again.
type flightShard[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]*flightCall[V]
	_  [48]byte // keep stripes off each other's cache line
}

func (c *Cache[K, V]) flightShard(key K) *flightShard[K, V] {
	return &c.flights[uint32(maphash.Comparable(c.seed, key))&c.shardMask]
}

// claim registers call for key, or hands back the flight already running for it.
func (c *Cache[K, V]) claim(key K, call *flightCall[V]) (winner *flightCall[V], running bool) {
	fs := c.flightShard(key)
	fs.mu.Lock()
	winner, running = fs.m[key]
	if !running {
		fs.m[key] = call
	}
	fs.mu.Unlock()
	return winner, running
}

// release drops the flight before waking its waiters, so the next call starts a fresh one.
func (c *Cache[K, V]) release(key K, call *flightCall[V]) {
	fs := c.flightShard(key)
	fs.mu.Lock()
	delete(fs.m, key)
	fs.mu.Unlock()
	call.finish()
}

// claimBatch starts a flight for every key. The owned subset is filtered into keys' own array, so the caller must own
// it; calls is index-aligned with owned. One slab backs every call.
func (c *Cache[K, V]) claimBatch(keys []K) (owned []K, calls []flightCall[V], joined []joinedFlight[K, V]) {
	calls = make([]flightCall[V], len(keys))
	owned, n := keys[:0], 0
	for _, key := range keys {
		call := &calls[n]
		call.err = ErrLoaderPanic // stands until assignLoaded, so a panicking loader still resolves the flight
		if winner, running := c.claim(key, call); running {
			joined = append(joined, joinedFlight[K, V]{key: key, call: winner})
			continue // a joined key consumes no slot, so owned and calls stay index-aligned
		}
		owned = append(owned, key)
		n++
	}
	return owned, calls[:n], joined
}

// assignLoaded hands the result to the owned flights; a key the loader skipped carries err, nil when it was simply
// not found. Items this call does not own are skipped, and the owned ones are appended to found when it is non-nil.
func assignLoaded[K comparable, V any](owned []K, calls []flightCall[V], loaded List[K, V], err error, found *List[K, V]) {
	for i, item := range loaded {
		j := i
		if j >= len(owned) || owned[j] != item.Key { // loaders answer in order unless they drop or add keys
			if j = indexOf(owned, item.Key); j < 0 {
				continue
			}
		}
		calls[j].val, calls[j].ok, calls[j].err = item.Value, true, nil
		if found != nil {
			*found = append(*found, item)
		}
	}
	for i := range calls {
		if !calls[i].ok {
			calls[i].err = err
		}
	}
}

// releaseAll is deferred by every batch load, so a panicking loader leaves no flight stuck.
func (c *Cache[K, V]) releaseAll(owned []K, calls []flightCall[V]) {
	for i := range calls {
		c.release(owned[i], &calls[i])
	}
}

func indexOf[K comparable](keys []K, key K) int {
	for i, k := range keys {
		if k == key {
			return i
		}
	}
	return -1
}
