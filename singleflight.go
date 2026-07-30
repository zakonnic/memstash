package memstash

import (
	"context"
	"math/bits"
	"sync"
	"sync/atomic"
)

// flightSlots is the inline capacity of one registry bucket. Eight pointers fill a cache line, and a bucket holding
// more than that has more loads in flight than the level behind it can absorb anyway.
const flightSlots = 8

// flightSpilled is the slot number of a call parked in a bucket's overflow map.
const flightSpilled = 0xFF

// flightCall is one singleflight flight. A flight nobody joined never synchronizes at all - it is released without a
// finish, and val, err and ok stay private to its owner. Once somebody joins, those three are written before over
// goes true and read after it, and the channel the waiter parks on is created on demand.
type flightCall[K comparable, V any] struct {
	key K
	val V
	err error

	// waiter is the parking spot the first waiter publishes; every later one reuses it.
	waiter atomic.Pointer[flightWaiter]
	over   atomic.Bool
	// at is where the registry parked this call: bucket in the high bits, slot in the low byte. Two bytes are
	// enough - Config.shardCount caps the bucket count at 128.
	at uint16
	ok bool
	// joined is set by a claim that handed this call out as the winner, and read by release under the same bucket
	// lock: a flight nobody joined skips finish entirely.
	joined bool
}

type flightWaiter struct{ ch chan struct{} }

// finish stores over before reading waiter, and wait publishes waiter before reading over. Go's atomics are
// sequentially consistent, so of those two pairs at least one side always sees the other - the finisher closes the
// channel, or the waiter never parks.
func (f *flightCall[K, V]) finish() {
	f.over.Store(true)
	if w := f.waiter.Load(); w != nil {
		close(w.ch)
	}
}

func (f *flightCall[K, V]) wait(ctx context.Context) error {
	if f.over.Load() {
		return nil
	}
	w := f.waiter.Load()
	if w == nil {
		fresh := &flightWaiter{ch: make(chan struct{})}
		if f.waiter.CompareAndSwap(nil, fresh) {
			w = fresh
		} else {
			w = f.waiter.Load()
		}
	}
	if f.over.Load() {
		return nil
	}
	select {
	case <-w.ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// flightBucket is one bucket of the in-flight registry: a fixed set of slots plus an overflow map for the rare bucket
// that fills up. Everything in it moves under mu, so a claim and a release each cost one uncontended lock and nothing
// else - the atomics a lock-free slot table would need are no cheaper than the lock itself.
type flightBucket[K comparable, V any] struct {
	mu     sync.Mutex
	live   uint8 // which slots are taken; usually one bit, so the scan below is a couple of iterations
	call   [flightSlots]*flightCall[K, V]
	hash   [flightSlots]uint64
	spill  map[K]*flightCall[K, V]
	nSpill int
	_      [32]byte // keep buckets off each other's cache lines
}

// claim registers call for key, or hands back the flight already running for it.
func (c *Cache[K, V]) claim(keyHash uint64, key K, call *flightCall[K, V]) (winner *flightCall[K, V], running bool) {
	bucket := uint32(keyHash) & c.shardMask
	b := &c.flights[bucket]

	b.mu.Lock()
	for m := b.live; m != 0; m &= m - 1 {
		i := bits.TrailingZeros8(m)
		if b.hash[i] == keyHash && b.call[i].key == key {
			winner = b.call[i]
			winner.joined = true
			b.mu.Unlock()
			return winner, true
		}
	}
	if b.nSpill > 0 {
		if p, ok := b.spill[key]; ok {
			p.joined = true
			b.mu.Unlock()
			return p, true
		}
	}
	call.key = key
	free := bits.TrailingZeros8(^b.live)
	if free == flightSlots {
		if b.spill == nil {
			b.spill = make(map[K]*flightCall[K, V])
		}
		b.spill[key] = call
		b.nSpill++
		call.at = uint16(bucket)<<8 | flightSpilled
		b.mu.Unlock()
		return call, false
	}
	b.hash[free] = keyHash
	b.call[free] = call
	b.live |= 1 << free
	call.at = uint16(bucket)<<8 | uint16(free)
	b.mu.Unlock()
	return call, false
}

// release drops the flight before waking its waiters, so the next call starts a fresh one. A claim can only join a
// call that is still registered, so reading joined under the same lock that unregisters it settles the question for
// good: false means nobody can ever wait on this call.
func (c *Cache[K, V]) release(call *flightCall[K, V]) {
	b := &c.flights[call.at>>8]
	b.mu.Lock()
	if slot := call.at & 0xFF; slot != flightSpilled {
		b.live &^= 1 << slot
		b.call[slot] = nil
	} else {
		delete(b.spill, call.key)
		b.nSpill--
	}
	joined := call.joined
	b.mu.Unlock()
	if joined {
		call.finish()
	}
}

// releaseAll is deferred by every batch load, so a panicking loader leaves no flight stuck.
func (c *Cache[K, V]) releaseAll(calls []flightCall[K, V]) {
	for i := range calls {
		c.release(&calls[i])
	}
}

type joinedFlight[K comparable, V any] struct {
	key  K
	call *flightCall[K, V]
}

// batchFlights is the singleflight state of one batch: the keys this call ended up owning, index-aligned with their
// flights, plus the flights it joined instead.
type batchFlights[K comparable, V any] struct {
	owned  []K
	calls  []flightCall[K, V]
	joined []joinedFlight[K, V]
	copied bool // owned no longer aliases the caller's batch
}

// claim registers keys[i], whose hash the caller already has. calls is one slab grown only within its initial
// capacity: the registry holds pointers into it. While every key so far is owned, owned is just a prefix of the
// caller's batch - a batch that misses everywhere (a cold cache) hands the loader that slice and allocates nothing
// for it.
func (fl *batchFlights[K, V]) claim(c *Cache[K, V], keys []K, i int, keyHash uint64) {
	key := keys[i]
	if fl.calls == nil {
		fl.calls = make([]flightCall[K, V], 0, len(keys)-i)
	}
	fl.calls = fl.calls[:len(fl.calls)+1]
	call := &fl.calls[len(fl.calls)-1]
	call.err = ErrLoaderPanic // stands until assignLoaded, so a panicking loader still resolves the flight
	if winner, running := c.claim(keyHash, key, call); running {
		fl.calls = fl.calls[:len(fl.calls)-1] // a joined key consumes no slot, so owned and calls stay index-aligned
		fl.joined = append(fl.joined, joinedFlight[K, V]{key: key, call: winner})
		return
	}
	if !fl.copied {
		if len(fl.owned) == i {
			fl.owned = keys[:i+1]
			return
		}
		fl.owned = append(make([]K, 0, len(keys)), fl.owned...)
		fl.copied = true
	}
	fl.owned = append(fl.owned, key)
}

// assignLoaded hands the result to the owned flights; a key the loader skipped carries err, nil when it was simply
// not found. Items this call does not own are skipped, and the owned ones are appended to found when it is non-nil.
func assignLoaded[K comparable, V any](owned []K, calls []flightCall[K, V], loaded List[K, V], err error, found *List[K, V]) {
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

func indexOf[K comparable](keys []K, key K) int {
	for i, k := range keys {
		if k == key {
			return i
		}
	}
	return -1
}
