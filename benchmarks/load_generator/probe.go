package load_generator

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zakonnic/memstash/tests/workload"
)

const (
	// probeMinOps is the smallest timed run, and all a fine-grained clock ever needs.
	probeMinOps = 100
	// probeMaxOps caps how much traffic one measurement may inject into the running scenario, however coarse the
	// clock turns out to be. A Windows tick is wide enough to reach this cap; a Linux one settles far below it.
	probeMaxOps = 1 << 17
	// probeKeys is how many distinct keys the timed loops walk. It has to be large: a probe cycling a handful of keys
	// would sit in CPU cache and report the cost of an empty table, not of a shard that has been evicting for hours. A
	// power of two, so the index is a mask - at a nanosecond per operation a division shows up in the result.
	probeKeys = 1 << 15
	// probeWindows is how many clock ticks a timed run must span, which bounds the quantisation error at 1/n.
	probeWindows = 10
)

// probeSink keeps the probe's reads from being optimized away.
var probeSink atomic.Int64

// clockTick is the smallest interval time.Now can resolve here: tens of nanoseconds on Linux, hundreds of
// microseconds on Windows. The probe sizes its runs against it, because on the coarse end probeMinOps calls of a
// sub-nanosecond read all land inside one tick and every figure reads as zero.
var clockTick = sync.OnceValue(func() time.Duration {
	tick := time.Second
	for range 16 {
		start := time.Now()
		var d time.Duration
		for d == 0 {
			d = time.Since(start)
		}
		if d < tick {
			tick = d
		}
	}
	return tick
})

// probe holds everything the timed loops touch. The keys are drawn and built up front: drawing costs an RNG call and
// building one formats a string, and either would swamp the operation being measured.
type probe[K comparable, V any] struct {
	setKeys   []K
	setValues []V
	// memKeys and getKeys each alternate a key the workload keeps hot with one above KeySpace that nothing ever
	// writes, so the reads split evenly between a hit and a miss - each on its own slot, spread across the table.
	// They hold different keys because sharing one set would let whichever measurement runs first pay for pulling
	// the slots into CPU cache and hand the other a warm table.
	memKeys []K
	getKeys []K

	// Op counts that filled the window last time. Starting from them keeps the steady state to a single timed run.
	setOps, getMemOps, getOps int
}

func newProbe[K comparable, V any](s Scenario[K, V]) *probe[K, V] {
	rng := workload.Random()
	writes := newZipf(rng, s.WriteKeySpace, s.ZipfS, s.ZipfV)
	draw := func() int {
		random := s.RandomPercent > 0 && rng.Intn(100) < s.RandomPercent
		return nextKey(rng, writes, s.WriteKeySpace, random)
	}

	p := &probe[K, V]{
		setKeys:   make([]K, probeKeys),
		setValues: make([]V, probeKeys),
		memKeys:   make([]K, probeKeys),
		getKeys:   make([]K, probeKeys),
		setOps:    probeMinOps,
		getMemOps: probeMinOps,
		getOps:    probeMinOps,
	}
	for i := range p.setKeys {
		key := s.Key(draw())
		p.setKeys[i], p.setValues[i] = key, s.Value(key)
	}
	// The two miss ranges start past KeySpace and past each other, so no key is shared between the reads.
	fillReadKeys(p.memKeys, s, draw, s.KeySpace+1)
	fillReadKeys(p.getKeys, s, draw, s.KeySpace+1+probeKeys)
	return p
}

func fillReadKeys[K comparable, V any](dst []K, s Scenario[K, V], draw func() int, missBase int) {
	for i := range dst {
		if i&1 == 1 {
			dst[i] = s.Key(missBase + i)
			continue
		}
		dst[i] = s.Key(draw())
	}
}

// measure runs op until the timed window is wide enough for the clock to resolve, and returns the mean nanoseconds
// per call along with the op count to start from next time. An operation slow enough to fill the window on its own -
// a Get that reaches L2 - stops the growth early, so a remote round trip never turns into a flood of them.
func measure(ops int, op func(n int) int) (float64, int) {
	window := probeWindows * clockTick()
	if ops < probeMinOps {
		ops = probeMinOps
	}
	for {
		start := time.Now()
		found := op(ops)
		elapsed := time.Since(start)
		probeSink.Add(int64(found))

		if elapsed < window && ops*2 <= probeMaxOps {
			ops *= 2
			continue
		}
		next := ops
		if elapsed > 4*window { // the operation got faster, or the run was preempted: come back down
			next = max(ops/2, probeMinOps)
		}
		return float64(elapsed.Nanoseconds()) / float64(ops), next
	}
}

// runProbe times Set, GetFromMemory and Get against the live cache and returns the mean nanoseconds of each. It runs
// on the monitor goroutine and draws its keys the way the workers do, so the figures carry the contention, the probe
// distances the scenario has shaped by then.
//
// Its operations go through the cache's own counters, and on a coarse clock they number in the hundreds of thousands:
// logStats cuts the probe's window out of every figure it takes from them.
func (r *runner[K, V]) runProbe(ctx context.Context) (getMemNs, getNs, setNs float64) {
	if r.cache == nil {
		return 0, 0, 0
	}
	if r.probe == nil {
		r.probe = newProbe(r.Scenario)
	}
	p := r.probe

	// Reads go first: the Sets below evict and rewrite, which would leave the slots the reads are about to walk
	// warmer and shallower than the workload left them.
	getMemNs, p.getMemOps = measure(p.getMemOps, func(n int) int {
		found := 0
		for i := range n {
			if _, ok := r.cache.GetFromMemory(p.memKeys[i&(probeKeys-1)]); ok {
				found++
			}
		}
		return found
	})

	getNs, p.getOps = measure(p.getOps, func(n int) int {
		found := 0
		for i := range n {
			if _, ok, _ := r.cache.Get(ctx, p.getKeys[i&(probeKeys-1)]); ok {
				found++
			}
		}
		return found
	})

	setNs, p.setOps = measure(p.setOps, func(n int) int {
		for i := range n {
			j := i & (probeKeys - 1)
			_ = r.cache.Set(ctx, p.setKeys[j], p.setValues[j]) // a failing L2 is the workload's business, not the probe's
		}
		return 0
	})

	return getMemNs, getNs, setNs
}
