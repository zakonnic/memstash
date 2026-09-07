//go:build growthsweep

// Growth-threshold sweep: fills a cache of MEMSTASH_CAP items with the given GrowthThreshold and reports what the
// filled table costs - achieved load, probe walks, TotalSize - together with the mean latency of a hit and of a miss.
// One configuration per process: the tables run to gigabytes, so the shell loop drives it.
//
//	MEMSTASH_KIND=uint64 MEMSTASH_GT=0.75 MEMSTASH_CAP=100000000 \
//	  go test -tags growthsweep -run TestGrowthSweep -v -timeout 2h .
package memstash

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/zakonnic/memstash/internal/itemstore"
)

const (
	gbHistMax = 256
	// gbProbes is how many keys each latency measurement walks: enough that the mean settles, short enough that a
	// configuration costs seconds rather than minutes.
	gbProbes = 5_000_000
	// gbWrites is how many keys each write measurement touches: enough for the mean, few enough (2% of the table) that
	// the table stays what the read measurements saw.
	gbWrites = 2_000_000
	// gbUUIDLen is the length of the textual UUID keys: 32 hex digits and 4 dashes.
	gbUUIDLen = 36
)

var gbSink uint64

// gbFacts is what one filled cache is worth reporting.
type gbFacts struct {
	live, slots             int
	chainSum                int64
	chainHist               []int
	hitWalk, missWalk       float64
	hitWalkP99, missWalkP99 int
	hitNs, missNs           float64
	batchNs                 float64
	overwriteNs, insertNs   float64
	deleteNs                float64
	totalSize               int64
	heapBytes               uint64
	fill                    time.Duration
}

func TestGrowthSweep(t *testing.T) {
	gt, err := strconv.ParseFloat(gbEnv("MEMSTASH_GT", "0.8333333"), 64)
	if err != nil {
		t.Fatalf("MEMSTASH_GT: %v", err)
	}
	capacity, err := strconv.ParseInt(gbEnv("MEMSTASH_CAP", "100000000"), 10, 64)
	if err != nil {
		t.Fatalf("MEMSTASH_CAP: %v", err)
	}
	kind := gbEnv("MEMSTASH_KIND", "uint64")
	if kind == "raw" {
		gbRunRaw(t)
		return
	}

	t.Logf("kind=%s threshold=%.4f capacity=%d GOMAXPROCS=%d", kind, gt, capacity, runtime.GOMAXPROCS(0))
	var facts gbFacts
	switch kind {
	case "uint64":
		facts = gbRunUint64(t, gt, capacity)
	case "string":
		facts = gbRunString(t, gt, capacity)
	default:
		t.Fatalf("MEMSTASH_KIND: want uint64 or string, got %q", kind)
	}
	gbPrint(t, kind, gt, facts)
}

func gbEnv(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// gbRunUint64 fills a uint64 -> uint64 cache and measures it.
func gbRunUint64(t *testing.T, gt float64, capacity int64) gbFacts {
	c, err := New[uint64, uint64](WithMemoryCapacity(capacity), WithGrowthThreshold(gt))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	key := func(i int64) uint64 { return uint64(i) * 0x9E3779B97F4A7C15 }
	fill := gbFill(int(capacity), func(i int) {
		_ = c.Set(context.Background(), key(int64(i)), uint64(i))
	})

	rng := rand.New(rand.NewPCG(1, 2))
	hits := make([]uint64, 0, gbProbes)
	for len(hits) < gbProbes {
		hits = append(hits, key(rng.Int64N(capacity)))
	}
	misses := make([]uint64, gbProbes)
	for i := range misses {
		misses[i] = key(capacity + rng.Int64N(capacity)) // past the filled range: never inserted
	}
	fresh := make([]uint64, gbWrites)
	for i := range fresh {
		fresh[i] = key(3*capacity + int64(i)) // a third range: inserted by the write measurement only
	}
	return gbMeasure(t, c, fill, hits, misses, fresh, uint64(7))
}

// gbRunString fills a uuid -> string cache and measures it. The keys are 36-byte UUIDs cut from one slab, so the
// 100M of them cost one allocation; the values are 10-20 byte windows into a shared blob, as the realistic speed
// benchmarks do - their bytes are identical across thresholds, so sharing them changes nothing being compared.
func gbRunString(t *testing.T, gt float64, capacity int64) gbFacts {
	c, err := New[string, string](WithMemoryCapacity(capacity), WithGrowthThreshold(gt))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// One slab for the keys the cache keeps, one for the miss probes.
	slab := make([]byte, gbUUIDLen*capacity)
	blob := make([]byte, 1<<20)
	for i := range blob {
		blob[i] = byte('a' + i%26)
	}
	keyAt := func(dst []byte, i int64) string {
		b := dst[gbUUIDLen*i : gbUUIDLen*i+gbUUIDLen : gbUUIDLen*i+gbUUIDLen]
		gbWriteUUID(b, uint64(i))
		return unsafe.String(&b[0], gbUUIDLen)
	}
	valueAt := func(i int64) string {
		size := 10 + int(uint64(i)%11)                            // 10..20 bytes
		off := int(uint64(i) * 0x9E3779B9 % uint64(len(blob)-32)) // spread over the blob
		return unsafe.String(&blob[off], size)
	}
	fill := gbFill(int(capacity), func(i int) {
		_ = c.Set(context.Background(), keyAt(slab, int64(i)), valueAt(int64(i)))
	})

	rng := rand.New(rand.NewPCG(1, 2))
	hits := make([]string, gbProbes)
	distinct := gbEnv("MEMSTASH_DISTINCT", "") != ""
	hitSlab := slab
	if distinct {
		hitSlab = make([]byte, gbUUIDLen*gbProbes) // separate bytes: the compare must read the stored key, as a client's would
	}
	for i := range hits {
		j := rng.Int64N(capacity)
		if distinct {
			b := hitSlab[gbUUIDLen*i : gbUUIDLen*i+gbUUIDLen : gbUUIDLen*i+gbUUIDLen]
			gbWriteUUID(b, uint64(j))
			hits[i] = unsafe.String(&b[0], gbUUIDLen)
			continue
		}
		hits[i] = keyAt(slab, j) // headers into the slab the cache already holds: the compare short-circuits on the pointer
	}
	missSlab := make([]byte, gbUUIDLen*gbProbes)
	misses := make([]string, gbProbes)
	for i := range misses {
		b := missSlab[gbUUIDLen*i : gbUUIDLen*i+gbUUIDLen : gbUUIDLen*i+gbUUIDLen]
		gbWriteUUID(b, uint64(capacity)+uint64(i)) // past the filled range: never inserted
		misses[i] = unsafe.String(&b[0], gbUUIDLen)
	}
	freshSlab := make([]byte, gbUUIDLen*gbWrites)
	fresh := make([]string, gbWrites)
	for i := range fresh {
		b := freshSlab[gbUUIDLen*i : gbUUIDLen*i+gbUUIDLen : gbUUIDLen*i+gbUUIDLen]
		gbWriteUUID(b, uint64(3*capacity)+uint64(i))
		fresh[i] = unsafe.String(&b[0], gbUUIDLen)
	}
	return gbMeasure(t, c, fill, hits, misses, fresh, "overwritten-value")
}

// gbFill runs set over [0, n) from GOMAXPROCS goroutines, interleaved so they spread over the shards.
func gbFill(n int, set func(i int)) time.Duration {
	workers := runtime.GOMAXPROCS(0)
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(from int) {
			defer wg.Done()
			for i := from; i < n; i += workers {
				set(i)
			}
		}(w)
	}
	wg.Wait()
	return time.Since(start)
}

// gbMeasure times a hit and a miss, then walks the tables. The hit set is filtered to the keys the cache still holds
// (the fill evicts a few near capacity), so the hit timing is all hits.
func gbMeasure[K comparable, V any](t *testing.T, c *Cache[K, V], fill time.Duration, hits, misses, fresh []K, value V) gbFacts {
	present := hits[:0]
	for _, k := range hits {
		if _, ok := c.GetFromMemory(k); ok {
			present = append(present, k)
		}
	}
	t.Logf("probe set: %d of %d sampled keys are still resident", len(present), len(hits))
	for _, k := range misses[:min(len(misses), 1000)] {
		if _, ok := c.GetFromMemory(k); ok {
			t.Fatalf("a miss probe key is in the cache")
		}
	}

	facts := gbFacts{fill: fill, chainHist: make([]int, gbHistMax+1)}
	facts.hitNs = gbLatency(c, present)
	facts.missNs = gbLatency(c, misses)
	facts.batchNs = gbBatchLatency(c, present)

	var hitHist, missHist [gbHistMax + 1]int
	hitSum, missSum := int64(0), int64(0)
	sample := min(len(present), 1_000_000)
	for _, k := range present[:sample] {
		probes, _ := gbWalk(c, k)
		hitSum += int64(probes)
		hitHist[min(probes, gbHistMax)]++
	}
	for _, k := range misses[:sample] {
		probes, _ := gbWalk(c, k)
		missSum += int64(probes)
		missHist[min(probes, gbHistMax)]++
	}
	facts.hitWalk = float64(hitSum) / float64(sample)
	facts.missWalk = float64(missSum) / float64(sample)
	facts.hitWalkP99 = gbPercentile(hitHist[:], sample, 0.99)
	facts.missWalkP99 = gbPercentile(missHist[:], sample, 0.99)

	gbScan(c, &facts)
	facts.totalSize = c.TotalSize()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	facts.heapBytes = ms.HeapAlloc

	// Writes last: they change the table the numbers above describe. One thread, so these are latencies too.
	ctx := context.Background()
	n := min(len(present)/2, len(fresh), gbWrites)
	start := time.Now()
	for _, k := range present[:n] {
		_ = c.Set(ctx, k, value)
	}
	facts.overwriteNs = float64(time.Since(start).Nanoseconds()) / float64(n)
	start = time.Now()
	for _, k := range fresh[:n] {
		_ = c.Set(ctx, k, value) // the cache is full: every insert evicts
	}
	facts.insertNs = float64(time.Since(start).Nanoseconds()) / float64(n)
	start = time.Now()
	for _, k := range present[n : 2*n] {
		_ = c.Delete(ctx, k)
	}
	facts.deleteNs = float64(time.Since(start).Nanoseconds()) / float64(n)
	return facts
}

// gbLatency is the mean nanoseconds of one GetFromMemory over the keys, after a warm-up pass.
func gbLatency[K comparable, V any](c *Cache[K, V], keys []K) float64 {
	if len(keys) == 0 {
		return 0
	}
	found := uint64(0)
	for _, k := range keys[:min(len(keys), 100_000)] {
		if _, ok := c.GetFromMemory(k); ok {
			found++
		}
	}
	start := time.Now()
	for _, k := range keys {
		if _, ok := c.GetFromMemory(k); ok {
			found++
		}
	}
	elapsed := time.Since(start)
	gbSink += found
	return float64(elapsed.Nanoseconds()) / float64(len(keys))
}

// gbWalk replays the lock-free probe and counts the slots it touches.
func gbWalk[K comparable, V any](c *Cache[K, V], key K) (probes int, hit bool) {
	keyHash := c.hasher.Hash(key)
	sh := &c.shards[uint32(keyHash)&c.shardMask]
	storage := sh.items.GetStorage()
	tagged := itemstore.TagOf(keyHash)
	home := storage.Home(keyHash)
	groupEnd := (home | 7) + 1
	for pos := home; ; pos++ {
		if pos == groupEnd && !storage.OverflowedTag(home, itemstore.OverflowBucket(keyHash)) {
			return probes, false
		}
		item := storage.At(pos)
		metaWord := item.Metadata()
		probes++
		if metaWord&itemstore.DeadOrTag == tagged && item.Entry().Key == key {
			return probes, true
		}
		if metaWord&itemstore.HasNext == 0 {
			return probes, false
		}
	}
}

// gbScan walks every slot of every shard: the live count, the slot count and how far each item sits from its home.
func gbScan[K comparable, V any](c *Cache[K, V], facts *gbFacts) {
	for i := range c.shards {
		storage := c.shards[i].items.GetStorage()
		facts.slots += storage.Len()
		for pos := 0; pos < storage.Len(); pos++ {
			item := storage.At(uint32(pos))
			metaWord := item.Metadata()
			if itemstore.Empty(metaWord) || metaWord&itemstore.Dead != 0 {
				continue
			}
			facts.live++
			home := storage.Home(c.hasher.Hash(item.Entry().Key))
			chain := int(storage.Dist(home, uint32(pos))) + 1
			facts.chainSum += int64(chain)
			facts.chainHist[min(chain, gbHistMax)]++
		}
	}
}

func gbPercentile(hist []int, total int, p float64) int {
	want, seen := int(float64(total)*p), 0
	for i, n := range hist {
		seen += n
		if seen >= want {
			return i
		}
	}
	return len(hist) - 1
}

func gbPrint(t *testing.T, kind string, gt float64, f gbFacts) {
	load := float64(f.live) / float64(f.slots)
	chain := float64(f.chainSum) / float64(max(f.live, 1))
	t.Logf("ROW %s %.4f live=%d slots=%d load=%.4f chain=%.3f chainP99=%d hitWalk=%.3f hitWalkP99=%d "+
		"missWalk=%.3f missWalkP99=%d hitNs=%.2f missNs=%.2f batchNs=%.2f overwriteNs=%.2f insertNs=%.2f deleteNs=%.2f "+
		"totalSize=%d heap=%d fillSec=%.1f",
		kind, gt, f.live, f.slots, load, chain, gbPercentile(f.chainHist, f.live, 0.99),
		f.hitWalk, f.hitWalkP99, f.missWalk, f.missWalkP99, f.hitNs, f.missNs, f.batchNs, f.overwriteNs, f.insertNs, f.deleteNs,
		f.totalSize, f.heapBytes, f.fill.Seconds())
	t.Logf("  TotalSize %s, heap %s, fill %s", gbBytes(f.totalSize), gbBytes(int64(f.heapBytes)),
		f.fill.Round(time.Millisecond))
}

func gbBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	}
	return fmt.Sprintf("%d B", n)
}

// gbWriteUUID formats a 36-byte textual UUID for n: two splitmix64 rounds give the 128 bits, laid out 8-4-4-4-12.
func gbWriteUUID(dst []byte, n uint64) {
	hi, lo := gbSplitMix(n), gbSplitMix(n^0x9E3779B97F4A7C15)
	const hex = "0123456789abcdef"
	pos := 0
	put := func(word uint64, digits int) {
		for i := digits - 1; i >= 0; i-- {
			if pos == 8 || pos == 13 || pos == 18 || pos == 23 {
				dst[pos] = '-'
				pos++
			}
			dst[pos] = hex[word>>(4*uint(i))&0xF]
			pos++
		}
	}
	put(hi, 16)
	put(lo, 16)
}

func gbSplitMix(x uint64) uint64 {
	x += 0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	return x ^ (x >> 31)
}

// gbRunRaw is the floor the cache is measured against: random reads over a plain array the size of the uint64 table
// at 100M (155M slots of 24 bytes), first independent - the loop the latency measurements use, where the CPU may
// overlap misses - then a dependent chain, where each read waits for the previous one: the memory latency itself,
// TLB walks included.
func gbRunRaw(t *testing.T) {
	const words = 155_189_248 * 3
	arr := make([]uint64, words)
	rng := rand.New(rand.NewPCG(3, 4))
	for i := range arr {
		arr[i] = rng.Uint64()
	}
	idx := make([]uint32, gbProbes)
	for i := range idx {
		idx[i] = uint32(rng.Int64N(words))
	}
	sum := uint64(0)
	start := time.Now()
	for _, i := range idx {
		sum += arr[i]
	}
	indep := float64(time.Since(start).Nanoseconds()) / gbProbes
	j := uint64(0)
	start = time.Now()
	for _, i := range idx {
		j = (arr[j] + uint64(i)) % words // the next address needs this read
	}
	dep := float64(time.Since(start).Nanoseconds()) / gbProbes
	gbSink += sum + j
	t.Logf("ROW raw floor independent=%.2f dependent=%.2f (ns per random 8-byte read over %s)", indep, dep,
		gbBytes(int64(words)*8))
}

// gbBatchLatency is the mean nanoseconds per key of BatchGetFromMemory over the keys in batches of 16: the same
// misses as gbLatency, issued in pipelined groups instead of one at a time.
func gbBatchLatency[K comparable, V any](c *Cache[K, V], keys []K) float64 {
	const batch = 16
	n := len(keys) - len(keys)%batch
	if n == 0 {
		return 0
	}
	dst := make(List[K, V], 0, batch)
	found := uint64(0)
	for i := 0; i < min(n, 100_000); i += batch {
		found += uint64(len(c.BatchGetFromMemory(keys[i:i+batch], dst[:0])))
	}
	start := time.Now()
	for i := 0; i < n; i += batch {
		found += uint64(len(c.BatchGetFromMemory(keys[i:i+batch], dst[:0])))
	}
	elapsed := time.Since(start)
	gbSink += found
	return float64(elapsed.Nanoseconds()) / float64(n)
}
