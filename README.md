# Memstash

**A blazing-fast, allocation-free, two-level cache for Go. Yet it's convenient and configurable.**

Memstash keeps your hot set in a lock-free in-memory tier (L1). When you need to share state across machines, or just survive a restart, plug in a second tier (L2) backed by Redis, memcached, or any store you wrap. The memory path stays allocation-free and lock-free: a hit is a single map lookup plus one atomic read. On a miss, memstash fetches from L2, promotes the value into memory, and takes care of writing it back.

```go
c, _ := memstash.New[string, string]()
_ = c.Set(ctx, "hello", "world")
v, ok, err := c.Get(ctx, "hello") // faster than getting from sync.Map
```

## Why memstash?

- **Very fast.** Outperforms [Ristretto](https://github.com/dgraph-io/ristretto) by ~6× and [Otter](https://github.com/maypok86/otter) by ~2× in our [benchmarks](#benchmarks).
- **Top-tier hit ratio.** The S3-FIFO policy keeps pace with the best W-TinyLFU caches (Otter, [Theine](https://github.com/Yiling-J/theine-go)) and leaves Ristretto far behind, holding up especially well under scans and one-hit wonders.
- **Lowest memory overhead.** 1.8× smaller footprint than Otter or [Bigcache](https://github.com/allegro/bigcache), see [benchmarks](#heap-footprint-lower-is-better). Less overhead means more keys.
- **Easy on the GC.** Inserts reuse memory blocks from a pool, so the steady state allocates nothing beyond the internal map entry.
- **Generic and type-safe.** `Cache[K, V]` works with any `comparable` key and any value. No `interface{}`, no casts.
- **Second-level cache out of the box.** Add an L2 (write-through or write-back), and after a restart or on a cold node, it reads from the shared tier instead of your database.
- **Adapters included.** Ready-made L2 adapters for Redis, memcached, SQL/PostgreSQL, MongoDB, DynamoDB, Badger, Tarantool and Aerospike - each in its own module so the core stays clean. With **write-back** and **auto-batching**.
- **Singleflight built in.** `GetOrLoad` collapses a stampede of concurrent misses on one key into a single load.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
  - [In-memory cache](#in-memory-cache)
  - [Read-through with a loader (singleflight)](#read-through-with-a-loader-singleflight)
  - [Two-level cache with Redis](#two-level-cache-with-redis)
- [Advanced Configuration](#advanced-configuration)
- [L2 Adapters](#l2-adapters)
- [Benchmarks](#benchmarks)

## Installation

```sh
go get github.com/zakonnic/memstash
```

Memstash requires Go 1.24+ and has a single core dependency ([xsync](https://github.com/puzpuzpuz/xsync)). Client SDKs are pulled in only by the specific L2 adapter module you import.

## Usage

### In-memory cache

The simplest setup: a bounded, in-process cache with no external dependencies.

```go
package main

import (
	"context"
	"fmt"

	"github.com/zakonnic/memstash"
)

func main() {
	ctx := context.Background()

	// Capacity is measured in weight units; without a cost function every item
	// weighs 1, so this holds 100k entries. An unconfigured cache defaults to 20k.
	c, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(100_000),
	)
	if err != nil {
		panic(err)
	}

	_ = c.Set(ctx, "greeting", "hello")
	if v, ok := c.GetFromMemory("greeting"); ok { // fastest read path: no locks, no allocations
		fmt.Println(v) // hello
	}
}
```

> `GetFromMemory` is the hottest, context-free read path. Use `Get` (which takes a `context.Context`) when an L2 is configured - it may hit the network on a memory miss.

### Read-through with a loader (singleflight)

The most common caching pattern: on a miss, load from the source of truth. Concurrent misses on the same key are automatically **coalesced into a single load**.

```go
c, _ := memstash.New[string, User](
	memstash.WithTTL(5*time.Minute),
)

user, err := c.GetOrLoad(ctx, "user:42", func(ctx context.Context, key string) (User, error) {
	return db.FindUser(ctx, key) // runs once even under a stampede
})
```

Prefer to fix the loader once at construction time? Use `NewLoadable`:

```go
lc, _ := memstash.NewLoadable(
	func(ctx context.Context, id string) (User, error) { return db.FindUser(ctx, id) },
)
user, err := lc.GetOrLoad(ctx, "user:42")
```

> Supports batch-loading with `NewBatchLoadable`, `BatchGetOrLoad`, etc.

### Two-level cache with Redis

Add a shared L2 in one call. Memory serves the hot set; anything evicted from L1 (or missing after a restart) is fetched from Redis and promoted back into memory. Writes are **write-back by default**: `Set` returns immediately and a background worker flushes to Redis. Single **Sets are grouped into batches** asynchronously. The example uses rueidis, but every client in the [adapters table](#l2-adapters) works the same way.

```go
import (
	"github.com/redis/rueidis"

	"github.com/zakonnic/memstash"
	rueidis_adapter "github.com/zakonnic/memstash/l2/rueidis_adapter"
)

client, _ := rueidis.NewClient(rueidis.ClientOption{
	InitAddress: []string{"127.0.0.1:6379"},
})

// JSON values, string keys, 10-minute TTL applied to both tiers (L1 uses the default capacity).
c, _ := rueidis_adapter.NewJSONCache[string, User](client, memstash.WithTTL(10*time.Minute))
defer c.Close()

_ = c.Set(ctx, "user:42", user)     // L1 now, Redis shortly after (write-back)
u, ok, err := c.Get(ctx, "user:42") // L1 hit → returns instantly; L1 miss → Redis, then promoted
```

> Tip: A common way to shard local caches without overlap is to key them by the Kafka partition - each partition is consumed by exactly one node, so the cache for a given object lives only on that node.

## Advanced Configuration

Memstash is configured with functional options passed to `New` (or to any adapter's `New*Cache`). Some common setups:

**Byte-budgeted cache** - bound by the byte size of stored data instead of item count; the per-item size (key and value bytes) is estimated automatically:

```go
c, _ := memstash.New[string, []byte](
	memstash.WithMemoryBudget(512 << 20), // ~512 MiB of keys and values
)
```

The built-in estimator covers types whose size is trivial to compute: numerics, pointer-free structs/arrays, strings, slices of fixed-size elements, and pointers to fixed-size types. For anything more complex, construction fails with `ErrBudgetNeedsCostFunc` - provide the byte size yourself:

It's used to calculate the cache capacity — the cache doesn't allocate a fixed block of memory.

```go
c, _ := memstash.New[string, User](
	memstash.WithMemoryBudget(512 << 20),
	memstash.WithCostFunc(func(k string, u User) uint32 { return uint32(len(k) + u.Bytes()) }),
)
```

**Synchronous writes on Set** - write-through policy, L2 updated synchronously:

```go
c, _ := rueidis_adapter.NewJSONCache[string, Session](client,
	memstash.WithWritePolicy(memstash.WriteThrough),
)
```

**Batch operations** - amortize the network round trip; adapters use native pipelining / multi-get where the client supports it:

```go
found, err := c.BatchGet(ctx, []string{"a", "b", "c"})            // one round trip to L2 for the misses
dst = c.BatchGetFromMemory([]string{"a", "b"}, dst)
err = c.BatchSet(ctx, memstash.List[string, User]{{Key: "a", Value: a}, {Key: "b", Value: b}})
err = c.BatchDelete(ctx, []string{"a", "b"})                      // follows the write policy, like BatchSet
```

**Observability and iteration** - `Stats()` returns operation counters (collected with striped counters, so an increment stays contention-free even under heavy parallelism). It's opt-in via `WithStats()`: off by default so a cache that doesn't read `Stats()` doesn't pay for it - otherwise counters stay at zero. `Iterator()` walks the live first-level entries lock-free, independent of stats:

```go
c, _ := memstash.New[string, User](
	memstash.WithStats(),
)
s := c.Stats() // s.Hits(), s.Misses(), s.Sets(), s.Deletes(), s.Gets(), s.HitRate(), s.MissRate(), ...
for key, value := range c.Iterator() {
	fmt.Println(key, value)
}
```

**Non-string keys with a custom key mapping** - provide a key function for the L2 storage key:

```go
c, _ := rueidis_adapter.NewJSONCache[int, User](client,
	l2.WithKeyFunc(func(id int) string { return "user:" + strconv.Itoa(id) }),
)
```

**Custom serializer** - `NewCache` takes any `memstash.Codec[V]`, so a binary format works just as well as JSON. You can encode each field directly instead of going through JSON:

```go
type Point struct {
	X, Y float64
}

type pointCodec struct{}

func (pointCodec) Marshal(p Point) ([]byte, error) {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], math.Float64bits(p.X))
	binary.LittleEndian.PutUint64(buf[8:16], math.Float64bits(p.Y))
	return buf, nil
}

func (pointCodec) Unmarshal(data []byte) (Point, error) {
	return Point{
		X: math.Float64frombits(binary.LittleEndian.Uint64(data[0:8])),
		Y: math.Float64frombits(binary.LittleEndian.Uint64(data[8:16])),
	}, nil
}

c, err := rueidis_adapter.NewCache[int, Point](client, pointCodec{},
	l2.WithKeyFunc(strconv.Itoa),
)
```

**Eviction policies** - four built-ins, selected with `WithPolicy`: `PolicyS3FIFO` (the default: quarantine + protected queue + ghost, the best all-rounder under scans and one-hit wonders), `PolicyClock` (GCLOCK, approximates LRU at FIFO cost), `PolicyWTinyLFU` (an admission window gated by a Count-Min frequency sketch that remembers keys across evictions - strong on skewed workloads), and `PolicySIEVE` (a single scan hand over the insertion order - the simplest, with an S3-FIFO-class hit rate). All share the same lock-free read path: a read only sets a 2-bit reference counter on the item's record.

**Custom eviction policy** - implement the `memstash.EvictionPolicy` interface (the same contract the built-ins use: `Add`/`Evict`/`Len`/`Sweep`/`Range`/`Bytes`, all called under the shard mutex) and plug its per-shard factory in:

```go
c, err := memstash.New[string, User](
	memstash.WithCustomEvictionPolicy(func(states memstash.ItemStates[string, User], shardCap int64) memstash.EvictionPolicy[string, User] {
		return newMyPolicy(states, shardCap) // states resolves QNode indices to item records
	}),
)
```

Full option list:

| Option | Purpose |
|---|---|
| `WithMemoryCapacity(n)` | L1 capacity in weight units (defaults to 20 000). |
| `WithMemoryBudget(bytes)` | L1 bound in bytes of stored keys and values; derives a size-based cost function automatically (mutually exclusive with `WithMemoryCapacity`). |
| `WithCostFunc(fn)` | Per-item weight function (e.g. size in bytes). |
| `WithTTL(d)` | Item lifetime (1-second resolution); applied to L2 writes too. |
| `WithPolicy(p)` | `PolicyS3FIFO` (default), `PolicyClock`, `PolicyWTinyLFU`, or `PolicySIEVE`. |
| `WithCustomEvictionPolicy(fn)` | Plug in your own eviction policy: a per-shard factory returning a `memstash.EvictionPolicy` implementation. |
| `WithShards(n)` | Number of eviction shards (default: auto by GOMAXPROCS). |
| `WithL2Cache(l2)` | Attach a second level directly. |
| `WithWritePolicy(p)` | `WriteBack` (default), `WriteThrough`, or `WriteDisabled`. |
| `WithWriteBackBuffer(n)` | Size of the async write-back buffer. |
| `WithBatchingForWriteBack()` / `WithNoBatchingForWriteBack()` / `WithAdaptiveBatchingForWriteBack()` | How the write-back worker drains its buffer to L2: coalesced into `BatchSet` (default), one `Set` per write, or adaptive. |
| `WithGhostSize(n)` | Capacity (in keys) of the S3-FIFO ghost queues and the W-TinyLFU frequency sketch. |
| `WithOnL2Error(fn)` | Handler for background L2 errors. |
| `WithStats()` | Enables the `Stats()` operation counters. Off by default. |

## L2 Adapters

Each adapter is a separate module (`memstash/l2/<name>_adapter`) so the core never pulls in a client SDK you don't use. Every adapter offers both an "adapter only" constructor (`New`, `NewJSON`, `NewBytes`) and an all-in-one two-level constructor (`NewCache`, `NewJSONCache`, `NewBytesCache`), plus native batch pipelining where the client supports it.

The write path favors throughput by default: instead of one round trip per key, the background write-back worker coalesces the Sets into the adapter's native `BatchSet` (an `MSET` or a pipeline).

| Module | Backend / client | context |
|---|---|---|
| `l2/goredis_adapter` | Redis - [redis/go-redis](https://github.com/redis/go-redis) | ✅ |
| `l2/rueidis_adapter` | Redis - [redis/rueidis](https://github.com/redis/rueidis) | ✅ |
| `l2/redispipe_adapter` | Redis - [joomcode/redispipe](https://github.com/joomcode/redispipe) | ✅ |
| `l2/redigo_adapter` | Redis - [gomodule/redigo](https://github.com/gomodule/redigo) | partial |
| `l2/gomemcache_adapter` | memcached - [bradfitz/gomemcache](https://github.com/bradfitz/gomemcache) | ❌ |
| `l2/rainycape_adapter` | memcached - [rainycape/memcache](https://github.com/rainycape/memcache) | ❌ |
| `l2/mc_adapter` | memcached - [memcachier/mc](https://github.com/memcachier/mc) | ❌ |
| `l2/valyala_adapter` | memcached - [valyala/ybc](https://github.com/valyala/ybc) (cgo) | ❌ |
| `l2/sql_adapter` | any [database/sql](https://pkg.go.dev/database/sql) engine (SQLite, MySQL, ...) | ✅ |
| `l2/pgx_adapter` | PostgreSQL - [jackc/pgx](https://github.com/jackc/pgx) (native, pipelined) | ✅ |
| `l2/badger_adapter` | embedded - [dgraph-io/badger](https://github.com/dgraph-io/badger) | ❌ |
| `l2/mongo_adapter` | MongoDB - [mongo-driver](https://github.com/mongodb/mongo-go-driver) | ✅ |
| `l2/dynamo_adapter` | DynamoDB - [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2) | ✅ |
| `l2/tarantool_adapter` | Tarantool - [go-tarantool](https://github.com/tarantool/go-tarantool) | ✅ |
| `l2/aerospike_adapter` | Aerospike - [aerospike-client-go](https://github.com/aerospike/aerospike-client-go) | ❌ |

Each adapter takes an interface rather than a concrete client, so it stays independent of the client library's version, and a few libraries are covered without a separate module: `sql_adapter` accepts any `{QueryContext, ExecContext}` (so pgx via database/sql works too), `badger_adapter` covers [badgerhold](https://github.com/timshannon/badgerhold) via `store.Badger()`, and `dynamo_adapter` covers [guregu/dynamo](https://github.com/guregu/dynamo) via its underlying `*dynamodb.Client`.

SQL, Tarantool and other stores without server-side expiration filter expired entries on read and expose a reaper (`DeleteExpired`) to purge them; the note in each package doc explains the specifics.

Rolling your own is straightforward: implement the `memstash.L2Cache[K, V]` interface (`Get`/`BatchGet`/`Set`/`BatchSet`/`Delete`/`BatchDelete`) and pass it to `WithL2Cache`.

## Benchmarks

[Measured](benchmarks/results/out.txt) on an AMD Ryzen 9 9900X (Go 1.26.4). Reproduce with:

```sh
# throughput and allocations (Get / Set / mixed 90-10) vs other libs and a plain sync.Map baseline
go -C benchmarks test -run xxx -bench . -benchmem

# hit ratio across Zipf, Zipf+scan, and one-hit-wonder workloads
go -C benchmarks test -run TestHitRate -v
```

![Read throughput](benchmarks/results/read_throughput.svg)

### Throughput - ns/op, lower is better

| Cache | GetHit | Get (50% hitrate) | Set | 90 Get / 10 Set | Set alloc |
|---|--:|------------------:|--:|----------------:|--:|
| **memstash-s3fifo** | **0.86** |          **1.16** | 26.7 |            3.97 | 0 B / 0 |
| **memstash-clock** | **0.86** |          **1.16** | 27.2 |            3.91 | 1 B / 0 |
| **memstash-wtinylfu** | **0.87** |          **1.14** | 27.5 |            3.84 | 0 B / 0 |
| **memstash-sieve** | **0.87** |          **1.11** | 26.2 |            4.01 | 0 B / 0 |
| theine-wtinylfu | 3.35 |              3.40 | 323.2 |           50.78 | 35 B / 0 |
| ristretto | 5.91 |              5.55 | 80.5 |           12.91 | 88 B / 1 |
| otter-wtinylfu | 6.07 |              1.72 | 387.2 |           51.18 | 48 B / 1 |
| bigcache | 9.10 |              7.43 | 37.8 |           21.46 | 24 B / 2 |
| freecache | 13.84 |             11.56 | 20.9 |           14.40 |  0 B / 0 |
| hashicorp-lru | 96.94 |             86.49 | 138.0 |           98.94 | 73 B / 0 |
| sync.Map\* | 1.64 |              1.80 | 11.6 |            4.19 | 63 B / 2 |

\* `sync.Map` performs no eviction - a lower-bound baseline, not a comparable cache.

### Parallel throughput - millions of ops/s, higher is better

| Cache | 100% reads | 75% reads | 50% reads | 25% reads | 0% (writes only) |
|---|--:|--:|--:|--:|-----------------:|
| **memstash-s3fifo** | **1070** | **139** | **85** | **62** |           **48** |
| **memstash-clock** | **1072** | **138** | **84** | **58** |           **47** |
| **memstash-wtinylfu** | **1063** | **140** | **84** | **55** |           **48** |
| **memstash-sieve** | **1072** | **142** | **78** | **64** |           **48** |
| otter-wtinylfu | 446 | 9.5 | 5.3 | 3.6 |              2.8 |
| theine-wtinylfu | 287 | 10 | 5.8 | 4.3 |              3.6 |
| ristretto | 169 | 28 | 18 | 12 |              9.2 |
| bigcache | 108 | 34 | 27 | 24 |               28 |
| freecache | 72 | 69 | 69 | 67 |               67 |
| hashicorp-lru | 10 | 10 | 9.6 | 9.8 |              9.6 |
| sync.Map\* | 563 | 156 | 110 | 86 |               75 |

Reads are only half the story. Once writes enter the mix, the W-TinyLFU caches (Otter, Theine) drop by more than an order of magnitude, while memstash stays within about 2× of the eviction-free `sync.Map` baseline. At a 50/50 read-write split it sustains **13–16× their throughput.**

### Hit ratio - higher is better

The "Est. Size" column is the cache's estimated memory footprint at the end of the one-hit-30% run (key + value bytes plus each implementation's own bookkeeping).

**Capacity = 500k items (~36% of the working set):**

| Cache | Zipf | Zipf+scan | One-hit 30% | Est. Size |
|---|--:|--:|--:|----------:|
| **memstash-s3fifo** | **58.12%** | **34.65%** | **36.39%** |     29 MB |
| memstash-wtinylfu | 57.96% | 34.78% | 36.40% |     29 MB |
| memstash-sieve | 57.73% | 34.07% | 35.86% |     30 MB |
| theine-wtinylfu | 57.28% | 34.52% | 35.72% |     54 MB |
| memstash-clock | 57.04% | 33.01% | 34.63% |     25 MB |
| otter-wtinylfu | 55.95% | 33.07% | 34.50% |     41 MB |
| hashicorp-lru | 55.64% | 31.61% | 32.99% |     45 MB |
| bigcache | 51.29% | 27.95% | 29.23% |     25 MB |
| freecache | 51.01% | 28.32% | 29.57% |     54 MB |
| ristretto | 11.57% | 9.01% | 8.82% |     26 MB |

**Capacity = 100k items (~7% of the working set):**

| Cache | Zipf | Zipf+scan | One-hit 30% | Est. Size |
|---|--:|--:|--:|--:|
| memstash-wtinylfu | 41.83% | 26.30% | 27.06% | 6.8 MB |
| theine-wtinylfu | 41.15% | 26.07% | 26.42% | 12 MB |
| **memstash-s3fifo** | **41.10%** | **26.33%** | **27.18%** | 6.8 MB |
| memstash-sieve | 39.62% | 25.20% | 25.14% | 6.7 MB |
| otter-wtinylfu | 37.10% | 22.90% | 23.03% | 7.3 MB |
| memstash-clock | 33.11% | 18.22% | 18.94% | 5.7 MB |
| hashicorp-lru | 30.03% | 15.62% | 16.53% | 9.6 MB |
| bigcache | 28.26% | 15.71% | 15.48% | 6.1 MB |
| freecache | 25.65% | 14.51% | 13.95% | 19 MB |
| ristretto | 3.84% | 2.72% | 2.82% | 4.7 MB |

**Capacity = 10k items (~1% of the working set):**

| Cache | Zipf | Zipf+scan | One-hit 30% | Est. Size |
|---|--:|--:|--:|--:|
| theine-wtinylfu | 15.38% | 9.25% | 10.26% | 1.5 MB |
| **memstash-s3fifo** | **14.78%** | **9.88%** | **10.29%** | 947 kB |
| memstash-wtinylfu | 14.47% | 8.33% | 8.63% | 946 kB |
| memstash-sieve | 13.51% | 8.83% | 8.82% | 929 kB |
| otter-wtinylfu | 13.10% | 7.60% | 8.04% | 804 kB |
| bigcache | 11.75% | 7.46% | 6.09% | 1.5 MB |
| freecache | 6.00% | 3.93% | 3.03% | 7.1 MB |
| memstash-clock | 5.34% | 3.50% | 2.62% | 781 kB |
| hashicorp-lru | 5.01% | 3.30% | 2.49% | 1.0 MB |
| ristretto | 0.48% | 0.28% | 0.31% | 807 kB |

### Heap footprint, lower is better

Each cache is filled with 100M `uint64 -> uint64` entries - 16 bytes of raw payload apiece - and the heap growth it
causes is read back from the Go runtime, so these are real resident bytes rather than the cache's own estimate. The
[measurements](benchmarks/results/heap-size-100kk.txt) run one contender at a time: a single pass costs several GiB. For string keys adapters key/value
was converted from `uint64` to `[8]byte`.

| Cache | Heap | B/entry |
|---|--:|--:|
| xsync.MapOf\* | 3.7 GiB | 39.24 |
| **memstash-s3fifo** | **4.1 GiB** | **43.92** |
| ristretto | 4.3 GiB | 46.40 |
| freecache | 5.7 GiB | 61.53 |
| otter-wtinylfu | 7.6 GiB | 81.98 |
| bigcache | 7.7 GiB | 84.84 |
| hashicorp-lru | 9.7 GiB | 104.2 |
| theine-wtinylfu | 11 GiB | 115.0 |

\* `xsync.MapOf` performs no eviction - a lower-bound baseline, not a comparable cache.
