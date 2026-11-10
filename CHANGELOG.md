# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org).

Every module - the root and each `l2/*_adapter` - is tagged with the same version.

## [Unreleased]

### Added

- **The load generator runs next to its L2 servers.** `make load-generator-docker` runs it as the compose service
  `load-generator` (profile `load`, so `make up` never starts it), and its L2 traffic skips the Docker Desktop port
  relay, which capped the L2 scenarios far below their target. `-config` now repeats, a later file overriding the
  fields it sets.

### Changed

- **The first level never holds a tombstone.** A deletion - `Delete`, an eviction, a lazy TTL removal - compacts its
  probe run on the spot with backward-shift deletion (Knuth's Algorithm R, with the fewest moves that close the
  hole, half of what taking the nearest item costs), so the table always holds exactly what inserting the live keys
  into a fresh table would:
  probe walks stay at their fresh-table values however long the cache churns, and dead slots never eat into the free
  space. Measured on the churn harness at the default preallocation ratio: 0 dead slots and 0 purges over 4M inserts
  with 30% deletes (before: 4-5% dead held there by a purge every ~630 inserts per shard), hit walks 2.45 slots
  instead of 3.0-3.6, p99 hit walk 18 instead of 27.
- **No same-size rebuilds.** With nothing to purge, a preallocated table is never allocated again after `New`, and a
  non-preallocated one only grows; the transient second table of a purge is gone. `Stats.TablePurges` is always 0
  and will be removed with the next stats revision.
- **The eviction queues run through the table.** Every slot carries its place in its eviction queue - `prev`/`next`,
  8 bytes in an array beside the items - and a compaction that moves an item moves its place along, so a policy
  names its victim by slot index and the cache reads it directly, with no lookup. The cache enqueues every new item
  at the back of queue 0 and removes every departing one on the spot: nothing is left behind for a sweep, a
  delete-then-set leaves no duplicate, and no item can drop out of the queues and become unevictable. A policy moves
  items between the `EvictionQueues` queues (`Items.Oldest`, `Newer`, `Requeue`, the `ItemInMainQueue` bit, and a
  scan `Hand` that follows its item through moves and removals). `EvictionPolicy` is
  `Add`/`Remove`/`Reweigh`/`Evict`/`Bytes`: `QNode{Idx, Cost}`, `Add` and `Evict` are as in 0.9.8; `Len`, `Sweep`
  and `Rebuild` are gone; `Remove` hears of every departing item while it is still queued and `Reweigh` of every
  overwrite that changes a weight, so S3-FIFO's small and W-TinyLFU's window weigh exactly what they hold. `Items`
  also gives `Cost` and `Fingerprint` - a key's home slot and tag bits, which S3-FIFO's ghost table keys on without
  hashing the key; W-TinyLFU's sketch hashes the key as in 0.9.8, since
  a fingerprint changes when the table grows and the sketch's history must not. The links take 8 bytes per slot: a
  preallocated 100M-item `uint64` cache measures 43.7 bytes per item instead of 41.0 (heap 4.1 GiB instead of 3.9);
  a table that is not preallocated runs at a lower load and pays ~8 bytes per item more (+12-14% at 1M items), except
  under SIEVE, which drops its own 16-byte node array and comes out ~3% smaller.
- **The item's meta word carries its distance from its home slot**, so a compaction reads the run it moves instead of
  rehashing every key it passes - the difference between 1.0 us and 0.6 us per delete on 100M string keys. It costs
  the six generation bits the seqlock had to spare (22 left, 2M writes to one slot before a stalled reader could
  validate against a stale word).
- **The slot tag carries all 13 hash bits.** A tag must never be 0, or an empty slot would pass the candidate compare;
  that used to be a forced top bit, leaving 4096 distinct tags. Now only a zero tag is remapped, so 8191 stay distinct:
  a foreign key in the probe path costs a key compare half as often, and S3-FIFO's ghost fingerprints collide half as
  often.
- **A shard's read line carries nothing a writer touches.** The table pointer and the move sequence share the first
  cache line and the mutex opens the second, where before a `Set` taking the mutex invalidated the line every reader
  loads on every probe. Worth 30-37% on every mixed read/write benchmark.
- Lock-free reads stay exact across the compaction: a miss that overlapped moves is re-probed (a per-shard move
  sequence on the line every read loads anyway), the third time under the shard mutex; hits are unchanged.
  `Iterator` and `SaveTo` may skip an item a concurrent deletion slid toward its home - the same weak consistency the
  documentation already claims for concurrent writes - and never yield a key that stays in the cache twice.
- `WithPreallocationRatio` is now a pure memory-for-latency dial, and `WithGrowthThreshold` only means growth; the
  rule that the ratio must stay below the threshold is gone.
- **Meta words are written plainly under the shard mutex on amd64.** One writer holds the mutex, so a meta write
  needs release ordering, not a locked instruction, and on amd64 a plain aligned store is release-ordered; five
  locked writes measure 18.5 ns against 1.5 ns plain, and an insert that evicts issues a dozen. Readers keep their
  atomic loads and compare-and-swaps; the race detector, which cannot see the ordering argument, builds the atomic
  variant (`internal/itemstore/write_generic.go`).
- **The hot paths inline in user programs too.** A generic method of this package is compiled where it is
  instantiated - in the user's module - and there a plain helper from `internal/itemstore` or `internal/eviction`
  inlines only if its body is in this package's export data, which the compiler writes only for helpers it inlined
  in this package's own build. Every such helper was a call in every program but this repository's own tests: the
  tag, the overflow bucket and the empty test on every read; the meta-word and weight stores, the generation and
  distance encoders, the queue node accessors and the policies' own helpers on every write - 683 call sites in the
  benchmarks module. The package now instantiates the cache once inside itself, which inlines them all there; the
  155 calls left go to functions that are not inlinable anywhere. The instance adds nothing to a program's binary and
  ~0.15 s to this package's compile. On the benchmarks module, which builds the way a user does: `GetHit` -16%, `Set`
  and `Delete` -4% to -6%, the reads and writes of `Mixed90_10` unchanged.
- **What the compaction costs.** Measured against the tombstone design: reads are ahead (`GetHit` -13%, `Get` -21%,
  `GetMiss` -9%, `DeleteMiss` -19%) and so is every mixed workload (`Mixed90_10` -41%, parallel throughput -11% at
  100% reads to -38% at 75%: the shard read line no longer bounces on every `Set`). Writes that remove still pay for a
  table that never holds a tombstone: `Set` at capacity costs +17%, and the `Delete`+`Set` pair of one hot key 48 ns
  against 34 ns, where the old design resurrected the item in place. Filling a cache that is not preallocated costs
  more too: 8M items into one shard +8% on the inserts alone and +11% with the table growing under them. A growth
  after the first eviction carries the eviction queues over in order; one before it, while the queues hold nothing
  but insertion order, queues the items in their new slot order and spares that pass. Hit rates are unchanged.
- **A compaction reads its run once.** It used to walk the run again for every item it moved, sort its notes of the
  run after the moves and rewrite the chain bits of the whole run. It now copies where the keys from the hole to the
  first slot nothing is chained past are homed, picks the moves on that copy and clears only the chain bits the
  deletion uncovered. An insert that evicts, single-threaded at the default preallocation ratio (the new
  `MemstashSetEvictSerial`, 8 interleaved rounds): -9% for S3-FIFO, -10% for Clock, -12% for SIEVE, -7% for
  W-TinyLFU. The parallel `Set` and `Delete` benchmarks do not move: the shard mutex and cache misses bound them.

### Fixed

- **A key in a table with one free slot left could be lost to reads.** A table that cannot grow, under a policy that
  evicts nothing, fills up to its last free slot, and a key can then walk around the whole table and settle in its
  home group, before its home. The group's overflow counters counted only the keys in another group, so a probe
  stopped at the group's edge: `Get` missed the key, and `Delete` left it in place until a `Set` overwrote it. Such a
  key is counted now. `TestFullTableKeepsASlotFree` hit it in 2% of its runs.
- **A lock-free read could return a torn multi-word value on arm64 and other weakly ordered CPUs.** An in-place
  overwrite of a value wider than a word marks the item's generation odd, writes the value, then marks it even; the
  odd mark is an atomic store, which on arm64 is a release (`STLR`) and orders nothing after it, so the value stores
  could become visible first, and a reader copying in that window validated against the old, even generation. The
  odd mark is now followed by the same acquire+release pivot the read side already uses. amd64 is unaffected - its
  stores never pass one another.
- **A lock-free read could return a value that an in-place overwrite wrote with several stores, half old and half
  new.** The overwrite skipped the generation bracket for any value of up to 8 bytes, and size does not say how many
  stores gc emits. On 386, arm and mips an 8-byte string, interface, int64 or float64 is two stores, so a reader could
  pair one string's pointer with another's length: 0.9.8 on 386 returned such strings in every 2 s run. On every
  platform a `struct{A, B uint32}` is stored field by field, and so is a `struct{_ [0]uint64; A, B uint32}`, which
  is word-sized and word-aligned like a `uint64`. `New` now classifies the value type once with reflect: only a
  single scalar no wider than a word, possibly wrapped in one-element arrays and structs whose other fields take no
  space, skips the bracket. Bracketing every overwrite instead cost a serial overwrite of `uint64` values +7%
  (14.5 -> 15.5 ns, p=0.026); the classified path is no slower than before (14.95 -> 14.56 ns, parallel `Set`
  unchanged).
- **32-bit platforms are supported again.** On a 32-bit int the growth test overflowed past ~524k items in a shard,
  and the shard then grew on every insert until it ran out of memory (250-500k items into one shard on 386). It is
  computed in int64 now, the table size cap fits an int, and the shard's cache-line padding follows the word size.
  New CI jobs run the suite on 386 and arm64, under qemu on s390x and riscv64, and vet every other port.
- **An in-place overwrite of a pointer value had no release ordering off amd64.** A reader on arm64 could get the new
  pointer and still see the old contents of what it points to. The overwrite now issues the same acquire+release
  pivot the seqlock uses before the store.
- **A write-back write could overtake the writes its key had already queued.** When a worker's buffer was full, the
  write ran synchronously on the caller's goroutine, ahead of everything still in the queue: `Set(v1)`, `Set(v2)`,
  `Set(v3)` could leave `v2` in L2, and a `Delete` could land before the queued `Set` that resurrected the key. A write
  that finds the buffer full now waits for room, so the writes of one key reach L2 in order; after `Close` it still
  runs synchronously. `OnL2Error` receives write-back errors on the worker goroutine and must not write to the cache.
- **`sql_adapter.DefaultTable` and `tarantool_adapter.DefaultSpace` were `"github.com/zakonnic/memstash_cache"`.** A
  module-path rename had rewritten both names, so an adapter built with an empty table or space name failed on every
  call (a SQL syntax error, a missing space). Both are `"memstash_cache"` again, the name the docker setup and the
  documented schema create.
- **A batch that repeated a key could leave an older value in L2.** Write-back coalesces a key's queued writes into
  one `BatchSet`, so a hot key can appear in it twice, and `L2Cache.BatchSet` relied on the adapter to keep the
  last value. The memcached adapters (gomemcache, mc, rainycape) sent the batch as concurrent `Set`s, Aerospike ran it
  in no fixed order outside inline processing, Tarantool's connection shards and redispipe on a cluster could send
  its commands out of order: any of them could land `v1` after `v2`. They now collapse a repeated key to its last
  value first (the new `l2.LastValues`), and the `BatchSet` contract says a key may repeat.
- **A batch loader that reordered its keys handed other callers the wrong values.** The keys a `BatchGetOrLoad` or
  `BatchGetAndRefresh` loader receives may be the very list the flights were registered from, and the loaded values
  were matched to the flights by position in it: a loader that sorted its keys for the query, as most do, answered the calls
  waiting on `a` with the value of `c`. Flights are now matched by their own key. A loader may reorder the slice; for
  `BatchGetOrLoad` that reorders the caller's `keys`, which it may alias.
- **The Badger adapter's `Get` could hand out memory Badger later unmaps.** It decoded the value inside
  `item.Value`, whose slice points into Badger's own memory and is valid only for the transaction; with a
  pass-through codec (`NewBytes`, `BytesCodec`) that slice went into the first level, and for a value in the
  value log a later GC or `Close` unmapped it - the next read of the cached value crashed the process. `Get` copies
  the value first, as `BatchGet` already did.
- **The rueidis and valkey adapters panicked on a one-master cluster.** They told a single server from a cluster by
  `len(client.Nodes()) <= 1`, and a cluster client with one master (a single-shard cluster, a local node with
  `cluster-enabled yes`) has one node: the adapters then took the ordered-MGET and dedicated-pipeline paths, where
  the client panics on keys from different slots - in `BatchGet`, in every `BatchSet` with a TTL, and in large
  `BatchDelete`s. They now ask the client's `Mode()`.
- **`NewBatchLoadable`'s `GetOrLoad` cached a zero value for a key the loader did not return.** Its single-key
  loader turned "the batch loader omitted the key" into `(zero, nil)`, which `GetOrLoad` stored in memory and wrote
  to L2, where it outlived restarts; `GetAndRefresh` did the same. The new `ErrKeyNotFound` is how a loader says the
  source has no such key: nothing is cached and `GetOrLoad` returns it. `NewBatchLoadable` answers an omitted key
  with it, `NewLoadable`'s synthesized batch loader leaves a key its loader reports that way out of the batch instead
  of failing it, and a `GetOrLoad` waiting on a batch load that omitted its key gets it instead of a zero value.
- **`NewWithConfig` wrote into the caller's `Config`.** A budget filled in `CostFunc` and moved itself into
  `MemoryCapacity`, and a `PreallocationRatio` set `PreallocateMap`, so a second cache built from the same `Config`
  failed with `ErrBudgetAndCapacity`. The constructor now works on its own copy.
- **An out-of-range `WritePolicy` passed validation and made `Set` panic.** `WritePolicy(3)` built a cache with no
  write-back workers that still took the write-back path, dividing by zero workers on the first `Set`, while
  `Delete` silently skipped L2; an unknown `WriteBackBatching` quietly behaved as `BatchingFull`. The constructor now
  rejects both, with the new `ErrUnknownWritePolicy` and `ErrUnknownBatching`.
- **A write-back batch that panicked in the adapter was reported for one key, sometimes the wrong one, and could
  take the next task down with it.** The worker recovered once per drain with the key of the drain's first task:
  `OnL2Error` heard of that key only - one that may already have reached L2 - and never of the rest of the failed
  batch, and a task taken off the queue to end the batch's run, a `Delete` for instance, was never sent. Every L2 call
  of the write-back now recovers on its own: the batch's keys all get the `ErrPanic`, and the worker goes on with the
  task it already holds.
- **A panic while a `GetBatched` worker filed a read took the worker down and left the reads it held waiting.** The
  memory check a worker makes before queuing a key for L2 can reclaim an expired item, and an `OnDeletion` handler
  that panicked there escaped the batch's recovery: the reads already collected and the one in hand were never
  answered, and the pool lost a worker for good - with all of them gone, every `GetBatched` miss waited for its
  context. The recovery now covers the whole batch: its callers get `ErrPanic` and the worker goes on.
- **`BatchGetOrLoad` could load a key that a concurrent load had just stored.** A flight that ended between the
  batch's memory pass and its claim on the key left the key in memory with its flight gone, so the batch owned it
  anew and sent it to L2 and the loader again. It now checks memory once more for the keys it owns, as `GetOrLoad`
  always did, and loads only those still missing.
- **`DeletionCause` values shifted with unrelated constants.** The causes shared an `iota` block with the
  `ItemDead`/`ItemChanceMask`/`ItemInMainQueue` bits, so they were 2..6 in 0.9.8 and 3..7 once a bit was added, and a
  cause stored or exported as a number meant something else after an upgrade. They have their own block now and stay
  at 1..5 (`CauseInvalidation` to `CauseOverflow`); the zero value is no cause.
- **A "forever" TTL overflowed.** `WithTTL(math.MaxInt64)` rounded the TTL up past the int64 range, and every item
  was expired the moment it was written; `SetWithTTL` with such a lifetime handed L2 a negative TTL;
  `l2.RedisMillis` turned it into one millisecond and `l2.MemcacheExpiration` into a negative expiration, both of
  which expire the key at once; the Aerospike adapter wrapped TTLs of 2^32 seconds or more into its reserved values
  (keep the old TTL, the namespace default). The roundings no longer overflow, a TTL is capped at ~146 years, and
  the Aerospike adapter stores anything past its range as "never expire".
- **`LoadFrom` with `LoadWithTTL` and `LoadToL2` gave L2 a different lifetime than memory.** Memory capped a
  restored item at the loading cache's TTL, L2 got the uncapped remainder from the snapshot - an hour where memory
  kept a minute - and a cache without a TTL kept the item forever while L2 expired it. L2 now gets the lifetime
  memory ended up with, rounded and capped the same way, and none from a cache without a TTL.
- **A call waiting on another's load got that caller's cancellation.** `GetOrLoad` and `BatchGetOrLoad` coalesce
  concurrent loads of a key, and the load runs under the context of whoever started it: when that context was
  canceled or timed out, every call waiting on the key received `context.Canceled` or `DeadlineExceeded`, though its
  own context was live. A waiter now loads the key itself when the load failed after the starter's context ended and
  its own context is fine. An error the loader hits on its own, its own timeout included, still reaches every waiter
  once.
- **L2 items with a TTL expired up to a second early in the SQL, pgx, Badger, DynamoDB and Tarantool adapters.**
  They stored the deadline as `time.Now().Add(ttl).Unix()`, rounded down, and read it back as expired once the
  clock's second reached it: every item lived somewhere in (ttl-1s, ttl], and one with a TTL under a second was
  usually gone before anyone read it. The new `l2.ExpiresAt` rounds the deadline up, and all five use it.
- **The redigo adapter failed every single command on a pool whose connections have no `DoContext`.** Its fallback
  to the plain `Do` tested the pooled connection, which always has `DoContext`, and never the connection that
  connection wraps: with a `Dial` that wraps the connection for tracing or metrics, `Get`, `Set`, `Delete` and the
  MGET/MSET batch paths all returned "connection does not support ConnWithContext". The fallback now runs on the
  answer redigo gives for exactly that case.
- **One failed item cancelled the rest of an L2 batch write in the adapters built on the `l2` batch helpers.**
  `l2.BatchSetConcurrent`/`BatchDeleteConcurrent` (gomemcache, mc, rainycape) and `BatchSetSequential`/
  `BatchDeleteSequential` (sql, aerospike fallbacks) stopped at the first error. A write-back batch carries up to 128
  unrelated keys, so one value too large for memcached, or one illegal key, cost every other write in it - and a
  lost delete leaves a key in L2 that the cache already dropped. Every item is tried now and the failures come back
  joined; only a canceled context ends the run early. `BatchGet*` still stop at the first error.
- **The rainycape adapter's `BatchGet` turned every failure into misses.** The client's `GetMulti` drops the errors
  of its per-server reads - an unreachable server, a batch that runs past the connection's deadline, an illegal
  key - and just answers with fewer items, so a failing memcached read as an empty one: no error, no `OnL2Error`,
  and the loaders ran for everything. `BatchGet` now runs concurrent `Get`s, which report their errors.
- **The memcached adapters failed on keys memcached rejects.** A key with a space or a control character, or longer
  than 250 bytes, made every gomemcache call on it fail with `ErrMalformedKey` - a `Get` included, which should have
  been a miss - and one such key failed a whole `GetMulti`, so every caller sharing a `GetBatched` batch with it got
  the error; mc and rainycape failed on the long ones. The new `l2.MemcacheKey` stores such a key under a stand-in
  derived from its SHA-256, and all three adapters use it, so they agree on it and any key reads back.
- **Keys and values some backends reject failed whole batches.** Badger refuses the empty key, keys with its internal
  `!badger!` prefix and keys over 65000 bytes; DynamoDB refuses an empty partition key and one over 2048 bytes, and a
  nil value, which its SDK sends as `{"B":null}`; Tarantool's varbinary field refuses the nil a `[]byte(nil)` encodes
  to. Each failed on its own and took down the batch around it - every other key of a write-back batch, a DynamoDB
  chunk of 25. The Badger and DynamoDB adapters now store such keys under `l2.HashedKey`, the stand-in the memcached
  adapters already use (`l2.MemcacheKey`), and a nil value goes out as an empty one.
- **The DynamoDB adapter gave up on a throttled batch after one immediate retry.** Unprocessed items were re-sent
  once, straight away, and the batch then failed: under throttling that lost the write - or the delete, leaving a
  key in L2 the cache had dropped - and a `BatchGet` whose answer passes the service's 16 MB page limit failed every
  time, since each page used up the retry. A round that makes progress now goes again at once, and one that does not
  waits first, doubling from 25 ms with jitter and within the context, five times in a row at most.
- **`Stats()` returned a live view, not the snapshot it was documented as.** Copying the struct copied the counters'
  shared stripes, so a value taken earlier kept counting and two of them could not be subtracted, and its methods on
  a pointer receiver made `c.Stats().Hits()` fail to compile. `Stats` is now a plain snapshot with value-receiver
  methods, read once from the counters, whose own shape - and the read and write paths - is unchanged.
- **`Close` did not wait for `GetAndRefresh` loads.** They ran on goroutines of their own with a context stripped of
  cancellation, so one started before `Close` wrote to memory and L2 - and could call `OnDeletion`, `OnL2Error` and
  `OnPanic` - after `Close` had returned, against an L2 client the caller had shut down by then. `Close` now cancels
  their context and waits for them; a refresh that would start once `Close` has begun does not start.
- **A shard table could grow, or be preallocated, to 2^32 slots, whose count wrapped to 0 in the table's uint32
  size.** The table then read its chunk directory as items. It takes a single shard of over 3.2 billion items to get
  there; the largest table is now the last whole chunk below 2^32.
- **`Wait` could block forever, and could not tell a finished checkpoint from a cache that was closing.** A stuck L2
  holds the write-back queue - and with it the checkpoint - up indefinitely, and `Wait` took no context; it also
  returned silently when `Close` began. It is now `Wait(ctx) error`: nil once everything enqueued before the call
  reached L2, `ctx.Err()` when the context ends first, and the new `ErrClosed` once the cache is closing. This changes
  its signature.
  own context was live. A waiter now loads the key itself when the error is a context error and its own context is
  fine.
- **A value read from L2 or loaded could overwrite a newer `Set`.** `Get`, `BatchGet`, `GetBatched`, `GetOrLoad`
  and `BatchGetOrLoad` put what L2 or the loader returned into memory; a `Set` that landed while that read was out
  was overwritten by the older value, which then stayed in memory until its TTL or eviction while L2 held the new
  one - and a loaded value went to L2 as well, over the `Set`. Such a value now only fills a key that memory holds no
  live value for, and a loaded value that loses that way does not go to L2 either. The call that read it still
  returns the value it read, as of that read. A value that loses is not counted in `Sets()`, replaces nothing (no
  `CauseReplacement`) and leaves the TTL of the one in memory alone. `GetAndRefresh` still replaces: that is its job.
- **A table that could not grow was filled to the last slot and then written past its end.** `EvictionPolicy.Evict`
  may answer "nothing to evict", and a custom policy that did so kept the shard over capacity; a preallocated table
  (or one at the size cap) then filled up, and the next new key walked past the last slot and was published outside
  the table - memory corruption, or an index panic. A table that can grow now grows before its last free slot is
  needed, whatever the growth threshold; one that cannot drops a new key silently while only one free slot is left,
  so every probe still has an empty slot to end at. An overwrite or a delete works as before.
- **A write could be expired at birth once every 2^18 clock units.** An expiry offset that wrapped onto 0 ("no TTL")
  was moved to 1, one unit later than asked. For a lifetime at the cap - a cache TTL of 131070-131071 s, or any
  `SetWithTTL` of 36 hours and more on a 1 s-unit cache - that put the offset past `ExpireMax`, which reads as
  expired: every such write during that one clock unit (once every ~73 hours at the 1 s unit) was lost at once. The
  wrap now moves the offset one unit earlier.
- **`RefreshTTLOnGet` skipped hits served by the slow read paths.** A read that overlaps a deletion's compaction
  re-probes, and after a second overlap reads under the shard mutex; both fallbacks granted the reference but left the
  expiry alone, so under steady deletes a hot key could expire while being read. Every hit path now extends the TTL.
- **A NaN key could lose other keys.** A key unequal to itself (a NaN float, or a struct or interface holding one)
  hashes differently on every call. Each `Set` stored another copy nobody could read; worse, when such an item sat
  far enough from its home that the table rehashes it to find the home, its removal decremented the overflow counter
  of a random group, and a live key there became unreachable. Such a key no longer enters memory, and its load is not
  registered for coalescing either, where a full bucket's overflow map could never delete it. Whether the key type
  can hold a NaN at all is decided once, at construction.
- **An expired item nothing read or evicted came back to life.** Expiry offsets sit on an 18-bit wrapping scale and
  expired items were removed only by a read that found them or by eviction, which runs only over capacity. An item
  left alone for `ExpireMax+2` clock units past its deadline (about 36 hours at the 1 s unit every TTL up to 36 hours
  gets) read as live again: `Get` served it, `Iterator` and `SaveTo` yielded it, and it flipped between dead and live
  every 36 hours from then on. The clock goroutine now sweeps each table for expired items, a small slice per clock
  unit: a lap takes under 32768 units, a quarter of the wrap. A process suspended for longer than the wrap can still
  meet such an item: 18 bits cannot tell.
- **A value too heavy for memory vanished from it without a trace.** An item heavier than its shard's share of
  `MemoryCapacity`/`MemoryBudget` (the limit is per shard, not per cache) was not stored, and for a new key nothing
  said so: no handler call, counted in `Sets()`, while every `Get` fetched it from L2, promoted it and dropped it again.
  `Stats.Rejected()` now counts the values memory did not take - these, and new keys refused by a table that cannot
  grow - and the capacity docs name the per-shard limit. `OnDeletion` still hears only about values that were in
  memory: the refused one is the caller's.
- **One value the codec rejected failed a whole L2 batch, and the error did not say which.** Every adapter returned
  from `BatchGet` at the first stored value that did not decode, and from `BatchSet` at the first value that did not
  encode - go-redis then sent nothing at all, redigo and Tarantool part of it. One bad entry in L2 (a changed value
  type, a corrupt write) failed every batch it was in, for every key; through `GetBatched` it failed unrelated
  concurrent reads, and a write-back batch lost the writes of up to 127 other keys. An adapter now leaves such an
  item out, carries on with the rest, and reports it as a `KeyError` naming the key (`l2.KeyErr`), joined with any
  others; Aerospike does the same for a single record's failure. The cache uses what a `BatchGet` did read alongside
  its error, and fails or reports only the keys a batch error names: a `GetBatched` caller whose key L2 read gets the
  value, one whose key is absent gets a plain miss, and `OnL2Error` hears only about the keys that failed. An error
  that names no key still concerns every key of the batch. **Contract change for adapters outside this repository:**
  values returned alongside a `BatchGet` error are now used, so return only keys that were read.
- **A value read from L2 or loaded could bring back a key deleted while it was read.** `Get`, `BatchGet`,
  `GetBatched`, `GetOrLoad`, `BatchGetOrLoad` and `GetAndRefresh` stored what L2 or the loader returned even when a
  `Delete` of that key landed while the read was out: the deleted value came back into memory and stayed until its
  TTL or eviction, while L2 no longer had it (a stress test resurrected about one in 2000 racing reads). Each shard now
  counts Deletes in eight stripes; a read notes its key's count before it goes out and stores its answer only while
  the count holds. A write-through `Delete` clears L2 before memory, so a read that starts after it cannot find the old
  value there either. With WriteBack the L2 delete is asynchronous: a read that reaches L2 before the worker does can
  still bring the old value back into memory. The count costs a `Delete` one atomic add, paid only once the cache
  can fill at all: from the start with an L2, from the first load without one.
- **An adapter that answered a key twice inflated the L2 counters.** `BatchGet` and `BatchGetOrLoad` counted every
  item the adapter returned as an L2 hit, so a duplicate or unasked item made `Gets()` exceed the keys read. The
  `L2Cache.BatchGet` contract now says so explicitly - only keys asked for, each at most once - and the counters no
  longer trust the answer's length: `BatchGetOrLoad` counts by key, `BatchGet` never more than the keys it asked for.
- **A custom eviction policy that named a bad victim could crash the process or corrupt its shard.** The cache
  trusted the index `Evict` returned: one past the table was dereferenced unchecked (a fault that kills the process,
  or a panic under the shard mutex that leaves the shard locked for good), and an empty slot was killed as if it held
  an item, so the shard undercounted its items and held more than its capacity. An index that names no live item now
  ends the eviction round, as "nothing to evict" does.
- **A corrupt snapshot could make `LoadFrom` allocate a gigabyte, and a long load could not be stopped.** A chunk
  length is bounded at 1 GiB, and `LoadFrom` allocated whatever the length claimed before reading a byte: a
  23-byte truncated file cost 1 GiB, two chunks per item twice that. Chunks past 64 KiB now grow as their bytes
  arrive. `ctx` now ends a load between items (before, it reached only the L2 writes), and an L2 write error comes
  back wrapped; the doc names every reason a load stops early, the items before it staying in the cache.
- **Three options did not do what their docs said.** `WithPreallocationRatio(0)` was documented to preallocate at
  the default ratio but did not preallocate at all; a `GrowthThreshold` below 1/8192 rounded to zero and grew the
  table on every insert, towards gigabytes; and a negative `ShardsCount`, `GhostSize` or buffer size quietly meant
  the default while negative worker counts were rejected. The ratio option now preallocates by itself, the growth
  line never rounds below one item in 4096 slots, and negative values are rejected (`ErrBadShardsCount`,
  `ErrBadGhostSize`, `ErrBadBufferSize`). `RefreshTTLOnGet` without a TTL is documented as having nothing to extend.
- **The load generator's hit rates were totals since start, not per interval.** `hit_rate_pct`, `mem_hit_rate_pct`
  and `l2_hit_rate_pct` came straight from the cache's counters while `ops_per_sec` and the CPU figures covered the
  interval since the previous line, and the snapshot kept for that window went unused; a shift in the working set
  showed up in the rates only as a slow drift. The rates now cover the interval too.
- **The load generator's stats counted its own probe.** The timing probe runs through the cache's own counters, half
  of its reads on keys nothing writes, so on a coarse clock it skewed the hit rates and totals of slow scenarios. The
  probe's window is now cut out of every figure taken from those counters; `ops_per_sec` and `errors_total` still
  cover the whole interval.
- **A load generator worker that stalled lost those ops for good.** A ticker drops the ticks a busy worker misses, so
  `ops_per_sec` could fall short of the target with capacity to spare. A worker now catches up on up to a second of
  backlog, and the start line carries the target as `target_rps`. An L1-only scenario no longer logs an `l2_client`.
- **The release targets left the load generator behind, and `release-check` passed without asking origin.** The
  importable `benchmarks/load_generator` module was neither tagged nor moved to the released core by
  `make require-core`, so after a release it would pin v0.9.7 forever; it is now tagged with every release and
  required at the same version as its core and adapters. `release-check` took "cannot reach origin" for "core not
  pushed yet" and passed; it now fails. `make push` refuses to run from any branch but main.

## [0.9.8] - 2026-08-13

### Fixed

- Every `l2/*_adapter` module required core `v0.9.6` while its code used APIs added in `v0.9.7` (`l2.ExtractKeyFunc`,
  `l2.ExtractOrderedMget`, `l2.ResolveOrderedMget`), so `go get` of any adapter at v0.9.7 resolved the core one
  version back and failed to compile. Adapters now require the core version they actually need.

## [0.9.7] - 2026-08-12

### Added

- **`GetBatched`** - `Get` that coalesces its L2 read with the ones other goroutines are making right then: a memory
  hit is unchanged, a miss joins one shared queue, and a worker pool fetches everything waiting there in a single
  `BatchGet`, so a key several callers want is read once. The caller blocks as in `Get`, and a full queue makes it
  wait rather than drop anything. Worth it where the adapter has no pipelining of its own (go-redis, `database/sql`)
  and callers outnumber its connection pool; with an auto-pipelining client plain `Get` stays ahead. Sized by
  **`WithGetBatchedBuffer`** (1024) and **`WithGetBatchedWorkers`** (4), started by the first call that reaches it.
- **`SetWithTTL`** - a lifetime for one entry. Needs a cache built with `WithTTL` (the expiry scale is fixed at
  construction), rounds the lifetime up to that scale's unit and caps it at the scale's range; the resulting lifetime
  goes to L2 as well. `WithRefreshTTLOnGet` still extends by the cache's own TTL, so a custom lifetime holds until the
  first read that refreshes it.
- **`ErrTTLDisabled`** - `SetWithTTL` on a cache without a TTL.
- **`l2/valkey_adapter`** - Valkey backend on [valkey-io/valkey-go](https://github.com/valkey-io/valkey-go), with the
  same batching as the rueidis adapter: MGET/MSET/DEL multi-key commands while a batch stays under the wire budget, a
  pipeline of per-key commands above it. Works against a standalone node or a cluster.
- **`l2.WithOrderedMget`** and **`memstash.DetectMode`** (`AutoDetect`/`Disabled`/`Enabled`) - let the Redis-family
  adapters read an MGET reply array by position instead of going through the key-to-reply map their client's
  multi-get helper builds. Valid only while every key of a batch reaches one server; adapters probe for that
  themselves through their new `IsOrderedMgetAvailable` method, so the default needs no configuration.

### Changed

- A shard's table is now allocated in chunks of 64Ki slots instead of one contiguous block, so a big cache no longer
  asks the allocator for a single multi-gigabyte run of memory. Slot indices, capacity and behavior are unchanged; a
  table that fits one chunk still gets exactly one allocation of its own size.
- The write-back worker carries a lifetime per queued write instead of always using the cache's TTL: batches now end
  where the lifetime changes, since `BatchSet` takes one per call. A restored snapshot's remaining lifetime reaches L2
  through the write-back path too, which it previously lost.
- `BatchGet` and `BatchDelete` on the go-redis and redispipe adapters map every key once and hand the result to
  whichever branch runs. A batch over the wire budget used to map its keys for the multi-key command, drop them, and
  map them all a second time for the pipeline - so with a prefixing key function it paid two string allocations per
  key. On redispipe the discarded copy was boxed as well.

### Fixed

- The write-back worker of a key is now picked with a remainder instead of a multiply-shift, so any `WriteBackWorkers`
  count spreads evenly by construction. It still divides the hash bits the shard index did not use.
- The rueidis and valkey adapters send oversized pipelines on a dedicated connection. Both clients auto-pipeline
  onto shared connections and only split off a pipeline past `ClientOption.BlockingPipeline`, which counts commands -
  a write-back batch of large values stays far below that limit yet holds the connection long enough to stall
  reads behind it. Cluster clients keep the shared pipeline (cross-slot batches can't be dedicated). Pool-based adapters
  (go-redis, redigo) take a connection per pipeline and were never affected.

### Removed

- **`l2/valyala_adapter`** - the memcached backend on [valyala/ybc](https://github.com/valyala/ybc). The client needs
  cgo and hasn't been touched upstream since 2018; the three pure-Go memcached adapters cover the same backend.

## [0.9.6] - 2026-07-31

### Added

- **`WithPreallocatedSize` / `Config.PreallocateMap`** - pre-allocate an FlatHashMap for every shard at construction,
to avoid it grows and memory allocations on Set at all.

### Changed

- Revert a Stats micro-refactoring that prevented inlining and added (embarrassingly, unforgivable) 0.06 ns to Get.

## [0.9.5] - 2026-07-30

Add GetAndRefresh, panic recovery, faster loads and deletes.

### Added

- **`GetAndRefresh` / `BatchGetAndRefresh`** - serve the cached value, even past its TTL, and reload the key in the
  background. The reload follows WritePolicy into L2.
- **`WithPanicHandler` / `Config.OnPanic`** - a panic on one of the cache's goroutines or inside a loader no longer
  ends the process, it reaches this handler. `handled` says the panic also went out as an error; under a synchronous
  `GetOrLoad` it still reaches the caller.
- **`Error`** - the base sentinel every package error wraps, so one `errors.Is` tells memstash errors from the rest.
  `ErrPanic` and `ErrLoaderPanic` among them.
- Benchmarks: `BenchmarkDelete` / `BenchmarkDeleteMiss`, etc.

### Changed

- **Singleflight optimizations**: `GetOrLoad`, `BatchGetOrLoad`, `BatchGetAndRefresh` are now cheaper.
- **Cheaper `Delete`**: deleting an absent key is nearly free, and re-inserting a deleted key no longer degrades
  lookups until the next rebuild. `BatchDelete` too.
- Dependency updates across the adapter modules.

### Fixed

- `BatchGetOrLoad` panicked when an L2 adapter answered with duplicate keys, or with keys nobody asked for.
- `BatchGetOrLoad` dropped the loader's partial answer when the loader also returned an error.
- A loader answering out of order matched its keys in quadratic time.
- `Wait` never returned if a write-back delivery panicked mid-run.

## [0.9.4] - 2026-07-27

Faster core, snapshots, removal events.

### Added

- **`SaveTo` / `LoadFrom`** - dump the first level to an `io.Writer` and load it back, with options to restore the
  original expirations (`LoadWithTTL`) and to mirror into L2 (`LoadToL2`).
- **`WithOnDeletion`** - a handler for every item leaving the first level, with the cause.
- Project plumbing: CI, golangci-lint config, pkg.go.dev examples, contribution guide, security policy, templates.

### Changed

- **Core rewrite: the table slots are the items themselves** - a hit resolves in one lookup, with no separate slot
  table and no item-state pool.
- **`LoadableCache` embeds `*Cache`**: every cache method is available on it directly.
- Integration-test services moved to ports 43210-43220.

### Fixed

- A generation wrapping to 0 made a live item read as an empty slot, hiding every key stored behind it.
- `Delete` ignored `WritePolicy` and always wrote to L2 synchronously.
- Benchmarks: Ristretto ran at 1.75% of its nominal capacity, which is what its low hit rate measured.

### Removed

- `LoadableCache.Cache()` - use the embedded field, `lc.Cache`.

## [0.9.3] - 2026-07-23

Faster reads, zero-alloc writes, batched memory reads.

### Added

- **`BatchGetFromMemory` / `BatchGetFromMemoryWithMissing`** - read many keys from L1 in a single synchronous call,
  appending hits into a caller-provided `List` (reuse it to keep the read path allocation-free). The `WithMissing`
  variant also returns the keys that missed.

### Changed

- **Allocation-free `Set`** and a leaner read path, from a hot-path rework: raw-pointer hash slots, a single unified
  item state across platforms, and other small optimizations.
- New realistic random-load and batch-read benchmarks.

### Removed

- Internal: the boxed item state and the inc-stats path.

## [0.9.2] - 2026-07-17

Inline entries, lower memory footprint.

### Added

- **Per-tier stats counters**: `MemoryHits`, `L2Hits`, `L2Gets`, `MemoryHitRate` and `L2HitRate` report which tier
  answered a read.

### Changed

- **An item's Entry lives inside its state record** instead of behind a pointer to a separately allocated box. Same
  API, same behaviour, ~18% less memory: 46.59 B/entry against 56.59 on 10M `uint64 -> uint64` entries, and filling
  the cache got ~30% faster because a `Set` no longer allocates per write.

## [0.9.1] - 2026-07-14

Rearchitected core, pluggable eviction, faster L2 adapters.

### Added

- **Pluggable eviction policies**: the public `EvictionPolicy` interface plus `WithCustomEvictionPolicy` for supplying
  your own.
- **`PolicySIEVE`** (NSDI'24) - a single insertion-ordered list with a hand that evicts unvisited items in place.
- **`PolicyWTinyLFU`** adapted for lock-free reads - a small admission window in front of a GCLOCK main store.
- `Stats()` for hit/miss and eviction metrics.
- `Iterator()` - range over the cached entries without taking a full snapshot.
- `BatchDelete` - remove several keys in one call.

### Changed

- **The core was rebuilt from the ground up**: item state, storage and eviction bookkeeping, radically cutting the
  per-entry memory footprint while staying allocation-free and lock-free on the hot path.
- L2 adapters: batching and pipelining paths tightened across all of them (goredis, redigo, redispipe, rueidis, sql,
  pgx, dynamo and the rest). Every `l2/*_adapter` submodule was bumped to `v0.9.1` to match the updated interface.

## [0.9.0] - 2026-07-07

First tagged release.

[0.9.7]: https://github.com/zakonnic/memstash/compare/v0.9.6...v0.9.7
[0.9.6]: https://github.com/zakonnic/memstash/compare/v0.9.5...v0.9.6
[0.9.5]: https://github.com/zakonnic/memstash/compare/v0.9.4...v0.9.5
[0.9.4]: https://github.com/zakonnic/memstash/compare/v0.9.3...v0.9.4
[0.9.3]: https://github.com/zakonnic/memstash/compare/v0.9.2...v0.9.3
[0.9.2]: https://github.com/zakonnic/memstash/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/zakonnic/memstash/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/zakonnic/memstash/releases/tag/v0.9.0
