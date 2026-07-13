# Integration tests and L1+L2 benchmarks

A separate Go module: pulls in all adapters and their client SDKs without polluting the core's dependencies.

## Running

```bash
# from the repository root
make up      # docker compose up -d --wait, with docker/docker-compose.override.yml merged in when present

go -C tests/integration test ./... -v            # tests (one file per adapter, shared scenario suite)
go -C tests/integration test ./... -short        # skip the slow TTL scenario
go -C tests/integration test -run xxx -bench . -benchtime 3s   # benchmarks

make down
```

Server addresses come from `MEMSTASH_TEST_<SERVER>_ADDR` environment variables (REDIS, REDIS_CLUSTER_ADDRS -
comma-separated seeds, MEMCACHED, POSTGRES, MYSQL, MONGO, DYNAMO, AEROSPIKE, TARANTOOL), with defaults matching
docker/docker-compose.yml: `127.0.0.1:6379`, `7001-7003`, `11211`, `5432`, `3306`, `27017`, `8000`, `3000`, `3301`.
Set them when remapping ports through docker-compose.override.yml (see the .example next to the compose file). If a
server isn't listening, the corresponding tests are **skipped**, not failed: a partial environment is useful on its
own. The redis cluster suite runs the cluster-capable adapters (rueidis, go-redis, redispipe) against the 3-master
cluster; redigo is single-node only.

`valyala_test.go` only builds with cgo enabled (the ybc client needs a C compiler). If there's no C compiler on the
system, run with `CGO_ENABLED=0` — the valyala file is excluded from the build and the other adapters are unaffected:

```bash
CGO_ENABLED=0 go -C tests/integration test ./... -v
```

## Scenario suite (runSuite, for each adapter)

- `SetGetDelete` — the basic contract through both tiers;
- `PromotionFromL2` — a second cache instance finds the value in L2 and promotes it into its own L1;
- `DeletePropagatesToL2` — a delete reaches the backend;
- `WriteBackFlushOnClose` — the async write doesn't lose data on Close;
- `WriteBackVisibleAfterWait` — default WriteBack: `Wait()` is a delivery checkpoint into L2 without stopping the cache (checked from a separate goroutine);
- `BatchSetGet` — the adapters' native batch paths (pipeline / DoMulti / SendMany / GetMulti): a single BatchSet, a mixed BatchGet (L1 + L2 + miss) with promotion;
- `EvictedItemsServedFromL2` — the target mode: a small L1 + a large TTL, keys evicted from memory are transparently read from L2 with promotion back;
- `GetOrLoadPrefersL2` — the loader isn't called when the value is already in L2;
- `TTLReachesL2` — the cache's TTL is faithfully passed to the backend (slow, skipped under -short);
- `ConcurrentMixed` — mixed concurrent load on both tiers.

Each subtest runs in its own key namespace (`l2.PrefixedString`), so tests and repeated runs don't see each other's
data and don't require cleaning up the servers.

## Benchmarks (real-world load profiles, L1 ≈ 10% of the keyspace)

| Benchmark | Profile |
|---|---|
| `BenchmarkZipfReadThrough` | read-through with a Zipf distribution: a hot head served from L1, a tail from the network; `l1hit-ratio` metric |
| `BenchmarkSessionReadMostly` | session store: 90% reads / 10% write-through writes, uniform access (worst case for L1) |
| `BenchmarkWriteBackIngest` | 100% writes with async WriteBack (metrics/counters); buffer flush happens outside the timer |
| `BenchmarkWriteThroughIngest` | the same writes done synchronously — a baseline showing the cost of the network round trip on Set |
| `BenchmarkGetOrLoadZipf` | read-through via GetOrLoad with singleflight; `loader-calls` metric (should be ~0) |

Benchmarks are run against the canonical clients of both backends (go-redis and bradfitz/gomemcache) — the adapters
differ only in protocol wiring, so the backend-level numbers are transferable.
