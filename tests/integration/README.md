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

## Benchmarks (realistic traces, every backend)

The workloads are scaled-down copies of the realistic workload scenarios, replayed cache-aside
(L1 → L2 → miss stores the deterministic value, write-back drains in the background). Keyspaces are sized so even a backend at tens of ms per L2 Get replays a meaningful slice of the
trace within one `-benchtime` window; `make integration-bench` covers all backends in a few minutes.

| Benchmark | Profile |
|---|---|
| `BenchmarkWebSessions` | Zipf over 20k session tokens, ~350-650 B JSON documents |
| `BenchmarkCDNAssets` | 75% Zipf over an 8k catalogue + 25% one-hit wonders, bimodal 0.6-32 KB values |
| `BenchmarkDBRows` | Zipf point lookups over 20k rows with a recurring 2k-row sequential scan |
| `BenchmarkAdapterBatch*` | raw adapter BatchGet/BatchSet paths on the redis adapters, three size profiles |

Each scenario runs on every adapter/backend pair. Clients are dialed once per
process and shared; one live cache per scenario+backend persists across the ramp-up runs, so the numbers describe a
converged warm cache (`l1hit-ratio`/`l2hit-ratio` are reported next to ns/op).
