// Command load_generator drives three memstash caches under continuous, independent load until interrupted
// (Ctrl+C / SIGTERM), writing a per-scenario stats snapshot once a minute. Values come from the workload package
// and are verified against a source-of-truth map after every Get; errors land in errors.log. Scenarios, their
// Redis L2, and every knob are configurable via config.yaml (see buildScenarios for the built-in defaults).
//
// Build with `make load-generator`; run `./benchmarks/bin/load-generator -log-dir <dir>`.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	rueidislib "github.com/redis/rueidis"

	"github.com/zakonnic/memstash"
	rueidis_adapter "github.com/zakonnic/memstash/l2/rueidis_adapter"
)

// defaultRedisClusterAddr is the docker-compose Redis cluster; scenarios 2 and 3 use it unless config overrides.
const defaultRedisClusterAddr = "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003"

func main() {
	logDir := flag.String("log-dir", ".", "directory to write the per-scenario JSON-lines log files into")
	configPath := flag.String("config", "config.yaml", "optional YAML file overriding per-scenario defaults")
	flag.Parse()

	if err := os.MkdirAll(*logDir, 0o755); err != nil {
		log.Fatalf("cannot create log dir %s: %v", *logDir, err)
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("cannot load config %s: %v", *configPath, err)
	}

	errLog, err := newErrorLog(filepath.Join(*logDir, "errors.log"))
	if err != nil {
		log.Fatalf("cannot open errors.log: %v", err)
	}
	defer errLog.Close()

	log.Println("building scenarios and their source-of-truth maps...")
	scenarios, err := buildScenarios(*logDir, cfg, errLog)
	if err != nil {
		log.Fatalf("cannot build scenarios: %v", err)
	}

	printScenarios(scenarios)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	for _, s := range scenarios {
		wg.Add(1)
		go s.run(ctx, &wg)
	}

	log.Printf("load generator running %d scenario(s), logging to %s once a minute; press Ctrl+C to stop", len(scenarios), *logDir)
	<-ctx.Done()
	log.Println("shutting down, flushing final stats...")
	wg.Wait()
	log.Println("stopped")
}

// printScenarios prints each scenario's effective parameters (after config overrides) to stdout.
func printScenarios(scenarios []*scenario) {
	fmt.Println("Scenarios:")
	for _, s := range scenarios {
		total := 0.0
		for _, r := range s.rps {
			total += r
		}
		fmt.Printf("\n[%s]\n", s.name)
		fmt.Printf("  %s\n", s.description)
		fmt.Printf("  cache size:      %d\n", s.cacheSize)
		fmt.Printf("  redis (L2):      %s\n", redisDisplay(s.redisAddr))
		fmt.Printf("  goroutines:      %d\n", s.goroutines)
		fmt.Printf("  rps (total):     %.0f\n", total)
		fmt.Printf("  read / write:    %d%% Get / %d%% Set\n", s.readPercent, 100-s.readPercent)
		fmt.Printf("  key space:       %d (Zipf s=%.2f)\n", s.keySpace, s.zipfS)
		fmt.Printf("  write key space: %d\n", s.writeKeySpace)
		fmt.Printf("  log file:        %s\n", s.logPath)
	}
	fmt.Println()
}

func redisDisplay(seeds []string) string {
	if len(seeds) == 0 {
		return "none (L1 only)"
	}
	return strings.Join(seeds, ",")
}

// evenSplit divides totalRPS evenly across n workers.
func evenSplit(n int, totalRPS float64) []float64 {
	rps := make([]float64, n)
	per := totalRPS / float64(n)
	for i := range rps {
		rps[i] = per
	}
	return rps
}

// buildCache builds a scenario's cache: L1-only when seeds is empty, else two-level over a rueidis client (which is
// then non-nil and must be closed after the cache).
func buildCache(seeds []string, opts ...memstash.Option) (*memstash.Cache[string, []byte], rueidislib.Client, error) {
	opts = append(opts, memstash.WithStats()) // the monitor reports the cache's own counters
	if len(seeds) == 0 {
		c, err := memstash.New[string, []byte](opts...)
		return c, nil, err
	}
	client, err := rueidislib.NewClient(rueidislib.ClientOption{InitAddress: seeds})
	if err != nil {
		return nil, nil, fmt.Errorf("dial redis %v: %w", seeds, err)
	}
	c, err := rueidis_adapter.NewBytesCache[string](client, opts...)
	if err != nil {
		client.Close()
		return nil, nil, err
	}
	return c, client, nil
}

func buildScenarios(logDir string, cfg fileConfig, errLog *errorLog) ([]*scenario, error) {
	// Scenario 1: web sessions, L1 only by default. Zipf reads over a key space ~7.5x L1 capacity.
	size1, err := effectiveSize(cfg, "scenario-1", 20_000)
	if err != nil {
		return nil, err
	}
	seeds1 := effectiveRedisAddress(cfg, "scenario-1", "")
	cache1, client1, err := buildCache(seeds1, memstash.WithMemoryCapacity(size1))
	if err != nil {
		return nil, err
	}
	s1 := &scenario{
		name: "scenario-1",
		description: "Web-session store (workload.SessionScenario, ~350-650 B JSON documents). Read-heavy, Zipf-skewed " +
			"over a key space ~7.5x L1 capacity: L1 holds the hot head, the tail misses (L1 only, no L2).",
		cache:         cache1,
		cacheSize:     size1,
		redisClient:   client1,
		redisAddr:     seeds1,
		goroutines:    10,
		rps:           evenSplit(10, 10_000),
		readPercent:   90,
		keySpace:      150_000,
		writeKeySpace: 150_000,
		zipfS:         1.1,
		value:         sessionValue,
		errLog:        errLog,
		logPath:       filepath.Join(logDir, "scenario-1.log"),
	}

	// Scenario 2: CDN assets, Redis cluster L2 by default. Zipf, balanced read/write, key space ~7.5x L1 capacity.
	size2, err := effectiveSize(cfg, "scenario-2", 20_000)
	if err != nil {
		return nil, err
	}
	seeds2 := effectiveRedisAddress(cfg, "scenario-2", defaultRedisClusterAddr)
	cache2, client2, err := buildCache(seeds2, memstash.WithMemoryCapacity(size2))
	if err != nil {
		return nil, err
	}
	s2 := &scenario{
		name: "scenario-2",
		description: "CDN / static assets (workload.CDNScenario, bimodal 0.6-64 KiB blobs). Zipf-skewed, balanced " +
			"read/write over a key space ~7.5x L1 capacity: L1 holds the hot head, L2 serves the tail. Redis L2.",
		cache:         cache2,
		cacheSize:     size2,
		redisClient:   client2,
		redisAddr:     seeds2,
		goroutines:    5,
		rps:           evenSplit(5, 10_000),
		readPercent:   50,
		keySpace:      150_000,
		writeKeySpace: 150_000,
		zipfS:         1.1,
		value:         cdnValue,
		errLog:        errLog,
		logPath:       filepath.Join(logDir, "scenario-2.log"),
	}

	// Scenario 3: DB rows, byte-weighted L1 (CostFunc). ~10 MB budget holds ~28k rows; key space ~7x that. Zipf,
	// read-heavy. Redis cluster L2.
	size3, err := effectiveSize(cfg, "scenario-3", 10_000_000)
	if err != nil {
		return nil, err
	}
	seeds3 := effectiveRedisAddress(cfg, "scenario-3", defaultRedisClusterAddr)
	cache3, client3, err := buildCache(seeds3,
		memstash.WithMemoryCapacity(size3),
		memstash.WithCostFunc(func(k string, v []byte) uint32 { return uint32(len(k) + len(v)) }),
	)
	if err != nil {
		return nil, err
	}
	rps3 := make([]float64, 40)
	rps3[0] = 10_000
	rest := evenSplit(39, 30_000)
	copy(rps3[1:], rest)
	s3 := &scenario{
		name: "scenario-3",
		description: "DB row cache (workload.DBScenario, ~300-380 B serialized rows). Read-heavy, Zipf-skewed, " +
			"byte-weighted L1 whose ~10 MB budget holds ~28k of the ~200k-key space; L2 serves the tail. Redis L2. " +
			"One goroutine drives 10k rps; the rest share 30k rps.",
		cache:         cache3,
		cacheSize:     size3,
		redisClient:   client3,
		redisAddr:     seeds3,
		goroutines:    40,
		rps:           rps3,
		readPercent:   90,
		keySpace:      200_000,
		writeKeySpace: 200_000,
		zipfS:         1.1,
		value:         dbValue,
		errLog:        errLog,
		logPath:       filepath.Join(logDir, "scenario-3.log"),
	}

	scenarios := []*scenario{s1, s2, s3}
	// Overrides can change writeKeySpace, so apply them before filling each source of truth.
	for _, s := range scenarios {
		if override, ok := cfg.Scenarios[s.name]; ok {
			if err := applyOverride(s, override); err != nil {
				closeAll(scenarios)
				return nil, err
			}
		}
	}
	for _, s := range scenarios {
		s.fillTruth()
	}
	return scenarios, nil
}

// closeAll releases caches and Redis clients when construction aborts partway.
func closeAll(scenarios []*scenario) {
	for _, s := range scenarios {
		if s.cache != nil {
			s.cache.Close()
		}
		if s.redisClient != nil {
			s.redisClient.Close()
		}
	}
}
