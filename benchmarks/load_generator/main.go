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
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/zakonnic/memstash"
)

// defaultRedisClusterAddr is the docker-compose Redis cluster; scenarios 2 and 3 use it unless config overrides.
const defaultRedisClusterAddr = "127.0.0.1:43211,127.0.0.1:43212,127.0.0.1:43213"

func main() {
	logDir := flag.String("log-dir", ".", "directory to write the per-scenario JSON-lines log files into")
	configPath := flag.String("config", "config.yaml", "optional YAML file overriding per-scenario defaults")
	selfTest := flag.Bool("selftest", false,
		"write one synthetic error of every kind and exit - checks the console and errors.log without running any load")
	flag.Parse()

	if err := os.MkdirAll(*logDir, 0o755); err != nil {
		log.Fatalf("cannot create log dir %s: %v", *logDir, err)
	}

	// Everything printed goes through the console, so nothing lands in the middle of the status block.
	con := newConsole(os.Stdout)
	log.SetOutput(con.writer())

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("cannot load config %s: %v", *configPath, err)
	}

	errPath := filepath.Join(*logDir, "errors.log")
	errLog, err := newErrorLog(errPath, con)
	if err != nil {
		log.Fatalf("cannot open errors.log: %v", err)
	}
	defer errLog.Close()
	defer logMainPanic(errLog) // registered after Close, so it runs first and still has the file

	if *selfTest {
		runSelfTest(con, errLog)
		log.Printf("self test done: %d error(s) written to %s", errLog.count.Load(), errPath)
		return
	}

	log.Println("building scenarios and their source-of-truth maps...")
	scenarios, err := buildScenarios(*logDir, cfg, errLog, con)
	if err != nil {
		log.Fatalf("cannot build scenarios: %v", err)
	}

	printScenarios(scenarios)

	truthHeap := measureTruthHeap(scenarios)
	log.Printf("source-of-truth maps and value blob hold %s of heap - the price of verifying every Get. "+
		"The stats report heap_alloc_bytes with it already subtracted; add "+
		"the two together for what the process really holds.", humanize.IBytes(uint64(truthHeap)))

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
	log.Printf("stopped; %d error(s) logged to %s", errLog.count.Load(), errPath)
}

// logMainPanic gives a panic that unwound all the way out of main a line in errors.log before the runtime prints it
// and kills the process. Must be deferred directly.
func logMainPanic(errLog *errorLog) {
	if r := recover(); r != nil {
		errLog.panicked("", "main", r)
		panic(r)
	}
}

func measureTruthHeap(scenarios []*scenario) int64 {
	runtime.GC()
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	truthHeap := int64(mem.HeapAlloc)
	for _, s := range scenarios {
		s.truthHeap = truthHeap
	}
	return truthHeap
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
		fmt.Printf("  key space:       %d (Zipf s=%.2f v=%.2f)\n", s.keySpace, s.zipfS, s.zipfV)
		fmt.Printf("  write key space: %d\n", s.writeKeySpace)
		fmt.Printf("  uniform keys:    %s\n", randomDisplay(s))
		fmt.Printf("  log file:        %s\n", s.logPath)
	}
	fmt.Println()
}

func randomDisplay(s *scenario) string {
	if s.randomPercent <= 0 {
		return "none (Zipf only, the tail stays cold)"
	}
	return fmt.Sprintf("%d%% of ops, a given key every %s, all keys in %s",
		s.randomPercent, s.randomPeriod().Round(time.Second), s.randomCover().Round(time.Minute))
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

func buildScenarios(logDir string, cfg fileConfig, errLog *errorLog, con *console) ([]*scenario, error) {
	var scenarios []*scenario
	// fail releases whatever was already built - the caches hold background goroutines and a Redis client each.
	fail := func(err error) ([]*scenario, error) {
		closeAll(scenarios)
		return nil, err
	}

	// Scenario 1: web sessions, L1 only by default. Zipf reads over a key space ~7.5x L1 capacity.
	size1, err := effectiveSize(cfg, "scenario-1", 20_000)
	if err != nil {
		return fail(err)
	}
	s1 := &scenario{
		name: "scenario-1",
		description: "Web-session store (workload.SessionScenario, ~170-490 B JSON documents). Read-heavy, Zipf-skewed: " +
			"L1 holds the hot head, the tail misses or goes to L2. The parameters below are the effective ones.",
		cacheSize:     size1,
		goroutines:    10,
		rps:           evenSplit(10, 10_000),
		readPercent:   90,
		keySpace:      150_000,
		writeKeySpace: 150_000,
		zipfS:         1.01,
		zipfV:         1,
		value:         sessionValue,
		errLog:        errLog,
		logPath:       filepath.Join(logDir, "scenario-1.log"),
	}
	scenarios = append(scenarios, s1)
	if err := s1.buildCache(effectiveRedisAddress(cfg, "scenario-1", ""), memstash.WithMemoryCapacity(size1)); err != nil {
		return fail(err)
	}

	// Scenario 2: CDN assets, Redis cluster L2 by default. Zipf, balanced read/write, key space ~7.5x L1 capacity.
	size2, err := effectiveSize(cfg, "scenario-2", 20_000)
	if err != nil {
		return fail(err)
	}
	s2 := &scenario{
		name: "scenario-2",
		description: "CDN / static assets (workload.CDNScenario, bimodal 0.6-64 KiB blobs, ~7.4 KiB average). " +
			"Zipf-skewed, balanced read/write: L1 holds the hot head, L2 serves the tail.",
		cacheSize:     size2,
		goroutines:    5,
		rps:           evenSplit(5, 10_000),
		readPercent:   50,
		keySpace:      150_000,
		writeKeySpace: 150_000,
		zipfS:         1.01,
		zipfV:         1.2,
		value:         cdnValue,
		errLog:        errLog,
		logPath:       filepath.Join(logDir, "scenario-2.log"),
	}
	scenarios = append(scenarios, s2)
	seeds2 := effectiveRedisAddress(cfg, "scenario-2", defaultRedisClusterAddr)
	if err := s2.buildCache(seeds2, memstash.WithMemoryCapacity(size2)); err != nil {
		return fail(err)
	}

	// Scenario 3: DB rows, byte-weighted L1 (CostFunc). ~10 MB budget holds ~36k rows; key space ~7x that. Zipf,
	// read-heavy. Redis cluster L2.
	size3, err := effectiveSize(cfg, "scenario-3", 10_000_000)
	if err != nil {
		return fail(err)
	}
	rps3 := make([]float64, 40)
	rps3[0] = 10_000
	rest := evenSplit(39, 30_000)
	copy(rps3[1:], rest)
	s3 := &scenario{
		name: "scenario-3",
		description: "DB row cache (workload.DBScenario, ~250 B serialized rows, ~270 B per item with the key). " +
			"Read-heavy Zipf point lookups plus uniformly drawn rows that ignore the popularity curve, over a " +
			"byte-weighted L1 (CostFunc), so its item count follows the value sizes; L2 serves the tail. The first " +
			"goroutine is the hot one, the rest share the remaining rps. The parameters below are the effective ones.",
		cacheSize:     size3,
		goroutines:    40,
		rps:           rps3,
		readPercent:   90,
		keySpace:      200_000,
		writeKeySpace: 200_000,
		zipfS:         1.1,
		zipfV:         1.5,
		randomPercent: 5, // the traffic that does not follow the popularity curve: batch jobs, crawlers, cold reads
		value:         dbValue,
		errLog:        errLog,
		logPath:       filepath.Join(logDir, "scenario-3.log"),
	}
	scenarios = append(scenarios, s3)
	seeds3 := effectiveRedisAddress(cfg, "scenario-3", defaultRedisClusterAddr)
	err = s3.buildCache(seeds3,
		memstash.WithMemoryCapacity(size3),
		memstash.WithCostFunc(func(k string, v []byte) uint32 { return uint32(len(k) + len(v)) }),
	)
	if err != nil {
		return fail(err)
	}

	// Overrides can change writeKeySpace, so apply them before filling each source of truth.
	for i, s := range scenarios {
		s.console, s.slot = con, i
		if override, ok := cfg.Scenarios[s.name]; ok {
			applyOverride(s, override)
		}
		if err := validateScenario(s); err != nil {
			return fail(err)
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
