// Command load_generator runs three independent, continuously-running load scenarios against three separate
// memstash caches in parallel, until interrupted (Ctrl+C / SIGTERM). Each scenario logs a memory/CPU/throughput
// snapshot once a minute to its own JSON-lines log file (scenario-1.log, scenario-2.log, scenario-3.log) in
// -log-dir. Any of the built-in defaults can be overridden per scenario via -config (config.yaml).
//
// Build:
//
//	make load-generator
//
// Run:
//
//	./benchmarks/bin/load-generator -log-dir ./benchmarks/load_generator
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/zakonnic/memstash"
)

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

	scenarios, err := buildScenarios(*logDir, cfg)
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

// printScenarios prints a human-readable description of every scenario and the parameters it is actually running
// with (i.e. after config.yaml overrides have been applied), so the console output can be checked against intent.
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
		fmt.Printf("  goroutines:      %d\n", s.goroutines)
		fmt.Printf("  rps (total):     %.0f\n", total)
		fmt.Printf("  read / write:    %d%% Get / %d%% Set\n", s.readPercent, 100-s.readPercent)
		fmt.Printf("  key space:       %d\n", s.keySpace)
		fmt.Printf("  write key space: %d\n", s.writeKeySpace)
		fmt.Printf("  log file:        %s\n", s.logPath)
	}
	fmt.Println()
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

func buildScenarios(logDir string, cfg fileConfig) ([]*scenario, error) {
	size1, err := effectiveSize(cfg, "scenario-1", memstash.DefaultMemoryCapacity)
	if err != nil {
		return nil, err
	}
	cache1, err := memstash.New[string, []byte](memstash.WithMemoryCapacity(size1))
	if err != nil {
		return nil, err
	}
	s1 := &scenario{
		name: "scenario-1",
		description: "10 goroutines, 10% Set / 90% Get, 10k rps total, default cache size. The write key space is " +
			"half the read key space, so ~half of all Gets target keys that were never Set (guaranteed misses by " +
			"design, on top of whatever eviction does).",
		cache:         cache1,
		cacheSize:     size1,
		goroutines:    10,
		rps:           evenSplit(10, 10_000),
		readPercent:   90,
		keySpace:      200_000,
		writeKeySpace: 100_000,
		newValue:      randomPayload,
		logPath:       filepath.Join(logDir, "scenario-1.log"),
	}

	size2, err := effectiveSize(cfg, "scenario-2", memstash.DefaultMemoryCapacity)
	if err != nil {
		return nil, err
	}
	cache2, err := memstash.New[string, []byte](memstash.WithMemoryCapacity(size2))
	if err != nil {
		return nil, err
	}
	s2 := &scenario{
		name: "scenario-2",
		description: "5 goroutines, 50% Set / 50% Get, 10k rps total, default cache size. Only 5% of the read key " +
			"space is ever written, so the vast majority of Gets miss - a small hot write set under heavy churn.",
		cache:         cache2,
		cacheSize:     size2,
		goroutines:    5,
		rps:           evenSplit(5, 10_000),
		readPercent:   50,
		keySpace:      200_000,
		writeKeySpace: 10_000,
		newValue:      randomPayload,
		logPath:       filepath.Join(logDir, "scenario-2.log"),
	}

	size3, err := effectiveSize(cfg, "scenario-3", 1_000_000)
	if err != nil {
		return nil, err
	}
	cache3, err := memstash.New[string, []byte](
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
		description: "40 goroutines, 10% Set / 90% Get, all keys are written (no artificial miss space), weighted " +
			"capacity of 1,000,000 bytes via a byte-length CostFunc, JSON-encoded User values. One goroutine alone " +
			"drives 10k rps; the remaining 39 share 30k rps (~769 rps each) for 40k rps total.",
		cache:         cache3,
		cacheSize:     size3,
		goroutines:    40,
		rps:           rps3,
		readPercent:   90,
		keySpace:      1_000_000,
		writeKeySpace: 1_000_000,
		newValue:      randomUserJSON,
		logPath:       filepath.Join(logDir, "scenario-3.log"),
	}

	scenarios := []*scenario{s1, s2, s3}
	for _, s := range scenarios {
		if override, ok := cfg.Scenarios[s.name]; ok {
			if err := applyOverride(s, override); err != nil {
				return nil, err
			}
		}
	}
	return scenarios, nil
}
