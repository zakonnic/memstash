package main

import (
	"bytes"
	"time"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/benchmarks/load_generator"
)

// bytesScenario is what all three built-ins are: string keys, []byte values. Another pair of types would work just
// as well - the engine builds a memstash.Cache[K, V] out of whatever the scenario is instantiated with.
type bytesScenario = load_generator.Scenario[string, []byte]

// buildScenarios returns the three built-in scenarios with config.yaml's overrides applied. Write your own set like
// this one and hand it to load_generator.New - nothing here is privileged.
func buildScenarios(cfg fileConfig) ([]bytesScenario, error) {
	// Scenario 3 is byte-weighted: its capacity counts bytes, so its item count follows the value sizes.
	byteWeighted := memstash.WithCostFunc(func(k string, v []byte) uint32 { return uint32(len(k) + len(v)) })

	// The first goroutine of scenario 3 is the hot one, the rest share the remaining rps.
	rps3 := make([]float64, 40)
	rps3[0] = 10_000
	copy(rps3[1:], load_generator.EvenSplit(39, 30_000))

	scenarios := []bytesScenario{
		{
			// Web sessions, L1 only by default. Zipf reads over a key space ~7.5x L1 capacity.
			Name: "scenario-1",
			Description: "Web-session store (workload.SessionScenario, ~170-490 B JSON documents). Read-heavy, " +
				"Zipf-skewed: L1 holds the hot head, the tail misses or goes to L2. The parameters below are the " +
				"effective ones.",
			CacheSize:     100_000,
			Goroutines:    10,
			RPS:           load_generator.EvenSplit(10, 1_000_000),
			ReadPercent:   90,
			KeySpace:      100_000_000,
			WriteKeySpace: 100_000_000,
			ZipfS:         1.01,
			ZipfV:         1,
			Value:         load_generator.SessionValue,
			Equal:         bytes.Equal,
		},
		{
			// CDN assets, Redis cluster L2 by default. Zipf, balanced read/write, key space ~7.5x L1 capacity.
			Name: "scenario-2",
			Description: "CDN / static assets (workload.CDNScenario, bimodal 0.6-64 KiB blobs, ~7.4 KiB average). " +
				"Zipf-skewed, balanced read/write: L1 holds the hot head, L2 serves the tail.",
			CacheSize:     20_000,
			CacheOptions:  []memstash.Option{memstash.WithTTL(10 * time.Minute)},
			L2ClientType:  load_generator.Rueidis,
			Address:       seeds("127.0.0.1:43211,127.0.0.1:43212,127.0.0.1:43213"),
			Goroutines:    5,
			RPS:           load_generator.EvenSplit(5, 10_000),
			ReadPercent:   50,
			KeySpace:      150_000,
			WriteKeySpace: 150_000,
			ZipfS:         1.01,
			ZipfV:         1.2,
			Value:         load_generator.CDNValue,
			Equal:         bytes.Equal,
		},
		{
			// DB rows over a byte-weighted L1. ~10 MB budget holds ~36k rows; key space ~7x that.
			Name: "scenario-3",
			Description: "DB row cache (workload.DBScenario, ~250 B serialized rows, ~270 B per item with the key). " +
				"Read-heavy Zipf point lookups plus uniformly drawn rows that ignore the popularity curve, over a " +
				"byte-weighted L1 (CostFunc), so its item count follows the value sizes; L2 serves the tail. The " +
				"first goroutine is the hot one, the rest share the remaining rps. The parameters below are the " +
				"effective ones.",
			CacheSize:     10_000_000,
			CacheOptions:  []memstash.Option{byteWeighted, memstash.WithTTL(time.Hour)},
			L2ClientType:  load_generator.Valkey,
			Address:       seeds("127.0.0.1:43221"),
			Goroutines:    40,
			RPS:           rps3,
			ReadPercent:   90,
			KeySpace:      200_000,
			WriteKeySpace: 200_000,
			ZipfS:         1.1,
			ZipfV:         1.5,
			RandomPercent: 5, // batch jobs, crawlers, cold reads: the traffic that does not follow the popularity curve
			Value:         load_generator.DBValue,
			Equal:         bytes.Equal,
		},
	}

	for i := range scenarios {
		if err := applyOverride(&scenarios[i], cfg); err != nil {
			return nil, err
		}
	}
	return scenarios, nil
}
