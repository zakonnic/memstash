package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/zakonnic/memstash"
)

// scenarioOverride is a scenario's config block; a nil pointer (or empty slice) keeps the built-in default.
type scenarioOverride struct {
	Size          *int64    `yaml:"size"` // cache capacity in weight units; becomes Scenario.CacheSize
	Goroutines    *int      `yaml:"goroutines"`
	RPS           []float64 `yaml:"rps"`
	ReadPercent   *int      `yaml:"read_percent"`
	KeySpace      *int      `yaml:"key_space"`
	WriteKeySpace *int      `yaml:"write_key_space"`
	ZipfS         *float64  `yaml:"zipf_s"` // Zipf skew (>1); higher = more concentrated on hot keys
	// RandomPercent is the share of operations drawing their key uniformly instead of from the Zipf head.
	RandomPercent *int `yaml:"random_percent"`
	// RedisAddress: "" means L1 only, a comma-separated list dials a cluster; omitted keeps the built-in default.
	RedisAddress *string `yaml:"redis_address"`
	// Workers is the write-back pool size (memstash.WithWriteBackWorkers); omitted keeps the cache's own default.
	Workers *int `yaml:"workers"`
}

// fileConfig is the root of config.yaml: an override block per scenario name.
type fileConfig struct {
	Scenarios map[string]scenarioOverride `yaml:"scenarios"`
}

// loadConfig reads and parses path; a missing file is not an error (the defaults are self-sufficient).
func loadConfig(path string) (fileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fileConfig{}, nil
		}
		return fileConfig{}, err
	}
	var cfg fileConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fileConfig{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return cfg, nil
}

// applyOverride merges the scenario's config block onto s, leaving any field the block didn't set untouched. The
// rest of the checking is New's job.
func applyOverride(s *bytesScenario, cfg fileConfig) error {
	o, ok := cfg.Scenarios[s.Name]
	if !ok {
		return nil
	}
	if o.Size != nil {
		if *o.Size <= 0 {
			return fmt.Errorf("%s: size=%d must be positive", s.Name, *o.Size)
		}
		s.CacheSize = *o.Size
	}
	if o.RedisAddress != nil {
		s.RedisAddress = redisSeeds(*o.RedisAddress)
	}
	if o.Goroutines != nil {
		s.Goroutines = *o.Goroutines
	}
	if len(o.RPS) > 0 {
		s.RPS = o.RPS
	}
	if o.ReadPercent != nil {
		s.ReadPercent = *o.ReadPercent
	}
	if o.KeySpace != nil {
		s.KeySpace = *o.KeySpace
	}
	if o.WriteKeySpace != nil {
		s.WriteKeySpace = *o.WriteKeySpace
	}
	if o.ZipfS != nil {
		s.ZipfS = *o.ZipfS
	}
	if o.RandomPercent != nil {
		s.RandomPercent = *o.RandomPercent
	}
	if o.Workers != nil {
		if *o.Workers <= 0 {
			return fmt.Errorf("%s: workers=%d must be positive", s.Name, *o.Workers)
		}
		// Appended last, so it overrides a WithWriteBackWorkers the scenario set itself.
		s.CacheOptions = append(s.CacheOptions, memstash.WithWriteBackWorkers(*o.Workers))
	}
	return nil
}

// redisSeeds splits a comma-separated address list into L2 seed nodes; blank means no L2.
func redisSeeds(addr string) []string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil
	}
	seeds := strings.Split(addr, ",")
	for i := range seeds {
		seeds[i] = strings.TrimSpace(seeds[i])
	}
	return seeds
}
