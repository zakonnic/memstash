package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// scenarioOverride is a scenario's config block; a nil pointer (or empty slice) keeps the built-in default.
type scenarioOverride struct {
	Size          *int64    `yaml:"size"` // cache capacity in weight units; passed to memstash.WithMemoryCapacity
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

// effectiveSize resolves the cache capacity for the named scenario.
func effectiveSize(cfg fileConfig, name string, defaultSize int64) (int64, error) {
	o, ok := cfg.Scenarios[name]
	if !ok || o.Size == nil {
		return defaultSize, nil
	}
	if *o.Size <= 0 {
		return 0, fmt.Errorf("%s: size=%d must be positive", name, *o.Size)
	}
	return *o.Size, nil
}

// effectiveRedisAddress resolves the scenario's L2 seed nodes: the config override (present but blank = L1 only) or
// defaultAddr, comma-split. An empty result means no L2.
func effectiveRedisAddress(cfg fileConfig, name, defaultAddr string) []string {
	addr := defaultAddr
	if o, ok := cfg.Scenarios[name]; ok && o.RedisAddress != nil {
		addr = *o.RedisAddress
	}
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

// applyOverride merges o onto s, leaving any field o didn't set untouched.
func applyOverride(s *scenario, o scenarioOverride) {
	if o.Goroutines != nil {
		s.goroutines = *o.Goroutines
	}
	if len(o.RPS) > 0 {
		s.rps = o.RPS
	}
	if o.ReadPercent != nil {
		s.readPercent = *o.ReadPercent
	}
	if o.KeySpace != nil {
		s.keySpace = *o.KeySpace
	}
	if o.WriteKeySpace != nil {
		s.writeKeySpace = *o.WriteKeySpace
	}
	if o.ZipfS != nil {
		s.zipfS = *o.ZipfS
	}
	if o.RandomPercent != nil {
		s.randomPercent = *o.RandomPercent
	}
}

// validateScenario checks the effective parameters, override or built-in default alike - a scenario nobody overrode
// is just as able to hold a value the generators refuse.
func validateScenario(s *scenario) error {
	// rand.NewZipf answers an out-of-range s or v with a nil generator, which surfaces as a panic on every worker at
	// its first draw and leaves the scenario running with no load at all.
	if s.zipfS <= 1 {
		return fmt.Errorf("%s: zipf_s=%g must be > 1 (math/rand.NewZipf requires it)", s.name, s.zipfS)
	}
	if s.zipfV < 1 {
		return fmt.Errorf("%s: zipf_v=%g must be >= 1 (math/rand.NewZipf requires it)", s.name, s.zipfV)
	}
	if len(s.rps) != s.goroutines {
		return fmt.Errorf("%s: rps has %d entries but goroutines=%d - config.yaml must give one rps value per goroutine",
			s.name, len(s.rps), s.goroutines)
	}
	if s.readPercent < 0 || s.readPercent > 100 {
		return fmt.Errorf("%s: read_percent=%d must be between 0 and 100", s.name, s.readPercent)
	}
	if s.randomPercent < 0 || s.randomPercent > 100 {
		return fmt.Errorf("%s: random_percent=%d must be between 0 and 100", s.name, s.randomPercent)
	}
	if s.writeKeySpace <= 0 || s.writeKeySpace > s.keySpace {
		return fmt.Errorf("%s: write_key_space=%d must be > 0 and <= key_space=%d", s.name, s.writeKeySpace, s.keySpace)
	}
	return nil
}
