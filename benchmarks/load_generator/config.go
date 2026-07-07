package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// scenarioOverride holds the fields config.yaml is allowed to override for a single scenario. Every field is a
// pointer (or a possibly-nil slice), so leaving it out of the file keeps the built-in default from buildScenarios.
type scenarioOverride struct {
	Size          *int64    `yaml:"size"` // cache capacity in weight units; passed to memstash.WithMemoryCapacity
	Goroutines    *int      `yaml:"goroutines"`
	RPS           []float64 `yaml:"rps"`
	ReadPercent   *int      `yaml:"read_percent"`
	KeySpace      *int      `yaml:"key_space"`
	WriteKeySpace *int      `yaml:"write_key_space"`
}

// fileConfig is the root of config.yaml: one optional override block per scenario, keyed by scenario name
// (scenario-1, scenario-2, scenario-3).
type fileConfig struct {
	Scenarios map[string]scenarioOverride `yaml:"scenarios"`
}

// loadConfig reads and parses path. A missing file is not an error - config.yaml only overrides the built-in
// defaults, it isn't required for the load generator to run.
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

// applyOverride merges o onto s, leaving any field o didn't set untouched, then validates the result.
func applyOverride(s *scenario, o scenarioOverride) error {
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

	if len(s.rps) != s.goroutines {
		return fmt.Errorf("%s: rps has %d entries but goroutines=%d - config.yaml must give one rps value per goroutine",
			s.name, len(s.rps), s.goroutines)
	}
	if s.readPercent < 0 || s.readPercent > 100 {
		return fmt.Errorf("%s: read_percent=%d must be between 0 and 100", s.name, s.readPercent)
	}
	if s.writeKeySpace <= 0 || s.writeKeySpace > s.keySpace {
		return fmt.Errorf("%s: write_key_space=%d must be > 0 and <= key_space=%d", s.name, s.writeKeySpace, s.keySpace)
	}
	return nil
}
