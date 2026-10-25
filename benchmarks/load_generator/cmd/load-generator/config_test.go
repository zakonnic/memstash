package main

import (
	"slices"
	"testing"
)

// The docker run layers docker/load-generator/config.yaml over config.yaml: both must parse, the second may change
// only the addresses, and everything else must stay as the host run had it.
func TestDockerConfigLayersOverBase(t *testing.T) {
	var cfgs []fileConfig
	for _, path := range []string{"../../config.yaml", "../../../../docker/load-generator/config.yaml"} {
		cfg, err := loadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		cfgs = append(cfgs, cfg)
	}
	base, err := buildScenarios(cfgs[:1])
	if err != nil {
		t.Fatal(err)
	}
	layered, err := buildScenarios(cfgs)
	if err != nil {
		t.Fatal(err)
	}

	for i, s := range layered {
		b := base[i]
		if s.Goroutines != b.Goroutines || !slices.Equal(s.RPS, b.RPS) || s.KeySpace != b.KeySpace ||
			s.CacheSize != b.CacheSize || s.ZipfS != b.ZipfS || s.RandomPercent != b.RandomPercent {
			t.Errorf("%s: the docker config changed more than the address", s.Name)
		}
	}
	if got := layered[2].Address; !slices.Equal(got, []string{"valkey:6379"}) {
		t.Errorf("scenario-3 address = %v, want valkey:6379", got)
	}
}
