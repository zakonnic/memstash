package load_generator

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/zakonnic/memstash"
)

// Scenario describes one cache under load: the shape of its key space, its mix of operations, and the values it
// holds. It is plain data - New builds the cache and the workers around it. K and V are the cache's own key and value
// types: the cache is built as memstash.Cache[K, V] and Value must produce a V for a K.
//
// Goroutines, KeySpace, ZipfV, Key, Equal and Codec take a default when left at zero (see the fields); everything
// else is used as written and checked by New.
type Scenario[K comparable, V any] struct {
	// Name names the scenario's log file, picks its console slot and prefixes the built-in keys; it must be unique
	// within the app. That prefix is what keeps scenarios sharing one L2 from overwriting each other's values.
	Name string
	// Description is printed by PrintScenarios and written to the log file once, at startup.
	Description string

	// CacheSize is the L1 capacity in weight units: items by default, whatever a cost function counts otherwise.
	CacheSize int64
	// CacheOptions are passed to the cache after the ones the app sets itself (stats, capacity, and the handlers
	// feeding errors.log), so they can override any of them. A non-string K with an L2 needs l2.WithKeyFunc here.
	CacheOptions []memstash.Option
	// RedisAddress lists the rueidis seed nodes used as L2; empty runs the scenario on L1 alone.
	RedisAddress []string
	// Codec encodes values for that L2. Defaults to raw pass-through for []byte and string values and to JSON for
	// everything else.
	Codec memstash.Codec[V]

	// Goroutines is how many workers drive the cache; defaults to len(RPS).
	Goroutines int
	// RPS is each worker's target rate, one entry per goroutine.
	RPS []float64
	// ReadPercent is the chance an operation is a Get rather than a Set, 0-100.
	ReadPercent int

	// Keys follow a Zipf distribution (skew ZipfS, index 0 hottest): Gets over [0, KeySpace), Sets over
	// [0, WriteKeySpace). Zipf never reaches the tail of the key space. To cover it, a share of operations uses
	// uniformly random keys instead (RandomPercent).
	KeySpace int // defaults to WriteKeySpace
	// WriteKeySpace is where the writes stop: nothing ever writes a key above it, so a Get that hits one is a value
	// from somewhere else and gets logged as such.
	WriteKeySpace int
	ZipfS         float64
	ZipfV         float64 // defaults to 1
	// RandomPercent is the traffic that ignores the popularity curve: batch jobs, crawlers, cold reads.
	RandomPercent int

	// Key turns the index the distributions draw into the key itself, so it must map distinct n to distinct keys.
	// String keys default to "<Name>:key-<n>"; any other key type has to bring its own.
	Key func(n int) K
	// Value returns the value stored under a key. It must be a pure deterministic function of the key: it is called
	// on every Set, and again on every Get that hits, to verify what came back.
	Value func(key K) V
	// Equal decides whether a value read back is the one that was written. Defaults to reflect.DeepEqual.
	Equal func(got, want V) bool
}

// EvenSplit divides totalRPS evenly across n workers, ready to drop into Scenario.RPS.
func EvenSplit(n int, totalRPS float64) []float64 {
	rps := make([]float64, n)
	per := totalRPS / float64(n)
	for i := range rps {
		rps[i] = per
	}
	return rps
}

// withDefaults fills the fields whose zero value means "not set" rather than a setting of its own.
func (s Scenario[K, V]) withDefaults() Scenario[K, V] {
	if s.Goroutines == 0 {
		s.Goroutines = len(s.RPS)
	}
	if s.KeySpace == 0 {
		s.KeySpace = s.WriteKeySpace
	}
	if s.ZipfV == 0 {
		s.ZipfV = 1
	}
	if s.Key == nil {
		s.Key = stringKeys[K](s.Name) // nil for every K but string
	}
	if s.Equal == nil {
		s.Equal = func(got, want V) bool { return reflect.DeepEqual(got, want) }
	}
	return s
}

// stringKeys is the built-in "<prefix>:key-<n>" key function. It only exists when K really is string; a named string
// type or anything else gets nil back and has to set Key itself.
func stringKeys[K comparable](prefix string) func(n int) K {
	if _, ok := any(*new(K)).(string); !ok {
		return nil
	}
	return func(n int) K {
		key, _ := any(fmt.Sprintf("%s:key-%d", prefix, n)).(K)
		return key
	}
}

// validate checks the effective parameters, defaults and caller's values alike - a scenario nobody touched is just
// as able to hold a value the generators refuse.
func (s Scenario[K, V]) validate() error {
	if s.Name == "" {
		return errors.New("scenario has no Name")
	}
	if s.Value == nil {
		return fmt.Errorf("%s: Value must be set - it is what every Get is verified against", s.Name)
	}
	if s.Key == nil {
		return fmt.Errorf("%s: Key must be set - the built-in key function only covers string keys", s.Name)
	}
	if s.CacheSize <= 0 {
		return fmt.Errorf("%s: CacheSize=%d must be positive", s.Name, s.CacheSize)
	}
	// rand.NewZipf answers an out-of-range S or V with a nil generator, which surfaces as a panic on every worker at
	// its first draw and leaves the scenario running with no load at all.
	if s.ZipfS <= 1 {
		return fmt.Errorf("%s: ZipfS=%g must be > 1 (math/rand.NewZipf requires it)", s.Name, s.ZipfS)
	}
	if s.ZipfV < 1 {
		return fmt.Errorf("%s: ZipfV=%g must be >= 1 (math/rand.NewZipf requires it)", s.Name, s.ZipfV)
	}
	if s.Goroutines <= 0 {
		return fmt.Errorf("%s: Goroutines=%d must be positive", s.Name, s.Goroutines)
	}
	if len(s.RPS) != s.Goroutines {
		return fmt.Errorf("%s: RPS has %d entries but Goroutines=%d - one rate per goroutine",
			s.Name, len(s.RPS), s.Goroutines)
	}
	if s.ReadPercent < 0 || s.ReadPercent > 100 {
		return fmt.Errorf("%s: ReadPercent=%d must be between 0 and 100", s.Name, s.ReadPercent)
	}
	if s.RandomPercent < 0 || s.RandomPercent > 100 {
		return fmt.Errorf("%s: RandomPercent=%d must be between 0 and 100", s.Name, s.RandomPercent)
	}
	if s.WriteKeySpace <= 0 || s.WriteKeySpace > s.KeySpace {
		return fmt.Errorf("%s: WriteKeySpace=%d must be > 0 and <= KeySpace=%d",
			s.Name, s.WriteKeySpace, s.KeySpace)
	}
	return nil
}

// totalRPS is the scenario's target rate across all its workers.
func (s Scenario[K, V]) totalRPS() float64 {
	var total float64
	for _, r := range s.RPS {
		total += r
	}
	return total
}

// randomRate is how many uniform draws per second the scenario makes; 0 when it makes none.
func (s Scenario[K, V]) randomRate() float64 {
	return s.totalRPS() * float64(s.RandomPercent) / 100
}

// randomPeriod is how long a given key waits between uniform draws.
func (s Scenario[K, V]) randomPeriod() time.Duration {
	rate := s.randomRate()
	if rate <= 0 {
		return 0
	}
	return time.Duration(float64(s.WriteKeySpace) / rate * float64(time.Second))
}

// randomCover is how long until every key has come up at least once: N*ln(N) draws, not N - the last few keys are
// the ones that keep the collector waiting.
func (s Scenario[K, V]) randomCover() time.Duration {
	rate := s.randomRate()
	if rate <= 0 || s.WriteKeySpace < 2 {
		return 0
	}
	n := float64(s.WriteKeySpace)
	return time.Duration(n * math.Log(n) / rate * float64(time.Second))
}
