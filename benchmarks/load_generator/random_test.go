package load_generator

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestNextKeyUniform: the random share must spread over the whole space, unlike the Zipf draw it replaces, which
// keeps landing on the same head.
func TestNextKeyUniform(t *testing.T) {
	const space, draws = 10_000, 200_000
	rng := rand.New(rand.NewSource(7))
	z := newZipf(rng, space, 1.1, 1)

	uniform, zipf := make([]int, space), make([]int, space)
	for range draws {
		uniform[nextKey(rng, z, space, true)]++
		zipf[nextKey(rng, z, space, false)]++
	}

	var uniformSeen, uniformHead, zipfHead int
	for i := range space {
		if uniform[i] > 0 {
			uniformSeen++
		}
		if i < space/100 {
			uniformHead += uniform[i]
			zipfHead += zipf[i]
		}
	}

	assert.Equal(t, space, uniformSeen, "20 draws per key on average: every key must have come up")
	assert.InEpsilon(t, draws/100, uniformHead, 0.1, "the hottest 1% of keys get 1% of the uniform draws")
	assert.Greater(t, zipfHead, draws/3, "while Zipf pours a third of everything into them")
}

// TestNextKeyReachesTail: on a space too large for Zipf to wander into, the uniform draw is what gets to the cold
// keys at all.
func TestNextKeyReachesTail(t *testing.T) {
	const space, draws = 1_000_000, 1_000_000
	rng := rand.New(rand.NewSource(9))
	z := newZipf(rng, space, 1.1, 1)

	uniform, zipf := make([]bool, space), make([]bool, space)
	var uniformSeen, zipfSeen int
	for range draws {
		if k := nextKey(rng, z, space, true); !uniform[k] {
			uniform[k], uniformSeen = true, uniformSeen+1
		}
		if k := nextKey(rng, z, space, false); !zipf[k] {
			zipf[k], zipfSeen = true, zipfSeen+1
		}
	}

	assert.InDelta(t, 1-1/math.E, float64(uniformSeen)/space, 0.01, "one draw per key covers 1-1/e of them")
	assert.Less(t, zipfSeen*3, uniformSeen, "Zipf spends the same draws redrawing its head")
}

// TestNextKeyStaysInSpace: an out-of-range index would build a key the truth map never heard of and report a bogus
// anomaly.
func TestNextKeyStaysInSpace(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	z := newZipf(rng, 5, 1.1, 1)
	for range 1000 {
		assert.Less(t, nextKey(rng, z, 5, true), 5)
		assert.Less(t, nextKey(rng, z, 5, false), 5)
	}
}

// TestRandomTiming: the two numbers the config is tuned against - how often a given key comes up, and how long until
// all of them have. The second is the first times ln(N), which is what makes uniform draws a slow way to cover a
// large space.
func TestRandomTiming(t *testing.T) {
	s := Scenario[string, []byte]{WriteKeySpace: 20_000_000, RPS: EvenSplit(40, 40_000), RandomPercent: 5}

	assert.Equal(t, 10_000*time.Second, s.randomPeriod(), "20M keys / (40k rps * 5%)")
	assert.InEpsilon(t, math.Log(20_000_000), s.randomCover().Seconds()/s.randomPeriod().Seconds(), 0.001)
	assert.InDelta(t, 46.7, s.randomCover().Hours(), 0.1)

	off := Scenario[string, []byte]{WriteKeySpace: 100, RPS: []float64{1_000}}
	assert.Zero(t, off.randomPeriod())
	assert.Zero(t, off.randomCover())
}
