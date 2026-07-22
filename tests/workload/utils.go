package workload

import (
	truerand "crypto/rand"
	"math"
	"math/big"
	"math/rand"
)

func Seed() int64 {
	seed, _ := truerand.Int(truerand.Reader, big.NewInt(math.MaxInt64))
	return seed.Int64()
}

func Random() *rand.Rand {
	return rand.New(rand.NewSource(Seed()))
}
