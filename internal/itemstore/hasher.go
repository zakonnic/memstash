package itemstore

import (
	"hash/maphash"
	"math/rand/v2"
	"reflect"
	"unsafe"
)

// Hasher is the cache's key hash, seeded per cache. An 8-byte key without floats is mixed in place - two multiplies -
// because maphash, for such a key, is a runtime call that costs more than the probe it feeds. Everything else goes
// through maphash. Floats stay off the mixed path so that +0 and -0, equal keys with different bits, keep hashing
// alike.
type Hasher[K comparable] struct {
	seed  maphash.Seed
	mix   uint64
	words bool
}

// NewHasher makes a hasher with fresh seeds.
func NewHasher[K comparable]() Hasher[K] {
	var zero K
	return Hasher[K]{
		seed:  maphash.MakeSeed(),
		mix:   rand.Uint64() | 1,
		words: unsafe.Sizeof(zero) == 8 && !hasFloat(reflect.TypeOf(zero)),
	}
}

// Hash is the key's 64-bit hash. The size test is a compile-time constant per key type, so a string key never sees
// the mixing branch and a word key pays one predictable branch.
func (h *Hasher[K]) Hash(key K) uint64 {
	if unsafe.Sizeof(key) == 8 && h.words {
		return mixWord(*(*uint64)(unsafe.Pointer(&key)) ^ h.mix)
	}
	return maphash.Comparable(h.seed, key)
}

// mixWord is the splitmix64 finalizer: two multiplies, and every output bit depends on every input bit.
func mixWord(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	x *= 0x94D049BB133111EB
	return x ^ x>>31
}

func hasFloat(t reflect.Type) bool {
	if t == nil {
		return false
	}
	switch t.Kind() {
	case reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return true
	case reflect.Array:
		return hasFloat(t.Elem())
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if hasFloat(t.Field(i).Type) {
				return true
			}
		}
	}
	return false
}
