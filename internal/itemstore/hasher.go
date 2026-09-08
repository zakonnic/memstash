package itemstore

import (
	"hash/maphash"
	"math/rand/v2"
	"reflect"
	"unsafe"
)

// Hasher is the cache's key hash, seeded per cache. An 8-byte key without floats is mixed in place (HashWord) because
// maphash, for such a key, is a runtime call that costs more than the probe it feeds; everything else goes through
// maphash (HashSlow). The two are separate methods, and the caller picks by WordKeys, because a method holding both
// paths is past the inlining budget - and a hash that is not inlined is a call on every probe. Floats stay off the
// mixed path so that +0 and -0, equal keys with different bits, keep hashing alike.
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

// WordKeys reports whether HashWord is the hash for this key type.
func (h *Hasher[K]) WordKeys() bool { return h.words }

// HashWord mixes the key's 8 bytes: the splitmix64 finalizer, where every output bit depends on every input bit. Only
// for a key type WordKeys approves.
func (h *Hasher[K]) HashWord(key K) uint64 {
	x := *(*uint64)(unsafe.Pointer(&key)) ^ h.mix
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	x *= 0x94D049BB133111EB
	return x ^ x>>31
}

// HashSlow is maphash over the key.
func (h *Hasher[K]) HashSlow(key K) uint64 { return maphash.Comparable(h.seed, key) }

// Hash is the key's hash by whichever path applies. For a cold caller; the hot ones branch on WordKeys themselves.
func (h *Hasher[K]) Hash(key K) uint64 {
	if h.words {
		return h.HashWord(key)
	}
	return h.HashSlow(key)
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
