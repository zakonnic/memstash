//go:build amd64 && !race

package itemstore

import (
	"sync/atomic"
	"unsafe"
)

// Writer-side stores. The shard mutex admits one writer, so a meta write needs release ordering, not atomicity
// against another writer - and on amd64 a plain aligned store is exactly that: stores become visible in program
// order, and the compiler keeps memory operations in program order. The locked instruction an atomic Store or Or
// compiles to costs several times the store itself (five of them: 18.5 ns locked, 1.5 ns plain), and an insert with
// an eviction issues a dozen. Readers keep their atomic loads and CASes. The race detector cannot see the ordering
// argument, so its builds take write_generic.go; the lost-update it would flag - a reader's touch CAS landing
// between a writer's load and store - costs that item one reference bit, nothing more.
func storeMeta(p *atomic.Uint64, v uint64) { *(*uint64)(unsafe.Pointer(p)) = v }

func orMeta(p *atomic.Uint64, bits uint64) uint64 {
	old := *(*uint64)(unsafe.Pointer(p))
	*(*uint64)(unsafe.Pointer(p)) = old | bits
	return old
}

func andMeta(p *atomic.Uint64, mask uint64) uint64 {
	old := *(*uint64)(unsafe.Pointer(p))
	*(*uint64)(unsafe.Pointer(p)) = old & mask
	return old
}

// StoreU32 and StoreI64 are the same for the shard's own words: the move sequence, the overflow counters, the weight.
func StoreU32(p *atomic.Uint32, v uint32) { *(*uint32)(unsafe.Pointer(p)) = v }
func StoreI64(p *atomic.Int64, v int64)   { *(*int64)(unsafe.Pointer(p)) = v }
