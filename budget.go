package memstash

import (
	"fmt"
	"math"
	"reflect"
	"unsafe"

	"github.com/zakonnic/memstash/internal/itemstate"
)

// perEntryOverheadBytes approximates the L1 cache item overhead: the 16-byte state record, the table slot,
// the eviction-queue node and the allocator rounding of the box itself.
const perEntryOverheadBytes = 48

// GetAutoCostFunc builds the automatic CostFunc behind Config.MemoryBudget. The cost of an item is its estimated
// resident size in bytes: unsafe.Sizeof(Entry) covers the inline part of the key and value, the payload sizers add
// the heap bytes they reference, and perEntryOverheadBytes accounts the cache's own bookkeeping. With such costs the
// sum of weights tracks the cache's real footprint, so MemoryCapacity can be the byte budget itself.
func GetAutoCostFunc[K comparable, V any]() (func(key K, value V) uint32, error) {
	keySize, ok := payloadSizer[K]()
	if !ok {
		return nil, fmt.Errorf("%w (key type %s)", ErrBudgetNeedsCostFunc, reflect.TypeFor[K]())
	}
	valSize, ok := payloadSizer[V]()
	if !ok {
		return nil, fmt.Errorf("%w (value type %s)", ErrBudgetNeedsCostFunc, reflect.TypeFor[V]())
	}
	fixed := int64(unsafe.Sizeof(itemstate.Entry[K, V]{})) + perEntryOverheadBytes
	if keySize == nil && valSize == nil {
		// Both types are fixed-size: every item costs the same.
		cost := uint32(min(fixed, math.MaxUint32))
		return func(K, V) uint32 { return cost }, nil
	}
	return func(key K, value V) uint32 {
		cost := fixed
		if keySize != nil {
			cost += keySize(key)
		}
		if valSize != nil {
			cost += valSize(value)
		}
		if cost > math.MaxUint32 {
			return math.MaxUint32
		}
		return uint32(cost)
	}, nil
}

// sliceLen mirrors the slice header layout far enough to read len without reflection.
type sliceLen struct {
	_   unsafe.Pointer
	len int
}

// payloadSizer returns a fast estimator of the heap bytes a value of T references beyond unsafe.Sizeof(T):
// nil (nothing referenced) for pointer-free types, len for strings and slices of pointer-free elements (spare
// capacity is not counted), the pointee size for pointers to pointer-free types. ok=false means T's size cannot be
// estimated cheaply - maps, interfaces, functions, channels, or any type referencing further allocations (a struct
// with a string field, a slice of slices, ...).
func payloadSizer[T any]() (fn func(T) int64, ok bool) {
	t := reflect.TypeFor[T]()
	if pointerFree(t) {
		return nil, true
	}
	switch t.Kind() {
	case reflect.String:
		return func(v T) int64 { return int64(len(*(*string)(unsafe.Pointer(&v)))) }, true
	case reflect.Slice:
		if !pointerFree(t.Elem()) {
			return nil, false
		}
		elemSize := int64(t.Elem().Size())
		return func(v T) int64 { return int64((*sliceLen)(unsafe.Pointer(&v)).len) * elemSize }, true
	case reflect.Pointer:
		if !pointerFree(t.Elem()) {
			return nil, false
		}
		pointeeSize := int64(t.Elem().Size())
		return func(v T) int64 {
			if *(*unsafe.Pointer)(unsafe.Pointer(&v)) == nil {
				return 0
			}
			return pointeeSize
		}, true
	}
	return nil, false
}

// pointerFree reports whether the type references no other allocations, i.e. unsafe.Sizeof covers it entirely.
func pointerFree(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return true
	case reflect.Array:
		return pointerFree(t.Elem())
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if !pointerFree(t.Field(i).Type) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
