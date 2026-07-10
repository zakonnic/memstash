package benchmarks

import (
	"reflect"
	"unsafe"

	"github.com/puzpuzpuz/xsync/v3"
)

const (
	wordSize          = unsafe.Sizeof(uintptr(0))
	hchanHeaderSize   = 96
	mapGroupSlots     = 8
	mapGroupCtrlBytes = 8
	mapHeaderSize     = 48

	// xsyncBucketBytes is the fixed size of one xsync.MapOf bucket.
	xsyncBucketBytes = 64
)

func SizeOf(v any) uint64 {
	if v == nil {
		return 0
	}
	s := newSizer()
	rv := reflect.ValueOf(v)
	return uint64(rv.Type().Size()) + s.sizeOf(rv)
}

// xsyncMapOfBytes estimates the real footprint of an xsync.MapOf.
func xsyncMapOfBytes[K comparable, V any](m *xsync.MapOf[K, V]) uint64 {
	if m == nil {
		return 0
	}
	stats := m.Stats()
	var zeroKey K
	var zeroVal V
	entryBytes := uint64(unsafe.Sizeof(zeroKey)) + uint64(unsafe.Sizeof(zeroVal))
	return uint64(stats.TotalBuckets)*xsyncBucketBytes + uint64(stats.Size)*entryBytes
}

// simulateMapBucketBytes builds a throwaway xsync.MapOf[K, V], inserts `count` entries (keys produced by keyAt,
// values left zero), and returns the bucket/overflow-chain overhead from its Stats() - not the entries themselves,
// see the call sites for why.
func simulateMapBucketBytes[K comparable, V any](count int, keyAt func(i int) K) uint64 {
	m := xsync.NewMapOf[K, V]()
	var zero V
	for i := 0; i < count; i++ {
		m.Store(keyAt(i), zero)
	}
	// no entry bytes because it is already counted by SizeOf
	return uint64(m.Stats().TotalBuckets) * xsyncBucketBytes
}

type visitKey struct {
	addr unsafe.Pointer
	typ  reflect.Type
}
type sizer struct {
	visited map[visitKey]struct{}
}

func newSizer() *sizer {
	return &sizer{visited: make(map[visitKey]struct{})}
}

func (s *sizer) markVisited(addr unsafe.Pointer, typ reflect.Type) (seen bool) {
	if addr == nil {
		return true
	}
	key := visitKey{addr: addr, typ: typ}
	if _, ok := s.visited[key]; ok {
		return true
	}
	s.visited[key] = struct{}{}
	return false
}

func (s *sizer) sizeOf(v reflect.Value) uint64 {
	switch v.Kind() {
	case reflect.Ptr:
		return s.sizeOfPointer(v)
	case reflect.Interface:
		return s.sizeOfInterface(v)
	case reflect.String:
		return s.sizeOfString(v)
	case reflect.Slice:
		return s.sizeOfSlice(v)
	case reflect.Map:
		return s.sizeOfMap(v)
	case reflect.Chan:
		return s.sizeOfChan(v)
	case reflect.Struct:
		return s.sizeOfStruct(v)
	case reflect.Array:
		return s.sizeOfArray(v)
	default:
		return 0
	}
}
func (s *sizer) sizeOfPointer(v reflect.Value) uint64 {
	if v.IsNil() {
		return 0
	}
	addr := unsafe.Pointer(v.Pointer())
	elemType := v.Type().Elem()
	if s.markVisited(addr, elemType) {
		return 0
	}
	elem := v.Elem()
	return uint64(elemType.Size()) + s.sizeOf(elem)
}
func (s *sizer) sizeOfInterface(v reflect.Value) uint64 {
	if v.IsNil() {
		return 0
	}
	dyn := v.Elem()

	if dyn.Kind() == reflect.Ptr || dyn.Kind() == reflect.Map ||
		dyn.Kind() == reflect.Chan || dyn.Kind() == reflect.Func ||
		dyn.Kind() == reflect.UnsafePointer {
		return s.sizeOf(dyn)
	}
	return uint64(dyn.Type().Size()) + s.sizeOf(dyn)
}
func (s *sizer) sizeOfString(v reflect.Value) uint64 {
	if v.Len() == 0 {
		return 0
	}
	data := unsafe.Pointer(unsafe.StringData(v.String()))
	if s.markVisited(data, v.Type()) {
		return 0
	}
	return uint64(v.Len())
}
func (s *sizer) sizeOfSlice(v reflect.Value) uint64 {
	if v.IsNil() {
		return 0
	}
	elemType := v.Type().Elem()
	data := unsafe.Pointer(v.Pointer())
	if s.markVisited(data, v.Type()) {
		return 0
	}
	total := uint64(v.Cap()) * uint64(elemType.Size())

	for i := 0; i < v.Len(); i++ {
		total += s.sizeOf(v.Index(i))
	}
	return total
}
func (s *sizer) sizeOfMap(v reflect.Value) uint64 {
	if v.IsNil() {
		return 0
	}
	addr := unsafe.Pointer(v.Pointer())
	if s.markVisited(addr, v.Type()) {
		return 0
	}
	total := s.mapOverhead(v)
	iter := v.MapRange()
	for iter.Next() {
		total += s.sizeOf(iter.Key())
		total += s.sizeOf(iter.Value())
	}
	return total
}

func (s *sizer) mapOverhead(v reflect.Value) uint64 {
	keySize := uint64(v.Type().Key().Size())
	valSize := uint64(v.Type().Elem().Size())
	groups := uint64(1)
	need := (uint64(v.Len()) + mapGroupSlots - 1) / mapGroupSlots
	for groups < need {
		groups *= 2
	}
	slotBytes := groups * mapGroupSlots * (keySize + valSize)
	ctrlBytes := groups * mapGroupCtrlBytes
	dirBytes := groups * uint64(wordSize)
	return mapHeaderSize + slotBytes + ctrlBytes + dirBytes
}
func (s *sizer) sizeOfChan(v reflect.Value) uint64 {
	if v.IsNil() {
		return 0
	}
	addr := unsafe.Pointer(v.Pointer())
	if s.markVisited(addr, v.Type()) {
		return 0
	}

	return hchanHeaderSize + uint64(v.Cap())*uint64(v.Type().Elem().Size())
}
func (s *sizer) sizeOfStruct(v reflect.Value) uint64 {
	var total uint64
	for i := 0; i < v.NumField(); i++ {
		total += s.sizeOf(s.exported(v.Field(i)))
	}
	return total
}
func (s *sizer) sizeOfArray(v reflect.Value) uint64 {
	elemKind := v.Type().Elem().Kind()
	switch elemKind {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32,
		reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128:
		return 0
	}
	var total uint64
	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i)
		if elem.CanAddr() && s.markVisited(unsafe.Pointer(elem.UnsafeAddr()), elem.Type()) {
			continue
		}
		total += s.sizeOf(elem)
	}
	return total
}

func (s *sizer) exported(v reflect.Value) reflect.Value {
	if v.CanInterface() || !v.CanAddr() {
		return v
	}
	return reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).Elem()
}
