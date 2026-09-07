//go:build !amd64 || race

package itemstore

import "sync/atomic"

// The atomic counterparts of write_amd64.go: what every other architecture needs for the ordering, and what the race
// detector needs to see the writer's stores as synchronization.
func storeMeta(p *atomic.Uint64, v uint64)         { p.Store(v) }
func orMeta(p *atomic.Uint64, bits uint64) uint64  { return p.Or(bits) }
func andMeta(p *atomic.Uint64, mask uint64) uint64 { return p.And(mask) }
func StoreU32(p *atomic.Uint32, v uint32)          { p.Store(v) }
func StoreI64(p *atomic.Int64, v int64)            { p.Store(v) }
