package memstash

import (
	"sync/atomic"

	"github.com/zakonnic/memstash/internal/itemstore"
)

// The hot paths reach internal/itemstore through these one-liners rather than directly, and the reason is the module
// boundary. This package's generic methods are compiled in the package that instantiates them - a user's program - and
// there the compiler inlines a helper from internal/itemstore only if its body reached the user through this package's
// export data. A generic body does not carry the bodies of the plain functions it calls; an inlinable function of this
// package does. So each helper a probe or a write needs is referenced from one of these, or it is a call on every
// operation in every program but this repository's own tests (measured: TagOf, OverflowBucket, Empty, NewQNode and the
// stores were calls in the benchmarks module and inlined in the root one).

func tagOf(hash uint64) uint64                             { return itemstore.TagOf(hash) }
func overflowBucket(hash uint64) uint32                    { return itemstore.OverflowBucket(hash) }
func empty(metaWord uint64) bool                           { return itemstore.Empty(metaWord) }
func expired(metaWord uint64, nowOff uint32) bool          { return itemstore.Expired(metaWord, nowOff) }
func dispOf(metaWord uint64) uint32                        { return itemstore.DispOf(metaWord) }
func newQNode(keyHash uint64, cost uint32) itemstore.QNode { return itemstore.NewQNode(keyHash, cost) }
func storeI64(p *atomic.Int64, v int64)                    { itemstore.StoreI64(p, v) }
func storeU32(p *atomic.Uint32, v uint32)                  { itemstore.StoreU32(p, v) }

// hash is the key's hash: the word mixer for a word key type, maphash otherwise. The branch lives here, in an
// inlinable method, rather than in Hasher.Hash, whose two paths together exceed the inlining budget.
func (c *Cache[K, V]) hash(key K) uint64 {
	if c.wordKeys {
		return c.hasher.HashWord(key)
	}
	return c.hasher.HashSlow(key)
}
