package memstash

import (
	"sync/atomic"

	"github.com/zakonnic/memstash/internal/itemstore"
)

// Generics compile in the package instantiating them, where a plain internal helper inlines only if its body is in
// this package's export data, and gc writes it there only for helpers it inlined here. This instance inlines them all.
var _ = New[string, string]

func tagOf(hash uint64) uint64                             { return itemstore.TagOf(hash) }
func overflowBucket(hash uint64) uint32                    { return itemstore.OverflowBucket(hash) }
func empty(metaWord uint64) bool                           { return itemstore.Empty(metaWord) }
func expired(metaWord uint64, nowOff uint32) bool          { return itemstore.Expired(metaWord, nowOff) }
func newQNode(keyHash uint64, cost uint32) itemstore.QNode { return itemstore.NewQNode(keyHash, cost) }
func storeI64(p *atomic.Int64, v int64)                    { itemstore.StoreI64(p, v) }
