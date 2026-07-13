//go:build cgo

// The valyala/ybc backend for the benchmarks; cgo-only for the same reason as valyala_test.go.
package integration

import (
	"sync"
	"testing"

	"github.com/valyala/ybc/libs/go/memcache"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	valyala_adapter "github.com/zakonnic/memstash/l2/valyala_adapter"
)

var lazyValyalaOnce sync.Once
var lazyValyala *memcache.Client

func init() {
	extraBenchBackend = append(extraBenchBackend, benchBackend{
		name: "memcached/valyala",
		addr: memcachedAddr(),
		factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, []byte] {
			lazyValyalaOnce.Do(func() {
				lazyValyala = &memcache.Client{ServerAddr: memcachedAddr()}
				lazyValyala.Start()
			})
			store, aerr := valyala_adapter.New[string, []byte](lazyValyala, l2.BytesCodec(), l2.WithKeyFunc(l2.PrefixedString(prefix)))
			return adapterCache(b, store, aerr, opts)
		},
	})
}
