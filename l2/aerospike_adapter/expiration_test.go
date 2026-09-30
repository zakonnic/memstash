package aerospike_adapter

import (
	"math"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"
)

// The top of the uint32 range holds the server's reserved values: a TTL that would land there never expires instead.
func TestExpiration(t *testing.T) {
	for _, tc := range []struct {
		ttl  time.Duration
		want uint32
	}{
		{0, as.TTLDontExpire},
		{-time.Second, as.TTLDontExpire},
		{1500 * time.Millisecond, 2},
		{(math.MaxUint32 - 2) * time.Second, math.MaxUint32 - 2},
		{(math.MaxUint32 - 1) * time.Second, as.TTLDontExpire},
		{math.MaxInt64, as.TTLDontExpire},
	} {
		if got := expiration(tc.ttl); got != tc.want {
			t.Errorf("expiration(%v) = %d, want %d", tc.ttl, got, tc.want)
		}
	}
}
