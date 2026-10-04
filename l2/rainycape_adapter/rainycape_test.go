package rainycape_adapter_test

import (
	"context"
	"testing"

	"github.com/rainycape/memcache"

	"github.com/zakonnic/memstash/l2"
	rainycape_adapter "github.com/zakonnic/memstash/l2/rainycape_adapter"
)

// A server that cannot be reached is an error, not a batch of misses: callers would reload everything from the source
// and never hear why.
func TestBatchGetReportsAnUnreachableServer(t *testing.T) {
	client, err := memcache.New("127.0.0.1:1") // nothing listens on port 1
	if err != nil {
		t.Fatal(err)
	}
	c, err := rainycape_adapter.New[string, string](client, l2.StringCodec())
	if err != nil {
		t.Fatal(err)
	}
	found, err := c.BatchGet(context.Background(), []string{"a", "b"})
	if err == nil {
		t.Fatalf("BatchGet against no server: %d found and no error", len(found))
	}
}
