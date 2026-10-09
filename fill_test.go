package memstash

import (
	"context"
	"testing"
	"time"

	"github.com/zakonnic/memstash/internal/itemstore"
)

// A fill gives way to a live value only: an item past its TTL is no newer write, and the fill replaces it.
func TestFillReplacesExpiredButKeepsLive(t *testing.T) {
	c, err := New[string, string](WithTTL(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()

	_ = c.Set(ctx, "k", "live")
	if c.setMemory("k", "fill", c.expireOffset(), fill, c.deleteMark("k")) {
		t.Fatal("a fill replaced a live value")
	}
	c.nowOff.Store((c.nowOff.Load() + c.ttlOff + 2) & itemstore.ExpireWrapMask) // past the item's deadline
	if !c.setMemory("k", "fill", c.expireOffset(), fill, c.deleteMark("k")) {
		t.Fatal("a fill gave way to an expired value")
	}
	if v, ok := c.GetFromMemory("k"); !ok || v != "fill" {
		t.Fatalf("got %q, %v", v, ok)
	}
}
