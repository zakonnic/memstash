package memstash

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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

// A cache nothing has filled yet leaves its Deletes unmarked; the first fill turns marking on before it reads, so a
// Delete during that load still refuses the value it loaded.
func TestDeletesMarkOnceAFillIsPossible(t *testing.T) {
	ctx := context.Background()
	c, err := New[string, string](WithMemoryCapacity(10), WithShardsCount(1))
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Delete(ctx, "k"))
	require.False(t, c.fills.Load())
	require.Zero(t, c.deleteMark("k"), "a Delete marked with no fill possible") // deleteMark itself turns fills on

	entered, gate := make(chan struct{}), make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = c.GetOrLoad(ctx, "k", func(context.Context, string) (string, error) {
			close(entered)
			<-gate
			return "v1", nil // read from the source before the Delete below
		})
	}()
	<-entered
	require.NoError(t, c.Delete(ctx, "k"))
	close(gate)
	<-done
	_, ok := c.GetFromMemory("k")
	require.False(t, ok, "the loaded value came back after the Delete")

	withL2, err := New[string, string](WithMemoryCapacity(10), WithL2Cache[string, string](nopL2{}))
	require.NoError(t, err)
	defer withL2.Close()
	require.True(t, withL2.fills.Load(), "with an L2 any Get may fill")
}

type nopL2 struct{}

func (nopL2) Get(context.Context, string) (string, bool, error)                   { return "", false, nil }
func (nopL2) BatchGet(context.Context, []string) (List[string, string], error)    { return nil, nil }
func (nopL2) Set(context.Context, string, string, time.Duration) error            { return nil }
func (nopL2) BatchSet(context.Context, List[string, string], time.Duration) error { return nil }
func (nopL2) Delete(context.Context, string) error                                { return nil }
func (nopL2) BatchDelete(context.Context, []string) error                         { return nil }
