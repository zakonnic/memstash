package badger_adapter_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"

	badger_adapter "github.com/zakonnic/memstash/l2/badger_adapter"
)

// A value past ValueThreshold lives in the mmap'd value log, and the slice Badger hands out points there only for the
// duration of the transaction: a value-log GC or Close unmaps it under anyone who kept it. Get must return its own copy.
func TestGetReturnsItsOwnCopy(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithValueThreshold(1 << 10).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c, err := badger_adapter.NewBytes[string](db)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	want := bytes.Repeat([]byte("v"), 32<<10)
	if err := c.Set(ctx, "k", want, 0); err != nil {
		t.Fatal(err)
	}
	first, ok, err := c.Get(ctx, "k")
	if err != nil || !ok {
		t.Fatalf("Get: found=%v err=%v", ok, err)
	}
	second, _, _ := c.Get(ctx, "k")
	if &first[0] == &second[0] {
		t.Fatal("two Gets returned the same buffer: it is Badger's own memory, not a copy")
	}
	if !bytes.Equal(first, want) {
		t.Fatal("Get returned a different value")
	}
}
