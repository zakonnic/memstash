package badger_adapter_test

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/zakonnic/memstash"

	badger_adapter "github.com/zakonnic/memstash/l2/badger_adapter"
)

// Keys Badger rejects as they are - empty, with its internal prefix, too long - work like any other, alone and in a
// batch next to ordinary ones.
func TestAwkwardKeys(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c, err := badger_adapter.NewBytes[string](db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	keys := []string{"plain", "", "!badger!x", string(bytes.Repeat([]byte("k"), 70000))}
	for _, key := range keys {
		if err := c.Set(ctx, key, []byte("v"), 0); err != nil {
			t.Fatalf("Set %.20q: %v", key, err)
		}
		if _, ok, err := c.Get(ctx, key); err != nil || !ok {
			t.Fatalf("Get %.20q: found=%v err=%v", key, ok, err)
		}
	}
	items := memstash.List[string, []byte]{}
	for _, key := range keys {
		items = append(items, memstash.KeyVal[string, []byte]{Key: key, Value: []byte("w")})
	}
	if err := c.BatchSet(ctx, items, 0); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}
	if found, err := c.BatchGet(ctx, keys); err != nil || len(found) != len(keys) {
		t.Fatalf("BatchGet: %d of %d, err %v", len(found), len(keys), err)
	}
	if err := c.BatchDelete(ctx, keys); err != nil {
		t.Fatalf("BatchDelete: %v", err)
	}
	if found, _ := c.BatchGet(ctx, keys); len(found) != 0 {
		t.Fatalf("%d keys survived BatchDelete", len(found))
	}
}

// A TTL under a second is still a TTL: the entry must be there right after the write, not expired by a deadline
// rounded down to the current second.
func TestShortTTLIsVisible(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c, err := badger_adapter.NewBytes[string](db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := range 20 {
		if err := c.Set(ctx, "k", []byte("v"), 300*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := c.Get(ctx, "k"); err != nil || !ok {
			t.Fatalf("write %d: gone right after it was written (err %v)", i, err)
		}
	}
}

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

var errRefused = errors.New("the test codec refuses this value")

// pickyCodec is a string codec that cannot encode "unencodable" nor decode "poison".
type pickyCodec struct{}

func (pickyCodec) Marshal(value string) ([]byte, error) {
	if value == "unencodable" {
		return nil, errRefused
	}
	return []byte(value), nil
}

func (pickyCodec) Unmarshal(data []byte) (string, error) {
	if string(data) == "poison" {
		return "", errRefused
	}
	return string(data), nil
}

// A value the codec rejects fails its own key, not the batch around it, and the error names the key.
func TestCodecErrorsStayWithTheirKey(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c, err := badger_adapter.New[string, string](db, pickyCodec{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	err = c.BatchSet(ctx, memstash.List[string, string]{
		{Key: "a", Value: "1"}, {Key: "broken", Value: "unencodable"}, {Key: "c", Value: "3"},
	}, 0)
	if !errors.Is(err, errRefused) || !strings.Contains(err.Error(), "broken") {
		t.Fatalf("BatchSet: %v", err)
	}
	if err := c.Set(ctx, "poisoned", "poison", 0); err != nil {
		t.Fatal(err)
	}
	found, err := c.BatchGet(ctx, []string{"a", "poisoned", "c"})
	if !errors.Is(err, errRefused) || !strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("BatchGet: %v", err)
	}
	if want := map[string]string{"a": "1", "c": "3"}; !maps.Equal(found.ToMap(), want) {
		t.Fatalf("BatchGet found %v, want %v", found.ToMap(), want)
	}
}
