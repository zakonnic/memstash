package rainycape_adapter_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/rainycape/memcache"

	"github.com/zakonnic/memstash"
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

// A batch the server answers in full is one GetMulti: a BatchGet that sends a Get per key fails here.
func TestBatchGetOfHitsSendsNoGet(t *testing.T) {
	server := newFakeMemcached(t, -1)
	c := newAdapter(t, server)
	found, err := c.BatchGet(context.Background(), []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(found); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("found %v, want a b c in the order asked", got)
	}
	if gets := server.gets.Load(); gets != 0 {
		t.Fatalf("%d Gets on top of the GetMulti", gets)
	}
}

// The server hangs up after the first answer of the GetMulti, which the client reports as a one-item result with no
// error. The keys it lost must be read again: without that, b and c come back as misses.
func TestBatchGetReadsAgainWhatGetMultiLost(t *testing.T) {
	server := newFakeMemcached(t, 1)
	c := newAdapter(t, server)
	found, err := c.BatchGet(context.Background(), []string{"a", "b", "c", "absent"})
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(found); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("found %v, want a b c", got)
	}
	if gets := server.gets.Load(); gets != 3 {
		t.Fatalf("%d Gets, want one for each of b, c and absent", gets)
	}
}

// With the Get of c failing too, the error must name the keys left unanswered and not a, which GetMulti returned:
// the cache reports a batch error for every key it concerns.
func TestBatchGetErrorNamesTheUnansweredKeys(t *testing.T) {
	server := newFakeMemcached(t, 1)
	server.broken = "c"
	c := newAdapter(t, server)
	found, err := c.BatchGet(context.Background(), []string{"a", "b", "c"})
	if len(found) == 0 || found[0].Key != "a" {
		t.Fatalf("found %v, want a first", keysOf(found))
	}
	var joined interface{ Unwrap() []error }
	if !errors.As(err, &joined) {
		t.Fatalf("error %v, want the failures joined", err)
	}
	var named []any
	for _, part := range joined.Unwrap() {
		var keyErr *memstash.KeyError
		if !errors.As(part, &keyErr) {
			t.Fatalf("error %v names no key", part)
		}
		named = append(named, keyErr.Key)
	}
	if !slices.Contains(named, any("c")) || slices.Contains(named, any("a")) {
		t.Fatalf("error names %v, want c and never a", named)
	}
}

func newAdapter(t *testing.T, server *fakeMemcached) *rainycape_adapter.Cache[string, string] {
	t.Helper()
	client, err := memcache.New(server.addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	c, err := rainycape_adapter.New[string, string](client, l2.StringCodec())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func keysOf(found memstash.List[string, string]) []string {
	keys := make([]string, len(found))
	for i, item := range found {
		keys[i] = item.Key
	}
	return keys
}

// fakeMemcached serves Get and GetMulti over the binary protocol, holding the keys a, b and c. A GetMulti gets at
// most cut answers and then a closed connection in place of the reply that ends the batch; cut < 0 never hangs up.
// A Get of the broken key gets a closed connection too.
type fakeMemcached struct {
	addr   string
	cut    int
	broken string
	gets   atomic.Int64
}

const (
	opGet   = 0x00
	opNoop  = 0x0a
	opGetKQ = 0x0d // a Get that names its key in the answer and stays silent on a miss: what GetMulti sends
)

func newFakeMemcached(t *testing.T, cut int) *fakeMemcached {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	s := &fakeMemcached{addr: listener.Addr().String(), cut: cut}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go s.serve(conn)
		}
	}()
	return s
}

func (s *fakeMemcached) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	header := make([]byte, 24)
	answered := 0
	for {
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		body := make([]byte, binary.BigEndian.Uint32(header[8:]))
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}
		key := string(body)
		held := key == "a" || key == "b" || key == "c"
		var err error
		switch op := header[1]; {
		case op == opGet && key == s.broken:
			return
		case op == opGet:
			s.gets.Add(1)
			if held {
				err = reply(conn, op, "", "value of "+key)
			} else {
				err = replyStatus(conn, op, 1) // key not found
			}
		case op == opGetKQ && held && answered != s.cut:
			answered++
			err = reply(conn, op, key, "value of "+key)
		case op == opNoop && s.cut >= 0:
			return
		case op == opNoop:
			err = replyStatus(conn, op, 0)
		}
		if err != nil {
			return
		}
	}
}

// reply writes a found item: four bytes of flags, the key when the command echoes it, and the value.
func reply(conn net.Conn, op byte, key, value string) error {
	const flags = 4
	packet := make([]byte, 24+flags, 24+flags+len(key)+len(value))
	packet[0], packet[1], packet[4] = 0x81, op, flags
	binary.BigEndian.PutUint16(packet[2:], uint16(len(key)))
	binary.BigEndian.PutUint32(packet[8:], uint32(flags+len(key)+len(value)))
	_, err := conn.Write(append(append(packet, key...), value...))
	return err
}

// replyStatus writes an answer with no body.
func replyStatus(conn net.Conn, op byte, status uint16) error {
	packet := make([]byte, 24)
	packet[0], packet[1] = 0x81, op
	binary.BigEndian.PutUint16(packet[6:], status)
	_, err := conn.Write(packet)
	return err
}
