package load_generator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testScenario is a tiny L1-only scenario: a key space small enough to fill in a blink, a rate low enough not to
// matter, and values the truth map can verify.
func testScenario(name string) Scenario[string, []byte] {
	return Scenario[string, []byte]{
		Name:          name,
		Description:   "lifecycle test",
		CacheSize:     100,
		RPS:           EvenSplit(2, 200),
		ReadPercent:   90,
		WriteKeySpace: 500,
		ZipfS:         1.01,
		Value:         SessionValue,
		Equal:         bytes.Equal,
	}
}

// newTestApp builds an app writing into a temp dir, with a stats interval short enough for a test to see a snapshot.
func newTestApp(t *testing.T, scenarios ...Scenario[string, []byte]) (*App[string, []byte], string) {
	t.Helper()
	dir := t.TempDir()
	out, err := os.Create(filepath.Join(dir, "console.txt"))
	require.NoError(t, err)
	t.Cleanup(func() { out.Close() })

	app, err := New(scenarios, WithLogDir(dir), WithOutput(out), WithStatsInterval(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { app.Shutdown() })
	return app, dir
}

// records returns the "msg" of every JSON line the scenario logged, in order.
func records(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var msgs []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		msgs = append(msgs, record["msg"].(string))
	}
	return msgs
}

// TestAppRunsUntilContextCanceled is the whole lifecycle end to end: New builds, Start drives the load until the
// context goes away, Shutdown flushes and releases. Real load over a real cache, so a scenario that cannot run at
// all shows up here.
func TestAppRunsUntilContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app, dir := newTestApp(t, testScenario("scenario-1"), testScenario("scenario-2"))

	assert.Positive(t, app.TruthHeap(), "the truth maps are filled before Start")

	done := make(chan struct{})
	go func() {
		defer close(done)
		app.Start(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after the context was canceled")
	}
	require.NoError(t, app.Shutdown())

	for _, name := range []string{"scenario-1", "scenario-2"} {
		msgs := records(t, filepath.Join(dir, name+".log"))
		require.NotEmpty(t, msgs, "%s logged nothing at all", name)
		assert.Equal(t, "scenario started", msgs[0])
		assert.Equal(t, "scenario stopped", msgs[len(msgs)-1])
		assert.Contains(t, msgs, "stats")
	}
	assert.Zero(t, app.Errors(), "verified load over an L1-only cache must not log a single error")
}

// TestShutdownStopsStart: whoever holds the App can stop the run without touching the context Start was given.
func TestShutdownStopsStart(t *testing.T) {
	app, _ := newTestApp(t, testScenario("scenario-1"))

	done := make(chan struct{})
	go func() {
		defer close(done)
		app.Start(context.Background())
	}()
	time.Sleep(50 * time.Millisecond)

	require.NoError(t, app.Shutdown())
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not make Start return")
	}

	require.NoError(t, app.Shutdown(), "a second Shutdown is a no-op")
	app.Start(context.Background()) // and Start after it must not resurrect anything
}

// TestNewRejectsBadScenarios: every scenario is checked before anything is built, so a typo fails at New instead of
// leaving a scenario running with no load.
func TestNewRejectsBadScenarios(t *testing.T) {
	valid := testScenario("scenario-1")
	tests := map[string]struct {
		scenarios []Scenario[string, []byte]
		wantErr   string
	}{
		"no scenarios":   {nil, "no scenarios"},
		"no name":        {[]Scenario[string, []byte]{{}}, "no Name"},
		"no value":       {[]Scenario[string, []byte]{{Name: "s", CacheSize: 1}}, "Value must be set"},
		"zipf too flat":  {[]Scenario[string, []byte]{withZipfS(valid, 1)}, "ZipfS=1 must be > 1"},
		"rps count":      {[]Scenario[string, []byte]{withGoroutines(valid, 3)}, "RPS has 2 entries but Goroutines=3"},
		"duplicate name": {[]Scenario[string, []byte]{valid, valid}, `duplicate scenario name "scenario-1"`},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			app, err := New(tt.scenarios, WithLogDir(t.TempDir()))
			assert.Nil(t, app)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func withZipfS(s Scenario[string, []byte], zipfS float64) Scenario[string, []byte] {
	s.ZipfS = zipfS
	return s
}

func withGoroutines(s Scenario[string, []byte], n int) Scenario[string, []byte] {
	s.Goroutines = n
	return s
}

// TestGenericScenario: K and V are the cache's own types, and nothing in the engine assumes strings or bytes. Here
// the keys are ints - so the scenario has to bring its own Key - and the values are structs the default Equal
// verifies with reflect.DeepEqual.
func TestGenericScenario(t *testing.T) {
	type row struct {
		ID   int
		Name string
	}
	rows := Scenario[int, row]{
		Name:          "rows",
		CacheSize:     100,
		RPS:           EvenSplit(2, 400),
		ReadPercent:   50,
		WriteKeySpace: 200,
		ZipfS:         1.01,
		Key:           func(n int) int { return n },
		Value:         func(key int) row { return row{ID: key, Name: fmt.Sprintf("row-%d", key)} },
	}

	noKey := rows
	noKey.Key = nil
	_, err := New([]Scenario[int, row]{noKey}, WithLogDir(t.TempDir()))
	require.ErrorContains(t, err, "Key must be set", "int keys have no built-in key function")

	dir := t.TempDir()
	out, err := os.Create(filepath.Join(dir, "console.txt"))
	require.NoError(t, err)
	t.Cleanup(func() { out.Close() })

	app, err := New([]Scenario[int, row]{rows}, WithLogDir(dir), WithOutput(out),
		WithStatsInterval(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { app.Shutdown() })

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	app.Start(ctx)
	require.NoError(t, app.Shutdown())

	assert.Zero(t, app.Errors(), "struct values must verify against the truth map like any other")
	assert.Contains(t, records(t, filepath.Join(dir, "rows.log")), "stats")
}

// TestScenarioDefaults: the fields a caller can leave out, and what they become.
func TestScenarioDefaults(t *testing.T) {
	s := Scenario[string, []byte]{RPS: EvenSplit(4, 1_000), WriteKeySpace: 1_000}.withDefaults()

	assert.Equal(t, 4, s.Goroutines, "one worker per rps entry")
	assert.Equal(t, 1_000, s.KeySpace, "reads cover the keys writes create")
	assert.Equal(t, float64(1), s.ZipfV)
}
