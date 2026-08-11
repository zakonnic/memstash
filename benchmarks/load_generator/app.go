// Package load_generator drives memstash caches under continuous, verified load. You describe the scenarios; it
// builds their caches, runs the workers, checks every Get against the scenario's Value function, and writes a stats
// snapshot per scenario once a minute. Anything that goes wrong - failed operations, values that don't match,
// panics - lands in errors.log next to the per-scenario logs.
//
//	scenarios := []load_generator.Scenario[string, []byte]{ ... }
//
//	app, err := load_generator.New(scenarios, load_generator.WithLogDir("var/load"))
//	if err != nil {
//		panic(err)
//	}
//	defer app.Shutdown()
//	app.Start(ctx) // blocks until ctx is canceled
//
// The scenarios' K and V are the cache's own types; caches of different types need an App each.
package load_generator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// errorLogFile is the name every app writes its errors under, inside the log directory.
const errorLogFile = "errors.log"

// Option configures an App: where its files go, where its display goes, and how often it reports.
type Option func(*options)

type options struct {
	logDir        string
	out           *os.File
	statsInterval time.Duration
	verify        bool
}

func defaultOptions() options {
	return options{logDir: ".", out: os.Stdout, statsInterval: time.Minute, verify: true}
}

// WithLogDir writes errors.log and the per-scenario logs into dir, which is created if it doesn't exist. Default is
// the working directory.
func WithLogDir(dir string) Option { return func(o *options) { o.logDir = dir } }

// WithOutput sends the status block and the mirrored log lines to f instead of os.Stdout. Without a terminal behind
// f - a file or a pipe - the display degrades to plain scrolling lines.
func WithOutput(f *os.File) Option { return func(o *options) { o.out = f } }

// WithStatsInterval sets how often each scenario writes a stats snapshot. Default is a minute.
func WithStatsInterval(d time.Duration) Option { return func(o *options) { o.statsInterval = d } }

// WithNoVerification stops checking what Gets return: no Value call per hit, no mismatch and no never-written-key
// errors. Failed operations, L2 errors and panics still reach errors.log.
func WithNoVerification() Option { return func(o *options) { o.verify = false } }

// openLogging creates the log directory and opens errors.log together with the console it mirrors into.
func (o options) openLogging() (*console, *errorLog, string, error) {
	if err := os.MkdirAll(o.logDir, 0o755); err != nil {
		return nil, nil, "", fmt.Errorf("create log dir %s: %w", o.logDir, err)
	}
	con := newConsole(o.out)
	path := filepath.Join(o.logDir, errorLogFile)
	errLog, err := newErrorLog(path, con)
	if err != nil {
		return nil, nil, "", fmt.Errorf("open %s: %w", path, err)
	}
	return con, errLog, path, nil
}

// App owns everything the scenarios need to run: their caches, Redis clients, log files and the console they share.
// New builds them, Start drives them, Shutdown releases them.
type App[K comparable, V any] struct {
	runners []*runner[K, V]
	console *console
	errLog  *errorLog
	errPath string

	wg sync.WaitGroup

	// mu covers the launch itself, so a Shutdown racing with Start either sees the workers or closes the door before
	// they start; it is never held while Start blocks on the run.
	mu       sync.Mutex
	started  bool
	closed   bool
	cancel   context.CancelFunc // stops the run Start is blocked in; nil until it launches
	stopping sync.Once
	stopErr  error
}

// New builds every scenario's cache and opens the log files; no load runs until Start, and nothing here needs a
// context.
//
// The App holds a Redis client and background goroutines per scenario, so call Shutdown even when Start never runs.
func New[K comparable, V any](scenarios []Scenario[K, V], opts ...Option) (*App[K, V], error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	if len(scenarios) == 0 {
		return nil, errors.New("no scenarios")
	}
	if o.statsInterval <= 0 {
		return nil, fmt.Errorf("stats interval %s must be positive", o.statsInterval)
	}

	con, errLog, errPath, err := o.openLogging()
	if err != nil {
		return nil, err
	}

	app := &App[K, V]{console: con, errLog: errLog, errPath: errPath}

	seen := make(map[string]struct{}, len(scenarios))
	for i, s := range scenarios {
		s = s.withDefaults()
		if err := s.validate(); err != nil {
			return nil, app.abort(err)
		}
		if _, dup := seen[s.Name]; dup {
			return nil, app.abort(fmt.Errorf("duplicate scenario name %q", s.Name))
		}
		seen[s.Name] = struct{}{}

		r := &runner[K, V]{
			Scenario:      s,
			errLog:        errLog,
			console:       con,
			slot:          i,
			logPath:       filepath.Join(o.logDir, s.Name+".log"),
			statsInterval: o.statsInterval,
			verify:        o.verify,
		}
		app.runners = append(app.runners, r)
		if err := r.open(); err != nil {
			return nil, app.abort(err)
		}
	}
	return app, nil
}

// abort releases whatever New already built - the caches hold background goroutines and a Redis client each - and
// hands the error back.
func (a *App[K, V]) abort(err error) error {
	for _, r := range a.runners {
		r.close()
	}
	a.errLog.Close()
	return err
}

// Start runs every scenario and blocks until ctx is canceled or Shutdown is called. Calling it a second time, or
// after Shutdown, returns at once. It leaves the caches open - Shutdown is what releases them.
func (a *App[K, V]) Start(ctx context.Context) {
	a.mu.Lock()
	if a.started || a.closed {
		a.mu.Unlock()
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	a.started, a.cancel = true, cancel
	for _, r := range a.runners {
		a.wg.Add(1)
		go r.run(runCtx, &a.wg)
	}
	a.mu.Unlock()

	<-runCtx.Done()
	a.wg.Wait()
}

// Shutdown stops the workers, lets every scenario write its final stats, flushes the write-back buffers to L2 and
// closes the caches and log files.
func (a *App[K, V]) Shutdown() error {
	a.stopping.Do(func() {
		// The teardown runs under mu: a Start racing with it either got its workers in - and wg.Wait covers them -
		// or arrives after and finds the door closed. Nothing the workers do takes mu, so holding it is free.
		a.mu.Lock()
		defer a.mu.Unlock()

		a.closed = true
		if a.cancel != nil {
			a.cancel()
		}
		a.wg.Wait()
		for _, r := range a.runners {
			r.close()
		}
		a.stopErr = a.errLog.Close()
	})
	return a.stopErr
}

// Writer returns the console writer: lines written through it scroll above the status block instead of landing in
// the middle of it. Point the standard logger at it with log.SetOutput(app.Writer()).
func (a *App[K, V]) Writer() io.Writer { return a.console.writer() }

// Errors is how many errors have reached errors.log so far.
func (a *App[K, V]) Errors() int64 { return a.errLog.count.Load() }

// ErrorLogPath is the file those errors go to.
func (a *App[K, V]) ErrorLogPath() string { return a.errPath }

// Recover gives a panic that unwound out of the caller a line in errors.log before the runtime prints it and kills
// the process, then re-panics. Must be deferred directly: defer app.Recover("main").
func (a *App[K, V]) Recover(where string) {
	if r := recover(); r != nil {
		a.errLog.panicked("", where, r)
		panic(r)
	}
}

// PrintScenarios writes every scenario's effective parameters - defaults filled in, whatever the caller passed
// applied - to w as one block.
func (a *App[K, V]) PrintScenarios(w io.Writer) error {
	var buf bytes.Buffer
	if _, err := fmt.Fprintln(&buf, "Scenarios:"); err != nil {
		return err
	}
	for _, r := range a.runners {
		if _, err := fmt.Fprintf(&buf, "\n[%s]\n", r.Name); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(&buf, "  %s\n", r.Description); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(&buf, "  cache size:      %d\n", r.CacheSize); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(&buf, "  redis (L2):      %s\n", redisDisplay(r.RedisAddress)); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(&buf, "  goroutines:      %d\n", r.Goroutines); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(&buf, "  rps (total):     %.0f\n", r.totalRPS()); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(&buf, "  read / write:    %d%% Get / %d%% Set\n", r.ReadPercent, 100-r.ReadPercent); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(&buf, "  key space:       %d (Zipf s=%.2f v=%.2f)\n", r.KeySpace, r.ZipfS, r.ZipfV); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(&buf, "  write key space: %d\n", r.WriteKeySpace); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(&buf, "  uniform keys:    %s\n", randomDisplay(r.Scenario)); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(&buf, "  log file:        %s\n", r.logPath); err != nil {
			return err
		}
	}
	if err := buf.WriteByte('\n'); err != nil {
		return err
	}
	_, err := w.Write(buf.Bytes())
	return err
}

func randomDisplay[K comparable, V any](s Scenario[K, V]) string {
	if s.RandomPercent <= 0 {
		return "none (Zipf only, the tail stays cold)"
	}
	return fmt.Sprintf("%d%% of ops, a given key every %s, all keys in %s",
		s.RandomPercent, s.randomPeriod().Round(time.Second), s.randomCover().Round(time.Minute))
}

func redisDisplay(seeds []string) string {
	if len(seeds) == 0 {
		return "none (L1 only)"
	}
	return strings.Join(seeds, ",")
}
