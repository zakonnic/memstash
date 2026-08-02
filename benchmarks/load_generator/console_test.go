package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestConsole(t *testing.T, tty bool) (*console, func() string) {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "console.txt"))
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	read := func() string {
		data, err := os.ReadFile(f.Name())
		require.NoError(t, err)
		return string(data)
	}
	return &console{out: f, tty: tty}, read
}

// TestConsoleStatusBlockStaysBelow: errors scroll, the status block is erased and redrawn under them, and a status
// update rewrites its own slot instead of appending.
func TestConsoleStatusBlockStaysBelow(t *testing.T) {
	con, read := newTestConsole(t, true)

	con.setStatus(0, "scenario-1 first")
	con.setStatus(1, "scenario-2 first")
	con.print("cache operation failed")
	con.setStatus(1, "scenario-2 second")

	out := read()
	assert.Contains(t, out, "\x1b[2F\x1b[J", "the two drawn status lines must be erased before anything is written over")
	assert.True(t, strings.HasSuffix(out, "scenario-1 first\nscenario-2 second\n"), "got: %q", out)
	assert.Equal(t, 1, strings.Count(out, "cache operation failed"), "an error is printed once and never redrawn")
	assert.Equal(t, 2, con.drawn)
}

// TestConsoleWithoutTerminal: with output redirected there is nothing to rewrite, so every line just scrolls.
func TestConsoleWithoutTerminal(t *testing.T) {
	con, read := newTestConsole(t, false)

	con.setStatus(0, "scenario-1 first")
	con.print("cache operation failed")
	con.setStatus(0, "scenario-1 second")

	out := read()
	assert.NotContains(t, out, "\x1b[")
	assert.Equal(t, []string{"scenario-1 first", "cache operation failed", "scenario-1 second"},
		strings.Split(strings.TrimSpace(out), "\n"))
}

// TestConsoleStatusFlattensNewlines: an embedded newline would be a screen row the redraw knows nothing about, so it
// is folded into the line instead.
func TestConsoleStatusFlattensNewlines(t *testing.T) {
	con, read := newTestConsole(t, true)

	con.setStatus(0, "first\nsecond\n")

	assert.Equal(t, 1, con.drawn)
	assert.Equal(t, "first second\n", read())
}

func TestWrapLine(t *testing.T) {
	assert.Equal(t, []string{"abc"}, wrapLine("abc", 5))
	assert.Equal(t, []string{"abc"}, wrapLine("abc", 3))
	assert.Equal(t, []string{"abc", "d"}, wrapLine("abcd", 3))
	assert.Equal(t, []string{"abc", "def"}, wrapLine("abcdef", 3))
	assert.Equal(t, []string{"жут", "ь"}, wrapLine("жуть", 3), "width is counted in runes, not bytes")
	assert.Equal(t, []string{"abc"}, wrapLine("abc", 0), "an unknown width must not lose the line")
}

// TestConsoleStatusWrapsInFull: a status line longer than the terminal is kept whole and erased in full, so nothing
// of the log line is lost and no leftover row stays behind.
func TestConsoleStatusWrapsInFull(t *testing.T) {
	con, read := newTestConsole(t, true)
	long := strings.Repeat("x", defaultWidth*2) // no terminal behind a file: draw falls back to defaultWidth

	con.setStatus(0, long)
	rows := con.drawn
	con.setStatus(0, long)

	assert.Equal(t, 3, rows, "two full rows one column short of the width, plus the remainder")
	out := read()
	assert.Contains(t, out, "\x1b[3F\x1b[J", "the redraw must erase every row the line occupies")
	assert.Equal(t, 2, strings.Count(strings.ReplaceAll(out, "\n", ""), long), "both draws wrote the line in full")
}
