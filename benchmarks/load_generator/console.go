package load_generator

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

// defaultWidth is the line budget used when the terminal width cannot be read.
const defaultWidth = 120

// console owns stdout: one status line per scenario stays pinned at the bottom and is rewritten in place, while
// everything else - errors, notices, the standard logger - scrolls above the block. Without a terminal (output
// redirected to a file or a pipe) it degrades to plain sequential lines.
type console struct {
	mu     sync.Mutex
	out    *os.File
	tty    bool
	status []string
	drawn  int // screen rows the status block currently occupies, i.e. how far up the cursor must go to erase it
}

func newConsole(out *os.File) *console {
	return &console{out: out, tty: enableVirtualTerminal(out)}
}

// setStatus replaces the slot's line and repaints the block in place. Slots grow on first use, so each scenario just
// picks an index.
func (c *console) setStatus(slot int, line string) {
	line = strings.ReplaceAll(strings.TrimRight(line, "\n"), "\n", " ") // a status must stay one screen line
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.status) <= slot {
		c.status = append(c.status, "")
	}
	c.status[slot] = line
	if !c.tty {
		fmt.Fprintln(c.out, line)
		return
	}
	c.erase()
	c.draw()
}

// print writes a line above the status block, which is erased first and redrawn after so it stays at the bottom.
func (c *console) print(line string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.erase()
	fmt.Fprintln(c.out, strings.TrimRight(line, "\n"))
	c.draw()
}

// writer adapts print to io.Writer: for the standard logger and for slog handlers.
func (c *console) writer() io.Writer { return consoleWriter{c} }

type consoleWriter struct{ con *console }

func (w consoleWriter) Write(p []byte) (int, error) {
	w.con.print(string(p))
	return len(p), nil
}

// statusWriter pins a scenario's log lines to its console slot instead of scrolling them.
type statusWriter struct {
	con  *console
	slot int
}

func (w statusWriter) Write(p []byte) (int, error) {
	w.con.setStatus(w.slot, string(p))
	return len(p), nil
}

func (c *console) erase() {
	if c.drawn == 0 {
		return
	}
	fmt.Fprintf(c.out, "\x1b[%dF\x1b[J", c.drawn) // up to the first status line, then erase to the end of the screen
	c.drawn = 0
}

func (c *console) draw() {
	if !c.tty {
		return // nothing gets rewritten, so there is no block to keep at the bottom
	}
	width := terminalWidth(c.out)
	for _, line := range c.status {
		if line == "" {
			continue
		}
		for _, row := range wrapLine(line, width-1) {
			fmt.Fprintln(c.out, row)
			c.drawn++
		}
	}
}

// wrapLine breaks a status line into screen rows itself instead of letting the terminal do it: erase counts the rows
// it wrote, and a row the terminal wrapped on its own would not be in that count.
func wrapLine(s string, width int) []string {
	runes := []rune(s)
	if width < 1 || len(runes) <= width {
		return []string{s}
	}
	rows := make([]string, 0, (len(runes)+width-1)/width)
	for start := 0; start < len(runes); start += width {
		rows = append(rows, string(runes[start:min(start+width, len(runes))]))
	}
	return rows
}
