//go:build !windows

package main

import (
	"os"

	"golang.org/x/sys/unix"
)

// enableVirtualTerminal reports whether f is a terminal; ANSI needs no enabling here.
func enableVirtualTerminal(f *os.File) bool {
	_, err := unix.IoctlGetWinsize(int(f.Fd()), unix.TIOCGWINSZ)
	return err == nil
}

func terminalWidth(f *os.File) int {
	ws, err := unix.IoctlGetWinsize(int(f.Fd()), unix.TIOCGWINSZ)
	if err != nil || ws.Col < 2 {
		return defaultWidth
	}
	return int(ws.Col)
}
