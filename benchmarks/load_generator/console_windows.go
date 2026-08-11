//go:build windows

package load_generator

import (
	"os"

	"golang.org/x/sys/windows"
)

// enableVirtualTerminal switches the console behind f into ANSI mode and reports whether f is a console at all -
// redirected output gets the plain fallback.
func enableVirtualTerminal(f *os.File) bool {
	h := windows.Handle(f.Fd())
	var mode uint32
	if err := windows.GetConsoleMode(h, &mode); err != nil {
		return false
	}
	if mode&windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING != 0 {
		return true
	}
	return windows.SetConsoleMode(h, mode|windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING) == nil
}

func terminalWidth(f *os.File) int {
	var info windows.ConsoleScreenBufferInfo
	if err := windows.GetConsoleScreenBufferInfo(windows.Handle(f.Fd()), &info); err != nil {
		return defaultWidth
	}
	if width := int(info.Window.Right - info.Window.Left + 1); width > 1 {
		return width
	}
	return defaultWidth
}
