//go:build windows

package main

import (
	"time"

	"golang.org/x/sys/windows"
)

// processCPUTime returns the total user+system CPU time consumed by this process so far.
func processCPUTime() (time.Duration, error) {
	var creation, exit, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(windows.CurrentProcess(), &creation, &exit, &kernel, &user); err != nil {
		return 0, err
	}
	return filetimeDuration(kernel) + filetimeDuration(user), nil
}

// filetimeDuration converts a FILETIME (100-nanosecond intervals) into a time.Duration.
func filetimeDuration(ft windows.Filetime) time.Duration {
	ticks := int64(ft.HighDateTime)<<32 | int64(ft.LowDateTime)
	return time.Duration(ticks) * 100 * time.Nanosecond
}
