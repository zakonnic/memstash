package main

import (
	"context"
	"log/slog"
	"math"
)

// fanout sends every record to all its handlers: one log call writes the JSON line to the log file and a compact copy
// to the console.
type fanout []slog.Handler

func (f fanout) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range f {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (f fanout) Handle(ctx context.Context, r slog.Record) error {
	var firstErr error
	for _, h := range f {
		if !h.Enabled(ctx, r.Level) {
			continue
		}
		if err := h.Handle(ctx, r.Clone()); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (f fanout) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := make(fanout, len(f))
	for i, h := range f {
		next[i] = h.WithAttrs(attrs)
	}
	return next
}

func (f fanout) WithGroup(name string) slog.Handler {
	next := make(fanout, len(f))
	for i, h := range f {
		next[i] = h.WithGroup(name)
	}
	return next
}

// statusOptions formats a scenario's log record for its one-line console slot: no timestamp or level (the line is
// rewritten in place, so "when" is now), and floats rounded to something readable.
var statusOptions = &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
	switch a.Key {
	case slog.TimeKey, slog.LevelKey:
		return slog.Attr{}
	}
	return roundFloat(a)
}}

// errorOptions formats an error record for the console: errors scroll away, so they keep a (short) timestamp.
var errorOptions = &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
	switch a.Key {
	case slog.TimeKey:
		return slog.String(slog.TimeKey, a.Value.Time().Format("15:04:05"))
	case slog.LevelKey:
		return slog.Attr{}
	}
	return a
}}

func roundFloat(a slog.Attr) slog.Attr {
	if a.Value.Kind() != slog.KindFloat64 {
		return a
	}
	return slog.Float64(a.Key, math.Round(a.Value.Float64()*100)/100)
}
