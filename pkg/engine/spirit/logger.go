// logger.go implements a slog handler that filters and routes Spirit's internal
// log output through the engine's log callback mechanism.
package spirit

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
)

// logCallback receives one Spirit log line on its way to the apply log stream.
type logCallback = func(level slog.Level, table, msg string)

// loadLogCallback reads a callback slot, returning nil when no callback is
// registered. A caller that clears the slot passes a nil callback, so an empty
// slot and a cleared one both read as nothing to route to.
func loadLogCallback(slot *atomic.Pointer[logCallback]) logCallback {
	cb := slot.Load()
	if cb == nil {
		return nil
	}
	return *cb
}

// spiritLogFilter filters out noisy Spirit debug logs (replication events, etc.).
// It checks the engine's debugLogs setting which can be changed at runtime.
// It also routes logs to ApplyLogStore via the onLog callback.
type spiritLogFilter struct {
	handler slog.Handler
	debug   *atomic.Bool                 // Engine's debugLogs setting, for the runtime toggle
	onLog   *atomic.Pointer[logCallback] // Engine's onLog callback slot (with table context)
	table   string                       // Table attr attached via Logger.With, used when a record carries none
}

// routesToApplyLog reports whether a line at this level is recorded in the
// apply log stream. Routing is bound to the apply being driven, not to how
// verbose the process's own logs are configured to be.
func (f *spiritLogFilter) routesToApplyLog(level slog.Level) bool {
	return level >= slog.LevelInfo && loadLogCallback(f.onLog) != nil
}

// Enabled answers for both of the filter's consumers, because slog skips
// building the record entirely when it reports false. The apply log stream and
// the process logs choose independently: a deployment that runs its own logs at
// warn still records the engine's account of the schema change, which is the
// only account an operator reads from the CLI or a failed apply's summary.
//
// Debug lines are not part of that account, so they stay the process logs'
// decision alone. The runtime debug toggle narrows what a process already
// emitting debug shows; it never widens a quieter one.
func (f *spiritLogFilter) Enabled(ctx context.Context, level slog.Level) bool {
	return f.routesToApplyLog(level) || f.handler.Enabled(ctx, level)
}

func (f *spiritLogFilter) Handle(ctx context.Context, r slog.Record) error {
	// Filter ALL debug logs unless spirit_debug_logs=true
	if r.Level < slog.LevelInfo && !f.debug.Load() {
		return nil
	}

	// Route INFO+ logs to ApplyLogStore. The callback is read once and called
	// through that copy: a drive clearing the slot as it hands the engine over
	// must not be able to null it between the check and the call.
	if onLog := loadLogCallback(f.onLog); onLog != nil && r.Level >= slog.LevelInfo {
		// Extract table name and error/reason from slog attributes. Record
		// attrs (call-site) take precedence over the logger-level table attr
		// captured in WithAttrs — slog routes Logger.With attrs through the
		// handler, so r.Attrs never sees them.
		tableName := f.table
		errorMsg := ""
		r.Attrs(func(a slog.Attr) bool {
			switch a.Key {
			case "table":
				tableName = a.Value.String()
			case "error", "reason":
				errorMsg = a.Value.String()
			}
			return true // continue to get all relevant attrs
		})
		// Include error/reason in the message if present
		msg := r.Message
		if errorMsg != "" {
			msg = fmt.Sprintf("%s: %s", r.Message, errorMsg)
		}
		onLog(r.Level, tableName, msg)
	}

	// The process logs keep their own verbosity. A record built only because
	// the apply log stream wanted it is not one this handler agreed to emit,
	// and passing it through would raise a deployment's log volume as a side
	// effect of an apply being driven.
	if !f.handler.Enabled(ctx, r.Level) {
		return nil
	}
	return f.handler.Handle(ctx, r)
}

func (f *spiritLogFilter) WithAttrs(attrs []slog.Attr) slog.Handler {
	table := f.table
	for _, a := range attrs {
		if a.Key == "table" {
			table = a.Value.String()
		}
	}
	return &spiritLogFilter{handler: f.handler.WithAttrs(attrs), debug: f.debug, onLog: f.onLog, table: table}
}

func (f *spiritLogFilter) WithGroup(name string) slog.Handler {
	return &spiritLogFilter{handler: f.handler.WithGroup(name), debug: f.debug, onLog: f.onLog, table: f.table}
}
