// logger.go implements a slog handler that filters and routes Spirit's internal
// log output through the engine's log callback mechanism.
package spirit

import (
	"context"
	"fmt"
	"log/slog"
)

// spiritLogFilter filters out noisy Spirit debug logs (replication events, etc.).
// It checks the engine's debugLogs setting which can be changed at runtime.
// It also routes logs to ApplyLogStore via the onLog callback.
type spiritLogFilter struct {
	handler  slog.Handler
	debugRef *bool                                      // Pointer to engine's debugLogs setting for runtime toggle
	onLogRef *func(level slog.Level, table, msg string) // Pointer to engine's onLog callback (with table context)
}

func (f *spiritLogFilter) Enabled(ctx context.Context, level slog.Level) bool {
	return f.handler.Enabled(ctx, level)
}

func (f *spiritLogFilter) Handle(ctx context.Context, r slog.Record) error {
	debugEnabled := f.debugRef != nil && *f.debugRef

	// Filter ALL debug logs unless spirit_debug_logs=true
	if r.Level < slog.LevelInfo && !debugEnabled {
		return nil
	}

	// Route INFO+ logs to ApplyLogStore
	if f.onLogRef != nil && *f.onLogRef != nil && r.Level >= slog.LevelInfo {
		// Extract table name and error/reason from slog attributes
		tableName := ""
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
		(*f.onLogRef)(r.Level, tableName, msg)
	}

	return f.handler.Handle(ctx, r)
}

func (f *spiritLogFilter) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &spiritLogFilter{handler: f.handler.WithAttrs(attrs), debugRef: f.debugRef, onLogRef: f.onLogRef}
}

func (f *spiritLogFilter) WithGroup(name string) slog.Handler {
	return &spiritLogFilter{handler: f.handler.WithGroup(name), debugRef: f.debugRef, onLogRef: f.onLogRef}
}
