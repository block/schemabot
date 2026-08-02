package spirit

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// attrRecord is one log record captured by attrCaptureHandler.
type attrRecord struct {
	msg   string
	level slog.Level
	attrs map[string]any
}

// attrCaptureHandler records every log line with its resolved attributes —
// both record attrs and logger-level attrs bound via With — so tests can
// assert which identity a line carries.
type attrCaptureHandler struct {
	mu      *sync.Mutex
	records *[]attrRecord
	bound   []slog.Attr
}

func newAttrCapture() (*attrCaptureHandler, func() []attrRecord) {
	var mu sync.Mutex
	var records []attrRecord
	h := &attrCaptureHandler{mu: &mu, records: &records}
	return h, func() []attrRecord {
		mu.Lock()
		defer mu.Unlock()
		return append([]attrRecord(nil), records...)
	}
}

func (h *attrCaptureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *attrCaptureHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]any, r.NumAttrs()+len(h.bound))
	for _, a := range h.bound {
		attrs[a.Key] = a.Value.Any()
	}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.records = append(*h.records, attrRecord{msg: r.Message, level: r.Level, attrs: attrs})
	return nil
}

func (h *attrCaptureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &attrCaptureHandler{mu: h.mu, records: h.records, bound: append(append([]slog.Attr(nil), h.bound...), attrs...)}
}

func (h *attrCaptureHandler) WithGroup(string) slog.Handler { return h }

// requireRecord returns the first captured record with the given message.
func requireRecord(t *testing.T, records []attrRecord, msg string) attrRecord {
	t.Helper()
	for _, r := range records {
		if r.msg == msg {
			return r
		}
	}
	require.Failf(t, "log record not captured", "no record with message %q in %d records", msg, len(records))
	return attrRecord{}
}

// identityBoundLogger returns a logger carrying a caller triage identity the
// way the tern layer binds drive loggers, plus the capture accessor.
func identityBoundLogger() (*slog.Logger, func() []attrRecord) {
	h, captured := newAttrCapture()
	logger := slog.New(h).With("apply_id", "apply-lf2", "repo", "org/repo", "pr", 42)
	return logger, captured
}

// A schema change whose request carries a caller-bound logger must emit its
// engine lines through that logger so they inherit the caller's triage
// identity; the credentials guard fires after the opening line, keeping the
// test hermetic.
func TestApply_RequestLoggerCarriesIdentity(t *testing.T) {
	logger, captured := identityBoundLogger()
	eng := New(Config{})

	_, err := eng.Apply(t.Context(), &engine.ApplyRequest{
		Database: "app_db",
		Logger:   logger,
	})
	require.ErrorContains(t, err, "DSN credentials required")

	line := requireRecord(t, captured(), "applying plan")
	assert.Equal(t, "apply-lf2", line.attrs["apply_id"])
	assert.Equal(t, "org/repo", line.attrs["repo"])
	assert.Equal(t, int64(42), line.attrs["pr"])
	assert.Equal(t, "app_db", line.attrs["database"])
}

// A request without a logger falls back to the engine's configured loggers,
// so a nil request logger never panics and keeps the engine-level identity.
func TestResolveChangeLoggers_NilFallsBackToEngineLoggers(t *testing.T) {
	h, _ := newAttrCapture()
	base := slog.New(h)
	eng := New(Config{Logger: base})

	logger, spiritLogger := eng.resolveChangeLoggers(nil)
	assert.Same(t, base, logger)
	assert.Same(t, eng.spiritLogger, spiritLogger)
}

// The Spirit logger derived from a request logger must keep the engine's
// runtime behaviors — debug filtering and apply-log routing via the onLog
// callback — while writing through the caller's handler so runner lines
// inherit the caller identity.
func TestResolveChangeLoggers_SpiritLoggerKeepsFilterAndRouting(t *testing.T) {
	logger, captured := identityBoundLogger()
	eng := New(Config{})

	var mu sync.Mutex
	var routed []capturedLog
	eng.SetLogCallback(func(level slog.Level, table, msg string) {
		mu.Lock()
		defer mu.Unlock()
		routed = append(routed, capturedLog{level: level, table: table, msg: msg})
	})

	_, spiritLogger := eng.resolveChangeLoggers(logger)

	spiritLogger.Debug("replication event") // filtered: debug logs are off
	spiritLogger.With("table", "customers").Info("copy rows complete")

	line := requireRecord(t, captured(), "copy rows complete")
	assert.Equal(t, "apply-lf2", line.attrs["apply_id"])
	assert.Equal(t, "org/repo", line.attrs["repo"])

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, routed, 1)
	assert.Equal(t, "copy rows complete", routed[0].msg)
	assert.Equal(t, "customers", routed[0].table)
}

// changeLogger and changeSpiritLogger expose the tracked change's loggers to
// the execution path and fall back to the engine's loggers when no change is
// tracked or the change carries none, so background execution never loses its
// identity nor panics on an engine-internal resume.
func TestChangeLoggers_TrackedChangeAndFallback(t *testing.T) {
	logger, _ := identityBoundLogger()
	eng := New(Config{})

	assert.Same(t, eng.logger, eng.changeLogger())
	assert.Same(t, eng.spiritLogger, eng.changeSpiritLogger())

	boundLogger, boundSpirit := eng.resolveChangeLoggers(logger)
	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{logger: boundLogger, spiritLogger: boundSpirit}
	eng.mu.Unlock()

	assert.Same(t, boundLogger, eng.changeLogger())
	assert.Same(t, boundSpirit, eng.changeSpiritLogger())

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{}
	eng.mu.Unlock()

	assert.Same(t, eng.logger, eng.changeLogger())
	assert.Same(t, eng.spiritLogger, eng.changeSpiritLogger())
}
