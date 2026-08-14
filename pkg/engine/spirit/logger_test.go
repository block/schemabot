package spirit

import (
	"bytes"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// capturedLog records one invocation of the engine's onLog callback.
type capturedLog struct {
	level slog.Level
	table string
	msg   string
}

// newFilterLogger builds a logger backed by a spiritLogFilter whose routed
// lines are appended to the returned slice, mirroring how the engine wires
// Spirit runner output into the apply log stream.
func newFilterLogger(t *testing.T) (*slog.Logger, func() []capturedLog) {
	t.Helper()
	logger, captured, _ := newFilterLoggerAt(t, slog.LevelDebug, true)
	return logger, captured
}

// newFilterLoggerAt builds the same filter with the process handler pinned to
// handlerLevel and, optionally, no apply-log callback installed. It returns the
// process handler's own output alongside the routed lines so a test can tell
// the filter's two consumers apart.
func newFilterLoggerAt(t *testing.T, handlerLevel slog.Level, routeToApplyLog bool) (*slog.Logger, func() []capturedLog, func() string) {
	t.Helper()
	var mu sync.Mutex
	var captured []capturedLog
	var processLogs bytes.Buffer
	onLog := func(level slog.Level, table, msg string) {
		mu.Lock()
		defer mu.Unlock()
		captured = append(captured, capturedLog{level: level, table: table, msg: msg})
	}
	if !routeToApplyLog {
		onLog = nil
	}
	debug := false
	filter := &spiritLogFilter{
		handler:  slog.NewTextHandler(&processLogs, &slog.HandlerOptions{Level: handlerLevel}),
		debugRef: &debug,
		onLogRef: &onLog,
	}
	return slog.New(filter), func() []capturedLog {
			mu.Lock()
			defer mu.Unlock()
			return append([]capturedLog(nil), captured...)
		}, func() string {
			mu.Lock()
			defer mu.Unlock()
			return processLogs.String()
		}
}

// TestSpiritLogFilter_TableFromLoggerWith verifies that a table attached via
// Logger.With — the way the engine tags each Spirit runner's logger — reaches
// the onLog callback even though slog never surfaces logger-level attrs on
// the record itself. This is what lets run-level Spirit lines ("copy rows
// complete", "apply complete") stay attributable to their table in the apply
// log stream.
func TestSpiritLogFilter_TableFromLoggerWith(t *testing.T) {
	logger, captured := newFilterLogger(t)

	logger.With("database", "app_db", "table", "customers").Info("copy rows complete")

	logs := captured()
	require.Len(t, logs, 1)
	assert.Equal(t, "customers", logs[0].table)
	assert.Equal(t, "copy rows complete", logs[0].msg)
}

// TestSpiritLogFilter_RecordAttrOverridesLoggerTable verifies that a table
// named at the log call site wins over the logger-level table. In a
// multi-table run the runner logger carries the full table list while
// Spirit's per-table lines name their specific table, and the specific one
// must be the one attributed.
func TestSpiritLogFilter_RecordAttrOverridesLoggerTable(t *testing.T) {
	logger, captured := newFilterLogger(t)

	logger.With("table", "customers, drinks").Info("checksum passed", "table", "drinks")

	logs := captured()
	require.Len(t, logs, 1)
	assert.Equal(t, "drinks", logs[0].table)
}

// TestSpiritLogFilter_TableSurvivesWithGroup verifies the captured table is
// preserved when the logger is further scoped with a group.
func TestSpiritLogFilter_TableSurvivesWithGroup(t *testing.T) {
	logger, captured := newFilterLogger(t)

	logger.With("table", "orders").WithGroup("copier").Info("copying rows")

	logs := captured()
	require.Len(t, logs, 1)
	assert.Equal(t, "orders", logs[0].table)
}

// TestSpiritLogFilter_ErrorAttrAppendedToMessage verifies error/reason attrs
// are folded into the routed message so fatal Spirit lines carry their cause
// alongside the table.
func TestSpiritLogFilter_ErrorAttrAppendedToMessage(t *testing.T) {
	logger, captured := newFilterLogger(t)

	logger.With("table", "baristas").Error("fatal error processing GTID rows event", "error", "row has 8 values")

	logs := captured()
	require.Len(t, logs, 1)
	assert.Equal(t, "baristas", logs[0].table)
	assert.Equal(t, "fatal error processing GTID rows event: row has 8 values", logs[0].msg)
	assert.Equal(t, slog.LevelError, logs[0].level)
}

// The apply log stream is the account of a schema change an operator reads from
// the CLI and from a failed apply's summary comment, so the engine's lines reach
// it on their own terms: a deployment that runs its own logs at warn still gets
// a complete stream, and the lines it admits for that stream stay out of the
// process logs it configured not to show them.
func TestSpiritLogFilter_RoutesToApplyLogBelowTheProcessLogLevel(t *testing.T) {
	logger, captured, processLogs := newFilterLoggerAt(t, slog.LevelError, true)

	logger.With("table", "orders").Info("copy rows complete")

	logs := captured()
	require.Len(t, logs, 1)
	assert.Equal(t, "copy rows complete", logs[0].msg)
	assert.Equal(t, "orders", logs[0].table)
	assert.Empty(t, processLogs(),
		"a line admitted for the apply log stream must not raise the process log volume")
}

// Lines the process logs do admit still reach both consumers.
func TestSpiritLogFilter_RoutesAndEmitsWhenBothConsumersWantTheLine(t *testing.T) {
	logger, captured, processLogs := newFilterLoggerAt(t, slog.LevelError, true)

	logger.With("table", "orders").Error("fatal error processing GTID rows event")

	require.Len(t, captured(), 1)
	assert.Contains(t, processLogs(), "fatal error processing GTID rows event")
}

// With no apply being driven there is no stream to record into, so the process
// log level alone decides — the filter never widens what a deployment emits.
func TestSpiritLogFilter_ProcessLogLevelDecidesWithoutAnApplyLogCallback(t *testing.T) {
	logger, _, processLogs := newFilterLoggerAt(t, slog.LevelError, false)

	logger.With("table", "orders").Info("copy rows complete")

	assert.Empty(t, processLogs())
}

func TestUniqueTableList(t *testing.T) {
	assert.Equal(t, "customers", uniqueTableList([]string{"customers"}))
	assert.Equal(t, "customers, drinks", uniqueTableList([]string{"customers", "drinks", "customers"}))
	assert.Equal(t, "", uniqueTableList(nil))
}
