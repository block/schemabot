package spirit

import (
	"bytes"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
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
	var onLog atomic.Pointer[logCallback]
	if routeToApplyLog {
		cb := logCallback(func(level slog.Level, table, msg string) {
			mu.Lock()
			defer mu.Unlock()
			captured = append(captured, capturedLog{level: level, table: table, msg: msg})
		})
		onLog.Store(&cb)
	}
	var debug atomic.Bool
	filter := &spiritLogFilter{
		handler: slog.NewTextHandler(&processLogs, &slog.HandlerOptions{Level: handlerLevel}),
		debug:   &debug,
		onLog:   &onLog,
	}
	routedLines := func() []capturedLog {
		mu.Lock()
		defer mu.Unlock()
		return append([]capturedLog(nil), captured...)
	}
	emittedProcessLogs := func() string {
		mu.Lock()
		defer mu.Unlock()
		return processLogs.String()
	}
	return slog.New(filter), routedLines, emittedProcessLogs
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

// A drive that finishes hands the engine back by unwiring its apply-log
// callback, and a Spirit runner still winding down keeps logging through the
// same filter. Reading the callback and the debug toggle out of the engine
// concurrently with those writes must stay safe: the log line either reaches
// the callback that was installed or is dropped, never dereferences a
// half-written slot.
func TestSpiritLogFilter_UnwiringTheCallbackWhileLoggingIsSafe(t *testing.T) {
	eng := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	spiritLogger := eng.spiritLogger.With("table", "orders")

	var routed atomic.Int64
	cb := func(slog.Level, string, string) { routed.Add(1) }

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		defer close(done)
		for range 2000 {
			spiritLogger.Info("copy rows complete")
			spiritLogger.Debug("replication event")
		}
	})
	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			eng.SetLogCallback(cb)
			eng.SetDebugLogs(true)
			eng.SetLogCallback(nil)
			eng.SetDebugLogs(false)
		}
	})
	wg.Wait()

	assert.LessOrEqual(t, routed.Load(), int64(2000),
		"only the info lines route, and each of them at most once")
}

// A drive hands the engine over by clearing its apply-log callback rather than
// leaving the finished drive's callback installed. A cleared slot is not an
// installed one: lines that arrive after it record nothing, and the filter
// never calls through the emptied slot.
func TestSpiritLogFilter_ClearedCallbackRecordsNothing(t *testing.T) {
	eng := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	spiritLogger := eng.spiritLogger.With("table", "orders")

	var routed atomic.Int64
	eng.SetLogCallback(func(slog.Level, string, string) { routed.Add(1) })
	spiritLogger.Info("copy rows complete")
	require.Equal(t, int64(1), routed.Load(), "an installed callback records the line")

	eng.SetLogCallback(nil)
	assert.NotPanics(t, func() { spiritLogger.Info("apply complete") })
	assert.Equal(t, int64(1), routed.Load(), "a cleared callback records nothing further")
}

// The filter a Spirit runner actually writes through is built by the engine —
// once at construction, and again per schema change when the caller supplies a
// logger carrying its triage identity. Both must route to the apply log stream
// on a deployment whose own logs sit above info, because that stream is the
// account an operator reads from the CLI and from a failed apply's summary.
func TestEngineBuildsSpiritLoggersThatRouteAboveTheProcessLogLevel(t *testing.T) {
	var processLogs bytes.Buffer
	quiet := func() *slog.Logger {
		return slog.New(slog.NewTextHandler(&processLogs, &slog.HandlerOptions{Level: slog.LevelError}))
	}

	eng := New(Config{Logger: quiet()})

	var mu sync.Mutex
	var routed []capturedLog
	eng.SetLogCallback(func(level slog.Level, table, msg string) {
		mu.Lock()
		defer mu.Unlock()
		routed = append(routed, capturedLog{level: level, table: table, msg: msg})
	})

	_, changeSpiritLogger := eng.resolveChangeLoggers(quiet())

	eng.spiritLogger.With("table", "orders").Info("copy rows complete")
	changeSpiritLogger.With("table", "drinks").Info("apply complete")

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, routed, 2)
	assert.Equal(t, "orders", routed[0].table)
	assert.Equal(t, "copy rows complete", routed[0].msg)
	assert.Equal(t, "drinks", routed[1].table)
	assert.Equal(t, "apply complete", routed[1].msg)
	assert.Empty(t, processLogs.String(),
		"neither logger may raise the volume of the process logs the deployment configured")
}
