package tern

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// newTernMetricsReader installs a manual OTel reader for the test's lifetime
// so the recorded counters can be collected and asserted.
func newTernMetricsReader(t *testing.T) *metric.ManualReader {
	t.Helper()
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(previousProvider)
		// t.Context() is already cancelled when cleanup runs; detach so the
		// provider shutdown is not spuriously aborted.
		require.NoError(t, mp.Shutdown(context.WithoutCancel(t.Context())))
	})
	return reader
}

// collectCounterPoints collects the named int64 counter's data points from
// the reader. A counter that never recorded returns no points.
func collectCounterPoints(t *testing.T, reader *metric.ManualReader, name string) []metricdata.DataPoint[int64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	var points []metricdata.DataPoint[int64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			points = append(points, sum.DataPoints...)
		}
	}
	return points
}

// counterAttr returns the string value of the named attribute on a counter
// data point, failing when the attribute is absent.
func counterAttr(t *testing.T, p metricdata.DataPoint[int64], key string) string {
	t.Helper()
	v, ok := p.Attributes.Value(attribute.Key(key))
	require.True(t, ok, "counter point is missing attribute %q", key)
	return v.AsString()
}

func reporterTestTask() *storage.Task {
	return &storage.Task{
		TaskIdentifier: "task-observed",
		Database:       "orders_db",
		DatabaseType:   "mysql",
		Engine:         "spirit",
		Environment:    "staging",
		TableName:      "orders",
		State:          state.Task.Running,
	}
}

const unrecognizedStatusMetric = "schemabot.engine.unrecognized_task_status_total"

const unrecognizedStatusWarn = "engine reported a task status with no state mapping; the work renders as Running until a mapping is added in pkg/state"

// An engine status with no task-state mapping fails open to Running, so the
// fallback must be observable: every sighting counts on the unrecognized
// status counter with the task's dimensions, while the warn — carrying the
// task's triage identifiers and the raw status — fires once per task and
// status pair rather than once per poll tick.
func TestUnrecognizedStatusReporter_WarnsOnceAndCountsEverySighting(t *testing.T) {
	reader := newTernMetricsReader(t)
	var records []capturedLog
	logger := slog.New(captureHandler{records: &records})
	task := reporterTestTask()
	var reporter unrecognizedStatusReporter

	reporter.observeTaskStatus(t.Context(), logger, task, "someNewEngineState")
	reporter.observeTaskStatus(t.Context(), logger, task, "someNewEngineState")

	points := collectCounterPoints(t, reader, unrecognizedStatusMetric)
	require.Len(t, points, 1)
	assert.Equal(t, int64(2), points[0].Value)
	assert.Equal(t, "orders_db", counterAttr(t, points[0], "database"))
	assert.Equal(t, "mysql", counterAttr(t, points[0], "database_type"))
	assert.Equal(t, "spirit", counterAttr(t, points[0], "engine"))
	assert.Equal(t, "staging", counterAttr(t, points[0], "environment"))
	assert.Equal(t, "someNewEngineState", counterAttr(t, points[0], "status"))

	warns := capturedLogsWithMessage(records, unrecognizedStatusWarn)
	require.Len(t, warns, 1)
	assert.Equal(t, slog.LevelWarn, warns[0].level)
	assert.Equal(t, "someNewEngineState", warns[0].attrs["raw_status"])
	assert.Equal(t, "task-observed", warns[0].attrs["task_id"])
	assert.Equal(t, "orders_db", warns[0].attrs["database"])
	assert.Equal(t, "mysql", warns[0].attrs["database_type"])
	assert.Equal(t, "staging", warns[0].attrs["environment"])
	assert.Equal(t, "orders", warns[0].attrs["table"])

	// A different unrecognized status on the same task is a distinct missing
	// mapping and earns its own warn.
	reporter.observeTaskStatus(t.Context(), logger, task, "anotherNewEngineState")
	warns = capturedLogsWithMessage(records, unrecognizedStatusWarn)
	require.Len(t, warns, 2)
	assert.Equal(t, "anotherNewEngineState", warns[1].attrs["raw_status"])
}

// Each shard of a table task is its own unit of engine work, so two shards
// reporting the same unrecognized status each get a first-sighting warn, and
// the warn names the shard.
func TestUnrecognizedStatusReporter_ShardsWarnSeparately(t *testing.T) {
	newTernMetricsReader(t)
	var records []capturedLog
	logger := slog.New(captureHandler{records: &records})
	task := reporterTestTask()
	var reporter unrecognizedStatusReporter

	reporter.observeShardStatus(t.Context(), logger, task, "-40", "someNewVitessState")
	reporter.observeShardStatus(t.Context(), logger, task, "-40", "someNewVitessState")
	reporter.observeShardStatus(t.Context(), logger, task, "40-80", "someNewVitessState")

	warns := capturedLogsWithMessage(records, unrecognizedStatusWarn)
	require.Len(t, warns, 2)
	assert.Equal(t, "-40", warns[0].attrs["shard"])
	assert.Equal(t, "40-80", warns[1].attrs["shard"])
}

// Recognized statuses — canonical task states and known engine strings alike —
// record nothing, so observing on every poll tick adds no telemetry for
// healthy work.
func TestUnrecognizedStatusReporter_RecognizedStatusRecordsNothing(t *testing.T) {
	reader := newTernMetricsReader(t)
	var records []capturedLog
	logger := slog.New(captureHandler{records: &records})
	task := reporterTestTask()
	var reporter unrecognizedStatusReporter

	for _, status := range []string{state.Task.Running, state.Task.WaitingForCutover, "copyRows", "STATE_COMPLETED"} {
		reporter.observeTaskStatus(t.Context(), logger, task, status)
		reporter.observeShardStatus(t.Context(), logger, task, "-40", status)
	}

	assert.Empty(t, collectCounterPoints(t, reader, unrecognizedStatusMetric))
	assert.Empty(t, capturedLogsWithMessage(records, unrecognizedStatusWarn))
}

// The raw status is engine-controlled text bound for structured logs and a
// metric attribute, so it is collapsed and clamped before use: whitespace
// runs (including newlines) become single spaces, over-long values are cut
// with a visible marker, and an empty status stays attributable.
func TestClampObservedStatus(t *testing.T) {
	assert.Equal(t, "phase one two", clampObservedStatus("phase\none\r\n  two"))
	assert.Equal(t, "(empty)", clampObservedStatus("   \n "))

	long := strings.Repeat("x", maxObservedStatusLen+50)
	clamped := clampObservedStatus(long)
	assert.Len(t, []rune(clamped), maxObservedStatusLen)
	assert.True(t, strings.HasSuffix(clamped, "…"))
	assert.Equal(t, strings.Repeat("x", maxObservedStatusLen-1)+"…", clamped)
}

// A remote data plane can report a task status this control plane has no
// mapping for (version skew across a data-plane upgrade). The sync must keep
// the row visible and blocking by treating it as running, and must surface
// that the state is a fallback rather than positive evidence: the drive warns
// with the task identifiers and the unrecognized-status counter records the
// sighting. A recognized remote status records neither.
func TestSyncStoredTasksFromRemoteTasks_ReportsUnrecognizedRemoteStatus(t *testing.T) {
	reader := newTernMetricsReader(t)
	var records []capturedLog

	syncOnce := func(t *testing.T, remoteStatus string) *storage.Task {
		t.Helper()
		storedApply := &storage.Apply{
			ID:              41,
			ApplyIdentifier: "apply-remote-skew",
			State:           state.Apply.Running,
		}
		storedTask := &storage.Task{
			ID:             57,
			TaskIdentifier: "task-remote-skew-" + remoteStatus,
			ApplyID:        storedApply.ID,
			Database:       "orders_db",
			DatabaseType:   "mysql",
			Engine:         "spirit",
			Environment:    "staging",
			TableName:      "orders",
			State:          state.Task.Running,
		}
		client := &GRPCClient{
			logger: slog.New(captureHandler{records: &records}),
			storage: &mockStorage{
				tasks: &mockTaskStore{tasks: []*storage.Task{storedTask}},
				logs:  &mockApplyLogStore{},
			},
		}
		require.NoError(t, client.syncStoredTasksFromRemoteTasks(t.Context(), storedApply,
			[]*storage.Task{storedTask}, []*ternv1.TableProgress{{
				TableName: "orders",
				Status:    remoteStatus,
			}}, time.Now()))
		return storedTask
	}

	t.Run("unrecognized remote status stays running and is reported", func(t *testing.T) {
		storedTask := syncOnce(t, "someNewDataPlaneState")

		assert.Equal(t, state.Task.Running, storedTask.State)

		warns := capturedLogsWithMessage(records, unrecognizedStatusWarn)
		require.Len(t, warns, 1)
		assert.Equal(t, "someNewDataPlaneState", warns[0].attrs["raw_status"])
		assert.Equal(t, storedTask.TaskIdentifier, warns[0].attrs["task_id"])

		points := collectCounterPoints(t, reader, unrecognizedStatusMetric)
		require.Len(t, points, 1)
		assert.Equal(t, int64(1), points[0].Value)
		assert.Equal(t, "someNewDataPlaneState", counterAttr(t, points[0], "status"))
	})

	t.Run("recognized remote status records nothing", func(t *testing.T) {
		records = nil
		storedTask := syncOnce(t, state.Task.Checksumming)

		assert.Equal(t, state.Task.Checksumming, storedTask.State)
		assert.Empty(t, capturedLogsWithMessage(records, unrecognizedStatusWarn))

		points := collectCounterPoints(t, reader, unrecognizedStatusMetric)
		require.Len(t, points, 1, "the counter must not gain points for a recognized status")
		assert.Equal(t, int64(1), points[0].Value)
	})
}

// capturedLogsWithMessage filters captured records down to the given message.
func capturedLogsWithMessage(records []capturedLog, msg string) []capturedLog {
	var out []capturedLog
	for _, rec := range records {
		if rec.msg == msg {
			out = append(out, rec)
		}
	}
	return out
}
