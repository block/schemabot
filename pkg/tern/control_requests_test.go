package tern

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A terminal apply moots every pending window/stop control request: a stop is
// settled, a revert or skip-revert can no longer act once the revert window
// is gone — including a request that lost to a contradictory command (e.g. a
// revert still pending after skip-revert finalized the apply) — and a cancel
// has nothing left to terminate. The sweep completes all of them so no request
// lingers pending forever.
func TestCompletePendingRequestsForTerminalApply(t *testing.T) {
	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-terminal-sweep",
		Database:        "testdb",
		Environment:     "staging",
		State:           state.Apply.Completed,
	}
	sweptOps := []storage.ControlOperation{
		storage.ControlOperationStop,
		storage.ControlOperationRevert,
		storage.ControlOperationSkipRevert,
		storage.ControlOperationCancel,
	}
	requests := make([]*storage.ApplyControlRequest, 0, len(sweptOps))
	for _, op := range sweptOps {
		requests = append(requests, &storage.ApplyControlRequest{
			ApplyID: apply.ID, Operation: op, Status: storage.ControlRequestPending,
		})
	}
	controlRequests := &testControlRequestStore{requests: requests}
	store := &mockStorage{controlRequests: controlRequests}

	require.NoError(t, completePendingRequestsForTerminalApply(t.Context(), store, apply))

	for _, op := range sweptOps {
		pending, err := controlRequests.GetPending(t.Context(), apply.ID, op)
		require.NoError(t, err)
		assert.Nil(t, pending, "pending %s request must be completed once the apply is terminal", op)
		swept, err := controlRequests.GetByOperation(t.Context(), apply.ID, op)
		require.NoError(t, err)
		require.NotNil(t, swept)
		assert.Equal(t, storage.ControlRequestCompleted, swept.Status)
	}
}

// A stopped apply is terminal but remains cancellable: the sweep must complete
// the mooted stop while keeping a pending cancel deliverable, so a cancel
// issued against the stopped apply is still delivered by the next drive
// instead of being silently consumed.
func TestCompletePendingRequestsForStoppedApplyKeepsCancelPending(t *testing.T) {
	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-stopped-sweep",
		Database:        "testdb",
		Environment:     "staging",
		State:           state.Apply.Stopped,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
		{ApplyID: apply.ID, Operation: storage.ControlOperationStop, Status: storage.ControlRequestPending},
		{ApplyID: apply.ID, Operation: storage.ControlOperationCancel, Status: storage.ControlRequestPending},
	}}
	store := &mockStorage{controlRequests: controlRequests}

	require.NoError(t, completePendingRequestsForTerminalApply(t.Context(), store, apply))

	pendingStop, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.Nil(t, pendingStop, "the stop is mooted once the apply settles stopped")
	pendingCancel, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.NotNil(t, pendingCancel, "a stopped apply remains cancellable; the pending cancel must stay deliverable")
}

// captureStalePendingSignal routes the default slog logger into a buffer and
// installs a manual OTel metric reader for the duration of the test, so
// assertions can observe both halves of the stalled-request signal (the warn
// log and the stale-pending counter) that pendingControlRequest emits.
func captureStalePendingSignal(t *testing.T) (*bytes.Buffer, *sdkmetric.ManualReader) {
	t.Helper()

	logBuf := &bytes.Buffer{}
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logBuf, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(previousProvider)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	return logBuf, reader
}

// stalePendingCounterPoints collects the stale-pending counter's data points as
// attribute-set-to-value pairs, returning an empty map when the counter was
// never recorded.
func stalePendingCounterPoints(t *testing.T, reader *sdkmetric.ManualReader) map[attribute.Set]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	points := map[attribute.Set]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.control_requests.stale_pending_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				points[dp.Attributes] = dp.Value
			}
		}
	}
	return points
}

// A pending control request the driver has been retrying past the stale
// threshold emits an actionable stalled signal on each consumption: a warn
// carrying the apply's triage attributes, the operation, and the request age,
// plus the stale-pending counter with the operation/database/deployment/
// environment attributes an operator needs to find the spinning retry loop.
// The request itself must stay pending — the signal observes the retry-forever
// loop without settling it.
func TestPendingControlRequestStaleEmitsSignal(t *testing.T) {
	logBuf, reader := captureStalePendingSignal(t)

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-stale-cancel",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "tern-a",
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
		CreatedAt:   time.Now().Add(-pendingControlStaleThreshold - time.Minute),
	}}}
	store := &mockStorage{controlRequests: controlRequests}

	controlReq, err := pendingControlRequest(t.Context(), store, apply, storage.ControlOperationCancel)
	require.NoError(t, err)
	require.NotNil(t, controlReq, "the stalled request must stay pending; the signal observes the retry loop without settling it")

	logged := logBuf.String()
	assert.Contains(t, logged, "level=WARN")
	assert.Contains(t, logged, "stalled past the stale threshold")
	assert.Contains(t, logged, "apply_id=apply-stale-cancel")
	assert.Contains(t, logged, "database=testdb")
	assert.Contains(t, logged, "operation=cancel")
	assert.Contains(t, logged, "requested_by=cli:alice")
	assert.Contains(t, logged, "pending_for=")

	points := stalePendingCounterPoints(t, reader)
	require.Len(t, points, 1)
	wantAttrs := attribute.NewSet(
		attribute.String("operation", "cancel"),
		attribute.String("database", "testdb"),
		attribute.String("database_type", storage.DatabaseTypeMySQL),
		attribute.String("deployment", "tern-a"),
		attribute.String("environment", "staging"),
	)
	assert.Equal(t, int64(1), points[wantAttrs])
}

// A freshly issued control request is the normal case — the driver consumes it
// within its first few polls — so consuming one younger than the stale
// threshold must stay quiet: no warn and no stale-pending counter.
func TestPendingControlRequestFreshStaysQuiet(t *testing.T) {
	logBuf, reader := captureStalePendingSignal(t)

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-fresh-cancel",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "tern-a",
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
		CreatedAt:   time.Now(),
	}}}
	store := &mockStorage{controlRequests: controlRequests}

	controlReq, err := pendingControlRequest(t.Context(), store, apply, storage.ControlOperationCancel)
	require.NoError(t, err)
	require.NotNil(t, controlReq)

	assert.NotContains(t, logBuf.String(), "stalled past the stale threshold")
	assert.Empty(t, stalePendingCounterPoints(t, reader), "a fresh pending request must not count as stalled")
}
