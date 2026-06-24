package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// installManualMeterReader points the global meter provider at an in-memory
// reader for the duration of a test so the test can assert metric deltas, then
// restores the previous provider.
func installManualMeterReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		require.NoError(t, mp.Shutdown(t.Context()))
	})
	return reader
}

// activeAppliesValue returns the schemabot.active_applies up/down counter value
// for a database/deployment/environment series, and whether such a series exists.
func activeAppliesValue(t *testing.T, reader *sdkmetric.ManualReader, database, deployment, environment string) (int64, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.active_applies" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "active_applies should be a Sum[int64] up/down counter")
			for _, dp := range sum.DataPoints {
				if attrEquals(dp.Attributes, "database", database) &&
					attrEquals(dp.Attributes, "deployment", deployment) &&
					attrEquals(dp.Attributes, "environment", environment) {
					return dp.Value, true
				}
			}
		}
	}
	return 0, false
}

func attrEquals(set attribute.Set, key, want string) bool {
	v, ok := set.Value(attribute.Key(key))
	return ok && v.AsString() == want
}

// The enqueue-time active-apply increment is keyed to a fan-out apply's primary
// deployment, and operation-scoped drives suppress the parent-level metric, so
// the operator projection that wins the parent state transition owns releasing
// it: it decrements once when a multi-operation rollout first reaches a terminal
// state, re-increments if a continuable failure reopens the parent, and leaves
// single-operation applies (which still decrement in their direct drive)
// untouched.
func TestUpdateApplyStateFromOperations_ActiveAppliesAccounting(t *testing.T) {
	const (
		database    = "testdb"
		deployment  = "eu"
		environment = "production"
	)

	newApply := func(parentState string) *storage.Apply {
		return &storage.Apply{
			ID:              42,
			ApplyIdentifier: "apply-active",
			State:           parentState,
			Database:        database,
			Deployment:      deployment,
			Environment:     environment,
		}
	}

	t.Run("multi-operation rollout decrements once on terminal", func(t *testing.T) {
		reader := installManualMeterReader(t)
		applyStore := &recordingApplyStore{swapped: true}
		svc := newOperatorStateTestService(&listingApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, State: state.ApplyOperation.Completed},
			{ID: 2, State: state.ApplyOperation.Completed},
		}}, applyStore)

		_, err := svc.updateApplyStateFromOperations(t.Context(), 1, newApply(state.Apply.Running), allowLeaseScopedFailedReopen)
		require.NoError(t, err)
		require.NotNil(t, applyStore.updated)
		assert.Equal(t, state.Apply.Completed, applyStore.updated.State)

		value, ok := activeAppliesValue(t, reader, database, deployment, environment)
		require.True(t, ok, "terminalizing a multi-op rollout must adjust the active-apply gauge")
		assert.Equal(t, int64(-1), value)
	})

	t.Run("single-operation rollout leaves the gauge to the direct drive", func(t *testing.T) {
		reader := installManualMeterReader(t)
		applyStore := &recordingApplyStore{swapped: true}
		svc := newOperatorStateTestService(&listingApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, State: state.ApplyOperation.Completed},
		}}, applyStore)

		_, err := svc.updateApplyStateFromOperations(t.Context(), 1, newApply(state.Apply.Running), allowLeaseScopedFailedReopen)
		require.NoError(t, err)

		_, ok := activeAppliesValue(t, reader, database, deployment, environment)
		assert.False(t, ok, "single-op projection must not touch the gauge; its drive owns the decrement")
	})

	t.Run("lost projection race does not adjust the gauge", func(t *testing.T) {
		reader := installManualMeterReader(t)
		applyStore := &recordingApplyStore{swapped: false}
		svc := newOperatorStateTestService(&listingApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, State: state.ApplyOperation.Completed},
			{ID: 2, State: state.ApplyOperation.Completed},
		}}, applyStore)

		_, err := svc.updateApplyStateFromOperations(t.Context(), 1, newApply(state.Apply.Running), allowLeaseScopedFailedReopen)
		require.NoError(t, err)

		_, ok := activeAppliesValue(t, reader, database, deployment, environment)
		assert.False(t, ok, "a CAS that loses the race must not adjust the gauge")
	})

	t.Run("continuable reopen re-increments the gauge", func(t *testing.T) {
		reader := installManualMeterReader(t)
		applyStore := &recordingApplyStore{swapped: true}
		svc := newOperatorStateTestService(&listingApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureContinue},
			{ID: 2, State: state.ApplyOperation.Running, OnFailure: storage.OnFailureContinue},
		}}, applyStore)

		_, err := svc.updateApplyStateFromOperations(t.Context(), 1, newApply(state.Apply.Failed), allowLeaseScopedFailedReopen)
		require.NoError(t, err)
		require.NotNil(t, applyStore.updated)
		assert.Equal(t, state.Apply.RunningDegraded, applyStore.updated.State)

		value, ok := activeAppliesValue(t, reader, database, deployment, environment)
		require.True(t, ok, "reopening a failed rollout must re-acquire the active-apply gauge")
		assert.Equal(t, int64(1), value)
	})
}
