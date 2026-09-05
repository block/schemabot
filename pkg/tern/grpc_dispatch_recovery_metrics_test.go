package tern

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// newDispatchRecoveryMetricsReader installs a manual OTel reader for the test's
// lifetime so the drive's dispatch recovery outcomes can be collected and
// asserted.
func newDispatchRecoveryMetricsReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		// t.Context() is already cancelled when cleanup runs; detach so the
		// provider shutdown is not spuriously aborted.
		require.NoError(t, mp.Shutdown(context.WithoutCancel(t.Context())))
	})
	return reader
}

// dispatchRecoveryOutcomes returns the outcome values recorded on the dispatch
// recovery counter, in no particular order.
func dispatchRecoveryOutcomes(t *testing.T, reader *sdkmetric.ManualReader) []string {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var outcomes []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.remote_apply_dispatch_recovery_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "the dispatch recovery counter must be an int64 sum")
			for _, dp := range sum.DataPoints {
				outcome, found := dp.Attributes.Value(attribute.Key("outcome"))
				require.True(t, found, "every recovery data point must carry an outcome")
				outcomes = append(outcomes, outcome.AsString())
			}
		}
	}
	return outcomes
}

func TestGRPCClient_AmbiguousDispatchCountsResolvedWhenTheLaterPollFails(t *testing.T) {
	// A recovery resolves the ambiguity the moment it adopts a remote apply id:
	// the control plane can find the change again, which is the whole question
	// the recovery exists to answer. Everything after that is ordinary driving,
	// so a poll that fails on the same drive must not erase the resolution — an
	// attempt with no outcome would understate the recovery rate operators watch.
	reader := newDispatchRecoveryMetricsReader(t)

	server := &capturingTernServer{
		remoteApplyID: "remote-already-running",
		progressErr:   status.Error(codes.InvalidArgument, `invalid apply_id "remote-already-running"`),
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              41,
		ApplyIdentifier: "apply-recovered-then-unpollable",
		PlanID:          100,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             55,
		TaskIdentifier: "task-recovered-then-unpollable",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Running,
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		logs:    &mockApplyLogStore{},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-recovered-then-unpollable",
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	require.Error(t, client.ResumeApply(ctx, apply), "the failing poll must still surface as an error")

	assert.Equal(t, "remote-already-running", apply.ExternalID, "the recovery must adopt the remote apply id it found")
	outcomes := dispatchRecoveryOutcomes(t, reader)
	assert.Contains(t, outcomes, "attempted")
	assert.Contains(t, outcomes, "resolved", "adopting a remote apply id resolves the ambiguity, whatever the poll does next")
	assert.NotContains(t, outcomes, "unresolved")
	assert.NotContains(t, outcomes, "failed")
}
