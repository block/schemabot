package api

import (
	"errors"
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

// reaperMetricReader points the global meter provider at a manual reader for the
// duration of a test and returns it.
func reaperMetricReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})
	return reader
}

// reapedDeployments returns the deployment attribute of every reaped-operation
// data point the reader has collected.
func reapedDeployments(t *testing.T, reader *sdkmetric.ManualReader) []string {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	var deployments []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.operator.stranded_operations_reaped_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				deployment, hasDeployment := dp.Attributes.Value(attribute.Key("deployment"))
				require.True(t, hasDeployment, "a reaped stranded operation must say which deployment it belonged to")
				deployments = append(deployments, deployment.AsString())
			}
		}
	}
	return deployments
}

func strandedReaperService(reaped []*storage.ReapedOperation, reapErr error) *Service {
	svc := newTestService()
	svc.storage = &mockStorageWithApplyStores{
		operations: &staticApplyOperationStore{reaped: reaped, reapErr: reapErr},
	}
	return svc
}

func settledOperation(deployment string) *storage.ReapedOperation {
	return &storage.ReapedOperation{
		Operation: &storage.ApplyOperation{ID: 1, Deployment: deployment, State: state.ApplyOperation.Completed},
		Parent: &storage.Apply{
			ApplyIdentifier: "apply-stranded",
			Database:        "payments",
			Deployment:      "primary",
			Environment:     "staging",
			State:           state.Apply.Completed,
		},
	}
}

// A reap pass writes each row on its own, so settlements that commit before a
// later row fails are real state changes an operator has to be able to find. The
// pass reports them even when it ends in an error, rather than discarding the
// evidence along with the failed pass.
func TestRunStrandedReaperPassReportsSettlementsThatLandedBeforeAFailure(t *testing.T) {
	reader := reaperMetricReader(t)
	svc := strandedReaperService([]*storage.ReapedOperation{settledOperation("region-a")}, errors.New("storage unavailable"))

	svc.runStrandedReaperPass(t.Context())

	assert.Equal(t, []string{"region-a"}, reapedDeployments(t, reader),
		"a settlement that committed before the pass failed must still be counted")
}

// The counter breaks down by the reaped row's own deployment, not its parent
// apply's: stranded rows arise in multi-deployment applies, where the parent
// carries only the primary deployment and attributing every region to it would
// erase the breakdown the metric exists for.
func TestRunStrandedReaperPassCountsTheOperationsOwnDeployment(t *testing.T) {
	reader := reaperMetricReader(t)
	svc := strandedReaperService([]*storage.ReapedOperation{
		settledOperation("region-a"),
		settledOperation("region-b"),
	}, nil)

	svc.runStrandedReaperPass(t.Context())

	assert.ElementsMatch(t, []string{"region-a", "region-b"}, reapedDeployments(t, reader),
		"each settled row is counted under the deployment it belonged to")
}

// Losing the election is the expected outcome on every instance but one, so a
// busy pass records nothing at all.
func TestRunStrandedReaperPassRecordsNothingWhenAnotherInstanceIsReaping(t *testing.T) {
	reader := reaperMetricReader(t)
	svc := strandedReaperService(nil, storage.ErrStrandedReaperBusy)

	svc.runStrandedReaperPass(t.Context())

	assert.Empty(t, reapedDeployments(t, reader), "an unelected pass settles nothing")
}
