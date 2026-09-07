package api

import (
	"context"
	"errors"
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
	"github.com/block/schemabot/pkg/testctx"
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
		cleanupCtx, cancel := testctx.Cleanup(t, 30*time.Second)
		defer cancel()
		require.NoError(t, mp.Shutdown(cleanupCtx))
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

// reapingTaskStore serves the reaper pass a canned retryable-task reap result.
type reapingTaskStore struct {
	storage.TaskStore
	reaped        []*storage.ReapedTask
	reapErr       error
	activeReaped  []*storage.ReapedTask
	activeReapErr error
}

func (s *reapingTaskStore) ReapStrandedActive(context.Context, int) ([]*storage.ReapedTask, error) {
	return s.activeReaped, s.activeReapErr
}

func (s *reapingTaskStore) ReapStrandedRetryable(context.Context, int) ([]*storage.ReapedTask, error) {
	return s.reaped, s.reapErr
}

func strandedReaperService(reaped []*storage.ReapedOperation, reapErr error) *Service {
	return strandedReaperServiceWithTasks(reaped, reapErr, nil, nil)
}

func strandedReaperServiceWithTasks(reaped []*storage.ReapedOperation, reapErr error, reapedTasks []*storage.ReapedTask, taskReapErr error) *Service {
	svc := newTestService()
	svc.storage = &mockStorageWithApplyStores{
		operations: &staticApplyOperationStore{reaped: reaped, reapErr: reapErr},
		tasks:      &reapingTaskStore{reaped: reapedTasks, reapErr: taskReapErr},
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

// claimFailureReasons returns the reason attribute of every operator
// claim-failure data point the reader has collected.
func claimFailureReasons(t *testing.T, reader *sdkmetric.ManualReader) []string {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	var reasons []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.operator.claim_failures_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				reason, hasReason := dp.Attributes.Value(attribute.Key("reason"))
				require.True(t, hasReason, "a claim failure must say why")
				reasons = append(reasons, reason.AsString())
			}
		}
	}
	return reasons
}

// The two reaps in one pass are independent maintenance sweeps: a storage
// failure in each is recorded under its own claim-failure reason, and the
// operation reap failing first does not stop the task reap from running.
func TestRunStrandedReaperPassRecordsEachReapFailureUnderItsOwnReason(t *testing.T) {
	reader := reaperMetricReader(t)
	svc := strandedReaperServiceWithTasks(
		nil, errors.New("operations table unavailable"),
		nil, errors.New("tasks table unavailable"))

	svc.runStrandedReaperPass(t.Context())

	assert.ElementsMatch(t, []string{"stranded_reaper_error", "stranded_task_reaper_error"},
		claimFailureReasons(t, reader),
		"both reaps run and each failure is attributable to its own sweep")
}

// Losing the task-reaper election is the expected outcome on every instance but
// one, so a busy task reap records no failure.
func TestRunStrandedReaperPassTaskReapBusyIsNotAFailure(t *testing.T) {
	reader := reaperMetricReader(t)
	svc := strandedReaperServiceWithTasks(nil, nil, nil, storage.ErrStrandedTaskReaperBusy)

	svc.runStrandedReaperPass(t.Context())

	assert.Empty(t, claimFailureReasons(t, reader), "an unelected task reap is not a failure")
}

// reaperPassDeadline bounds how long a pass may take to reach a state a test is
// waiting for. Every sweep in these tests is served by an in-memory double, so
// anything approaching this is a sweep that never ran.
const reaperPassDeadline = 10 * time.Second

// gatedApplyOperationStore holds its sweep open until the gate is closed, so a
// test can observe what the pass does while one sweep is still scanning.
type gatedApplyOperationStore struct {
	storage.ApplyOperationStore
	gate <-chan struct{}
}

func (s *gatedApplyOperationStore) ReapStranded(ctx context.Context, _ int) ([]*storage.ReapedOperation, error) {
	select {
	case <-s.gate:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// startedTaskStore reports that its sweep ran by closing started.
type startedTaskStore struct {
	storage.TaskStore
	started chan struct{}
}

func (s *startedTaskStore) ReapStrandedActive(context.Context, int) ([]*storage.ReapedTask, error) {
	return nil, nil
}

func (s *startedTaskStore) ReapStrandedRetryable(context.Context, int) ([]*storage.ReapedTask, error) {
	close(s.started)
	return nil, nil
}

// The two sweeps in a pass share no lock, connection, or row, and the
// retryable-task sweep is the one that frees a remote drive waiting out a dead
// pause. A slow operation scan therefore must not hold it behind: both sweeps
// run in the same pass however long either takes.
func TestRunStrandedReaperPassDoesNotHoldOneSweepBehindTheOther(t *testing.T) {
	taskSweepStarted := make(chan struct{})
	operationSweepGate := make(chan struct{})

	svc := newTestService()
	svc.storage = &mockStorageWithApplyStores{
		operations: &gatedApplyOperationStore{gate: operationSweepGate},
		tasks:      &startedTaskStore{started: taskSweepStarted},
	}

	ctx, cancel := context.WithTimeout(t.Context(), reaperPassDeadline)
	defer cancel()

	passDone := make(chan struct{})
	go func() {
		svc.runStrandedReaperPass(ctx)
		close(passDone)
	}()

	select {
	case <-taskSweepStarted:
	case <-ctx.Done():
		t.Fatal("the retryable-task sweep never ran while the operation sweep was still scanning")
	}
	close(operationSweepGate)

	select {
	case <-passDone:
	case <-ctx.Done():
		t.Fatal("the pass did not return once both sweeps were released")
	}
}

// A task reap pass that settled rows before failing reports the settlements —
// the writes are committed whatever the pass does next — and still records the
// failure. The settled rows carry both the task and its parent so the log line
// can name the apply an operator will be triaging.
func TestRunStrandedReaperPassReportsTaskSettlementsThatLandedBeforeAFailure(t *testing.T) {
	reader := reaperMetricReader(t)
	svc := strandedReaperServiceWithTasks(nil, nil, []*storage.ReapedTask{{
		Task: &storage.Task{
			TaskIdentifier: "task-stranded",
			TableName:      "users",
			State:          state.Task.Failed,
			ErrorMessage:   "connection reset during copy",
		},
		Parent: &storage.Apply{
			ApplyIdentifier: "apply-stranded",
			Database:        "payments",
			Environment:     "staging",
			State:           state.Apply.Failed,
		},
	}}, errors.New("tasks table unavailable"))

	svc.runStrandedReaperPass(t.Context())

	assert.Equal(t, []string{"stranded_task_reaper_error"}, claimFailureReasons(t, reader),
		"the failure is still recorded after the committed settlements are reported")
}
