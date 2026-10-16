package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
		controls:   &reapingControlRequestStore{fakeControlRequestStore: &fakeControlRequestStore{}},
	}
	return svc
}

// reapingControlRequestStore serves the reaper pass a canned stranded listing
// and records the settlements the pass makes against it.
type reapingControlRequestStore struct {
	*fakeControlRequestStore
	stranded []*storage.StrandedControlRequest
	listErr  error
}

func (s *reapingControlRequestStore) ListStrandedPending(context.Context, int) ([]*storage.StrandedControlRequest, error) {
	return s.stranded, s.listErr
}

// strandedControlRequestReaperService wires a pass whose control-request sweep
// finds the given stranded requests and whose apply store serves the apply
// they belong to, with the other sweeps finding nothing.
func strandedControlRequestReaperService(apply *storage.Apply, stranded []*storage.StrandedControlRequest, pending ...storage.ControlOperation) (*Service, *reapingControlRequestStore) {
	pendingByOp := make(map[storage.ControlOperation]*storage.ApplyControlRequest, len(pending))
	for _, op := range pending {
		pendingByOp[op] = &storage.ApplyControlRequest{ApplyID: apply.ID, Operation: op, Status: storage.ControlRequestPending}
	}
	controls := &reapingControlRequestStore{
		fakeControlRequestStore: &fakeControlRequestStore{pending: pendingByOp},
		stranded:                stranded,
	}
	svc := newTestService()
	svc.storage = &mockStorageWithApplyStores{
		applies:    &getApplyStore{apply: apply},
		operations: &staticApplyOperationStore{},
		tasks:      &reapingTaskStore{},
		controls:   controls,
	}
	return svc, controls
}

func strandedRequest(apply *storage.Apply, op storage.ControlOperation) *storage.StrandedControlRequest {
	return &storage.StrandedControlRequest{
		Request: &storage.ApplyControlRequest{ApplyID: apply.ID, Operation: op, Status: storage.ControlRequestPending, RequestedBy: "operator"},
		Apply:   apply,
	}
}

// A start left pending on a completed apply is answered by the reaper with the
// same outcome the drive would have disclosed: the apply completed, so the
// start took effect and completes. A failed apply fails the start with the
// apply's own reason, so the operator reads that the command did not reach the
// outcome they asked for.
func TestRunStrandedReaperPassAnswersAStartStrandedOnASettledApply(t *testing.T) {
	t.Run("completed apply completes the start", func(t *testing.T) {
		apply := &storage.Apply{ID: 9, ApplyIdentifier: "apply-done", State: state.Apply.Completed}
		svc, controls := strandedControlRequestReaperService(apply,
			[]*storage.StrandedControlRequest{strandedRequest(apply, storage.ControlOperationStart)},
			storage.ControlOperationStart)

		svc.runStrandedReaperPass(t.Context())

		assert.Equal(t, []storage.ControlOperation{storage.ControlOperationStart}, controls.completed)
		assert.Empty(t, controls.failed)
	})

	t.Run("failed apply fails the start with the apply's reason", func(t *testing.T) {
		apply := &storage.Apply{ID: 9, ApplyIdentifier: "apply-failed", State: state.Apply.Failed, ErrorMessage: "target rejected the schema change"}
		svc, controls := strandedControlRequestReaperService(apply,
			[]*storage.StrandedControlRequest{strandedRequest(apply, storage.ControlOperationStart)},
			storage.ControlOperationStart)

		svc.runStrandedReaperPass(t.Context())

		assert.Empty(t, controls.completed)
		assert.Equal(t, []storage.ControlOperation{storage.ControlOperationStart}, controls.failed)
		assert.Equal(t, []string{"target rejected the schema change"}, controls.failureReasons)
	})
}

// A revert or skip-revert left pending on a settled apply is answered by the
// reaper from the outcome, the same way the drive tail's terminal sweep answers
// it: a revert took effect only if the apply reverted, and a skip-revert only if
// it completed. A revert the completed schema change outran fails with a reason
// saying the change is live, rather than staying pending and being re-listed on
// every pass. A request for a retired operation has no driver left to serve it,
// so the reaper completes it.
func TestRunStrandedReaperPassAnswersRevertPhaseRequestsStrandedOnASettledApply(t *testing.T) {
	retired := storage.RetiredControlOperations()
	require.NotEmpty(t, retired, "the retired-operation case needs a retired operation to strand")
	tests := []struct {
		name          string
		applyState    string
		op            storage.ControlOperation
		wantCompleted []storage.ControlOperation
		wantFailed    []storage.ControlOperation
		wantReason    string
	}{
		{name: "a revert the completed apply outran fails", applyState: state.Apply.Completed, op: storage.ControlOperationRevert,
			wantFailed: []storage.ControlOperation{storage.ControlOperationRevert},
			wantReason: "the schema change completed before the revert could take effect; the change is live on the target"},
		{name: "a skip-revert on a completed apply completes", applyState: state.Apply.Completed, op: storage.ControlOperationSkipRevert,
			wantCompleted: []storage.ControlOperation{storage.ControlOperationSkipRevert}},
		{name: "a revert on a reverted apply completes", applyState: state.Apply.Reverted, op: storage.ControlOperationRevert,
			wantCompleted: []storage.ControlOperation{storage.ControlOperationRevert}},
		{name: "a retired operation's request completes", applyState: state.Apply.Completed, op: retired[0],
			wantCompleted: []storage.ControlOperation{retired[0]}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apply := &storage.Apply{ID: 9, ApplyIdentifier: "apply-settled", State: tt.applyState}
			svc, controls := strandedControlRequestReaperService(apply,
				[]*storage.StrandedControlRequest{strandedRequest(apply, tt.op)}, tt.op)

			svc.runStrandedReaperPass(t.Context())

			assert.Equal(t, tt.wantCompleted, controls.completed)
			assert.Equal(t, tt.wantFailed, controls.failed)
			if tt.wantReason != "" {
				assert.Equal(t, []string{tt.wantReason}, controls.failureReasons)
			}
			assert.Empty(t, controls.pending, "a request the reaper visited must not stay pending for the next pass")
		})
	}
}

// The listing is per request but the settlement is per apply: it reloads the
// apply and answers every pending request on it in one visit, so an apply that
// stranded both a stop and a cancel is settled once, not once per row.
func TestRunStrandedReaperPassSettlesEachStrandedApplyOnce(t *testing.T) {
	apply := &storage.Apply{ID: 9, ApplyIdentifier: "apply-done", State: state.Apply.Cancelled}
	svc, controls := strandedControlRequestReaperService(apply,
		[]*storage.StrandedControlRequest{
			strandedRequest(apply, storage.ControlOperationStop),
			strandedRequest(apply, storage.ControlOperationCancel),
		},
		storage.ControlOperationStop, storage.ControlOperationCancel)

	svc.runStrandedReaperPass(t.Context())

	assert.Equal(t, []storage.ControlOperation{storage.ControlOperationStop, storage.ControlOperationCancel}, controls.completed,
		"one visit answers both requests; a second visit would complete them again")
}

// A storage failure listing stranded control requests is recorded under the
// sweep's own reason, and losing its election is not a failure.
func TestRunStrandedReaperPassRecordsControlRequestSweepOutcomes(t *testing.T) {
	t.Run("listing failure", func(t *testing.T) {
		reader := reaperMetricReader(t)
		svc := strandedReaperService(nil, nil)
		svc.storage.(*mockStorageWithApplyStores).controls = &reapingControlRequestStore{
			fakeControlRequestStore: &fakeControlRequestStore{},
			listErr:                 errors.New("control requests table unavailable"),
		}

		svc.runStrandedReaperPass(t.Context())

		assert.Equal(t, []string{"stranded_control_request_reaper_error"}, claimFailureReasons(t, reader))
	})

	t.Run("busy is not a failure", func(t *testing.T) {
		reader := reaperMetricReader(t)
		svc := strandedReaperService(nil, nil)
		svc.storage.(*mockStorageWithApplyStores).controls = &reapingControlRequestStore{
			fakeControlRequestStore: &fakeControlRequestStore{},
			listErr:                 storage.ErrStrandedControlRequestReaperBusy,
		}

		svc.runStrandedReaperPass(t.Context())

		assert.Empty(t, claimFailureReasons(t, reader), "an unelected control-request sweep is not a failure")
	})
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
	stubApplyOperationStore
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
		controls:   &reapingControlRequestStore{fakeControlRequestStore: &fakeControlRequestStore{}},
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

// expiringApplyStore serves the retryable-expiry pass a fixed outcome — a set
// of expirations, or a storage failure — so the pass can be exercised without a
// database. It records the limit it was handed, and honours the caller's
// context so a pass cut short by shutdown can be modelled.
type expiringApplyStore struct {
	storage.ApplyStore
	expirations []*storage.RetryableApplyExpiration
	expireErr   error
	limit       int
}

func (s *expiringApplyStore) ExpireRetryable(ctx context.Context, limit int) ([]*storage.RetryableApplyExpiration, error) {
	s.limit = limit
	// Expirations and an error are mutually exclusive, and deliberately so: the
	// real pass settles its whole batch in one transaction, so a failure rolls
	// back everything it had matched. A double that could hand back both would
	// let a test assert on partial reporting the store cannot produce.
	if s.expireErr != nil {
		return nil, s.expireErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.expirations, nil
}

// Get serves the expired apply the pass reloads to publish its terminal summary.
func (s *expiringApplyStore) Get(_ context.Context, id int64) (*storage.Apply, error) {
	for _, expiration := range s.expirations {
		if expiration.Apply.ID == id {
			return expiration.Apply, nil
		}
	}
	return nil, nil
}

// retryableExpiryService wires a service whose expiry pass is served by applies,
// returning the store its apply-log writes land in.
func retryableExpiryService(applies storage.ApplyStore) (*Service, *capturingApplyLogStore) {
	applyLogs := &capturingApplyLogStore{}
	svc := newTestService()
	svc.storage = &mockStorageWithApplyStores{applies: applies, applyLogs: applyLogs}
	return svc, applyLogs
}

// expiredApply is a failed_retryable apply the pass has just settled, carrying
// the full triage attribute set an operator reads the expiry line for.
func expiredApply() *storage.Apply {
	return &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-42",
		Database:        "payments",
		DatabaseType:    "mysql",
		Deployment:      "region-a",
		Environment:     "staging",
		Repository:      "org/repo",
		PullRequest:     123,
		State:           state.Apply.Failed,
		Attempt:         storage.MaxRecoveryAttempts,
		ExternalID:      "remote-apply-7",
	}
}

func budgetExpiration() []*storage.RetryableApplyExpiration {
	return []*storage.RetryableApplyExpiration{
		{Apply: expiredApply(), Reason: storage.RetryableExpirationAttemptBudget},
	}
}

// Expiry is what makes a retryable failure permanent, so it belongs in the
// apply's own log stream: that stream is what the CLI and the PR summary
// render, and an apply whose last entry is a paused attempt reads as one that
// went terminal for no stated reason.
func TestRunRetryableExpiryPassRecordsWhyRecoveryStoppedInTheApplyLog(t *testing.T) {
	svc, applyLogs := retryableExpiryService(&expiringApplyStore{expirations: budgetExpiration()})

	svc.runRetryableExpiryPass(t.Context())

	require.Len(t, applyLogs.logs, 1)
	entry := applyLogs.logs[0]
	assert.Equal(t, storage.LogLevelError, entry.Level)
	assert.Equal(t, int64(42), entry.ApplyID)
	assert.Contains(t, entry.Message,
		fmt.Sprintf("%d of %d attempts", storage.MaxRecoveryAttempts, storage.MaxRecoveryAttempts))
	assert.Contains(t, entry.Message, string(storage.RetryableExpirationAttemptBudget))
	assert.Equal(t, state.Apply.FailedRetryable, entry.OldState)
	assert.Equal(t, state.Apply.Failed, entry.NewState)
}

// Expiry is the transition that makes a retrying apply terminal, and no driver
// holds the apply to publish that outcome. The pass publishes the apply's
// terminal summary itself, after recording why recovery stopped, so the PR
// gets its failure summary and its Check Run leaves "in progress" instead of
// keeping the last progress comment for an apply that has already failed.
func TestRunRetryableExpiryPassPublishesTheTerminalSummary(t *testing.T) {
	tasks := &terminalSummaryTaskStore{tasks: []*storage.Task{{TaskIdentifier: "task-1", ApplyID: 42, State: state.Task.Failed}}}
	applyLogs := &capturingApplyLogStore{}
	svc := newTestService()
	svc.storage = &mockStorageWithApplyStores{applies: &expiringApplyStore{expirations: budgetExpiration()}, tasks: tasks, applyLogs: applyLogs}
	var published []recordedTerminalSummary
	svc.OnApplyTerminalSummary = func(_ context.Context, a *storage.Apply, ts []*storage.Task) error {
		require.Len(t, applyLogs.logs, 1, "why recovery stopped is logged before the summary renders the apply's failure details")
		published = append(published, recordedTerminalSummary{apply: a, tasks: ts})
		return nil
	}

	svc.runRetryableExpiryPass(t.Context())

	require.Len(t, published, 1, "the expired apply's terminal summary is published once")
	assert.Equal(t, "apply-42", published[0].apply.ApplyIdentifier)
	assert.Equal(t, state.Apply.Failed, published[0].apply.State)
	require.Len(t, published[0].tasks, 1)
	assert.Equal(t, "task-1", published[0].tasks[0].TaskIdentifier)
	assert.True(t, tasks.called, "the summary reloads every task of the apply")
}

// A retryable-apply expiry is a control-plane lifecycle transition an operator
// triages from logs alone, so the expiry line must carry the apply's full
// triage attributes — including external_id, the join key to the data plane's
// logs — plus the expiry-specific attempt and reason.
func TestRunRetryableExpiryPassLogsCarryFullApplyAttrs(t *testing.T) {
	var logBuf bytes.Buffer
	svc, _ := retryableExpiryService(&expiringApplyStore{expirations: budgetExpiration()})
	svc.logger = slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	svc.runRetryableExpiryPass(t.Context())

	line := requireLogLine(t, decodeLogLines(t, logBuf.Bytes()), "operator: retryable apply expired")
	assert.Equal(t, "apply-42", line["apply_id"])
	assert.Equal(t, "payments", line["database"])
	assert.Equal(t, "mysql", line["database_type"])
	assert.Equal(t, "staging", line["environment"])
	assert.Equal(t, "org/repo", line["repo"])
	assert.Equal(t, float64(123), line["pr"])
	assert.Equal(t, "region-a", line["deployment"])
	assert.Equal(t, state.Apply.Failed, line["state"])
	assert.Equal(t, "remote-apply-7", line["external_id"])
	assert.Equal(t, float64(storage.MaxRecoveryAttempts), line["attempt"])
	assert.Equal(t, string(storage.RetryableExpirationAttemptBudget), line["reason"])
}

// A storage error is the one ending of an expiry pass that is a fault, and it is
// counted apart from the stranded sweeps so an operator alerting on claim
// failures can tell which sweep is failing.
func TestRunRetryableExpiryPassStorageErrorIsAFailure(t *testing.T) {
	reader := reaperMetricReader(t)
	svc, _ := retryableExpiryService(&expiringApplyStore{
		expireErr: errors.New("applies table unavailable"),
	})

	svc.runRetryableExpiryPass(t.Context())

	assert.Equal(t, []string{"expire_retryable_error"}, claimFailureReasons(t, reader),
		"the failure is recorded under the expiry sweep's own reason")
}

// Losing the expiry election is the expected outcome on every instance but one,
// so a busy pass records no failure.
func TestRunRetryableExpiryPassBusyIsNotAFailure(t *testing.T) {
	reader := reaperMetricReader(t)
	svc, _ := retryableExpiryService(&expiringApplyStore{expireErr: storage.ErrRetryableExpiryBusy})

	svc.runRetryableExpiryPass(t.Context())

	assert.Empty(t, claimFailureReasons(t, reader), "an unelected expiry pass is not a failure")
}

// A shutdown cancels the pass mid-query. The next instance to be elected runs
// the same pass moments later, so the ending is a routine deploy and not a
// fault: it must not tick the claim-failure counter operators alert on.
func TestRunRetryableExpiryPassInterruptedByShutdownIsNotAFailure(t *testing.T) {
	reader := reaperMetricReader(t)
	svc, _ := retryableExpiryService(&expiringApplyStore{})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	svc.runRetryableExpiryPass(ctx)

	assert.Empty(t, claimFailureReasons(t, reader),
		"an expiry pass cut short by shutdown must not tick the claim-failure counter")
}

// One instance expires per pass, so the batch bound is the fleet-wide drain
// rate per interval rather than a per-driver rate — an unbounded pass would let
// a large backlog hold a single transaction open across the whole set.
func TestRunRetryableExpiryPassBoundsTheBatch(t *testing.T) {
	applies := &expiringApplyStore{}
	svc, _ := retryableExpiryService(applies)

	svc.runRetryableExpiryPass(t.Context())

	assert.Equal(t, retryableExpiryBatch, applies.limit)
}
