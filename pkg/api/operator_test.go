package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/panicsafe"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

func TestDriversConfig(t *testing.T) {
	t.Run("default drivers", func(t *testing.T) {
		config := &ServerConfig{}
		assert.Equal(t, 0, config.Drivers)
		assert.Equal(t, 4, DefaultDrivers)
	})

	t.Run("configured drivers", func(t *testing.T) {
		config := &ServerConfig{Drivers: 3}
		assert.Equal(t, 3, config.Drivers)
	})
}

// recordingApplyOperationStore records the state-mutating call made by
// markOperationFromApplyState. It embeds the interface so only the methods the
// test exercises need implementations; any other call panics, which keeps the
// test honest about the code path it covers.
type recordingApplyOperationStore struct {
	storage.ApplyOperationStore
	updateStateID    int64
	updateStateValue string
	updateStateErr   error
}

func (r *recordingApplyOperationStore) UpdateState(_ context.Context, id int64, newState string) error {
	r.updateStateID = id
	r.updateStateValue = newState
	return r.updateStateErr
}

type mockStorageWithApplyOperations struct {
	mockStorage
	applyOps storage.ApplyOperationStore
}

func (m *mockStorageWithApplyOperations) ApplyOperations() storage.ApplyOperationStore {
	return m.applyOps
}

func newOperatorTestService(opStore storage.ApplyOperationStore) *Service {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithApplyOperations{applyOps: opStore}, testServerConfig(), nil, logger)
}

// failingStopReconciliationApplyStore errors on the stop-reconciliation claim.
// It embeds the interface so only that method needs an implementation; any
// other call panics, which keeps the test honest about the code path it covers.
type failingStopReconciliationApplyStore struct {
	storage.ApplyStore
	err error
}

func (s *failingStopReconciliationApplyStore) FindNextApplyForStopReconciliation(context.Context, string) (*storage.Apply, error) {
	return nil, s.err
}

// TestRecoverApplyPendingStop_ClaimErrorDoesNotConsumeTick verifies the driver
// ladder keeps moving when the stop-reconciliation claim itself fails: a failed
// claim did no work and holds no lease, so the tick must fall through to the
// operation claim instead of a persistent storage error on this first rung
// starving every claim the driver runs after it.
func TestRecoverApplyPendingStop_ClaimErrorDoesNotConsumeTick(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		applies: &failingStopReconciliationApplyStore{err: errors.New("storage unavailable")},
	}, testServerConfig(), nil, logger)

	consumed := svc.recoverApplyPendingStop(t.Context(), 1, driverLeaseOwner(1))

	assert.False(t, consumed, "a failed stop-reconciliation claim must not consume the driver tick")
}

type noopProgressObserver struct{}

func (noopProgressObserver) OnProgress(*storage.Apply, []*storage.Task) {}

func (noopProgressObserver) OnTerminal(*storage.Apply, []*storage.Task) {}

func TestResumeClaimedApplyRoutesRecoveredObserver(t *testing.T) {
	deploymentClient := &mockTernClient{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		plans:   &staticPlanStore{},
		applies: &staticApplyStore{},
	}, &ServerConfig{}, map[string]tern.Client{
		"east/staging": deploymentClient,
	}, logger)
	observer := noopProgressObserver{}
	svc.OnApplyRecovered = func(apply *storage.Apply) {
		svc.SetApplyObserver(apply.Database, apply.Deployment, apply.Environment, apply.ID, observer)
	}
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-42",
		Database:        "appdb",
		Deployment:      "east",
		Environment:     "staging",
		State:           state.Apply.Pending,
	}

	resumed, err := svc.resumeClaimedApply(t.Context(), 1, apply, 0, "")

	require.NoError(t, err)
	assert.True(t, resumed)
	assert.Same(t, apply, deploymentClient.resumeApply)
	assert.Equal(t, int64(42), deploymentClient.observerApplyID)
	assert.Equal(t, observer, deploymentClient.observer)
}

// A multi-operation drive must not register the per-driver progress/terminal
// observer: the aggregate terminal summary is published once by the projection
// CAS winner, not per deployment. resumeClaimedApplyWithOptions suppresses the
// OnApplyRecovered hook so no observer is set for the drive.
func TestResumeClaimedApplyWithOptions_SuppressesRecoveredObserverForMultiOp(t *testing.T) {
	deploymentClient := &mockTernClient{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		plans:   &staticPlanStore{},
		applies: &staticApplyStore{},
	}, &ServerConfig{}, map[string]tern.Client{
		"east/staging": deploymentClient,
	}, logger)
	observerSet := false
	svc.OnApplyRecovered = func(apply *storage.Apply) {
		observerSet = true
		svc.SetApplyObserver(apply.Database, apply.Deployment, apply.Environment, apply.ID, noopProgressObserver{})
	}
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-42",
		Database:        "appdb",
		Deployment:      "east",
		Environment:     "staging",
		State:           state.Apply.Pending,
	}

	resumed, err := svc.resumeClaimedApplyWithOptions(t.Context(), 1, apply, 0, "east",
		resumeClaimedApplyOptions{suppressRecoveredObserver: true})

	require.NoError(t, err)
	assert.True(t, resumed)
	assert.Same(t, apply, deploymentClient.resumeApply)
	assert.False(t, observerSet, "a multi-op drive must not fire the per-driver observer hook")
	assert.Zero(t, deploymentClient.observerApplyID, "no observer must be registered for a multi-op drive")
	assert.Nil(t, deploymentClient.observer)
}

// An operator drive binds a drive-scoped logger once at the claim boundary, so
// every log line of the drive carries the apply's identity (apply_id, repo, pr,
// database, environment) and the effective deployment without each call site
// hand-listing them — including calls that pass no identity attrs at all, such
// as the missing-apply-log-store warning. Mutable state is not frozen into the
// bound logger: lines carry a state attribute only where the call site supplies
// the value that is current at emit time.
func TestResumeClaimedApply_DriveLogsCarryApplyIdentity(t *testing.T) {
	deploymentClient := &mockTernClient{}
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	svc := New(&mockStorageWithApplyStores{
		plans:   &staticPlanStore{},
		applies: &staticApplyStore{},
	}, &ServerConfig{}, map[string]tern.Client{
		"east/staging": deploymentClient,
	}, logger)
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-42",
		Database:        "appdb",
		DatabaseType:    "mysql",
		Deployment:      "east",
		Environment:     "staging",
		Repository:      "org/repo",
		PullRequest:     123,
		State:           state.Apply.Pending,
	}

	resumed, err := svc.resumeClaimedApply(t.Context(), 1, apply, 0, "")
	require.NoError(t, err)
	assert.True(t, resumed)

	lines := decodeLogLines(t, logBuf.Bytes())

	assertDriveIdentity := func(line map[string]any) {
		t.Helper()
		assert.Equal(t, "apply-42", line["apply_id"])
		assert.Equal(t, "appdb", line["database"])
		assert.Equal(t, "mysql", line["database_type"])
		assert.Equal(t, "staging", line["environment"])
		assert.Equal(t, "org/repo", line["repo"])
		assert.Equal(t, float64(123), line["pr"])
		assert.Equal(t, "east", line["deployment"])
		assert.Equal(t, float64(1), line["driver"])
	}

	claimed := requireLogLine(t, lines, "operator: claimed apply")
	assertDriveIdentity(claimed)
	assert.Equal(t, state.Apply.Pending, claimed["state"],
		"claim line must snapshot the state current at claim time")

	// The call site passes only the message — every identity attr must come
	// from the bound logger.
	noStore := requireLogLine(t, lines, "operator: no apply log store configured; the apply's own log will not state that a driver claimed it to resume it")
	assertDriveIdentity(noStore)

	resumedLine := requireLogLine(t, lines, "operator: resumed apply")
	assertDriveIdentity(resumedLine)
	assert.Equal(t, state.Apply.Pending, resumedLine["previous_state"])
	assert.NotContains(t, resumedLine, "state",
		"mutable state must not be frozen into the bound drive logger")
}

// expiringApplyStore serves the expiry maintenance pass a fixed outcome — a set
// of retryable-apply expirations, or a storage failure — so the pass can be
// exercised without a database. It counts its calls so a test can tell whether
// the claim ladder reached its first rung at all.
type expiringApplyStore struct {
	storage.ApplyStore
	expirations []*storage.RetryableApplyExpiration
	expireErr   error
	calls       int
}

func (s *expiringApplyStore) ExpireRetryable(ctx context.Context) ([]*storage.RetryableApplyExpiration, error) {
	s.calls++
	if s.expireErr != nil {
		return nil, s.expireErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.expirations, nil
}

// Expiry is what makes a retryable failure permanent, so it belongs in the
// apply's own log stream: that stream is what the CLI and the PR summary
// render, and an apply whose last entry is a paused attempt reads as one that
// went terminal for no stated reason.
func TestExpireRetryableApplies_RecordsWhyRecoveryStoppedInTheApplyLog(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-42",
		Database:        "appdb",
		Environment:     "staging",
		State:           state.Apply.Failed,
		Attempt:         storage.MaxRecoveryAttempts,
	}
	applyLogs := &capturingApplyLogStore{}
	svc := New(&mockStorageWithApplyStores{
		applies: &expiringApplyStore{expirations: []*storage.RetryableApplyExpiration{
			{Apply: apply, Reason: storage.RetryableExpirationAttemptBudget},
		}},
		applyLogs: applyLogs,
	}, testServerConfig(), nil, slog.Default())

	svc.expireRetryableApplies(t.Context(), 1)

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

// A retryable-apply expiry is a control-plane lifecycle transition an operator
// triages from logs alone, so the expiry line must carry the apply's full
// triage attributes — including external_id, the join key to the data plane's
// logs — plus the expiry-specific attempt and reason.
func TestExpireRetryableApplies_LogsCarryFullApplyAttrs(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-42",
		Database:        "appdb",
		DatabaseType:    "mysql",
		Deployment:      "east",
		Environment:     "staging",
		Repository:      "org/repo",
		PullRequest:     123,
		State:           state.Apply.Failed,
		Attempt:         3,
		ExternalID:      "remote-apply-7",
	}
	svc := New(&mockStorageWithApplyStores{
		applies: &expiringApplyStore{expirations: []*storage.RetryableApplyExpiration{
			{Apply: apply, Reason: storage.RetryableExpirationAttemptBudget},
		}},
	}, testServerConfig(), nil, logger)

	svc.expireRetryableApplies(t.Context(), 1)

	lines := decodeLogLines(t, logBuf.Bytes())
	line := requireLogLine(t, lines, "operator: retryable apply expired")
	assert.Equal(t, "apply-42", line["apply_id"])
	assert.Equal(t, "appdb", line["database"])
	assert.Equal(t, "mysql", line["database_type"])
	assert.Equal(t, "staging", line["environment"])
	assert.Equal(t, "org/repo", line["repo"])
	assert.Equal(t, float64(123), line["pr"])
	assert.Equal(t, "east", line["deployment"])
	assert.Equal(t, state.Apply.Failed, line["state"])
	assert.Equal(t, "remote-apply-7", line["external_id"])
	assert.Equal(t, float64(3), line["attempt"])
	assert.Equal(t, string(storage.RetryableExpirationAttemptBudget), line["reason"])
	assert.Equal(t, float64(1), line["driver"])
}

// decodeLogLines parses newline-delimited slog JSON output into one map per line.
func decodeLogLines(t *testing.T, output []byte) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for raw := range bytes.SplitSeq(bytes.TrimSpace(output), []byte("\n")) {
		if len(raw) == 0 {
			continue
		}
		var line map[string]any
		require.NoError(t, json.Unmarshal(raw, &line), "log output must be valid JSON: %s", raw)
		lines = append(lines, line)
	}
	return lines
}

// requireLogLine returns the first log line with the given message, failing the
// test when none exists.
func requireLogLine(t *testing.T, lines []map[string]any, msg string) map[string]any {
	t.Helper()
	for _, line := range lines {
		if line["msg"] == msg {
			return line
		}
	}
	require.Failf(t, "log line not found", "no log line with msg %q in %d lines", msg, len(lines))
	return nil
}

// TestMarkOperationFromApplyState_MirrorsFailedRetryable verifies that a parent
// apply in failed_retryable mirrors that state (not a terminal one) onto the
// operation row via UpdateState, leaving it reclaimable for retry. Returning
// marked=true lets the caller re-derive the parent state from its children.
func TestMarkOperationFromApplyState_MirrorsFailedRetryable(t *testing.T) {
	opStore := &recordingApplyOperationStore{}
	svc := newOperatorTestService(opStore)

	op := &storage.ApplyOperation{ID: 7, Deployment: "region-a"}
	apply := &storage.Apply{
		ID:              3,
		ApplyIdentifier: "apply-retryable",
		State:           state.Apply.FailedRetryable,
		Environment:     "staging",
	}

	marked, err := svc.markOperationFromApplyState(t.Context(), 1, op, apply)
	require.NoError(t, err)
	assert.True(t, marked, "failed_retryable parent must mark the operation so derived apply state is recomputed")
	assert.Equal(t, int64(7), opStore.updateStateID, "the claimed operation row must be the one updated")
	assert.Equal(t, state.Apply.FailedRetryable, opStore.updateStateValue,
		"failed_retryable must be mirrored down, not a terminal state")
}

// listingApplyOperationStore returns a fixed set of operation rows from
// ListByApply so the derived-apply-state projection can be exercised against a
// multi-deployment sibling set.
type listingApplyOperationStore struct {
	storage.ApplyOperationStore
	ops []*storage.ApplyOperation
}

func (s *listingApplyOperationStore) ListByApply(context.Context, int64) ([]*storage.ApplyOperation, error) {
	return s.ops, nil
}

// recordingApplyStore captures the projection persisted by UpdateDerivedState so
// the test can assert the derived state and completed_at stamping. swapped is
// returned to model whether the compare-and-swap matched the expected state.
type recordingApplyStore struct {
	storage.ApplyStore
	updated       *storage.Apply
	expectedState string
	swapped       bool
}

func (s *recordingApplyStore) UpdateDerivedState(_ context.Context, applyID int64, expectedState, newState, errorMessage string, startedAt, completedAt *time.Time) (bool, error) {
	s.expectedState = expectedState
	if !s.swapped {
		return false, nil
	}
	s.updated = &storage.Apply{
		ID:           applyID,
		State:        newState,
		ErrorMessage: errorMessage,
		StartedAt:    startedAt,
		CompletedAt:  completedAt,
	}
	return true, nil
}

func newOperatorStateTestService(opStore storage.ApplyOperationStore, applyStore storage.ApplyStore) *Service {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithApplyStoresAndOperations{applyOps: opStore, applies: applyStore}, testServerConfig(), nil, logger)
}

type mockStorageWithApplyStoresAndOperations struct {
	mockStorage
	applyOps  storage.ApplyOperationStore
	applies   storage.ApplyStore
	applyLogs storage.ApplyLogStore
}

func (m *mockStorageWithApplyStoresAndOperations) ApplyOperations() storage.ApplyOperationStore {
	return m.applyOps
}
func (m *mockStorageWithApplyStoresAndOperations) Applies() storage.ApplyStore { return m.applies }
func (m *mockStorageWithApplyStoresAndOperations) ApplyLogs() storage.ApplyLogStore {
	return m.applyLogs
}

// TestUpdateApplyStateFromOperations_ContinuePolicy verifies that the operator's
// apply-state writer projects the rollout policy over the sibling set: under
// on_failure "continue" a terminally failed deployment holds the apply running
// while a sibling is still pending, while the default policy fails closed and
// terminalizes the apply.
func TestUpdateApplyStateFromOperations_ContinuePolicy(t *testing.T) {
	cases := []struct {
		name      string
		ops       []*storage.ApplyOperation
		wantState string
		wantDone  bool
	}{
		{
			name: "continue holds the apply running_degraded past a failed deployment",
			ops: []*storage.ApplyOperation{
				{ID: 1, State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureContinue},
				{ID: 2, State: state.ApplyOperation.Pending, OnFailure: storage.OnFailureContinue},
			},
			wantState: state.Apply.RunningDegraded,
			wantDone:  false,
		},
		{
			name: "default policy terminalizes the apply on a failed deployment",
			ops: []*storage.ApplyOperation{
				{ID: 1, State: state.ApplyOperation.Failed},
				{ID: 2, State: state.ApplyOperation.Pending},
			},
			wantState: state.Apply.Failed,
			wantDone:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applyStore := &recordingApplyStore{swapped: true}
			svc := newOperatorStateTestService(&listingApplyOperationStore{ops: tc.ops}, applyStore)

			apply := &storage.Apply{
				ID:              3,
				ApplyIdentifier: "apply-projection",
				State:           state.Apply.Pending,
				Environment:     "staging",
			}

			_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
			require.NoError(t, err)
			require.NotNil(t, applyStore.updated, "derived state differs from current, so the apply must be persisted")
			assert.Equal(t, state.Apply.Pending, applyStore.expectedState, "the write must compare-and-swap on the state read before deriving")
			assert.Equal(t, tc.wantState, applyStore.updated.State)
			if tc.wantDone {
				assert.NotNil(t, applyStore.updated.CompletedAt, "terminal derived state stamps completed_at")
			} else {
				assert.Nil(t, applyStore.updated.CompletedAt, "non-terminal derived state leaves completed_at nil")
			}
		})
	}
}

// The projection is often the last writer standing on an apply — the drives
// that produced the operation states may be gone by the time it runs (a crashed
// driver, a lease-lost settle, stop reconciliation). When it swaps the parent
// state it must record the transition in the apply's own durable log so the
// timeline states how the derived state was reached; a projection that loses
// the compare-and-swap must not append a stale entry.
func TestUpdateApplyStateFromOperations_SwapAppendsDurableApplyLog(t *testing.T) {
	newService := func(swapped bool, applyLogs storage.ApplyLogStore) (*Service, *recordingApplyStore) {
		applyStore := &recordingApplyStore{swapped: swapped}
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		svc := New(&mockStorageWithApplyStoresAndOperations{
			applyOps: &listingApplyOperationStore{ops: []*storage.ApplyOperation{
				{ID: 7, State: state.ApplyOperation.Cancelled},
			}},
			applies:   applyStore,
			applyLogs: applyLogs,
		}, testServerConfig(), nil, logger)
		return svc, applyStore
	}
	apply := func() *storage.Apply {
		return &storage.Apply{
			ID:              3,
			ApplyIdentifier: "apply-projection",
			State:           state.Apply.Running,
			Environment:     "staging",
		}
	}

	t.Run("swapped projection appends the transition", func(t *testing.T) {
		applyLogs := &capturingApplyLogStore{}
		svc, _ := newService(true, applyLogs)

		result, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply(), rejectFailedApplyReopen)
		require.NoError(t, err)
		require.True(t, result.Swapped, "the projection must win the compare-and-swap in this scenario")

		require.Len(t, applyLogs.logs, 1, "a swapped projection must append exactly one durable log entry")
		entry := applyLogs.logs[0]
		assert.Equal(t, int64(3), entry.ApplyID)
		assert.Equal(t, storage.LogLevelInfo, entry.Level)
		assert.Equal(t, storage.LogEventStateTransition, entry.EventType)
		assert.Equal(t, storage.LogSourceSchemaBot, entry.Source)
		assert.Equal(t, state.Apply.Running, entry.OldState)
		assert.Equal(t, state.Apply.Cancelled, entry.NewState)
		assert.Contains(t, entry.Message, "derived")
		assert.Contains(t, entry.Message, state.Apply.Cancelled)
	})

	t.Run("lost compare-and-swap appends nothing", func(t *testing.T) {
		applyLogs := &capturingApplyLogStore{}
		svc, _ := newService(false, applyLogs)

		result, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply(), rejectFailedApplyReopen)
		require.NoError(t, err)
		require.False(t, result.Swapped)

		assert.Empty(t, applyLogs.logs, "a stale projection must not write a misleading transition entry")
	})

	t.Run("swap that only stamps started_at appends nothing", func(t *testing.T) {
		applyLogs := &capturingApplyLogStore{}
		applyStore := &recordingApplyStore{swapped: true}
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		svc := New(&mockStorageWithApplyStoresAndOperations{
			applyOps: &listingApplyOperationStore{ops: []*storage.ApplyOperation{
				{ID: 7, State: state.ApplyOperation.Running},
			}},
			applies:   applyStore,
			applyLogs: applyLogs,
		}, testServerConfig(), nil, logger)

		result, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply(), rejectFailedApplyReopen)
		require.NoError(t, err)
		require.True(t, result.Swapped, "the started_at stamp must win the compare-and-swap in this scenario")

		assert.Empty(t, applyLogs.logs, "a swap that leaves the state unchanged is not a state transition")
	})
}

// TestUpdateApplyStateFromOperations_ManifestGatesCompletion verifies that an
// apply carrying a generation manifest reaches a whole-generation verdict only
// when every declared operation has attached and finished. A deployment-keyed
// apply's operations attach one dispatch at a time, so the projection must not
// read "all attached operations succeeded" (or "an attached operation
// reverted") as the generation's outcome while declared siblings are still on
// their way — it holds the apply running instead, for completed and reverted
// alike. Failure verdicts pass through unheld, and an apply without a manifest
// keeps the attached-rows-only semantics.
func TestUpdateApplyStateFromOperations_ManifestGatesCompletion(t *testing.T) {
	const shardA, shardB = "ns/-80/users", "ns/80-/users"
	cases := []struct {
		name      string
		manifest  []string
		ops       []*storage.ApplyOperation
		wantState string
		wantDone  bool
	}{
		{
			name:     "completed attached subset holds the apply running",
			manifest: []string{shardA, shardB},
			ops: []*storage.ApplyOperation{
				{ID: 1, OperationKey: shardA, State: state.ApplyOperation.Completed},
			},
			wantState: state.Apply.Running,
			wantDone:  false,
		},
		{
			name:     "full manifest attached and completed completes the apply",
			manifest: []string{shardA, shardB},
			ops: []*storage.ApplyOperation{
				{ID: 1, OperationKey: shardA, State: state.ApplyOperation.Completed},
				{ID: 2, OperationKey: shardB, State: state.ApplyOperation.Completed},
			},
			wantState: state.Apply.Completed,
			wantDone:  true,
		},
		{
			name:     "reverted attached subset holds the apply running",
			manifest: []string{shardA, shardB},
			ops: []*storage.ApplyOperation{
				{ID: 1, OperationKey: shardA, State: state.ApplyOperation.Reverted},
			},
			wantState: state.Apply.Running,
			wantDone:  false,
		},
		{
			name:     "full manifest attached with a revert takes the reverted verdict",
			manifest: []string{shardA, shardB},
			ops: []*storage.ApplyOperation{
				{ID: 1, OperationKey: shardA, State: state.ApplyOperation.Reverted},
				{ID: 2, OperationKey: shardB, State: state.ApplyOperation.Completed},
			},
			wantState: state.Apply.Reverted,
			wantDone:  true,
		},
		{
			name:     "failure passes through while manifest operations are missing",
			manifest: []string{shardA, shardB},
			ops: []*storage.ApplyOperation{
				{ID: 1, OperationKey: shardA, State: state.ApplyOperation.Failed},
			},
			wantState: state.Apply.Failed,
			wantDone:  true,
		},
		{
			name:     "no manifest keeps attached-rows-only completion",
			manifest: nil,
			ops: []*storage.ApplyOperation{
				{ID: 1, OperationKey: shardA, State: state.ApplyOperation.Completed},
			},
			wantState: state.Apply.Completed,
			wantDone:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applyStore := &recordingApplyStore{swapped: true}
			svc := newOperatorStateTestService(&listingApplyOperationStore{ops: tc.ops}, applyStore)

			apply := &storage.Apply{
				ID:                    3,
				ApplyIdentifier:       "apply-manifest",
				State:                 state.Apply.Pending,
				Environment:           "staging",
				ExpectedOperationKeys: tc.manifest,
			}

			result, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
			require.NoError(t, err)
			require.NotNil(t, applyStore.updated, "derived state differs from pending, so the apply must be persisted")
			assert.Equal(t, tc.wantState, applyStore.updated.State)
			assert.Equal(t, tc.wantDone, result.BecameTerminal)
			if tc.wantDone {
				assert.NotNil(t, applyStore.updated.CompletedAt, "terminal derived state stamps completed_at")
			} else {
				assert.Nil(t, applyStore.updated.CompletedAt, "a held apply must not carry a completion time")
			}
		})
	}
}

// A held apply already projected to running must not be rewritten on every
// poll: when the manifest gate holds completion and the apply is already
// running, the projection is a no-op rather than a repeated swap.
func TestUpdateApplyStateFromOperations_ManifestHoldIsStableWhileRunning(t *testing.T) {
	applyStore := &recordingApplyStore{swapped: true}
	svc := newOperatorStateTestService(&listingApplyOperationStore{ops: []*storage.ApplyOperation{
		{ID: 1, OperationKey: "ns/-80/users", State: state.ApplyOperation.Completed},
	}}, applyStore)

	apply := &storage.Apply{
		ID:                    3,
		ApplyIdentifier:       "apply-manifest-stable",
		State:                 state.Apply.Running,
		Environment:           "staging",
		ExpectedOperationKeys: []string{"ns/-80/users", "ns/80-/users"},
		StartedAt:             &time.Time{},
	}

	result, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
	require.NoError(t, err)
	assert.False(t, result.Swapped, "holding at the current running state is not a swap")
	assert.Nil(t, applyStore.updated, "a stable hold must not rewrite the apply row")
	assert.Equal(t, state.Apply.Running, result.DerivedState)
	assert.True(t, result.ManifestHeld,
		"the result must mark the manifest hold so callers can tell a held rollout from a stranded parent")
}

// releaseLatchControlStore returns a fixed release latch from GetByOperation so
// the operator's apply-state writer can be driven with or without a release. It
// records the queried applyID so a test can assert the latch is read for the
// apply being projected, not some other apply.
type releaseLatchControlStore struct {
	storage.ControlRequestStore
	release        *storage.ApplyControlRequest
	queriedApply   int64
	queriedRelease bool
}

func (s *releaseLatchControlStore) GetByOperation(_ context.Context, applyID int64, op storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	if op == storage.ControlOperationRelease {
		s.queriedApply = applyID
		s.queriedRelease = true
		return s.release, nil
	}
	return nil, nil
}

// TestUpdateApplyStateFromOperations_PausePolicy verifies the operator projects
// the release latch over an on_failure "pause" rollout: an unreleased pause
// holds the apply paused while a later sibling is still pending, and an operator
// release latches the rollout open so the same failure projects running_degraded
// like continue.
func TestUpdateApplyStateFromOperations_PausePolicy(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, State: state.ApplyOperation.Failed, OnFailure: storage.OnFailurePause},
		{ID: 2, State: state.ApplyOperation.Pending, OnFailure: storage.OnFailurePause},
	}
	cases := []struct {
		name      string
		release   *storage.ApplyControlRequest
		wantState string
	}{
		{
			name:      "unreleased pause holds the apply paused",
			release:   nil,
			wantState: state.Apply.Paused,
		},
		{
			name:      "released pause projects running_degraded like continue",
			release:   &storage.ApplyControlRequest{Operation: storage.ControlOperationRelease, Status: storage.ControlRequestCompleted},
			wantState: state.Apply.RunningDegraded,
		},
		{
			name:      "failed release does not latch and stays paused",
			release:   &storage.ApplyControlRequest{Operation: storage.ControlOperationRelease, Status: storage.ControlRequestFailed},
			wantState: state.Apply.Paused,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applyStore := &recordingApplyStore{swapped: true}
			control := &releaseLatchControlStore{release: tc.release}
			logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
			svc := New(&mockStorageWithControlAndOps{
				applies:  applyStore,
				applyOps: &listingApplyOperationStore{ops: ops},
				control:  control,
			}, testServerConfig(), nil, logger)

			apply := &storage.Apply{
				ID:              3,
				ApplyIdentifier: "apply-pause",
				State:           state.Apply.Pending,
				Environment:     "staging",
			}

			_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
			require.NoError(t, err)
			require.NotNil(t, applyStore.updated)
			assert.Equal(t, tc.wantState, applyStore.updated.State)
			require.True(t, control.queriedRelease, "a pause rollout must read the release latch")
			assert.Equal(t, apply.ID, control.queriedApply, "the release latch must be read for the apply being projected")
		})
	}
}

// TestUpdateApplyStateFromOperations_NoPauseSkipsReleaseLatch verifies the
// operator does not read the release latch when no operation uses on_failure
// pause: a non-pause rollout's projection cannot depend on a release, so it pays
// neither the read nor its failure mode.
func TestUpdateApplyStateFromOperations_NoPauseSkipsReleaseLatch(t *testing.T) {
	applyStore := &recordingApplyStore{swapped: true}
	control := &releaseLatchControlStore{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithControlAndOps{
		applies: applyStore,
		applyOps: &listingApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureContinue},
			{ID: 2, State: state.ApplyOperation.Pending, OnFailure: storage.OnFailureContinue},
		}},
		control: control,
	}, testServerConfig(), nil, logger)

	apply := &storage.Apply{
		ID:              3,
		ApplyIdentifier: "apply-continue",
		State:           state.Apply.Pending,
		Environment:     "staging",
	}

	_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
	require.NoError(t, err)
	assert.False(t, control.queriedRelease, "a rollout with no pause operation must not read the release latch")
}

func TestUpdateApplyStateFromOperations_FinalizerPendingIsNonTerminal(t *testing.T) {
	applyStore := &recordingApplyStore{swapped: true}
	svc := newOperatorStateTestService(&listingApplyOperationStore{ops: []*storage.ApplyOperation{
		{ID: 1, State: state.ApplyOperation.Completed, OperationKind: storage.ApplyOperationKindWork},
		{ID: 2, State: state.ApplyOperation.Completed, OperationKind: storage.ApplyOperationKindWork},
		{ID: 3, State: state.ApplyOperation.Pending, OperationKind: storage.ApplyOperationKindGroupFinalizer},
	}}, applyStore)

	apply := &storage.Apply{
		ID:              3,
		ApplyIdentifier: "apply-finalizer-pending",
		State:           state.Apply.Running,
		Environment:     "staging",
	}

	_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
	require.NoError(t, err)
	require.NotNil(t, applyStore.updated, "the pending finalizer must keep the aggregate non-terminal")
	assert.Equal(t, state.Apply.Pending, applyStore.updated.State)
	assert.Nil(t, applyStore.updated.CompletedAt)
}

// TestUpdateApplyStateFromOperations_ReopenFailedGuard verifies the terminal
// guard's reopen exception. Under on_failure "continue" a sibling failure can
// terminalize the parent apply to failed before the rollout settles; once a
// live sibling still derives the projection running_degraded, a lease-holding
// caller may reopen the parent failed → running_degraded so the remaining
// siblings run to completion. The exception is deliberately narrow: only a
// failed parent over a genuinely failed child base may reopen, only to
// running_degraded, and only when the caller holds the apply lease. Every other
// terminal-to-non-terminal transition
// — including reviving a failed parent from an unscoped reconciliation path, and
// any genuinely terminal verdict (completed/cancelled/reverted) — stays an error.
func TestUpdateApplyStateFromOperations_ReopenFailedGuard(t *testing.T) {
	cases := []struct {
		name       string
		parent     string
		ops        []*storage.ApplyOperation
		reopen     failedApplyReopenPolicy
		wantErr    bool
		wantState  string
		wantUpdate bool
	}{
		{
			name:   "lease-scoped reopen holds the failed apply running_degraded for a live sibling",
			parent: state.Apply.Failed,
			ops: []*storage.ApplyOperation{
				{ID: 1, State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureContinue},
				{ID: 2, State: state.ApplyOperation.Running, OnFailure: storage.OnFailureContinue},
			},
			reopen:     allowLeaseScopedFailedReopen,
			wantState:  state.Apply.RunningDegraded,
			wantUpdate: true,
		},
		{
			name:   "unscoped reconciliation refuses to revive a failed apply",
			parent: state.Apply.Failed,
			ops: []*storage.ApplyOperation{
				{ID: 1, State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureContinue},
				{ID: 2, State: state.ApplyOperation.Running, OnFailure: storage.OnFailureContinue},
			},
			reopen:  rejectFailedApplyReopen,
			wantErr: true,
		},
		{
			name:   "completed apply is never revived even with the lease",
			parent: state.Apply.Completed,
			ops: []*storage.ApplyOperation{
				{ID: 1, State: state.ApplyOperation.Running, OnFailure: storage.OnFailureContinue},
				{ID: 2, State: state.ApplyOperation.Completed, OnFailure: storage.OnFailureContinue},
			},
			reopen:  allowLeaseScopedFailedReopen,
			wantErr: true,
		},
		{
			name:   "stale failed apply over a non-failed child base is not reopened",
			parent: state.Apply.Failed,
			ops: []*storage.ApplyOperation{
				{ID: 1, State: state.ApplyOperation.Running, OnFailure: storage.OnFailureContinue},
				{ID: 2, State: state.ApplyOperation.Completed, OnFailure: storage.OnFailureContinue},
			},
			reopen:  allowLeaseScopedFailedReopen,
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applyStore := &recordingApplyStore{swapped: true}
			svc := newOperatorStateTestService(&listingApplyOperationStore{ops: tc.ops}, applyStore)

			// A terminal parent always carries a stamped completed_at; seed one
			// so the reopen-clears-completed_at assertion is meaningful.
			completedAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			apply := &storage.Apply{
				ID:              7,
				ApplyIdentifier: "apply-reopen",
				State:           tc.parent,
				Environment:     "staging",
				CompletedAt:     &completedAt,
			}

			_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, tc.reopen)
			if tc.wantErr {
				require.Error(t, err)
				assert.Nil(t, applyStore.updated, "a rejected transition must not persist the apply")
				return
			}
			require.NoError(t, err)
			if !tc.wantUpdate {
				assert.Nil(t, applyStore.updated)
				return
			}
			require.NotNil(t, applyStore.updated, "a reopened apply must be persisted")
			assert.Equal(t, tc.wantState, applyStore.updated.State)
			assert.Nil(t, applyStore.updated.CompletedAt, "a reopened running apply clears completed_at")
		})
	}
}

// stubTaskStore returns a fixed task set for GetByApplyOperationID so an
// operation's own drive result can be derived from its tasks.
type stubTaskStore struct {
	storage.TaskStore
	tasks []*storage.Task
}

func (s *stubTaskStore) GetByApplyOperationID(context.Context, int64) ([]*storage.Task, error) {
	return s.tasks, nil
}

// markFailedRecordingApplyOperationStore records MarkFailed so a test can assert
// the operation row was persisted failed with its own task's message.
type markFailedRecordingApplyOperationStore struct {
	storage.ApplyOperationStore
	called    bool
	failedID  int64
	failedMsg string
}

func (s *markFailedRecordingApplyOperationStore) MarkFailed(_ context.Context, id int64, errMsg string) error {
	s.called = true
	s.failedID = id
	s.failedMsg = errMsg
	return nil
}

type mockStorageWithTasksAndOperations struct {
	mockStorage
	tasks    storage.TaskStore
	applyOps storage.ApplyOperationStore
}

func (m *mockStorageWithTasksAndOperations) Tasks() storage.TaskStore { return m.tasks }

func (m *mockStorageWithTasksAndOperations) ApplyOperations() storage.ApplyOperationStore {
	return m.applyOps
}

// TestMarkOperationFromOwnResult_PersistsFailedIndependentOfParent verifies that
// the drive path records a failed deployment from the operation's OWN tasks
// regardless of the parent apply's state. Under the on_failure "continue"
// projection the parent is held running while sibling deployments are still in
// flight; deriving the operation from its own failing task still marks the row
// failed (with that task's message) rather than leaving it claimable to be
// re-driven, so the deployment-order gate and the parent re-derivation observe
// the real outcome.
func TestMarkOperationFromOwnResult_PersistsFailedIndependentOfParent(t *testing.T) {
	opStore := &markFailedRecordingApplyOperationStore{}
	taskStore := &stubTaskStore{tasks: []*storage.Task{
		{State: state.Task.Completed},
		{State: state.Task.Failed, ErrorMessage: "spirit: cutover failed"},
	}}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithTasksAndOperations{tasks: taskStore, applyOps: opStore}, testServerConfig(), nil, logger)

	op := &storage.ApplyOperation{ID: 9, Deployment: "region-b", OnFailure: storage.OnFailureContinue}

	marked, err := svc.markOperationFromOwnResult(t.Context(), 1, op)
	require.NoError(t, err)
	assert.True(t, marked, "a failed operation must be durably recorded so it is not re-claimed")
	assert.True(t, opStore.called, "the operation row must be marked failed from its own tasks")
	assert.Equal(t, int64(9), opStore.failedID, "the claimed operation row must be the one marked failed")
	assert.Equal(t, "spirit: cutover failed", opStore.failedMsg,
		"the failure message must come from the operation's own failing task")
}

// TestMarkOperationFromOwnResult_LeavesNonTerminalClaimable verifies that an
// operation whose own tasks are still running is left claimable (marked=false,
// no terminal write) so a later poll re-leases and resumes it, rather than being
// prematurely terminalized from a still-in-flight drive.
func TestMarkOperationFromOwnResult_LeavesNonTerminalClaimable(t *testing.T) {
	opStore := &markFailedRecordingApplyOperationStore{}
	taskStore := &stubTaskStore{tasks: []*storage.Task{
		{State: state.Task.Running},
		{State: state.Task.Completed},
	}}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithTasksAndOperations{tasks: taskStore, applyOps: opStore}, testServerConfig(), nil, logger)

	op := &storage.ApplyOperation{ID: 11, Deployment: "region-c", OnFailure: storage.OnFailureContinue}

	marked, err := svc.markOperationFromOwnResult(t.Context(), 1, op)
	require.NoError(t, err)
	assert.False(t, marked, "a still-running operation must be left claimable for a later poll")
	assert.False(t, opStore.called, "no terminal write should occur for a non-terminal operation")
}

// updateStateRecordingApplyOperationStore records UpdateState so a test can
// assert a parked operation is persisted at waiting_for_cutover (completed_at nil).
type updateStateRecordingApplyOperationStore struct {
	storage.ApplyOperationStore
	called       bool
	updatedID    int64
	updatedState string
}

func (s *updateStateRecordingApplyOperationStore) UpdateState(_ context.Context, id int64, newState string) error {
	s.called = true
	s.updatedID = id
	s.updatedState = newState
	return nil
}

// TestMarkOperationFromOwnResult_PersistsWaitingForCutover verifies that an
// operation whose own tasks have parked at the cutover barrier is durably
// recorded at waiting_for_cutover via UpdateState (not a terminal write), so the
// row survives the copy drive's release and the deployment-ordered cutover claim
// can pick it up later.
func TestMarkOperationFromOwnResult_PersistsWaitingForCutover(t *testing.T) {
	opStore := &updateStateRecordingApplyOperationStore{}
	taskStore := &stubTaskStore{tasks: []*storage.Task{
		{State: state.Task.WaitingForCutover},
		{State: state.Task.WaitingForCutover},
	}}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithTasksAndOperations{tasks: taskStore, applyOps: opStore}, testServerConfig(), nil, logger)

	op := &storage.ApplyOperation{ID: 13, Deployment: "region-d", CutoverPolicy: storage.CutoverPolicyBarrier}

	marked, err := svc.markOperationFromOwnResult(t.Context(), 1, op)
	require.NoError(t, err)
	assert.True(t, marked, "a parked operation must be durably recorded so the copy claim does not re-drive it")
	assert.True(t, opStore.called, "the operation row must be updated to waiting_for_cutover from its own tasks")
	assert.Equal(t, int64(13), opStore.updatedID)
	assert.True(t, state.IsState(opStore.updatedState, state.Apply.WaitingForCutover),
		"the parked operation must be persisted at waiting_for_cutover, got %q", opStore.updatedState)
}

// TestUpdateApplyStateFromOperations_StampsAggregateFailureMessage verifies that
// when the rollout settles to failed the parent apply's ErrorMessage is surfaced
// from the first failed operation, not from the last-driven (here, successful)
// sibling. The failing deployment ran first and the apply carries no prior
// message; the derived failed verdict must be accompanied by that deployment's
// reason so an operator sees why the apply failed.
func TestUpdateApplyStateFromOperations_StampsAggregateFailureMessage(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "region-a", State: state.ApplyOperation.Failed, ErrorMessage: "spirit: cutover failed"},
		{ID: 2, Deployment: "region-b", State: state.ApplyOperation.Completed},
	}
	applyStore := &recordingApplyStore{swapped: true}
	svc := newOperatorStateTestService(&listingApplyOperationStore{ops: ops}, applyStore)

	apply := &storage.Apply{
		ID:              3,
		ApplyIdentifier: "apply-projection",
		State:           state.Apply.Running,
		Environment:     "staging",
	}

	_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
	require.NoError(t, err)
	require.NotNil(t, applyStore.updated, "derived failed state differs from running, so the apply must be persisted")
	assert.Equal(t, state.Apply.Failed, applyStore.updated.State)
	assert.Equal(t, "deployment region-a failed: spirit: cutover failed", applyStore.updated.ErrorMessage,
		"the failure reason must come from the failed operation, not the successful last sibling")
}

// TestUpdateApplyStateFromOperations_FirstFailedDeploymentWins verifies that
// when more than one deployment fails the surfaced reason comes from the first
// failed operation in deployment order — the order ListByApply returns rows in,
// matching the order the claim gate drives them. The rollout's failure verdict
// is the first failure, so a later failed sibling's message must not override it.
func TestUpdateApplyStateFromOperations_FirstFailedDeploymentWins(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "region-a", State: state.ApplyOperation.Failed, ErrorMessage: "spirit: region-a cutover failed"},
		{ID: 2, Deployment: "region-b", State: state.ApplyOperation.Failed, ErrorMessage: "spirit: region-b cutover failed"},
	}
	applyStore := &recordingApplyStore{swapped: true}
	svc := newOperatorStateTestService(&listingApplyOperationStore{ops: ops}, applyStore)

	apply := &storage.Apply{
		ID:              3,
		ApplyIdentifier: "apply-projection",
		State:           state.Apply.Running,
		Environment:     "staging",
	}

	_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
	require.NoError(t, err)
	require.NotNil(t, applyStore.updated)
	assert.Equal(t, state.Apply.Failed, applyStore.updated.State)
	assert.Equal(t, "deployment region-a failed: spirit: region-a cutover failed", applyStore.updated.ErrorMessage,
		"the reason must come from the first failed deployment in order, not a later failed sibling")
}

// TestUpdateApplyStateFromOperations_KeepsExistingMessageWhenNoOperationCarriesOne
// verifies that a derived failed verdict preserves the apply's existing message
// when no failed operation row carries one, rather than blanking the reason.
func TestUpdateApplyStateFromOperations_KeepsExistingMessageWhenNoOperationCarriesOne(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "region-a", State: state.ApplyOperation.Failed},
		{ID: 2, Deployment: "region-b", State: state.ApplyOperation.Completed},
	}
	applyStore := &recordingApplyStore{swapped: true}
	svc := newOperatorStateTestService(&listingApplyOperationStore{ops: ops}, applyStore)

	apply := &storage.Apply{
		ID:              3,
		ApplyIdentifier: "apply-projection",
		State:           state.Apply.Running,
		Environment:     "staging",
		ErrorMessage:    "prior reason",
	}

	_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
	require.NoError(t, err)
	require.NotNil(t, applyStore.updated)
	assert.Equal(t, state.Apply.Failed, applyStore.updated.State)
	assert.Equal(t, "prior reason", applyStore.updated.ErrorMessage,
		"with no operation-level message the existing apply reason must be preserved")
}

// TestUpdateApplyStateFromOperations_ReturnsProjectionResult verifies the
// structured projection result the writer returns: whether the compare-and-swap
// advanced the apply row, the previous and derived states, and whether the swap
// terminalized a previously non-terminal apply. Callers in the multi-deployment
// fan-out work key the single-publisher terminal summary off this result, so the
// fields must distinguish a winning terminal swap from a non-terminal swap, a
// no-op match, and a lost race.
func TestUpdateApplyStateFromOperations_ReturnsProjectionResult(t *testing.T) {
	startedAt := time.Now().Add(-time.Minute)
	cases := []struct {
		name     string
		ops      []*storage.ApplyOperation
		apply    *storage.Apply
		casMatch bool
		want     applyProjectionResult
	}{
		{
			name: "winning swap to terminal",
			ops: []*storage.ApplyOperation{
				{ID: 1, Deployment: "region-a", State: state.ApplyOperation.Failed, ErrorMessage: "boom"},
				{ID: 2, Deployment: "region-b", State: state.ApplyOperation.Completed},
			},
			apply:    &storage.Apply{ID: 3, ApplyIdentifier: "apply-a", State: state.Apply.Running, StartedAt: &startedAt},
			casMatch: true,
			want:     applyProjectionResult{Swapped: true, PreviousState: state.Apply.Running, DerivedState: state.Apply.Failed, BecameTerminal: true, OperationCount: 2},
		},
		{
			name: "winning non-terminal swap",
			ops: []*storage.ApplyOperation{
				{ID: 1, Deployment: "region-a", State: state.ApplyOperation.Running},
				{ID: 2, Deployment: "region-b", State: state.ApplyOperation.Pending},
			},
			apply:    &storage.Apply{ID: 3, ApplyIdentifier: "apply-b", State: state.Apply.Pending},
			casMatch: true,
			want:     applyProjectionResult{Swapped: true, PreviousState: state.Apply.Pending, DerivedState: state.Apply.Running, BecameTerminal: false, OperationCount: 2},
		},
		{
			name: "no-op match",
			ops: []*storage.ApplyOperation{
				{ID: 1, Deployment: "region-a", State: state.ApplyOperation.Running},
				{ID: 2, Deployment: "region-b", State: state.ApplyOperation.Running},
			},
			apply:    &storage.Apply{ID: 3, ApplyIdentifier: "apply-c", State: state.Apply.Running, StartedAt: &startedAt},
			casMatch: true,
			want:     applyProjectionResult{Swapped: false, PreviousState: state.Apply.Running, DerivedState: state.Apply.Running, BecameTerminal: false, OperationCount: 2},
		},
		{
			name: "lost race",
			ops: []*storage.ApplyOperation{
				{ID: 1, Deployment: "region-a", State: state.ApplyOperation.Failed, ErrorMessage: "boom"},
				{ID: 2, Deployment: "region-b", State: state.ApplyOperation.Completed},
			},
			apply:    &storage.Apply{ID: 3, ApplyIdentifier: "apply-d", State: state.Apply.Running, StartedAt: &startedAt},
			casMatch: false,
			want:     applyProjectionResult{Swapped: false, PreviousState: state.Apply.Running, DerivedState: state.Apply.Failed, BecameTerminal: false, OperationCount: 2},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applyStore := &recordingApplyStore{swapped: tc.casMatch}
			svc := newOperatorStateTestService(&listingApplyOperationStore{ops: tc.ops}, applyStore)

			got, err := svc.updateApplyStateFromOperations(t.Context(), 1, tc.apply, allowLeaseScopedFailedReopen)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// fakeControlRequestStore is a minimal ControlRequestStore for control-request
// reconciliation tests: GetPending returns the configured pending request for
// the operation, and CompletePending records the operations it was asked to
// complete.
type fakeControlRequestStore struct {
	storage.ControlRequestStore
	pending   map[storage.ControlOperation]*storage.ApplyControlRequest
	completed []storage.ControlOperation
}

func (s *fakeControlRequestStore) GetPending(_ context.Context, _ int64, op storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	return s.pending[op], nil
}

func (s *fakeControlRequestStore) CompletePending(_ context.Context, _ int64, op storage.ControlOperation) error {
	s.completed = append(s.completed, op)
	return nil
}

// markPendingStoppedRecordingStore records MarkPendingStoppedByApply so a test
// can assert the operator stop reconciliation terminalized the pending siblings.
type markPendingStoppedRecordingStore struct {
	storage.ApplyOperationStore
	called     bool
	stoppedFor int64
	count      int64
	ops        []*storage.ApplyOperation
}

func (s *markPendingStoppedRecordingStore) ListByApply(_ context.Context, _ int64) ([]*storage.ApplyOperation, error) {
	return s.ops, nil
}

func (s *markPendingStoppedRecordingStore) MarkPendingStoppedByApply(_ context.Context, applyID int64) (int64, error) {
	s.called = true
	s.stoppedFor = applyID
	return s.count, nil
}

// getApplyStore returns a fixed apply from Get so
// completePendingControlRequestsIfApplyResolved can be driven against a chosen
// terminal/non-terminal state.
type getApplyStore struct {
	storage.ApplyStore
	apply *storage.Apply
}

func (s *getApplyStore) Get(_ context.Context, _ int64) (*storage.Apply, error) {
	return s.apply, nil
}

type mockStorageWithControlAndOps struct {
	mockStorage
	applies  storage.ApplyStore
	applyOps storage.ApplyOperationStore
	control  storage.ControlRequestStore
}

func (m *mockStorageWithControlAndOps) Applies() storage.ApplyStore { return m.applies }
func (m *mockStorageWithControlAndOps) ApplyOperations() storage.ApplyOperationStore {
	return m.applyOps
}
func (m *mockStorageWithControlAndOps) ControlRequests() storage.ControlRequestStore {
	return m.control
}

func newStopReconcileTestService(applies storage.ApplyStore, ops storage.ApplyOperationStore, control storage.ControlRequestStore) *Service {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithControlAndOps{applies: applies, applyOps: ops, control: control}, testServerConfig(), nil, logger)
}

// TestStopPendingOperationsForPendingStop verifies the operator terminalizes
// pending siblings only when a stop is actually pending for the apply.
func TestStopPendingOperationsForPendingStop(t *testing.T) {
	apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-stop", Environment: "staging"}

	t.Run("stops pending siblings when a stop is pending", func(t *testing.T) {
		ops := &markPendingStoppedRecordingStore{count: 2}
		control := &fakeControlRequestStore{pending: map[storage.ControlOperation]*storage.ApplyControlRequest{
			storage.ControlOperationStop: {ApplyID: apply.ID, Operation: storage.ControlOperationStop, Status: storage.ControlRequestPending},
		}}
		svc := newStopReconcileTestService(&getApplyStore{}, ops, control)

		require.NoError(t, svc.stopPendingOperationsForPendingStop(t.Context(), 1, apply))
		assert.True(t, ops.called, "a pending stop must terminalize pending siblings")
		assert.Equal(t, apply.ID, ops.stoppedFor)
	})

	t.Run("no-op when no stop is pending", func(t *testing.T) {
		ops := &markPendingStoppedRecordingStore{}
		control := &fakeControlRequestStore{}
		svc := newStopReconcileTestService(&getApplyStore{}, ops, control)

		require.NoError(t, svc.stopPendingOperationsForPendingStop(t.Context(), 1, apply))
		assert.False(t, ops.called, "without a pending stop, no siblings are stopped")
	})
}

// TestCompletePendingControlRequestsIfApplyResolved verifies the operator
// completes pending stop and cancel requests only once the apply has settled
// terminally, and keeps a pending cancel deliverable when the terminal state
// is stopped — a stopped apply remains cancellable, so completing the cancel
// there would consume a command the next drive still has to deliver.
//
// A stop also resolves on an apply the rollout projection is holding open once
// it has reached every operation. A stopped operation still holds its target,
// so a stopped rollout can carry a running-family verdict; the stop request is
// what start consults, and leaving it pending there would refuse the very start
// that resumes the stopped operation.
func TestCompletePendingControlRequestsIfApplyResolved(t *testing.T) {
	pendingRequest := func(op storage.ControlOperation) *storage.ApplyControlRequest {
		return &storage.ApplyControlRequest{ApplyID: 9, Operation: op, Status: storage.ControlRequestPending}
	}

	t.Run("completes the stop when the apply is terminal", func(t *testing.T) {
		applies := &getApplyStore{apply: &storage.Apply{ID: 9, ApplyIdentifier: "apply-done", State: state.Apply.Failed}}
		control := &fakeControlRequestStore{pending: map[storage.ControlOperation]*storage.ApplyControlRequest{
			storage.ControlOperationStop: pendingRequest(storage.ControlOperationStop),
		}}
		svc := newStopReconcileTestService(applies, &markPendingStoppedRecordingStore{}, control)

		require.NoError(t, svc.completePendingControlRequestsIfApplyResolved(t.Context(), 1, 9))
		require.Len(t, control.completed, 1, "a terminal apply with a pending stop completes the request")
		assert.Equal(t, storage.ControlOperationStop, control.completed[0])
	})

	t.Run("completes the cancel when the apply is terminal", func(t *testing.T) {
		applies := &getApplyStore{apply: &storage.Apply{ID: 9, ApplyIdentifier: "apply-done", State: state.Apply.Cancelled}}
		control := &fakeControlRequestStore{pending: map[storage.ControlOperation]*storage.ApplyControlRequest{
			storage.ControlOperationCancel: pendingRequest(storage.ControlOperationCancel),
		}}
		svc := newStopReconcileTestService(applies, &markPendingStoppedRecordingStore{}, control)

		require.NoError(t, svc.completePendingControlRequestsIfApplyResolved(t.Context(), 1, 9))
		require.Len(t, control.completed, 1, "a terminal apply with a pending cancel completes the request")
		assert.Equal(t, storage.ControlOperationCancel, control.completed[0])
	})

	t.Run("keeps the cancel pending when the apply settled stopped", func(t *testing.T) {
		applies := &getApplyStore{apply: &storage.Apply{ID: 9, ApplyIdentifier: "apply-stopped", State: state.Apply.Stopped}}
		control := &fakeControlRequestStore{pending: map[storage.ControlOperation]*storage.ApplyControlRequest{
			storage.ControlOperationStop:   pendingRequest(storage.ControlOperationStop),
			storage.ControlOperationCancel: pendingRequest(storage.ControlOperationCancel),
		}}
		svc := newStopReconcileTestService(applies, &markPendingStoppedRecordingStore{}, control)

		require.NoError(t, svc.completePendingControlRequestsIfApplyResolved(t.Context(), 1, 9))
		require.Len(t, control.completed, 1, "a stopped apply completes the stop but must keep the cancel deliverable")
		assert.Equal(t, storage.ControlOperationStop, control.completed[0])
	})

	t.Run("leaves both requests pending while an operation the stop has not reached is running", func(t *testing.T) {
		applies := &getApplyStore{apply: &storage.Apply{ID: 9, ApplyIdentifier: "apply-running", State: state.Apply.Running}}
		control := &fakeControlRequestStore{pending: map[storage.ControlOperation]*storage.ApplyControlRequest{
			storage.ControlOperationStop:   pendingRequest(storage.ControlOperationStop),
			storage.ControlOperationCancel: pendingRequest(storage.ControlOperationCancel),
		}}
		ops := &markPendingStoppedRecordingStore{ops: []*storage.ApplyOperation{
			{ID: 1, Deployment: "region-a", State: state.ApplyOperation.Running},
		}}
		svc := newStopReconcileTestService(applies, ops, control)

		require.NoError(t, svc.completePendingControlRequestsIfApplyResolved(t.Context(), 1, 9))
		assert.Empty(t, control.completed, "an apply with live work must not complete any control request")
	})

	t.Run("completes only the stop when a held-open apply has stopped every operation", func(t *testing.T) {
		applies := &getApplyStore{apply: &storage.Apply{ID: 9, ApplyIdentifier: "apply-degraded", State: state.Apply.RunningDegraded}}
		control := &fakeControlRequestStore{pending: map[storage.ControlOperation]*storage.ApplyControlRequest{
			storage.ControlOperationStop:   pendingRequest(storage.ControlOperationStop),
			storage.ControlOperationCancel: pendingRequest(storage.ControlOperationCancel),
		}}
		ops := &markPendingStoppedRecordingStore{ops: []*storage.ApplyOperation{
			{ID: 1, Deployment: "region-a", State: state.ApplyOperation.Failed},
			{ID: 2, Deployment: "region-b", State: state.ApplyOperation.Stopped},
		}}
		svc := newStopReconcileTestService(applies, ops, control)

		require.NoError(t, svc.completePendingControlRequestsIfApplyResolved(t.Context(), 1, 9))
		require.Len(t, control.completed, 1, "the stop landed on every operation, so its request completes; the cancel is still deliverable")
		assert.Equal(t, storage.ControlOperationStop, control.completed[0])
	})

	t.Run("no-op when nothing is pending", func(t *testing.T) {
		applies := &getApplyStore{apply: &storage.Apply{ID: 9, ApplyIdentifier: "apply-done", State: state.Apply.Failed}}
		control := &fakeControlRequestStore{}
		svc := newStopReconcileTestService(applies, &markPendingStoppedRecordingStore{}, control)

		require.NoError(t, svc.completePendingControlRequestsIfApplyResolved(t.Context(), 1, 9))
		assert.Empty(t, control.completed, "no pending requests means nothing to complete")
	})
}

// TestUpdateApplyStateFromOperations_StaleWriteSkipped verifies that when the
// compare-and-swap misses — another drive advanced the apply between the
// operator's read and write — the operator skips quietly rather than erroring or
// reviving a stale projection.
func TestUpdateApplyStateFromOperations_StaleWriteSkipped(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, State: state.ApplyOperation.Failed},
		{ID: 2, State: state.ApplyOperation.Pending},
	}
	applyStore := &recordingApplyStore{swapped: false}
	svc := newOperatorStateTestService(&listingApplyOperationStore{ops: ops}, applyStore)

	apply := &storage.Apply{
		ID:              3,
		ApplyIdentifier: "apply-projection",
		State:           state.Apply.Pending,
		Environment:     "staging",
	}

	_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Pending, applyStore.expectedState, "the write must compare-and-swap on the state read before deriving")
	assert.Nil(t, applyStore.updated, "a CAS miss must not record a persisted projection")
}

// casApplyStore models the applies row as a compare-and-swap against a single
// durable state. Get returns the durable state so a reload observes writes made
// by an earlier UpdateDerivedState, and UpdateDerivedState only swaps when the
// caller's expected state matches the durable state — exactly like the SQL CAS.
type casApplyStore struct {
	storage.ApplyStore
	mu       sync.Mutex
	template storage.Apply
	state    string
}

func (s *casApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.template
	a.State = s.state
	return &a, nil
}

func (s *casApplyStore) UpdateDerivedState(_ context.Context, _ int64, expectedState, newState, _ string, _, _ *time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != expectedState {
		return false, nil
	}
	s.state = newState
	return true, nil
}

func (s *casApplyStore) currentState() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// recoverOperationStore backs the single claimed operation through the recover
// flow: Get/ListByApply return the live row and MarkFailed transitions it.
type recoverOperationStore struct {
	storage.ApplyOperationStore
	mu sync.Mutex
	op *storage.ApplyOperation
}

func (s *recoverOperationStore) Get(context.Context, int64) (*storage.ApplyOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op := *s.op
	return &op, nil
}

func (s *recoverOperationStore) ListByApply(context.Context, int64) ([]*storage.ApplyOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op := *s.op
	return []*storage.ApplyOperation{&op}, nil
}

func (s *recoverOperationStore) MarkFailed(_ context.Context, _ int64, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.op.State = state.ApplyOperation.Failed
	s.op.ErrorMessage = errMsg
	return nil
}

func (s *recoverOperationStore) Heartbeat(context.Context, int64) error { return nil }

// recoverTestStorage wires the stores the recover flow touches, including the
// plan lookup the routing tern client requires to build.
type recoverTestStorage struct {
	mockStorage
	applies storage.ApplyStore
	ops     storage.ApplyOperationStore
}

func (s *recoverTestStorage) Applies() storage.ApplyStore                  { return s.applies }
func (s *recoverTestStorage) ApplyOperations() storage.ApplyOperationStore { return s.ops }
func (s *recoverTestStorage) Plans() storage.PlanStore                     { return &staticPlanStore{} }

// When a multi-deployment operation has no tasks, the recover flow fails it
// closed. By the time it fails, the pre-drive projection has already moved the
// durable parent apply from pending to running, so the failure projection must
// compare-and-swap against the reloaded running state — not the stale pending
// apply it started the drive with — or the parent is stranded running.
func TestRecoverMultiApplyOperation_FailsTaskLessOperationAgainstReloadedParent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	applyStore := &casApplyStore{
		template: storage.Apply{
			ID:              7,
			ApplyIdentifier: "apply-multi-op-recover",
			Database:        "testdb",
			DatabaseType:    storage.DatabaseTypeMySQL,
			Environment:     "staging",
		},
		state: state.Apply.Pending,
	}
	opStore := &recoverOperationStore{op: &storage.ApplyOperation{
		ID:         42,
		ApplyID:    7,
		Deployment: "west",
		State:      state.ApplyOperation.Running,
	}}
	deploymentClient := &mockTernClient{resumeErr: tern.ErrNoTasksForApplyOperation}

	svc := New(
		&recoverTestStorage{applies: applyStore, ops: opStore},
		testServerConfig(),
		map[string]tern.Client{"west/staging": deploymentClient},
		logger,
	)

	svc.recoverMultiApplyOperation(t.Context(), 1, &storage.ApplyOperation{
		ID:         42,
		ApplyID:    7,
		Deployment: "west",
		State:      state.ApplyOperation.Running,
	}, storage.OperationLease{})

	assert.Equal(t, state.Apply.Failed, applyStore.currentState(),
		"the parent apply must be failed after the task-less operation is terminalized against the reloaded running state")
	assert.Equal(t, state.ApplyOperation.Failed, opStore.op.State,
		"the task-less operation row must be marked failed")
}

// cutoverOpStore backs the cutover claim path: FindNextApplyOperationCutover
// hands back the barrier-parked operation whose turn it is, ListByApply reports a
// genuine multi-operation set (so the operation-lease-only drive is valid), and
// MarkFailed terminalizes the claimed row.
type cutoverOpStore struct {
	storage.ApplyOperationStore
	mu      sync.Mutex
	op      *storage.ApplyOperation
	sibling *storage.ApplyOperation
	claimed bool
}

func (s *cutoverOpStore) FindNextApplyOperationCutover(context.Context, string) (*storage.ApplyOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimed = true
	op := *s.op
	return &op, nil
}

func (s *cutoverOpStore) Get(context.Context, int64) (*storage.ApplyOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op := *s.op
	return &op, nil
}

func (s *cutoverOpStore) ListByApply(context.Context, int64) ([]*storage.ApplyOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op := *s.op
	sibling := *s.sibling
	return []*storage.ApplyOperation{&op, &sibling}, nil
}

func (s *cutoverOpStore) MarkFailed(_ context.Context, _ int64, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.op.State = state.ApplyOperation.Failed
	s.op.ErrorMessage = errMsg
	return nil
}

func (s *cutoverOpStore) Heartbeat(context.Context, int64) error { return nil }

// The cutover claim path drives a barrier-parked operation through its swap via
// ResumeApplyOperationCutover, not the copy-phase ResumeApplyOperation, and only
// when the claimed operation belongs to a genuine multi-operation apply so the
// operation-lease-only drive (with parent-write suppression) is valid.
func TestRecoverApplyOperationCutover_RoutesThroughCutoverDrive(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	applyStore := &casApplyStore{
		template: storage.Apply{
			ID:              7,
			ApplyIdentifier: "apply-cutover",
			Database:        "testdb",
			DatabaseType:    storage.DatabaseTypeMySQL,
			Environment:     "staging",
		},
		state: state.Apply.Running,
	}
	opStore := &cutoverOpStore{
		op: &storage.ApplyOperation{
			ID:         42,
			ApplyID:    7,
			Deployment: "west",
			State:      state.ApplyOperation.CuttingOver,
			LeaseOwner: "driver-1",
			LeaseToken: "token-1",
		},
		sibling: &storage.ApplyOperation{
			ID:         41,
			ApplyID:    7,
			Deployment: "east",
			State:      state.ApplyOperation.Completed,
		},
	}
	// Fail closed on no tasks so the drive short-circuits to the task-less
	// terminalization; the routing assertion only needs the cutover entrypoint to
	// have been called.
	deploymentClient := &mockTernClient{resumeErr: tern.ErrNoTasksForApplyOperation}

	svc := New(
		&recoverTestStorage{applies: applyStore, ops: opStore},
		testServerConfig(),
		map[string]tern.Client{"west/staging": deploymentClient},
		logger,
	)

	consumed := svc.recoverApplyOperationCutover(t.Context(), 1, "driver-1")

	assert.True(t, consumed, "claiming a parked cutover must consume the tick")
	assert.True(t, opStore.claimed, "the cutover claim predicate must be queried")
	deploymentClient.resumeMu.Lock()
	cutoverID := deploymentClient.resumeCutoverOperationID
	copyID := deploymentClient.resumeOperationID
	deploymentClient.resumeMu.Unlock()
	assert.Equal(t, int64(42), cutoverID, "the operation must be driven through the cutover entrypoint")
	assert.Equal(t, int64(0), copyID, "the cutover claim must not route through the copy-phase entrypoint")
	assert.Equal(t, state.ApplyOperation.Failed, opStore.op.State,
		"the task-less cutover operation must be terminalized failed")
}

// A claimed cutover operation that is not part of a multi-operation apply must
// not be driven: the operation-lease-only path (and its parent-write
// suppression) is only correct for a genuine fan-out, so a single-operation set
// fails closed without calling any resume entrypoint.
func TestRecoverApplyOperationCutover_RejectsSingleOperationSet(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	applyStore := &casApplyStore{
		template: storage.Apply{ID: 7, ApplyIdentifier: "apply-cutover-single", Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging"},
		state:    state.Apply.Running,
	}
	opStore := &recoverOperationStore{op: &storage.ApplyOperation{
		ID:         42,
		ApplyID:    7,
		Deployment: "west",
		State:      state.ApplyOperation.CuttingOver,
		LeaseOwner: "driver-1",
		LeaseToken: "token-1",
	}}
	deploymentClient := &mockTernClient{}

	svc := New(
		&recoverTestStorage{applies: applyStore, ops: &singleCutoverOpStore{recoverOperationStore: opStore}},
		testServerConfig(),
		map[string]tern.Client{"west/staging": deploymentClient},
		logger,
	)

	consumed := svc.recoverApplyOperationCutover(t.Context(), 1, "driver-1")

	assert.True(t, consumed, "claiming any operation consumes the tick even when it is rejected")
	deploymentClient.resumeMu.Lock()
	cutoverID := deploymentClient.resumeCutoverOperationID
	copyID := deploymentClient.resumeOperationID
	deploymentClient.resumeMu.Unlock()
	assert.Equal(t, int64(0), cutoverID, "a single-operation set must not be driven through cutover")
	assert.Equal(t, int64(0), copyID, "a single-operation set must not be driven through copy")
}

// singleCutoverOpStore exposes a recoverOperationStore (single-operation
// ListByApply) through the cutover claim predicate.
type singleCutoverOpStore struct {
	*recoverOperationStore
}

func (s *singleCutoverOpStore) FindNextApplyOperationCutover(context.Context, string) (*storage.ApplyOperation, error) {
	op := *s.op
	return &op, nil
}

// panickingResumeClient simulates an engine whose resume path hits a code or
// data fault (for example malformed stored metadata) and panics mid-drive.
type panickingResumeClient struct {
	mockTernClient
	panicValue string
}

func (c *panickingResumeClient) ResumeApply(context.Context, *storage.Apply) error {
	panic(c.panicValue)
}

func (c *panickingResumeClient) ResumeApplyOperation(context.Context, *storage.Apply, int64) error {
	panic(c.panicValue)
}

// recordingTaskStore serves a fixed task set and records state writes, so tests
// can assert what the panic containment persisted.
type recordingTaskStore struct {
	storage.TaskStore
	tasks   []*storage.Task
	updated []*storage.Task
}

func (s *recordingTaskStore) GetByApplyID(_ context.Context, applyID int64) ([]*storage.Task, error) {
	tasks := make([]*storage.Task, 0, len(s.tasks))
	for _, task := range s.tasks {
		if task.ApplyID == applyID {
			tasks = append(tasks, task)
		}
	}
	return tasks, nil
}

func (s *recordingTaskStore) GetByApplyOperationID(_ context.Context, applyOperationID int64) ([]*storage.Task, error) {
	tasks := make([]*storage.Task, 0, len(s.tasks))
	for _, task := range s.tasks {
		if task.ApplyOperationID != nil && *task.ApplyOperationID == applyOperationID {
			tasks = append(tasks, task)
		}
	}
	return tasks, nil
}

func (s *recordingTaskStore) Update(_ context.Context, task *storage.Task) error {
	s.updated = append(s.updated, task)
	return nil
}

// panicContainmentApplyStore serves a single apply row and records the
// terminal write the panic containment path performs.
type panicContainmentApplyStore struct {
	storage.ApplyStore
	apply        *storage.Apply
	updateCalled bool
}

func (s *panicContainmentApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	if s.apply == nil {
		return nil, nil
	}
	fresh := *s.apply
	return &fresh, nil
}

func (s *panicContainmentApplyStore) Update(_ context.Context, apply *storage.Apply) error {
	s.updateCalled = true
	s.apply = apply
	return nil
}

// staticOperationLookupStore serves one operation row for routing lookups.
type staticOperationLookupStore struct {
	storage.ApplyOperationStore
	op *storage.ApplyOperation
}

func (s *staticOperationLookupStore) Get(context.Context, int64) (*storage.ApplyOperation, error) {
	return s.op, nil
}

// A panic inside the engine drive must be contained to the claimed apply: the
// resume call returns an error instead of crashing the driver, the apply is
// marked failed (permanent) so recovery does not re-claim the poisoned row and
// panic again, and the apply's tasks are failed so dependent state can settle.
// The containment write persists the reloaded row, so a field a peer wrote
// between the claim and the panic survives instead of being clobbered by the
// claim-time snapshot.
func TestResumeClaimedApply_DrivePanicFailsApply(t *testing.T) {
	client := &panickingResumeClient{panicValue: "corrupt engine metadata"}
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-42",
		Database:        "appdb",
		Deployment:      "east",
		Environment:     "staging",
		State:           state.Apply.Running,
		LeaseOwner:      "driver-0",
		LeaseToken:      "lease-token",
	}
	applyStore := &panicContainmentApplyStore{apply: apply}
	taskStore := &recordingTaskStore{tasks: []*storage.Task{
		{ID: 9, ApplyID: 42, State: state.Task.Running},
	}}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		plans:   &staticPlanStore{},
		applies: applyStore,
		tasks:   taskStore,
	}, &ServerConfig{}, map[string]tern.Client{
		"east/staging": client,
	}, logger)

	ctx := storage.WithApplyLease(t.Context(), apply.Lease())
	// The drive holds the claim-time snapshot while a peer writes the stored
	// row (a skip-revert dispatch) before the panic is contained.
	claimed := *apply
	peerWrite := time.Now()
	apply.RevertSkippedAt = &peerWrite

	var resumed bool
	var err error
	require.NotPanics(t, func() {
		resumed, err = svc.resumeClaimedApply(ctx, 0, &claimed, 0, "")
	})

	require.Error(t, err)
	var drivePanic *panicsafe.Error
	require.ErrorAs(t, err, &drivePanic, "a contained drive panic must surface as a panicsafe error")
	assert.Equal(t, "corrupt engine metadata", drivePanic.Value)
	assert.False(t, resumed)

	require.True(t, applyStore.updateCalled, "the apply row must be written to its failed state")
	written := applyStore.apply
	assert.True(t, state.IsState(written.State, state.Apply.Failed),
		"the apply must be failed (permanent), not failed_retryable, so it is not re-claimed and re-panicked")
	assert.Contains(t, written.ErrorMessage, "corrupt engine metadata")
	require.NotNil(t, written.CompletedAt)
	require.NotNil(t, written.RevertSkippedAt,
		"a peer update stored between the claim and the panic must survive the containment write")

	require.Len(t, taskStore.updated, 1, "the in-flight task must be settled")
	assert.True(t, state.IsState(taskStore.updated[0].State, state.Task.Failed))
	assert.Contains(t, taskStore.updated[0].ErrorMessage, "corrupt engine metadata")
}

// A drive panic contained while holding only an operation lease fails the
// operation's tasks but leaves the parent applies row untouched: under the
// multi-operation fan-out the parent state is owned by the rollout projection,
// which settles it from the failed operation row.
func TestResumeClaimedApply_DrivePanicUnderOperationLeaseLeavesParentToProjection(t *testing.T) {
	client := &panickingResumeClient{panicValue: "corrupt operation metadata"}
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-42",
		Database:        "appdb",
		Deployment:      "east",
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	operationID := int64(7)
	applyStore := &panicContainmentApplyStore{apply: apply}
	taskStore := &recordingTaskStore{tasks: []*storage.Task{
		{ID: 9, ApplyID: 42, ApplyOperationID: &operationID, State: state.Task.Running},
	}}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		plans:   &staticPlanStore{},
		applies: applyStore,
		tasks:   taskStore,
		operations: &staticOperationLookupStore{op: &storage.ApplyOperation{
			ID:         operationID,
			ApplyID:    42,
			Deployment: "east",
			State:      state.ApplyOperation.Running,
		}},
	}, &ServerConfig{}, map[string]tern.Client{
		"east/staging": client,
	}, logger)

	ctx := storage.WithOperationLease(t.Context(), storage.OperationLease{
		ApplyID:     42,
		OperationID: operationID,
		Owner:       "driver-0",
		Token:       "op-token",
	})
	var resumed bool
	var err error
	require.NotPanics(t, func() {
		resumed, err = svc.resumeClaimedApply(ctx, 0, apply, operationID, "east")
	})

	var drivePanic *panicsafe.Error
	require.ErrorAs(t, err, &drivePanic)
	assert.False(t, resumed)

	assert.False(t, applyStore.updateCalled,
		"an operation-lease-only drive must not write the parent applies row; the rollout projection owns it")
	assert.True(t, state.IsState(apply.State, state.Apply.Running))

	require.Len(t, taskStore.updated, 1, "the operation's task must be failed so the operation row can settle")
	assert.True(t, state.IsState(taskStore.updated[0].State, state.Task.Failed))
	assert.Contains(t, taskStore.updated[0].ErrorMessage, "corrupt operation metadata")
}

// unclaimableParentApplyStore backs the unclaimable-parent reconcile with a
// fixed answer from Get: a storage error, a missing row, or a parent apply.
type unclaimableParentApplyStore struct {
	storage.ApplyStore
	apply  *storage.Apply
	getErr error
}

func (s *unclaimableParentApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.apply, nil
}

// releaseRecordingOperationStore records whether the reconcile released the
// operation lease. It embeds the interface so any other call panics, keeping
// the test honest about the code path it covers.
type releaseRecordingOperationStore struct {
	storage.ApplyOperationStore
	released bool
}

func (s *releaseRecordingOperationStore) ReleaseClaim(context.Context, storage.OperationLease) (bool, error) {
	s.released = true
	return true, nil
}

// When the parent apply cannot be established — the load fails or the row is
// missing — the driver must retain the operation lease it just claimed and
// fail closed: releasing on uncertainty would let a peer immediately re-claim
// into the same failure, thrashing claims, while retaining the lease bounds
// retries to the lease staleness window. Only a confirmed non-terminal parent
// releases the lease for an immediate retry.
func TestReconcileUnclaimableParent_RetainsLeaseWhenParentUnknown(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	op := &storage.ApplyOperation{
		ID:         42,
		ApplyID:    7,
		Deployment: "west",
		State:      state.ApplyOperation.Running,
	}
	opLease := storage.OperationLease{ApplyID: 7, OperationID: 42, Owner: "driver-1", Token: "token-1"}

	t.Run("parent load error", func(t *testing.T) {
		opStore := &releaseRecordingOperationStore{}
		svc := New(&recoverTestStorage{
			applies: &unclaimableParentApplyStore{getErr: errors.New("connection reset")},
			ops:     opStore,
		}, testServerConfig(), nil, logger)

		svc.reconcileUnclaimableParent(t.Context(), 1, op, opLease)

		assert.False(t, opStore.released,
			"a parent load error must retain the operation lease, not release it")
	})

	t.Run("parent missing", func(t *testing.T) {
		opStore := &releaseRecordingOperationStore{}
		svc := New(&recoverTestStorage{
			applies: &unclaimableParentApplyStore{},
			ops:     opStore,
		}, testServerConfig(), nil, logger)

		svc.reconcileUnclaimableParent(t.Context(), 1, op, opLease)

		assert.False(t, opStore.released,
			"a missing parent apply must retain the operation lease, not release it")
	})

	t.Run("non-terminal parent releases", func(t *testing.T) {
		opStore := &releaseRecordingOperationStore{}
		svc := New(&recoverTestStorage{
			applies: &unclaimableParentApplyStore{apply: &storage.Apply{
				ID:              7,
				ApplyIdentifier: "apply-unclaimable",
				Database:        "testdb",
				DatabaseType:    storage.DatabaseTypeMySQL,
				Environment:     "staging",
				State:           state.Apply.Running,
			}},
			ops: opStore,
		}, testServerConfig(), nil, logger)

		svc.reconcileUnclaimableParent(t.Context(), 1, op, opLease)

		assert.True(t, opStore.released,
			"a confirmed non-terminal parent must release the operation lease for an immediate retry")
	})
}

// claimLadderOperationStore drives the operation-level claim ladder over a
// single operation row: the cutover probe finds nothing, and the operation
// claim leases the row while it is claimable — or panics when configured, to
// model a fault in the claim machinery itself.
type claimLadderOperationStore struct {
	*recoverOperationStore
	claims     int
	claimPanic string
}

func (s *claimLadderOperationStore) FindNextApplyOperationCutover(context.Context, string) (*storage.ApplyOperation, error) {
	return nil, nil
}

func (s *claimLadderOperationStore) FindNextApplyOperation(_ context.Context, owner string) (*storage.ApplyOperation, error) {
	s.claims++
	if s.claimPanic != "" {
		panic(s.claimPanic)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.op == nil || state.IsTerminalApplyState(s.op.State) {
		return nil, nil
	}
	s.op.LeaseOwner = owner
	s.op.LeaseToken = "op-lease-token"
	op := *s.op
	return &op, nil
}

// operationClaimApplyStore serves the apply-side calls the operation-level
// claim ladder makes around an operation claim: the retryable-expiry pass, the
// stop-reconciliation probe, the parent claim, and the post-drive
// reload/derivation.
type operationClaimApplyStore struct {
	storage.ApplyStore
	mu             sync.Mutex
	apply          *storage.Apply
	expireErr      error
	stopProbePanic string
	stopProbes     int
	updateCalled   bool
}

func (s *operationClaimApplyStore) ExpireRetryable(context.Context) ([]*storage.RetryableApplyExpiration, error) {
	return nil, s.expireErr
}

func (s *operationClaimApplyStore) FindNextApplyForStopReconciliation(context.Context, string) (*storage.Apply, error) {
	s.mu.Lock()
	s.stopProbes++
	s.mu.Unlock()
	if s.stopProbePanic != "" {
		panic(s.stopProbePanic)
	}
	return nil, nil
}

func (s *operationClaimApplyStore) FindNextApplyForOperationProjection(context.Context, string) (*storage.Apply, error) {
	return nil, nil
}

func (s *operationClaimApplyStore) ClaimApplyByID(_ context.Context, _ int64, owner string) (*storage.Apply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.apply == nil || state.IsTerminalApplyState(s.apply.State) {
		return nil, nil
	}
	s.apply.LeaseOwner = owner
	s.apply.LeaseToken = "lease-token"
	return s.apply, nil
}

func (s *operationClaimApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.apply == nil {
		return nil, nil
	}
	fresh := *s.apply
	return &fresh, nil
}

func (s *operationClaimApplyStore) Update(_ context.Context, apply *storage.Apply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateCalled = true
	s.apply = apply
	return nil
}

func (s *operationClaimApplyStore) UpdateDerivedState(_ context.Context, _ int64, expectedState, newState, errorMessage string, _, _ *time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.apply == nil || !state.IsState(s.apply.State, expectedState) {
		return false, nil
	}
	s.apply.State = newState
	s.apply.ErrorMessage = errorMessage
	return true, nil
}

// Retryable-apply expiry is best-effort maintenance: a storage failure there
// must not stop a driver from claiming operation work in the same tick, or a
// transient expiry error would starve every queued apply behind it.
func TestRecoverApplies_ExpiryErrorDoesNotBlockOperationClaim(t *testing.T) {
	applies := &operationClaimApplyStore{expireErr: errors.New("storage unavailable")}
	ops := &claimLadderOperationStore{recoverOperationStore: &recoverOperationStore{}}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{applies: applies, operations: ops}, testServerConfig(), nil, logger)

	svc.recoverApplies(t.Context(), 1)

	assert.Equal(t, 1, ops.claims,
		"FindNextApplyOperation must run even when ExpireRetryable fails")
}

// A panic in the operation claim machinery itself (outside the engine drive)
// must not kill the driver goroutine: the tick boundary contains it and the
// driver polls again on the next tick.
func TestDriveTick_ContainsOperationClaimPanic(t *testing.T) {
	applies := &operationClaimApplyStore{}
	ops := &claimLadderOperationStore{
		recoverOperationStore: &recoverOperationStore{},
		claimPanic:            "claim scan hit a corrupt row",
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{applies: applies, operations: ops}, testServerConfig(), nil, logger)

	require.NotPanics(t, func() { svc.driveTick(t.Context(), 0) })
	require.NotPanics(t, func() { svc.driveTick(t.Context(), 0) })
	assert.Equal(t, 2, ops.claims, "the driver must keep polling after each contained panic")
}

// A panic in the pre-claim stop-reconciliation probe is contained the same
// way: the driver survives and probes again on the next tick instead of
// crashing the process.
func TestDriveTick_ContainsStopReconciliationProbePanic(t *testing.T) {
	applies := &operationClaimApplyStore{stopProbePanic: "stop probe hit a corrupt row"}
	ops := &claimLadderOperationStore{recoverOperationStore: &recoverOperationStore{}}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{applies: applies, operations: ops}, testServerConfig(), nil, logger)

	require.NotPanics(t, func() { svc.driveTick(t.Context(), 0) })
	require.NotPanics(t, func() { svc.driveTick(t.Context(), 0) })
	assert.Equal(t, 2, applies.stopProbes, "the driver must keep probing after each contained panic")
	assert.Zero(t, ops.claims, "the panic consumed the tick before the operation claim")
}

// One poisoned apply must degrade only itself: the tick that claims its
// operation contains the drive panic, the operation row and parent apply are
// failed so neither is re-claimed, and the driver keeps claiming fresh work on
// subsequent ticks instead of crashing the process.
func TestDriveTick_ContainsDrivePanicUnderOperationClaimAndKeepsClaiming(t *testing.T) {
	client := &panickingResumeClient{panicValue: "corrupt engine metadata"}
	operationID := int64(7)
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-42",
		Database:        "appdb",
		Deployment:      "east",
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	op := &storage.ApplyOperation{
		ID:         operationID,
		ApplyID:    42,
		Deployment: "east",
		State:      state.ApplyOperation.Running,
	}
	applies := &operationClaimApplyStore{apply: apply}
	ops := &claimLadderOperationStore{recoverOperationStore: &recoverOperationStore{op: op}}
	taskStore := &recordingTaskStore{tasks: []*storage.Task{
		{ID: 9, ApplyID: 42, ApplyOperationID: &operationID, State: state.Task.Running},
	}}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		plans:      &staticPlanStore{},
		applies:    applies,
		tasks:      taskStore,
		controls:   &fakeControlRequestStore{},
		operations: ops,
	}, testServerConfig(), map[string]tern.Client{
		"east/staging": client,
	}, logger)

	require.NotPanics(t, func() { svc.driveTick(t.Context(), 0) })
	assert.Equal(t, 1, ops.claims)
	assert.True(t, state.IsState(op.State, state.ApplyOperation.Failed),
		"the poisoned operation row must be failed so it is not re-claimed")
	assert.Contains(t, op.ErrorMessage, "corrupt engine metadata")
	assert.True(t, state.IsState(applies.apply.State, state.Apply.Failed),
		"the parent apply must be failed under the dual-lease containment")

	require.NotPanics(t, func() { svc.driveTick(t.Context(), 0) })
	assert.Equal(t, 2, ops.claims,
		"the driver must keep claiming after containing the panic, and the failed operation must not be claimable")
}

// tasklessOperationStores holds the four stores markOperationFromOwnResult
// touches for a task-less operation: its (empty) task set, the operation row as
// the drive left it, the parent apply, and the plan that decides whether an
// operation with no tasks is a legitimate VSchema-only drive.
type tasklessOperationStores struct {
	mockStorage
	tasks    storage.TaskStore
	applyOps storage.ApplyOperationStore
	applies  storage.ApplyStore
	plans    storage.PlanStore
}

func (m *tasklessOperationStores) Tasks() storage.TaskStore { return m.tasks }
func (m *tasklessOperationStores) ApplyOperations() storage.ApplyOperationStore {
	return m.applyOps
}
func (m *tasklessOperationStores) Applies() storage.ApplyStore { return m.applies }
func (m *tasklessOperationStores) Plans() storage.PlanStore    { return m.plans }

// driveWrittenApplyOperationStore returns the operation row as the drive left it
// and records any further write, so a test can assert the drive's outcome is
// read back rather than re-derived.
type driveWrittenApplyOperationStore struct {
	storage.ApplyOperationStore
	row     *storage.ApplyOperation
	written bool
}

func (s *driveWrittenApplyOperationStore) Get(context.Context, int64) (*storage.ApplyOperation, error) {
	return s.row, nil
}

func (s *driveWrittenApplyOperationStore) MarkFailed(context.Context, int64, string) error {
	s.written = true
	return nil
}

func (s *driveWrittenApplyOperationStore) MarkCompleted(context.Context, int64) error {
	s.written = true
	return nil
}

func (s *driveWrittenApplyOperationStore) UpdateState(context.Context, int64, string) error {
	s.written = true
	return nil
}

type stubApplyLookupStore struct {
	storage.ApplyStore
	apply *storage.Apply
}

func (s *stubApplyLookupStore) Get(context.Context, int64) (*storage.Apply, error) {
	return s.apply, nil
}

type stubPlanByIDStore struct {
	storage.PlanStore
	plan *storage.Plan
}

func (s *stubPlanByIDStore) GetByID(context.Context, int64) (*storage.Plan, error) {
	return s.plan, nil
}

// TestMarkOperationFromOwnResult_TasklessVSchemaOnlyKeepsDriveOutcome verifies
// that a task-less VSchema-only work operation is reported as already written
// whenever the drive left it terminal. Such an operation has no task rows to
// derive a state from, so the drive owns its outcome; an operation-lease-only
// drive also leaves the parent apply running, so re-deriving the operation from
// the parent would take the "leave claimable" branch and return updated=false —
// which stops the caller from projecting the parent and strands a failed or
// stopped rollout with its target blocked.
func TestMarkOperationFromOwnResult_TasklessVSchemaOnlyKeepsDriveOutcome(t *testing.T) {
	vschemaOnlyPlan := &storage.Plan{
		ID: 7,
		Namespaces: map[string]*storage.NamespacePlanData{
			"commerce": {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}},
		},
	}

	for _, tc := range []struct {
		name       string
		driveState string
	}{
		{name: "completed", driveState: state.ApplyOperation.Completed},
		{name: "failed", driveState: state.ApplyOperation.Failed},
		{name: "stopped", driveState: state.ApplyOperation.Stopped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := &storage.ApplyOperation{
				ID:            21,
				ApplyID:       3,
				Deployment:    "region-a",
				OperationKind: storage.ApplyOperationKindWork,
			}
			opStore := &driveWrittenApplyOperationStore{
				row: &storage.ApplyOperation{ID: op.ID, ApplyID: op.ApplyID, State: tc.driveState},
			}
			logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
			svc := New(&tasklessOperationStores{
				tasks:    &stubTaskStore{},
				applyOps: opStore,
				// The parent is still running: an operation-lease-only drive never
				// writes it, and the projection this method gates has not run yet.
				applies: &stubApplyLookupStore{apply: &storage.Apply{ID: 3, PlanID: 7, State: state.Apply.Running}},
				plans:   &stubPlanByIDStore{plan: vschemaOnlyPlan},
			}, testServerConfig(), nil, logger)

			marked, err := svc.markOperationFromOwnResult(t.Context(), 1, op)
			require.NoError(t, err)
			assert.True(t, marked,
				"the drive's %s outcome must be reported as written so the caller projects the parent", tc.driveState)
			assert.False(t, opStore.written,
				"the operation row the drive already wrote must not be written again")
		})
	}
}

// TestMarkOperationFromOwnResult_TasklessVSchemaOnlyMirrorsParentWhenDriveLeftItOpen
// verifies the other half: when the drive left a task-less VSchema-only
// operation non-terminal — the retryable-failure contract, where the operator is
// meant to re-drive it — the operation mirrors the parent apply instead, and a
// still-running parent leaves the row claimable for the next poll.
func TestMarkOperationFromOwnResult_TasklessVSchemaOnlyMirrorsParentWhenDriveLeftItOpen(t *testing.T) {
	op := &storage.ApplyOperation{
		ID:            22,
		ApplyID:       4,
		Deployment:    "region-a",
		OperationKind: storage.ApplyOperationKindWork,
	}
	opStore := &driveWrittenApplyOperationStore{
		row: &storage.ApplyOperation{ID: op.ID, ApplyID: op.ApplyID, State: state.ApplyOperation.Running},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&tasklessOperationStores{
		tasks:    &stubTaskStore{},
		applyOps: opStore,
		applies:  &stubApplyLookupStore{apply: &storage.Apply{ID: 4, PlanID: 8, State: state.Apply.Running}},
		plans: &stubPlanByIDStore{plan: &storage.Plan{
			ID: 8,
			Namespaces: map[string]*storage.NamespacePlanData{
				"commerce": {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}},
			},
		}},
	}, testServerConfig(), nil, logger)

	marked, err := svc.markOperationFromOwnResult(t.Context(), 1, op)
	require.NoError(t, err)
	assert.False(t, marked, "a still-running operation must be left claimable for a later poll")
	assert.False(t, opStore.written, "no state write should occur while the operation is still in flight")
}
