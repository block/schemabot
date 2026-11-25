//go:build integration

package sqlstore

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/changeevents"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/testutil"
)

const changeEventTestDeadline = 5 * time.Second

// A worker reports its own IDs across retries and takeovers. A worker that
// reaches the engine can report its committed outcome. Reconciliation alone
// leaves operation finish reporting to the aggregate apply outcome.
func TestChangeEventLifecycle(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		clearTables(t)
		db, err := sql.Open("block-mysql", testDSNChangedRows)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		db.SetMaxOpenConns(1)
		testChangeEventLifecycle(t, db, NewMySQL)
	})
	t.Run("PostgreSQL", func(t *testing.T) {
		_, db := testutil.StartPostgres(t, "change_events")
		applyPostgresTestSchema(t, db)
		testChangeEventLifecycle(t, db, NewPostgres)
	})
}

type renderedChangeEvent struct {
	Summary json.RawMessage
	Details changeevents.EventDetails
}

func testChangeEventLifecycle(t *testing.T, db *sql.DB, newStore func(*sql.DB, ...storage.Option) *Storage) {
	t.Helper()
	ctx := t.Context()
	delivered := make(chan renderedChangeEvent, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var body renderedChangeEvent
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		delivered <- body
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	sender, err := changeevents.New([]changeevents.Destination{{
		URL: server.URL, Environments: []string{"staging"},
		Events: []string{changeevents.OperationStarted, changeevents.OperationFinished, changeevents.ApplyFinished},
		Body:   `{"summary":{"app":{{json .App}},"description":{{json .Description}}},"details":{{json .Details}}}`,
	}}, slog.Default())
	require.NoError(t, err)
	defer sender.Close()

	option := storage.WithChangeEvents(sender, func(database string) string { return "registered-app" }, slog.Default())
	store := newStore(db, option)
	lock := createTestLock(t, store, "events_db", "mysql")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_control", 1, state.Apply.Running, "staging")
	op := &storage.ApplyOperation{ApplyID: apply.ID, Deployment: "local-engine"}
	opID, err := store.ApplyOperations().Insert(ctx, op)
	require.NoError(t, err)
	unstartedID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{ApplyID: apply.ID, Deployment: "never-started"})
	require.NoError(t, err)
	require.NoError(t, store.ApplyOperations().MarkTerminal(ctx, unstartedID, state.ApplyOperation.Cancelled))

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "first")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, opID, claimed.ID)
	first := storage.WithOperationChangeAttempt(storage.WithOperationLease(ctx, claimed.Lease()), claimed)
	store.ObserveOperationDispatch(first, apply)
	store.ObserveOperationDispatch(first, apply)
	audit, err := store.ApplyLogs().GetByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Empty(t, audit, "dispatch and operation.started require no audit records")

	restartedStore := newStore(db, option)
	_, err = store.db.ExecContext(ctx, `UPDATE apply_operations SET updated_at = ? WHERE id = ?`,
		time.Now().Add(-2*storage.ApplyLeaseStaleAfter), opID)
	require.NoError(t, err)
	recovered, err := restartedStore.ApplyOperations().FindNextApplyOperation(ctx, "recovered")
	require.NoError(t, err)
	require.NotNil(t, recovered)
	require.Equal(t, opID, recovered.ID)
	require.False(t, recovered.StartsAttempt)
	reattached := storage.WithOperationChangeAttempt(storage.WithOperationLease(ctx, recovered.Lease()), recovered)
	require.ErrorIs(t, restartedStore.ApplyOperations().MarkFailed(first, opID, "refused write"), storage.ErrApplyLeaseLost)
	require.NoError(t, restartedStore.ApplyOperations().MarkFailedRetryable(reattached, opID, "retry after engine failure"))
	require.NoError(t, restartedStore.ApplyOperations().MarkFailedRetryable(reattached, opID, "retry after engine failure"))

	apply.State = state.Apply.FailedRetryable
	require.NoError(t, store.Applies().Update(ctx, apply))
	retry, err := restartedStore.ApplyOperations().FindNextApplyOperation(ctx, "retry")
	require.NoError(t, err)
	require.NotNil(t, retry)
	require.Equal(t, opID, retry.ID)
	require.Equal(t, 1, retry.Attempt)
	retrying := storage.WithOperationChangeAttempt(storage.WithOperationLease(ctx, retry.Lease()), retry)
	restartedStore.ObserveOperationDispatch(retrying, apply)
	require.NoError(t, restartedStore.ApplyOperations().MarkCompleted(retrying, opID))
	require.NoError(t, restartedStore.ApplyOperations().MarkCompleted(retrying, opID))

	now := time.Now()
	swapped, err := store.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.FailedRetryable, state.Apply.Completed, "", nil, &now)
	require.NoError(t, err)
	require.True(t, swapped)
	swapped, err = store.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.FailedRetryable, state.Apply.Failed, "", nil, &now)
	require.NoError(t, err)
	require.False(t, swapped)

	opIdentity := " apply=apply_control operation=" + strconv.FormatInt(opID, 10) + " deployment=local-engine"
	want := []string{
		"operation.started" + opIdentity + " attempt=1",
		"operation.started" + opIdentity + " attempt=2",
		"operation.finished" + opIdentity + " attempt=2 outcome=completed",
		"apply.finished apply=apply_control outcome=completed deployments=local-engine(completed:1),never-started(cancelled:1)",
	}
	deadline := time.NewTimer(changeEventTestDeadline)
	defer deadline.Stop()
	var snapshots []changeevents.EventDetails
	for _, event := range want {
		select {
		case received := <-delivered:
			snapshots = append(snapshots, received.Details)
			description := strconv.Quote(event + " https://github.com/org/repo/pull/123")
			require.JSONEq(t, `{"app":"registered-app","description":`+description+`}`, string(received.Summary))
		case <-deadline.C:
			t.Fatalf("did not receive %s", event)
		}
	}
	seen := make(map[string]bool)
	for i, outcome := range []string{"", "", "completed", "completed"} {
		details := snapshots[i]
		require.NoError(t, uuid.Validate(details.EventID))
		require.False(t, seen[details.EventID])
		seen[details.EventID] = true
		require.Equal(t, apply.ApplyIdentifier, details.ApplyID)
		require.Equal(t, apply.Database, details.Database)
		require.Equal(t, apply.DatabaseType, details.DatabaseType)
		require.Equal(t, apply.Repository, details.Repository)
		require.Equal(t, apply.PullRequest, details.PullRequest)
		require.Equal(t, outcome, details.Outcome)
		if i < 3 {
			require.Equal(t, opID, details.OperationID)
			require.Equal(t, "local-engine", details.Deployment)
			require.Equal(t, []int{1, 2, 2}[i], details.Attempt)
			require.True(t, details.CompletedAt.IsZero())
		} else {
			require.Zero(t, details.OperationID)
			saved, err := store.Applies().Get(ctx, apply.ID)
			require.NoError(t, err)
			require.NotNil(t, saved.CompletedAt)
			require.True(t, saved.CompletedAt.Equal(details.CompletedAt))
		}
	}
	audit, err = store.ApplyLogs().GetByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Empty(t, audit, "change events use no audit records")

	cancelLock := createTestLock(t, store, "events_cancel_db", "mysql")
	cancelled := createTestApply(t, store, cancelLock, "apply_cancelled", 2)
	cancelled.State = state.Apply.Cancelled
	cancelled.CompletedAt = &now
	require.NoError(t, store.Applies().Update(ctx, cancelled))
	require.NoError(t, store.Applies().Update(ctx, cancelled))
	select {
	case received := <-delivered:
		require.JSONEq(t, `{"app":"registered-app","description":"apply.finished apply=apply_cancelled outcome=cancelled https://github.com/org/repo/pull/123"}`, string(received.Summary))
	case <-deadline.C:
		t.Fatal("cancelling before dispatch did not publish an aggregate outcome")
	}
	audit, err = store.ApplyLogs().GetByApply(ctx, cancelled.ID)
	require.NoError(t, err)
	require.Empty(t, audit)

	remoteLock := createTestLock(t, store, "events_remote_db", "mysql")
	remote := &storage.Apply{
		ApplyIdentifier: "apply_remote_copy", LockID: remoteLock.ID, PlanID: 3,
		Database: remoteLock.DatabaseName, DatabaseType: remoteLock.DatabaseType, Environment: "staging",
		Engine: storage.EngineForType(remoteLock.DatabaseType), State: state.Apply.Running,
		IdempotencyKey: "schemabot:v1:remote-dispatch",
	}
	remote.SetOptions(storage.ApplyOptions{ChangeEventApp: "events-app"})
	remote.ID, err = store.Applies().Create(ctx, remote)
	require.NoError(t, err)
	remoteOpID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{ApplyID: remote.ID, Deployment: "remote-engine"})
	require.NoError(t, err)
	remoteClaim, err := store.ApplyOperations().FindNextApplyOperation(ctx, "remote-first")
	require.NoError(t, err)
	require.NotNil(t, remoteClaim)
	require.Equal(t, remoteOpID, remoteClaim.ID)
	_, err = store.db.ExecContext(ctx, `UPDATE apply_operations SET updated_at = ? WHERE id = ?`,
		time.Now().Add(-2*storage.ApplyLeaseStaleAfter), remoteOpID)
	require.NoError(t, err)
	remoteRecovered, err := restartedStore.ApplyOperations().FindNextApplyOperation(ctx, "remote-recovered")
	require.NoError(t, err)
	require.NotNil(t, remoteRecovered)
	require.Equal(t, remoteOpID, remoteRecovered.ID)
	require.False(t, remoteRecovered.StartsAttempt)
	remoteDrive := storage.WithOperationChangeAttempt(storage.WithOperationLease(ctx, remoteRecovered.Lease()), remoteRecovered)
	restartedStore.ObserveOperationDispatch(remoteDrive, remote)
	require.NoError(t, restartedStore.ApplyOperations().MarkCompleted(remoteDrive, remoteOpID))
	remote.State = state.Apply.Completed
	remote.CompletedAt = &now
	require.NoError(t, store.Applies().Update(ctx, remote))

	select {
	case received := <-delivered:
		description := strconv.Quote("operation.finished apply=apply_remote_copy operation=" + strconv.FormatInt(remoteOpID, 10) + " deployment=remote-engine attempt=1 outcome=completed")
		require.JSONEq(t, `{"app":"events-app","description":`+description+`}`, string(received.Summary))
	case <-deadline.C:
		t.Fatal("a recovered worker that reached the engine did not report its finish")
	}
	audit, err = store.ApplyLogs().GetByApply(ctx, remote.ID)
	require.NoError(t, err)
	require.Empty(t, audit)

	testRetryAndRolloutFinishEvents(t, store, delivered)
}

// A parent-lease retry can record the same failed_retryable state even after
// the next attempt has reached the engine. A rollout can likewise settle while
// its parent already says failed, but only a winning state swap can report it.
func testRetryAndRolloutFinishEvents(t *testing.T, store *Storage, delivered <-chan renderedChangeEvent) {
	t.Helper()
	ctx := t.Context()
	now := time.Now()
	retryLock := createTestLock(t, store, "events_retry_db", "mysql")
	apply := createTestApplyWithStateAndEnv(t, store, retryLock, "apply_retry", 4, state.Apply.FailedRetryable, "staging")
	opID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "local", State: state.ApplyOperation.FailedRetryable, StartedAt: &now,
		ErrorMessage: "retryable engine error",
	})
	require.NoError(t, err)
	_, err = store.Tasks().Create(ctx, &storage.Task{
		TaskIdentifier: "retry_task", ApplyID: apply.ID, ApplyOperationID: &opID, PlanID: apply.PlanID,
		Database: apply.Database, DatabaseType: apply.DatabaseType, Environment: apply.Environment, Engine: apply.Engine,
		State: state.Task.FailedRetryable, CreatedAt: now, UpdatedAt: now,
	})
	require.NoError(t, err)
	claim, err := store.ApplyOperations().FindNextApplyOperation(ctx, "retry-same-state")
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.Equal(t, opID, claim.ID)
	require.Equal(t, 1, claim.Attempt)
	require.True(t, claim.StartsAttempt)
	require.Equal(t, state.ApplyOperation.FailedRetryable, claim.State)
	parent, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "retry-same-state")
	require.NoError(t, err)
	require.NotNil(t, parent)
	drive := storage.WithOperationChangeAttempt(storage.WithOperationLease(ctx, claim.Lease()), claim)
	store.ObserveOperationDispatch(drive, apply)
	if _, mysql := store.applyOperations.dialect.(MySQLDialect); mysql {
		// Match the stored heartbeat so the first outcome write changes zero
		// rows under MySQL's changed-rows behavior.
		_, err := store.db.ExecContext(ctx, "SET timestamp = 1700000000")
		require.NoError(t, err)
		_, err = store.db.ExecContext(ctx, "UPDATE apply_operations SET updated_at = NOW() WHERE id = ?", opID)
		require.NoError(t, err)
	}
	require.NoError(t, store.ApplyOperations().MarkFailedRetryable(drive, opID, "retryable engine error"))
	if _, mysql := store.applyOperations.dialect.(MySQLDialect); mysql {
		_, err := store.db.ExecContext(ctx, "SET timestamp = DEFAULT")
		require.NoError(t, err)
	}
	require.NoError(t, store.ApplyOperations().MarkFailedRetryable(drive, opID, "repeat after outcome"))

	_, err = store.db.ExecContext(ctx, `UPDATE apply_operations SET updated_at = ? WHERE id = ?`,
		now.Add(-2*storage.ApplyLeaseStaleAfter), opID)
	require.NoError(t, err)
	_, err = store.db.ExecContext(ctx, `UPDATE applies SET updated_at = ? WHERE id = ?`,
		now.Add(-2*storage.ApplyLeaseStaleAfter), apply.ID)
	require.NoError(t, err)
	recovered, err := store.ApplyOperations().FindNextApplyOperation(ctx, "recover-same-state")
	require.NoError(t, err)
	require.NotNil(t, recovered)
	require.Equal(t, opID, recovered.ID)
	require.Equal(t, claim.Attempt, recovered.Attempt)
	require.False(t, recovered.StartsAttempt)
	recovery := storage.WithOperationChangeAttempt(storage.WithOperationLease(ctx, recovered.Lease()), recovered)
	store.ObserveOperationDispatch(recovery, apply)
	require.NoError(t, store.ApplyOperations().MarkFailedRetryable(recovery, opID, "retryable engine error"))

	for i, reopen := range []bool{false, true} {
		identifier := "apply_rollout_" + strconv.Itoa(i)
		lock := createTestLock(t, store, identifier, "mysql")
		rollout := createTestApplyWithStateAndEnv(t, store, lock, identifier, int64(5+i), state.Apply.Running, "staging")
		for _, op := range []*storage.ApplyOperation{
			{ApplyID: rollout.ID, Deployment: "failed", State: state.ApplyOperation.Failed},
			{ApplyID: rollout.ID, Deployment: "started", State: state.ApplyOperation.Running, StartedAt: &now},
			{ApplyID: rollout.ID, Deployment: "queued", State: state.ApplyOperation.Pending},
		} {
			_, err = store.ApplyOperations().Insert(ctx, op)
			require.NoError(t, err)
		}
		ops, err := store.ApplyOperations().ListByApply(ctx, rollout.ID)
		require.NoError(t, err)
		rollout.State = state.Apply.Failed
		require.NoError(t, store.Applies().Update(ctx, rollout))
		from := state.Apply.Failed
		if reopen {
			swapped, err := store.Applies().UpdateDerivedState(ctx, rollout.ID, from, state.Apply.RunningDegraded, "", nil, nil)
			require.NoError(t, err)
			require.True(t, swapped)
			from = state.Apply.RunningDegraded
		}
		require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, ops[1].ID))
		swapped, err := store.Applies().UpdateDerivedState(ctx, rollout.ID, from, state.Apply.Failed, "", nil, &now)
		require.NoError(t, err)
		require.True(t, swapped)
	}

	identity := " apply=apply_retry operation=" + strconv.FormatInt(opID, 10) + " deployment=local attempt=2"
	deadline := time.NewTimer(changeEventTestDeadline)
	defer deadline.Stop()
	for _, want := range []string{
		"operation.started" + identity,
		"operation.finished" + identity + " outcome=failed_retryable",
		"apply.finished apply=apply_rollout_1 outcome=failed deployments=failed(failed:1),queued(pending:1),started(completed:1)",
	} {
		select {
		case received := <-delivered:
			description := strconv.Quote(want + " https://github.com/org/repo/pull/123")
			require.JSONEq(t, `{"app":"registered-app","description":`+description+`}`, string(received.Summary))
		case <-deadline.C:
			t.Fatalf("did not receive %s", want)
		}
	}
}
