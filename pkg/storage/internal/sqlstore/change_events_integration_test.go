//go:build integration

package sqlstore

import (
	"database/sql"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

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
		testChangeEventLifecycle(t, testDB, NewMySQL)
	})
	t.Run("PostgreSQL", func(t *testing.T) {
		_, db := testutil.StartPostgres(t, "change_events")
		applyPostgresTestSchema(t, db)
		testChangeEventLifecycle(t, db, NewPostgres)
	})
}

func testChangeEventLifecycle(t *testing.T, db *sql.DB, newStore func(*sql.DB, ...storage.Option) *Storage) {
	t.Helper()
	ctx := t.Context()
	delivered := make(chan string, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		delivered <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	sender, err := changeevents.New([]changeevents.Destination{{
		URL: server.URL, Environments: []string{"staging"},
		Events: []string{changeevents.OperationStarted, changeevents.OperationFinished, changeevents.ApplyFinished},
		Body:   `{"app":{{json .App}},"description":{{json .Description}}}`,
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
	for _, event := range want {
		select {
		case received := <-delivered:
			description := strconv.Quote(event + " https://github.com/org/repo/pull/123")
			require.JSONEq(t, `{"app":"registered-app","description":`+description+`}`, received)
		case <-deadline.C:
			t.Fatalf("did not receive %s", event)
		}
	}
	audit, err = store.ApplyLogs().GetByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, audit, 2, "only the observed operation finish and the aggregate finish are recorded")

	cancelLock := createTestLock(t, store, "events_cancel_db", "mysql")
	cancelled := createTestApply(t, store, cancelLock, "apply_cancelled", 2)
	cancelled.State = state.Apply.Cancelled
	cancelled.CompletedAt = &now
	require.NoError(t, store.Applies().Update(ctx, cancelled))
	require.NoError(t, store.Applies().Update(ctx, cancelled))
	select {
	case received := <-delivered:
		require.JSONEq(t, `{"app":"registered-app","description":"apply.finished apply=apply_cancelled outcome=cancelled https://github.com/org/repo/pull/123"}`, received)
	case <-deadline.C:
		t.Fatal("cancelling before dispatch did not publish an aggregate outcome")
	}
	audit, err = store.ApplyLogs().GetByApply(ctx, cancelled.ID)
	require.NoError(t, err)
	require.Len(t, audit, 1)

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
		require.JSONEq(t, `{"app":"events-app","description":`+description+`}`, received)
	case <-deadline.C:
		t.Fatal("a recovered worker that reached the engine did not report its finish")
	}
	audit, err = store.ApplyLogs().GetByApply(ctx, remote.ID)
	require.NoError(t, err)
	require.Len(t, audit, 1, "the worker records its operation finish only")
	require.Equal(t, changeevents.OperationFinished, audit[0].EventType)
}
