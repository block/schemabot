package storagetest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// endedOperationFixtureOp is one operation of an apply built by
// createEndedOperationApply: its shard, the state it is left in, its
// on_failure policy (halt when empty), and the state of its one task.
type endedOperationFixtureOp struct {
	shard     string
	state     string
	onFailure string
	taskState string
}

// createEndedOperationApply creates an apply in applyState, deferring its
// cutover when deferred is set, with one operation and one task per op. It
// returns the apply and each operation's ID keyed by shard.
func createEndedOperationApply(t *testing.T, store storage.Storage, name string, planID int64, applyState string, deferred bool, ops ...endedOperationFixtureOp) (*storage.Apply, map[string]int64) {
	t.Helper()
	ctx := t.Context()
	apply := CreateApplyWithStateAndEnv(t, store, CreateLock(t, store, name+"_db", storage.DatabaseTypeMySQL), name, planID, applyState, "production")
	if deferred {
		apply.Options = []byte(`{"defer_cutover":true}`)
		require.NoError(t, store.Applies().Update(ctx, apply))
	}
	created := time.Now().UTC().Truncate(time.Second)
	ids := make(map[string]int64, len(ops))
	for _, op := range ops {
		id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "region-a",
			OperationKey:  "commerce/" + op.shard + "/users",
			OperationKind: storage.ApplyOperationKindWork,
			OnFailure:     op.onFailure,
		})
		require.NoError(t, err)
		if op.state != state.ApplyOperation.Pending {
			require.NoError(t, store.ApplyOperations().UpdateState(ctx, id, op.state))
		}
		task := newTask(apply, "task_"+name+"_"+op.shard, "users", created)
		task.ApplyOperationID = &id
		task.State = op.taskState
		_, err = store.Tasks().Create(ctx, task)
		require.NoError(t, err)
		ids[op.shard] = id
	}
	return apply, ids
}

// requestControlBy records a pending control request on the apply, made by
// the given caller.
func requestControlBy(t *testing.T, store storage.Storage, applyID int64, operation storage.ControlOperation, requestedBy string) {
	t.Helper()
	_, _, err := store.ControlRequests().RequestPending(t.Context(), &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   operation,
		Status:      storage.ControlRequestPending,
		RequestedBy: requestedBy,
		Metadata:    []byte(`{}`),
	})
	require.NoError(t, err)
}

// spendRetryBudget claims and releases a failed_retryable apply until its
// attempt counter reaches the recovery budget. The parity suite spends it
// through the storage interface because it cannot backdate the counter with
// dialect-specific SQL.
func spendRetryBudget(t *testing.T, store storage.Storage, applyID int64) {
	t.Helper()
	ctx := t.Context()
	for range storage.MaxRecoveryAttempts {
		claimed, err := store.Applies().ClaimApplyByID(ctx, applyID, "test-driver")
		require.NoError(t, err)
		require.NotNil(t, claimed)
		claimed.State = state.Apply.FailedRetryable
		require.NoError(t, store.Applies().Update(storage.WithApplyLease(ctx, claimed.Lease()), claimed))
		released, err := store.Applies().ReleaseClaim(ctx, claimed.Lease())
		require.NoError(t, err)
		require.True(t, released)
	}
}

func endedApplyIdentifiers(applies []*storage.EndedOperationApply) []string {
	identifiers := make([]string, len(applies))
	for i, ended := range applies {
		identifiers[i] = ended.Apply.ApplyIdentifier
	}
	return identifiers
}

// testEndedOperationApplies covers the two sweeps that end an apply deferring
// its cutover once one of its operations ends before the cutover: the list the
// reaper stops, and the settle that fails the apply once nothing is driven.
func testEndedOperationApplies(t *testing.T, h Harness) {
	// The fixtures are MySQL applies, so the list names that type.
	pausableTypes := []string{storage.DatabaseTypeMySQL}

	// A deferred apply whose shard -40 failed under the halt policy while
	// shard 40-80 waits at the cutover can never run the cutover for every
	// shard, so the reaper lists it to stop, naming -40 as the cause.
	t.Run("ListAppliesHeldByEndedOperation_HaltedFailureHoldsWaitingShard", func(t *testing.T) {
		store := h.NewStorage(t)
		apply, ops := createEndedOperationApply(t, store, "apply_ended_halted", 951, state.Apply.RunningDegraded, true,
			endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed},
			endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover},
		)

		listed, err := store.Applies().ListAppliesHeldByEndedOperation(t.Context(), pausableTypes, 10)
		require.NoError(t, err)
		require.Len(t, listed, 1)
		assert.Equal(t, apply.ApplyIdentifier, listed[0].Apply.ApplyIdentifier)
		require.NotNil(t, listed[0].EndedOperation)
		assert.Equal(t, ops["-40"], listed[0].EndedOperation.ID)
		assert.Equal(t, state.ApplyOperation.Failed, listed[0].EndedOperation.State)
	})

	// The list leaves alone every apply where the failure does not end the
	// rollout or where something is already ending it: an apply that does not
	// defer its cutover, a failure under the continue policy, an apply with a
	// stop or cancel pending from anyone, and an apply with nothing left
	// running or waiting.
	t.Run("ListAppliesHeldByEndedOperation_SkipsAppliesNotHeld", func(t *testing.T) {
		store := h.NewStorage(t)
		failed := endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed}
		waiting := endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover}

		createEndedOperationApply(t, store, "apply_ended_not_deferred", 952, state.Apply.RunningDegraded, false, failed, waiting)
		createEndedOperationApply(t, store, "apply_ended_continue", 953, state.Apply.RunningDegraded, true,
			endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.Failed, onFailure: storage.OnFailureContinue, taskState: state.Task.Failed},
			waiting)
		stopped, _ := createEndedOperationApply(t, store, "apply_ended_operator_stop", 954, state.Apply.RunningDegraded, true, failed, waiting)
		requestControlBy(t, store, stopped.ID, storage.ControlOperationStop, "operator-a")
		cancelled, _ := createEndedOperationApply(t, store, "apply_ended_cancel", 955, state.Apply.RunningDegraded, true, failed, waiting)
		requestControlBy(t, store, cancelled.ID, storage.ControlOperationCancel, "operator-a")
		createEndedOperationApply(t, store, "apply_ended_nothing_held", 956, state.Apply.RunningDegraded, true, failed,
			endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.Stopped, taskState: state.Task.Stopped})

		listed, err := store.Applies().ListAppliesHeldByEndedOperation(t.Context(), pausableTypes, 10)
		require.NoError(t, err)
		assert.Empty(t, endedApplyIdentifiers(listed))
	})

	// The list returns only applies of the database types the caller names, so
	// an apply on an engine whose stop would end the work is never stopped.
	t.Run("ListAppliesHeldByEndedOperation_OnlyNamedDatabaseTypes", func(t *testing.T) {
		store := h.NewStorage(t)
		createEndedOperationApply(t, store, "apply_ended_other_type", 964, state.Apply.RunningDegraded, true,
			endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed},
			endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover},
		)

		listed, err := store.Applies().ListAppliesHeldByEndedOperation(t.Context(), []string{storage.DatabaseTypeVitess}, 10)
		require.NoError(t, err)
		assert.Empty(t, endedApplyIdentifiers(listed))
	})

	// A shard waiting on a retry ends the apply only once the retry can never
	// come. With budget left the apply is retrying, not held; once the budget
	// is spent the waiting shard is held behind it and the apply is listed.
	t.Run("ListAppliesHeldByEndedOperation_SpentRetryHoldsWaitingShard", func(t *testing.T) {
		store := h.NewStorage(t)
		apply, ops := createEndedOperationApply(t, store, "apply_ended_spent_retry", 957, state.Apply.FailedRetryable, true,
			endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.FailedRetryable, taskState: state.Task.FailedRetryable},
			endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover},
		)

		listed, err := store.Applies().ListAppliesHeldByEndedOperation(t.Context(), pausableTypes, 10)
		require.NoError(t, err)
		assert.Empty(t, endedApplyIdentifiers(listed), "an apply with retry budget left is not held")

		spendRetryBudget(t, store, apply.ID)
		listed, err = store.Applies().ListAppliesHeldByEndedOperation(t.Context(), pausableTypes, 10)
		require.NoError(t, err)
		require.Len(t, listed, 1)
		assert.Equal(t, apply.ApplyIdentifier, listed[0].Apply.ApplyIdentifier)
		require.NotNil(t, listed[0].EndedOperation)
		assert.Equal(t, ops["-40"], listed[0].EndedOperation.ID)
	})

	// Once SchemaBot's stop has reached every shard, the settle fails the
	// apply. The stopped shard fails naming -40 as the cause and keeps its
	// task stopped, so its copy is saved for the next apply to resume. The
	// shard that never started is cancelled. The settle waits for every shard
	// to be quiet, and never takes an apply an operator stopped, which the
	// operator may start again.
	t.Run("SettleAppliesStoppedForEndedOperation_FailsStoppedShards", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		apply, ops := createEndedOperationApply(t, store, "apply_ended_settle", 958, state.Apply.RunningDegraded, true,
			endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed},
			endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover},
			endedOperationFixtureOp{shard: "80-", state: state.ApplyOperation.Pending, taskState: state.Task.Pending},
		)
		operatorStopped, _ := createEndedOperationApply(t, store, "apply_ended_settle_operator", 959, state.Apply.RunningDegraded, true,
			endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed},
			endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.Stopped, taskState: state.Task.Stopped},
		)
		requestControlBy(t, store, operatorStopped.ID, storage.ControlOperationStop, "operator-a")
		requestControlBy(t, store, apply.ID, storage.ControlOperationStop, storage.EndedOperationStopCaller)

		settled, err := store.Applies().SettleAppliesStoppedForEndedOperation(ctx, 10)
		require.NoError(t, err)
		assert.Empty(t, endedApplyIdentifiers(settled), "a shard still waiting at the cutover keeps the apply unsettled")

		require.NoError(t, store.ApplyOperations().UpdateState(ctx, ops["40-80"], state.ApplyOperation.Stopped))
		waitingTask, err := store.Tasks().Get(ctx, "task_apply_ended_settle_40-80")
		require.NoError(t, err)
		require.NotNil(t, waitingTask)
		waitingTask.State = state.Task.Stopped
		require.NoError(t, store.Tasks().Update(ctx, waitingTask))

		settled, err = store.Applies().SettleAppliesStoppedForEndedOperation(ctx, 10)
		require.NoError(t, err)
		require.Len(t, settled, 1)
		assert.Equal(t, apply.ApplyIdentifier, settled[0].Apply.ApplyIdentifier)
		assert.Equal(t, state.Apply.RunningDegraded, settled[0].SettledFrom)
		assert.Equal(t, state.Apply.Failed, settled[0].Apply.State)
		require.NotNil(t, settled[0].EndedOperation)
		assert.Equal(t, ops["-40"], settled[0].EndedOperation.ID)

		reloaded, err := store.Applies().Get(ctx, apply.ID)
		require.NoError(t, err)
		require.NotNil(t, reloaded)
		assert.Equal(t, state.Apply.Failed, reloaded.State)
		assert.NotNil(t, reloaded.CompletedAt)

		assertOperation := func(shard, wantState string, wantEndedBy int64) {
			t.Helper()
			op, err := store.ApplyOperations().Get(ctx, ops[shard])
			require.NoError(t, err)
			require.NotNil(t, op)
			assert.Equal(t, wantState, op.State, "operation %s", shard)
			assert.Equal(t, wantEndedBy, op.EndedByOperationID, "operation %s", shard)
		}
		assertOperation("-40", state.ApplyOperation.Failed, 0)
		assertOperation("40-80", state.ApplyOperation.Failed, ops["-40"])
		assertOperation("80-", state.ApplyOperation.Cancelled, 0)

		assertTask := func(shard, wantState string) {
			t.Helper()
			task, err := store.Tasks().Get(ctx, "task_apply_ended_settle_"+shard)
			require.NoError(t, err)
			require.NotNil(t, task)
			assert.Equal(t, wantState, task.State, "task of %s", shard)
		}
		assertTask("-40", state.Task.Failed)
		assertTask("40-80", state.Task.Stopped)
		assertTask("80-", state.Task.Cancelled)

		operatorApply, err := store.Applies().Get(ctx, operatorStopped.ID)
		require.NoError(t, err)
		require.NotNil(t, operatorApply)
		assert.Equal(t, state.Apply.RunningDegraded, operatorApply.State, "an operator's stop leaves the apply to the operator")
	})

	// A shard a driver still holds is that drive's to finish, even when its
	// row already reads stopped, so the settle waits for the lease to go.
	t.Run("SettleAppliesStoppedForEndedOperation_WaitsForDrive", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		apply, ops := createEndedOperationApply(t, store, "apply_ended_settle_driven", 960, state.Apply.Running, true,
			endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.Pending, taskState: state.Task.Pending},
			endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.Pending, taskState: state.Task.Pending},
		)
		claim := func(owner string) *storage.ApplyOperation {
			t.Helper()
			claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, owner)
			require.NoError(t, err)
			require.NotNil(t, claimed)
			return claimed
		}
		failedDrive := claim("driver-a")
		require.Equal(t, ops["-40"], failedDrive.ID)
		heldDrive := claim("driver-b")
		require.Equal(t, ops["40-80"], heldDrive.ID)

		// -40 fails and its drive ends; 40-80's drive stops its shard but has
		// not yet released it.
		require.NoError(t, store.ApplyOperations().UpdateState(ctx, ops["-40"], state.ApplyOperation.Failed))
		released, err := store.ApplyOperations().ReleaseClaim(ctx, failedDrive.Lease())
		require.NoError(t, err)
		require.True(t, released)
		apply.State = state.Apply.RunningDegraded
		require.NoError(t, store.Applies().Update(ctx, apply))
		requestControlBy(t, store, apply.ID, storage.ControlOperationStop, storage.EndedOperationStopCaller)
		require.NoError(t, store.ApplyOperations().UpdateState(ctx, ops["40-80"], state.ApplyOperation.Stopped))

		settled, err := store.Applies().SettleAppliesStoppedForEndedOperation(ctx, 10)
		require.NoError(t, err)
		assert.Empty(t, endedApplyIdentifiers(settled), "a held lease keeps the apply unsettled")

		released, err = store.ApplyOperations().ReleaseClaim(ctx, heldDrive.Lease())
		require.NoError(t, err)
		require.True(t, released)
		settled, err = store.Applies().SettleAppliesStoppedForEndedOperation(ctx, 10)
		require.NoError(t, err)
		assert.Equal(t, []string{apply.ApplyIdentifier}, endedApplyIdentifiers(settled))
	})

	// A spent retry settles through the same path. The shard waiting on the
	// retry fails with its task, and the apply reports failed_retryable as the
	// state it settled from.
	t.Run("SettleAppliesStoppedForEndedOperation_SpentRetry", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		apply, ops := createEndedOperationApply(t, store, "apply_ended_settle_retry", 961, state.Apply.FailedRetryable, true,
			endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.FailedRetryable, taskState: state.Task.FailedRetryable},
			endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.Stopped, taskState: state.Task.Stopped},
		)
		spendRetryBudget(t, store, apply.ID)
		requestControlBy(t, store, apply.ID, storage.ControlOperationStop, storage.EndedOperationStopCaller)

		settled, err := store.Applies().SettleAppliesStoppedForEndedOperation(ctx, 10)
		require.NoError(t, err)
		require.Len(t, settled, 1)
		assert.Equal(t, state.Apply.FailedRetryable, settled[0].SettledFrom)
		require.NotNil(t, settled[0].EndedOperation)
		assert.Equal(t, ops["-40"], settled[0].EndedOperation.ID)

		retried, err := store.ApplyOperations().Get(ctx, ops["-40"])
		require.NoError(t, err)
		require.NotNil(t, retried)
		assert.Equal(t, state.ApplyOperation.Failed, retried.State)
		stopped, err := store.ApplyOperations().Get(ctx, ops["40-80"])
		require.NoError(t, err)
		require.NotNil(t, stopped)
		assert.Equal(t, state.ApplyOperation.Failed, stopped.State)
		assert.Equal(t, ops["-40"], stopped.EndedByOperationID)

		retriedTask, err := store.Tasks().Get(ctx, "task_apply_ended_settle_retry_-40")
		require.NoError(t, err)
		require.NotNil(t, retriedTask)
		assert.Equal(t, state.Task.Failed, retriedTask.State)
		stoppedTask, err := store.Tasks().Get(ctx, "task_apply_ended_settle_retry_40-80")
		require.NoError(t, err)
		require.NotNil(t, stoppedTask)
		assert.Equal(t, state.Task.Stopped, stoppedTask.State)
	})

	// Retry expiry leaves an apply carrying SchemaBot's stop to the settle,
	// which keeps the stopped shards' copies saved and names their cause.
	// Expiry still takes an apply in the same state without that stop.
	t.Run("ExpireRetryable_SkipsApplyStoppedForEndedOperation", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		ops := []endedOperationFixtureOp{
			{shard: "-40", state: state.ApplyOperation.FailedRetryable, taskState: state.Task.FailedRetryable},
			{shard: "40-80", state: state.ApplyOperation.Stopped, taskState: state.Task.Stopped},
		}
		held, _ := createEndedOperationApply(t, store, "apply_ended_expiry_held", 962, state.Apply.FailedRetryable, true, ops...)
		spendRetryBudget(t, store, held.ID)
		requestControlBy(t, store, held.ID, storage.ControlOperationStop, storage.EndedOperationStopCaller)
		plain, _ := createEndedOperationApply(t, store, "apply_ended_expiry_plain", 963, state.Apply.FailedRetryable, true, ops...)
		spendRetryBudget(t, store, plain.ID)

		expired, err := store.Applies().ExpireRetryable(ctx, 10)
		require.NoError(t, err)
		expiredIDs := make([]string, len(expired))
		for i, apply := range expired {
			expiredIDs[i] = apply.Apply.ApplyIdentifier
		}
		assert.Equal(t, []string{plain.ApplyIdentifier}, expiredIDs)

		reloaded, err := store.Applies().Get(ctx, held.ID)
		require.NoError(t, err)
		require.NotNil(t, reloaded)
		assert.Equal(t, state.Apply.FailedRetryable, reloaded.State)
	})
}
