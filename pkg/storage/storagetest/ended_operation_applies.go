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
// createEndedOperationApply: its rollout target (empty for the apply's one
// target), its shard, the state it is left in, its on_failure policy (halt
// when empty), and the state of its one task. A claimed operation is claimed
// by a driver before it is moved to its state, as a started shard is, and a
// driven one keeps that driver's lease afterwards.
type endedOperationFixtureOp struct {
	target    string
	shard     string
	state     string
	onFailure string
	taskState string
	claimed   bool
	driven    bool
}

// key names the operation in the IDs createEndedOperationApply returns: its
// shard, prefixed by its target when it has one.
func (op endedOperationFixtureOp) key() string {
	if op.target == "" {
		return op.shard
	}
	return op.target + "/" + op.shard
}

// createEndedOperationApply creates an apply in applyState, deferring its
// cutover when deferred is set, with one operation and one task per op. It
// returns the apply and each operation's ID keyed by op.key.
//
// Every operation is inserted pending, the claimed ones are claimed in order,
// and only then is each moved to its state, so a failure the fixture records
// does not hold back the claim of a shard that started beside it. The claimed
// operations must come first, and the apply's fan-out cap bounds how many
// there can be.
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
			ApplyID: apply.ID, Deployment: "region-a", Target: op.target,
			OperationKey:  "commerce/" + op.key() + "/users",
			OperationKind: storage.ApplyOperationKindWork,
			OnFailure:     op.onFailure,
			CutoverPolicy: storage.CutoverPolicyParallel,
		})
		require.NoError(t, err)
		task := newTask(apply, "task_"+name+"_"+op.key(), "users", created)
		task.ApplyOperationID = &id
		task.State = op.taskState
		_, err = store.Tasks().Create(ctx, task)
		require.NoError(t, err)
		ids[op.key()] = id
	}
	leases := make(map[string]storage.OperationLease, len(ops))
	for i, op := range ops {
		if !op.claimed && !op.driven {
			continue
		}
		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-"+op.key())
		require.NoError(t, err)
		require.NotNil(t, claimed, "claim of operation %s", op.key())
		require.Equal(t, ids[op.key()], claimed.ID, "operation %d of the fixture is claimed in order", i)
		leases[op.key()] = claimed.Lease()
	}
	for _, op := range ops {
		if op.state != state.ApplyOperation.Pending {
			require.NoError(t, store.ApplyOperations().UpdateState(ctx, ids[op.key()], op.state))
		}
		lease, claimed := leases[op.key()]
		if claimed && !op.driven {
			released, err := store.ApplyOperations().ReleaseClaim(ctx, lease)
			require.NoError(t, err)
			require.True(t, released, "release claim of operation %s", op.key())
		}
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

// spendRetryBudget spends the retry budget of each given apply's one
// failed_retryable operation. Each round redispatches every one of those
// operations, which spends one attempt of its budget and raises its apply's
// attempt to it, then fails each retryably again and hands its lease back, as
// a drive whose retry failed does, and ages the failure past the redispatch
// backoff, as the next retry would after waiting it out. The applies share
// each round because a deferred apply's waiting operation holds back the
// redispatch of a later deferred apply's until it is claimed. The parity suite
// spends the budget through the storage interface because it cannot backdate
// the counter with dialect-specific SQL.
func spendRetryBudget(t *testing.T, h Harness, store storage.Storage, applyIDs ...int64) {
	t.Helper()
	ctx := t.Context()
	for range storage.MaxRecoveryAttempts {
		claims := make([]*storage.ApplyOperation, 0, len(applyIDs))
		claimedApplies := make([]int64, 0, len(applyIDs))
		for range applyIDs {
			claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "retry-driver")
			require.NoError(t, err)
			require.NotNil(t, claimed, "the waiting operation is redispatched")
			claims = append(claims, claimed)
			claimedApplies = append(claimedApplies, claimed.ApplyID)
		}
		require.ElementsMatch(t, applyIDs, claimedApplies, "each apply's waiting operation is redispatched once a round")
		for _, claimed := range claims {
			require.NoError(t, store.ApplyOperations().MarkFailedRetryable(
				storage.WithOperationLease(ctx, claimed.Lease()), claimed.ID, "data plane unavailable"))
			released, err := store.ApplyOperations().ReleaseFinishedClaim(ctx, claimed.Lease())
			require.NoError(t, err)
			require.True(t, released)
			h.AgeApplyOperation(t, store, claimed.ID, storage.MaxRecoveryBackoff+time.Second)
		}
	}
}

func endedApplyIdentifiers(applies []*storage.EndedOperationApply) []string {
	identifiers := make([]string, len(applies))
	for i, ended := range applies {
		identifiers[i] = ended.Apply.ApplyIdentifier
	}
	return identifiers
}

// endedOperationStopTypes are the database types the ended operation sweep
// stops in these tests. The fixtures are MySQL applies, so it names that type,
// and retry expiry is passed the same list, as the reaper passes both.
var endedOperationStopTypes = []string{storage.DatabaseTypeMySQL}

// testEndedOperationApplies covers the two sweeps that end an apply deferring
// its cutover once one of its operations ends before the cutover: the list the
// reaper stops, and the settle that fails the apply once nothing is driven.
func testEndedOperationApplies(t *testing.T, h Harness) {
	pausableTypes := endedOperationStopTypes

	// A deferred apply whose shard -40 failed under the halt policy while
	// shard 40-80 waits at the cutover can never run the cutover for every
	// shard, so the reaper lists it to stop, naming -40 as the cause.
	t.Run("ListAppliesHeldByEndedOperation_HaltedFailureHoldsWaitingShard", func(t *testing.T) {
		store := h.NewStorage(t)
		apply, ops := createEndedOperationApply(t, store, "apply_ended_halted", 951, state.Apply.RunningDegraded, true,
			endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed, claimed: true},
			endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover, driven: true},
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
		failed := endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed, claimed: true}
		waiting := endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover, driven: true}

		createEndedOperationApply(t, store, "apply_ended_not_deferred", 952, state.Apply.RunningDegraded, false, failed, waiting)
		createEndedOperationApply(t, store, "apply_ended_continue", 953, state.Apply.RunningDegraded, true,
			endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.Failed, onFailure: storage.OnFailureContinue, taskState: state.Task.Failed, claimed: true},
			waiting)
		stopped, _ := createEndedOperationApply(t, store, "apply_ended_operator_stop", 954, state.Apply.RunningDegraded, true, failed, waiting)
		cancelled, _ := createEndedOperationApply(t, store, "apply_ended_cancel", 955, state.Apply.RunningDegraded, true, failed, waiting)
		createEndedOperationApply(t, store, "apply_ended_nothing_held", 956, state.Apply.RunningDegraded, true, failed,
			endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.Stopped, taskState: state.Task.Stopped})
		requestControlBy(t, store, stopped.ID, storage.ControlOperationStop, "operator-a")
		requestControlBy(t, store, cancelled.ID, storage.ControlOperationCancel, "operator-a")

		listed, err := store.Applies().ListAppliesHeldByEndedOperation(t.Context(), pausableTypes, 10)
		require.NoError(t, err)
		assert.Empty(t, endedApplyIdentifiers(listed))
	})

	// The list returns only applies of the database types the caller names, so
	// an apply on an engine whose stop would end the work is never stopped.
	t.Run("ListAppliesHeldByEndedOperation_OnlyNamedDatabaseTypes", func(t *testing.T) {
		store := h.NewStorage(t)
		createEndedOperationApply(t, store, "apply_ended_other_type", 964, state.Apply.RunningDegraded, true,
			endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed, claimed: true},
			endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover, driven: true},
		)

		listed, err := store.Applies().ListAppliesHeldByEndedOperation(t.Context(), []string{storage.DatabaseTypeVitess}, 10)
		require.NoError(t, err)
		assert.Empty(t, endedApplyIdentifiers(listed))
	})

	// A shard waiting on a retry ends the apply only once the retry can never
	// come. With budget left the apply is retrying, not held; once the budget
	// is spent the waiting shard is held behind it and the apply is listed.
	// Retry expiry leaves the same apply alone while the waiting shard's drive
	// holds it, so the two sweeps never both take it.
	t.Run("ListAppliesHeldByEndedOperation_SpentRetryHoldsWaitingShard", func(t *testing.T) {
		store := h.NewStorage(t)
		apply, ops := createEndedOperationApply(t, store, "apply_ended_spent_retry", 957, state.Apply.FailedRetryable, true,
			endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.FailedRetryable, taskState: state.Task.FailedRetryable, claimed: true},
			endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover, driven: true},
		)

		listed, err := store.Applies().ListAppliesHeldByEndedOperation(t.Context(), pausableTypes, 10)
		require.NoError(t, err)
		assert.Empty(t, endedApplyIdentifiers(listed), "an apply with retry budget left is not held")

		spendRetryBudget(t, h, store, apply.ID)
		listed, err = store.Applies().ListAppliesHeldByEndedOperation(t.Context(), pausableTypes, 10)
		require.NoError(t, err)
		require.Len(t, listed, 1)
		assert.Equal(t, apply.ApplyIdentifier, listed[0].Apply.ApplyIdentifier)
		require.NotNil(t, listed[0].EndedOperation)
		assert.Equal(t, ops["-40"], listed[0].EndedOperation.ID)

		expired, err := store.Applies().ExpireRetryable(t.Context(), endedOperationStopTypes, 10)
		require.NoError(t, err)
		assert.Empty(t, expired, "retry expiry leaves an apply a drive still holds")
	})

	// The cutover runs for one rollout member at a time. When target a's shard
	// fails under halt while target b's shard waits at the cutover, target b
	// waits on the rollout's barrier, not on target a's cutover, so the apply
	// is left for the operator to decide. The same failure beside a waiting
	// shard of its own target is listed.
	t.Run("ListAppliesHeldByEndedOperation_SkipsOtherMemberAtBarrier", func(t *testing.T) {
		store := h.NewStorage(t)
		createEndedOperationApply(t, store, "apply_ended_other_member", 965, state.Apply.RunningDegraded, true,
			endedOperationFixtureOp{target: "a", shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed, claimed: true},
			endedOperationFixtureOp{target: "b", shard: "-40", state: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover, driven: true},
		)
		sameMember, _ := createEndedOperationApply(t, store, "apply_ended_same_member", 966, state.Apply.RunningDegraded, true,
			endedOperationFixtureOp{target: "a", shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed, claimed: true},
			endedOperationFixtureOp{target: "a", shard: "40-80", state: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover, driven: true},
		)

		listed, err := store.Applies().ListAppliesHeldByEndedOperation(t.Context(), pausableTypes, 10)
		require.NoError(t, err)
		assert.Equal(t, []string{sameMember.ApplyIdentifier}, endedApplyIdentifiers(listed))
	})

	// The drive holding a copying shard is what delivers the stop and saves
	// the copy, so a copying shard nobody drives is left to recovery, which
	// claims it again. A shard parked at the cutover is listed with or without
	// a live drive: once SchemaBot's stop is recorded, the claim of a parked
	// shard takes it, so a drive delivers the stop even after the server that
	// drove it restarted.
	t.Run("ListAppliesHeldByEndedOperation_ParkedShardNeedsNoLiveDrive", func(t *testing.T) {
		store := h.NewStorage(t)
		parked, _ := createEndedOperationApply(t, store, "apply_ended_undriven_parked", 968, state.Apply.RunningDegraded, true,
			endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed, claimed: true},
			endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover, claimed: true},
		)
		driven, _ := createEndedOperationApply(t, store, "apply_ended_driven", 969, state.Apply.RunningDegraded, true,
			endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed, claimed: true},
			endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover, driven: true},
		)
		// Created last: its released copying shard is claimable, so a fixture
		// built after it would claim that shard instead of its own.
		createEndedOperationApply(t, store, "apply_ended_undriven_copy", 967, state.Apply.RunningDegraded, true,
			endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed, claimed: true},
			endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.Running, taskState: state.Task.Running, claimed: true},
		)

		listed, err := store.Applies().ListAppliesHeldByEndedOperation(t.Context(), pausableTypes, 10)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{parked.ApplyIdentifier, driven.ApplyIdentifier}, endedApplyIdentifiers(listed))
	})

	// The parked shard that nobody drives is claimed once SchemaBot's stop is
	// pending, which is how the stop reaches it.
	t.Run("ListAppliesHeldByEndedOperation_UndrivenParkedShardIsClaimedForTheStop", func(t *testing.T) {
		store := h.NewStorage(t)
		parked, parkedOps := createEndedOperationApply(t, store, "apply_ended_undriven_parked", 968, state.Apply.RunningDegraded, true,
			endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed, claimed: true},
			endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover, claimed: true},
		)
		claimed, err := store.ApplyOperations().FindNextApplyOperation(t.Context(), "driver-recovery")
		require.NoError(t, err)
		require.Nil(t, claimed, "a parked shard with a sibling is not claimed before a stop is pending")

		requestControlBy(t, store, parked.ID, storage.ControlOperationStop, storage.EndedOperationStopCaller)
		claimed, err = store.ApplyOperations().FindNextApplyOperation(t.Context(), "driver-recovery")
		require.NoError(t, err)
		require.NotNil(t, claimed, "the parked shard is claimed once the stop is pending")
		assert.Equal(t, parkedOps["40-80"], claimed.ID)
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
		spendRetryBudget(t, h, store, apply.ID)
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

	// A cancel an operator asked for still has engine work to cancel, so the
	// settle leaves the apply to it, even with SchemaBot's stop pending too.
	t.Run("SettleAppliesStoppedForEndedOperation_LeavesApplyToPendingCancel", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		ops := []endedOperationFixtureOp{
			{shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed},
			{shard: "40-80", state: state.ApplyOperation.Stopped, taskState: state.Task.Stopped},
		}
		cancelled, _ := createEndedOperationApply(t, store, "apply_ended_settle_cancel", 969, state.Apply.RunningDegraded, true, ops...)
		requestControlBy(t, store, cancelled.ID, storage.ControlOperationStop, storage.EndedOperationStopCaller)
		requestControlBy(t, store, cancelled.ID, storage.ControlOperationCancel, "operator-a")
		plain, _ := createEndedOperationApply(t, store, "apply_ended_settle_no_cancel", 970, state.Apply.RunningDegraded, true, ops...)
		requestControlBy(t, store, plain.ID, storage.ControlOperationStop, storage.EndedOperationStopCaller)

		settled, err := store.Applies().SettleAppliesStoppedForEndedOperation(ctx, 10)
		require.NoError(t, err)
		assert.Equal(t, []string{plain.ApplyIdentifier}, endedApplyIdentifiers(settled))

		reloaded, err := store.Applies().Get(ctx, cancelled.ID)
		require.NoError(t, err)
		require.NotNil(t, reloaded)
		assert.Equal(t, state.Apply.RunningDegraded, reloaded.State)
	})

	// A pending shard may already have engine work under way: a task past
	// pending, or an engine handle saved for it. The stop has yet to reach that
	// work, so the settle leaves the apply to stop reconciliation rather than
	// cancel the shard over it.
	t.Run("SettleAppliesStoppedForEndedOperation_LeavesPendingShardThatMayHaveStarted", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		failed := endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed}
		stopped := endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.Stopped, taskState: state.Task.Stopped}

		runningTask, _ := createEndedOperationApply(t, store, "apply_ended_settle_running_task", 971, state.Apply.RunningDegraded, true,
			failed, stopped, endedOperationFixtureOp{shard: "80-", state: state.ApplyOperation.Pending, taskState: state.Task.Running})
		requestControlBy(t, store, runningTask.ID, storage.ControlOperationStop, storage.EndedOperationStopCaller)

		handled, handledOps := createEndedOperationApply(t, store, "apply_ended_settle_handle", 972, state.Apply.RunningDegraded, true,
			failed, stopped, endedOperationFixtureOp{shard: "80-", state: state.ApplyOperation.Pending, taskState: state.Task.Pending})
		require.NoError(t, store.ApplyOperations().SaveExternalID(ctx, handled.ID, handledOps["80-"], "remote-apply-1", "remote-operation-1"))
		requestControlBy(t, store, handled.ID, storage.ControlOperationStop, storage.EndedOperationStopCaller)

		untouched, _ := createEndedOperationApply(t, store, "apply_ended_settle_untouched", 973, state.Apply.RunningDegraded, true,
			failed, stopped, endedOperationFixtureOp{shard: "80-", state: state.ApplyOperation.Pending, taskState: state.Task.Pending})
		requestControlBy(t, store, untouched.ID, storage.ControlOperationStop, storage.EndedOperationStopCaller)

		settled, err := store.Applies().SettleAppliesStoppedForEndedOperation(ctx, 10)
		require.NoError(t, err)
		assert.Equal(t, []string{untouched.ApplyIdentifier}, endedApplyIdentifiers(settled))
	})

	// Stop delivery moves a shard nothing claimed from pending to stopped. It
	// never started, so the settle cancels it, with its task, rather than
	// report a failure for work it never did. The shard that copied and was
	// stopped at the cutover still fails naming -40.
	t.Run("SettleAppliesStoppedForEndedOperation_CancelsStoppedShardThatNeverStarted", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		apply, ops := createEndedOperationApply(t, store, "apply_ended_settle_unstarted", 974, state.Apply.RunningDegraded, true,
			endedOperationFixtureOp{shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed, claimed: true},
			endedOperationFixtureOp{shard: "40-80", state: state.ApplyOperation.Stopped, taskState: state.Task.Stopped, claimed: true},
			endedOperationFixtureOp{shard: "80-", state: state.ApplyOperation.Stopped, taskState: state.Task.Pending},
		)
		requestControlBy(t, store, apply.ID, storage.ControlOperationStop, storage.EndedOperationStopCaller)

		settled, err := store.Applies().SettleAppliesStoppedForEndedOperation(ctx, 10)
		require.NoError(t, err)
		assert.Equal(t, []string{apply.ApplyIdentifier}, endedApplyIdentifiers(settled))

		copied, err := store.ApplyOperations().Get(ctx, ops["40-80"])
		require.NoError(t, err)
		require.NotNil(t, copied)
		assert.Equal(t, state.ApplyOperation.Failed, copied.State)
		assert.Equal(t, ops["-40"], copied.EndedByOperationID)

		unstarted, err := store.ApplyOperations().Get(ctx, ops["80-"])
		require.NoError(t, err)
		require.NotNil(t, unstarted)
		assert.Equal(t, state.ApplyOperation.Cancelled, unstarted.State)
		assert.Zero(t, unstarted.EndedByOperationID)

		unstartedTask, err := store.Tasks().Get(ctx, "task_apply_ended_settle_unstarted_80-")
		require.NoError(t, err)
		require.NotNil(t, unstartedTask)
		assert.Equal(t, state.Task.Cancelled, unstartedTask.State)
	})

	// Each stopped shard names the failure that ended its own rollout member,
	// so target b's stopped shard names target b's failure rather than the
	// apply's first.
	t.Run("SettleAppliesStoppedForEndedOperation_NamesEachMembersOwnFailure", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		apply, ops := createEndedOperationApply(t, store, "apply_ended_settle_members", 975, state.Apply.RunningDegraded, true,
			endedOperationFixtureOp{target: "a", shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed},
			endedOperationFixtureOp{target: "b", shard: "-40", state: state.ApplyOperation.Failed, taskState: state.Task.Failed},
			endedOperationFixtureOp{target: "a", shard: "40-80", state: state.ApplyOperation.Stopped, taskState: state.Task.Stopped},
			endedOperationFixtureOp{target: "b", shard: "40-80", state: state.ApplyOperation.Stopped, taskState: state.Task.Stopped},
		)
		requestControlBy(t, store, apply.ID, storage.ControlOperationStop, storage.EndedOperationStopCaller)

		settled, err := store.Applies().SettleAppliesStoppedForEndedOperation(ctx, 10)
		require.NoError(t, err)
		require.Len(t, settled, 1)
		require.NotNil(t, settled[0].EndedOperation)
		assert.Equal(t, ops["a/-40"], settled[0].EndedOperation.ID)

		for _, member := range []string{"a", "b"} {
			op, err := store.ApplyOperations().Get(ctx, ops[member+"/40-80"])
			require.NoError(t, err)
			require.NotNil(t, op)
			assert.Equal(t, state.ApplyOperation.Failed, op.State, "target %s", member)
			assert.Equal(t, ops[member+"/-40"], op.EndedByOperationID, "target %s", member)
		}
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
		plain, _ := createEndedOperationApply(t, store, "apply_ended_expiry_plain", 963, state.Apply.FailedRetryable, true, ops...)
		spendRetryBudget(t, h, store, held.ID, plain.ID)
		requestControlBy(t, store, held.ID, storage.ControlOperationStop, storage.EndedOperationStopCaller)

		expired, err := store.Applies().ExpireRetryable(ctx, endedOperationStopTypes, 10)
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
