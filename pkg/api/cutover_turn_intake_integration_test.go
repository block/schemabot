//go:build integration

package api

import (
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

// A cutover request for a multi-member rollout is accepted when a parked
// member could take it now. Under an ordered cutover policy that is the first
// parked member no earlier member holds, and the request is bound to that
// member so the one command cannot pass on to the next. An earlier member that
// is still copying holds nothing. A request that arrives while an earlier
// member's failure, retry, or stop holds every parked member is refused with
// the name of that member and what clears it, rather than queued for a drive
// that will not take it. Unordered rollouts, and shards within one member, are
// accepted exactly as before.
func TestCutoverIntakeFollowsDeploymentOrder(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(stor, &ServerConfig{}, nil, logger)

	requestCutover := func(t *testing.T, applyID int64) error {
		t.Helper()
		apply := getApply(t, ctx, stor, applyID)
		_, _, err := svc.executeCutoverForApply(ctx, nil, apply, apply.ApplyIdentifier, "cli:alice")
		return err
	}
	requirePending := func(t *testing.T, applyID int64, pending bool) {
		t.Helper()
		req, err := stor.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationCutover)
		require.NoError(t, err)
		if pending {
			require.NotNil(t, req, "an accepted cutover is recorded for the drive to take")
			return
		}
		require.Nil(t, req, "a refused cutover leaves nothing queued")
	}

	t.Run("AcceptsWhileAnEarlierMemberIsStillCopying", func(t *testing.T) {
		resetMatrixTables(t, ctx, db)
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: "cutover-intake-earlier-copying",
			parentState:     state.Apply.Running,
			cutoverPolicy:   storage.CutoverPolicyBarrier,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"eu", "us", "au"},
			perOpState: map[string]string{
				"eu": state.ApplyOperation.Running,
				"us": state.ApplyOperation.WaitingForCutover,
				"au": state.ApplyOperation.WaitingForCutover,
			},
			perTaskState: map[string]string{
				"eu": state.Task.Running,
				"us": state.Task.WaitingForCutover,
				"au": state.Task.WaitingForCutover,
			},
		})

		require.NoError(t, requestCutover(t, seed.applyID))
		req, err := stor.ControlRequests().GetPending(ctx, seed.applyID, storage.ControlOperationCutover)
		require.NoError(t, err)
		require.NotNil(t, req, "an accepted cutover is recorded for the drive to take")
		boundID, err := req.CutoverOperationID()
		require.NoError(t, err)
		assert.Equal(t, seed.opID("us"), boundID, "the request is bound to the first parked member, not held behind eu's copy")
	})

	t.Run("RefusalNamesTheWayPastARetryingMember", func(t *testing.T) {
		resetMatrixTables(t, ctx, db)
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: "cutover-intake-retrying-ahead",
			parentState:     state.Apply.Running,
			cutoverPolicy:   storage.CutoverPolicyParallel,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"eu", "us"},
			perOpState: map[string]string{
				"eu": state.ApplyOperation.FailedRetryable,
				"us": state.ApplyOperation.WaitingForCutover,
			},
			perTaskState: map[string]string{
				"eu": state.Task.Running,
				"us": state.Task.WaitingForCutover,
			},
		})

		err := requestCutover(t, seed.applyID)
		require.Error(t, err)
		assert.Equal(t, http.StatusConflict, controlOperationHTTPStatus(err))
		assert.EqualError(t, err, "cutover is held: eu is retrying a failure ahead of us; run cutover again once eu recovers")
		requirePending(t, seed.applyID, false)
	})

	// eu is still running its copy while its data plane rides out a retry:
	// the drive keeps its lease, so the operation row stays running and only
	// its task records the retry. That retry holds us all the same, and the
	// refusal names it as one.
	t.Run("RefusalNamesARetryTheDriveIsRidingOut", func(t *testing.T) {
		resetMatrixTables(t, ctx, db)
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: "cutover-intake-live-retry-ahead",
			parentState:     state.Apply.Running,
			cutoverPolicy:   storage.CutoverPolicyParallel,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"eu", "us"},
			perOpState: map[string]string{
				"eu": state.ApplyOperation.Running,
				"us": state.ApplyOperation.WaitingForCutover,
			},
			perTaskState: map[string]string{
				"eu": state.Task.FailedRetryable,
				"us": state.Task.WaitingForCutover,
			},
		})

		err := requestCutover(t, seed.applyID)
		require.Error(t, err)
		assert.Equal(t, http.StatusConflict, controlOperationHTTPStatus(err))
		assert.EqualError(t, err, "cutover is held: eu is retrying a failure ahead of us; run cutover again once eu recovers")
		requirePending(t, seed.applyID, false)
	})

	// eu is still copying and us and au are parked. Each cutover command
	// starts one member's cutover: the first is bound to us, and once us is
	// cutting over the next is bound to au rather than answered as already in
	// progress. With both cutting over and nothing left parked, a third
	// command is answered as already in progress and records nothing.
	t.Run("ConsecutiveCommandsCutOverEachParkedMember", func(t *testing.T) {
		resetMatrixTables(t, ctx, db)
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: "cutover-intake-consecutive",
			parentState:     state.Apply.Running,
			cutoverPolicy:   storage.CutoverPolicyParallel,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"eu", "us", "au"},
			perOpState: map[string]string{
				"eu": state.ApplyOperation.Running,
				"us": state.ApplyOperation.WaitingForCutover,
				"au": state.ApplyOperation.WaitingForCutover,
			},
			perTaskState: map[string]string{
				"eu": state.Task.Running,
				"us": state.Task.WaitingForCutover,
				"au": state.Task.WaitingForCutover,
			},
		})
		command := func(t *testing.T) int {
			t.Helper()
			apply := getApply(t, ctx, stor, seed.applyID)
			resp, status, err := svc.executeCutoverForApply(ctx, nil, apply, apply.ApplyIdentifier, "cli:alice")
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.True(t, resp.Accepted)
			return status
		}
		boundTo := func(t *testing.T) int64 {
			t.Helper()
			req, err := stor.ControlRequests().GetPending(ctx, seed.applyID, storage.ControlOperationCutover)
			require.NoError(t, err)
			require.NotNil(t, req, "an accepted cutover is recorded for the drive to take")
			boundID, err := req.CutoverOperationID()
			require.NoError(t, err)
			return boundID
		}
		// takeCutover stands in for the bound member's drive taking the
		// request: the request completes and the member starts cutting over.
		takeCutover := func(t *testing.T, member string) {
			t.Helper()
			require.NoError(t, stor.ControlRequests().CompletePending(ctx, seed.applyID, storage.ControlOperationCutover))
			require.NoError(t, stor.ApplyOperations().UpdateState(ctx, seed.opID(member), state.ApplyOperation.CuttingOver))
			tasks, err := stor.Tasks().GetByApplyOperationID(ctx, seed.opID(member))
			require.NoError(t, err)
			for _, task := range tasks {
				task.State = state.Task.CuttingOver
				require.NoError(t, stor.Tasks().Update(ctx, task))
			}
		}

		assert.Equal(t, http.StatusOK, command(t))
		assert.Equal(t, seed.opID("us"), boundTo(t), "the first command cuts over the first parked member")
		takeCutover(t, "us")

		assert.Equal(t, http.StatusOK, command(t), "us cutting over does not make the command for au redundant")
		assert.Equal(t, seed.opID("au"), boundTo(t), "the second command cuts over the next parked member, not us again")
		takeCutover(t, "au")

		assert.Equal(t, http.StatusAccepted, command(t), "with nothing left parked the command is answered as already in progress")
		requirePending(t, seed.applyID, false)
	})

	t.Run("RefusalNamesTheWayPastAFailedMember", func(t *testing.T) {
		resetMatrixTables(t, ctx, db)
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: "cutover-intake-failed-ahead",
			parentState:     state.Apply.Running,
			cutoverPolicy:   storage.CutoverPolicyBarrier,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"eu", "us"},
			perOpState: map[string]string{
				"eu": state.ApplyOperation.Failed,
				"us": state.ApplyOperation.WaitingForCutover,
			},
			perTaskState: map[string]string{
				"eu": state.Task.Failed,
				"us": state.Task.WaitingForCutover,
			},
		})

		err := requestCutover(t, seed.applyID)
		require.Error(t, err)
		assert.Equal(t, http.StatusConflict, controlOperationHTTPStatus(err))
		assert.EqualError(t, err, "cutover is held: eu failed ahead of us, and on_failure halt holds every later cutover; stop the schema change, or fix the failure and apply again",
			"a failed member never completes, so the refusal does not tell the operator to wait for it")
		requirePending(t, seed.applyID, false)
	})

	t.Run("AcceptsWhenTheParkedDeploymentIsAtItsTurn", func(t *testing.T) {
		resetMatrixTables(t, ctx, db)
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: "cutover-intake-in-turn",
			parentState:     state.Apply.Running,
			cutoverPolicy:   storage.CutoverPolicyParallel,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"eu", "us"},
			perOpState: map[string]string{
				"eu": state.ApplyOperation.WaitingForCutover,
				"us": state.ApplyOperation.Running,
			},
			perTaskState: map[string]string{
				"eu": state.Task.WaitingForCutover,
				"us": state.Task.Running,
			},
		})

		require.NoError(t, requestCutover(t, seed.applyID))
		requirePending(t, seed.applyID, true)
		req, err := stor.ControlRequests().GetPending(ctx, seed.applyID, storage.ControlOperationCutover)
		require.NoError(t, err)
		boundID, err := req.CutoverOperationID()
		require.NoError(t, err)
		assert.Equal(t, seed.opID("eu"), boundID, "the request is bound to the first parked member no earlier member holds")
	})

	t.Run("RollingRolloutAcceptsAsBefore", func(t *testing.T) {
		resetMatrixTables(t, ctx, db)
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: "cutover-intake-rolling",
			parentState:     state.Apply.Running,
			cutoverPolicy:   storage.CutoverPolicyRolling,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"eu", "us"},
			perOpState: map[string]string{
				"eu": state.ApplyOperation.Running,
				"us": state.ApplyOperation.WaitingForCutover,
			},
			perTaskState: map[string]string{
				"eu": state.Task.Running,
				"us": state.Task.WaitingForCutover,
			},
		})

		require.NoError(t, requestCutover(t, seed.applyID), "the turn gate applies only to ordered cutover policies")
		requirePending(t, seed.applyID, true)
	})

	t.Run("ShardsOfOneDeploymentAcceptAsBefore", func(t *testing.T) {
		resetMatrixTables(t, ctx, db)
		applyID := seedShardedCutoverApply(t, stor)

		require.NoError(t, requestCutover(t, applyID), "a shard still copying does not hold its parked sibling's cutover")
		requirePending(t, applyID, true)
	})
}

// seedShardedCutoverApply stores a running barrier apply whose one deployment
// fans out into two shard work operations: the first still copying, the second
// parked at cutover.
func seedShardedCutoverApply(t *testing.T, stor storage.Storage) int64 {
	t.Helper()
	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: "cutover-intake-shards",
		Database:        "payments",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		Environment:     "staging",
		Deployment:      "eu",
		Caller:          "matrix-test",
		Engine:          storage.EngineForType(storage.DatabaseTypeMySQL),
		State:           state.Apply.Running,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	shard := func(operationKey, opState, taskState string) *storage.ApplyOperationWithTasks {
		return &storage.ApplyOperationWithTasks{
			Operation: &storage.ApplyOperation{
				Deployment:    "eu",
				OperationKey:  operationKey,
				OperationKind: storage.ApplyOperationKindWork,
				Target:        "payments-eu",
				State:         opState,
				CutoverPolicy: storage.CutoverPolicyBarrier,
				OnFailure:     storage.OnFailureHalt,
				CreatedAt:     now,
				UpdatedAt:     now,
			},
			Tasks: []*storage.Task{{
				TaskIdentifier: "cutover-intake-shards-" + operationKey,
				Database:       "payments",
				DatabaseType:   storage.DatabaseTypeMySQL,
				Engine:         storage.EngineForType(storage.DatabaseTypeMySQL),
				Repository:     "octocat/hello-world",
				PullRequest:    1,
				Environment:    "staging",
				State:          taskState,
				Options:        storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
				Namespace:      "commerce",
				TableName:      "widgets",
				DDL:            "ALTER TABLE widgets ADD COLUMN c int",
				DDLAction:      "alter",
				CreatedAt:      now,
				UpdatedAt:      now,
			}},
		}
	}
	applyID, err := stor.Applies().CreateWithGroupedOperations(t.Context(), apply, []*storage.ApplyOperationWithTasks{
		shard("commerce/-80/widgets", state.ApplyOperation.Running, state.Task.Running),
		shard("commerce/80-/widgets", state.ApplyOperation.WaitingForCutover, state.Task.WaitingForCutover),
	})
	require.NoError(t, err, "seed sharded apply")
	return applyID
}
