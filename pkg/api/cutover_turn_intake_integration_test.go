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

// A cutover request for a multi-member rollout is accepted only when a parked
// member could take it now. Under an ordered cutover policy that is the parked
// member whose turn it is, and the request is bound to that member so the one
// command cannot pass on to the next. A request that arrives while every
// parked member still waits on an earlier one is refused with the name of the
// member holding the turn, rather than queued for a drive that will not take
// it. Unordered rollouts, and shards within one member, are accepted exactly as
// before.
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

	t.Run("RefusesWhenNoParkedDeploymentIsAtItsTurn", func(t *testing.T) {
		resetMatrixTables(t, ctx, db)
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: "cutover-intake-out-of-turn",
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

		err := requestCutover(t, seed.applyID)
		require.Error(t, err)
		assert.Equal(t, http.StatusConflict, controlOperationHTTPStatus(err))
		assert.EqualError(t, err, "cutover follows rollout order: eu is ahead of us and is running; run cutover again once eu has completed")
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
		assert.EqualError(t, err, "cutover follows rollout order: eu is failed ahead of us, and on_failure halt holds every later cutover; stop the schema change, or fix the failure and apply again",
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
		assert.Equal(t, seed.opID("eu"), boundID, "the request is bound to the member whose turn it is")
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
