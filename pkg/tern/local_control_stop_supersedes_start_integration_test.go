//go:build integration

package tern

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A rollout's control plane operations share one data plane apply, and the
// start that resumed it stays pending there until the rollout settles, because
// each operation is claimed on its behalf one at a time. When the operator
// stops the rollout again, the stop the control plane forwards must complete
// that start in the same write that records the stop. Otherwise the data plane
// drive consumes the stop, finds the start still queued, and resumes the copy
// in the same claim, so the operator's later stop loses to their earlier start.
func TestLocalClient_StopSupersedesPendingStartOnRolloutApply(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	client, stor := newStopSupersedesStartClient(t)
	apply := seedResumingRolloutApply(t, stor, []string{"shard-001", "shard-002"})
	requestPendingStart(t, stor, apply)

	stopResp, err := client.Stop(t.Context(), &ternv1.StopRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
		Caller:      "operator-stop",
	})
	require.NoError(t, err)
	require.True(t, stopResp.Accepted)

	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationStop, storage.ControlRequestPending)
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationStart, storage.ControlRequestCompleted)

	standDown, err := client.stopStandsDownUnlessStartPending(t.Context(), client.logger, apply)
	require.NoError(t, err)
	assert.True(t, standDown, "once the stop lands, no queued start may resume the rollout in the same claim")
}

// An apply driven under the parent lease consumes its start when its drive
// resumes, so a stop recorded while that start is pending leaves it to the
// drive rather than completing it.
func TestLocalClient_StopLeavesPendingStartToParentLeaseDrive(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	client, stor := newStopSupersedesStartClient(t)
	apply, _ := seedRunningApplyWithTask(t, stor, storage.DatabaseTypeMySQL, storage.EngineSpirit)
	requestPendingStart(t, stor, apply)

	stopResp, err := client.Stop(t.Context(), &ternv1.StopRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
		Caller:      "operator-stop",
	})
	require.NoError(t, err)
	require.True(t, stopResp.Accepted)

	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationStop, storage.ControlRequestPending)
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationStart, storage.ControlRequestPending)
}

func newStopSupersedesStartClient(t *testing.T) (*LocalClient, storage.Storage) {
	t.Helper()
	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	t.Cleanup(func() { utils.CloseAndLog(stor) })
	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(client) })
	return client, stor
}

// seedResumingRolloutApply stores a running apply whose copy work is split
// across one operation per target, the shape a rollout's shared data plane
// apply takes once its operations attach.
func seedResumingRolloutApply(t *testing.T, stor storage.Storage, targets []string) *storage.Apply {
	t.Helper()
	ctx := t.Context()
	now := time.Now()
	identifier := fmt.Sprintf("apply-stop-supersedes-start-%d", now.UnixNano())
	apply := &storage.Apply{
		ApplyIdentifier: identifier,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testdb",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Running,
		Environment:     localClientTestEnvironment,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	groups := make([]*storage.ApplyOperationWithTasks, 0, len(targets))
	for _, target := range targets {
		groups = append(groups, &storage.ApplyOperationWithTasks{
			Operation: &storage.ApplyOperation{
				Deployment:   "testdb",
				OperationKey: target,
				Target:       target,
				State:        state.ApplyOperation.Running,
				CreatedAt:    now,
				UpdatedAt:    now,
			},
			Tasks: []*storage.Task{{
				TaskIdentifier: identifier + "-" + target,
				Database:       "testdb",
				DatabaseType:   storage.DatabaseTypeMySQL,
				Engine:         storage.EngineSpirit,
				State:          state.Task.Running,
				Namespace:      "testdb",
				TableName:      "users",
				DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
				DDLAction:      "alter",
				Environment:    localClientTestEnvironment,
				CreatedAt:      now,
				UpdatedAt:      now,
			}},
		})
	}
	applyID, err := stor.Applies().CreateWithGroupedOperations(ctx, apply, groups)
	require.NoError(t, err)
	apply.ID = applyID
	return apply
}

func requestPendingStart(t *testing.T, stor storage.Storage, apply *storage.Apply) {
	t.Helper()
	_, alreadyPending, err := stor.ControlRequests().RequestPending(t.Context(), &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator-start",
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)
}
