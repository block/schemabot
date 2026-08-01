//go:build integration

package tern

import (
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// Operators claim from whatever Tern storage they poll and resolve the drive
// client afterwards, so in a storage database shared by more than one tern a
// claim can hand another tern's apply to a client bound to the wrong target
// (the in-process harness routes around exactly this — see
// integration/grpc_server_test.go). The drive must fail closed on that shape:
// refuse the claim, never touch the engine, and leave the apply for an
// operator whose client matches.
func TestLocalClient_ResumeRefusesApplyOutsideDatabaseScope(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	// The client under test is bound to "testdb"; the apply below belongs to a
	// different tern's database sharing this storage.
	client, eng := newTasklessControlClient(t, dsn, stor)

	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-foreign-%d", time.Now().UnixNano()),
		Database:       "otherdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "otherdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"otherdb": {
				Tables: []storage.TableChange{
					{Namespace: "otherdb", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN x INT", Operation: "alter"},
				},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)

	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: "apply-foreign-scope",
		PlanID:          planID,
		Database:        "otherdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "otherdb",
		Environment:     localClientTestEnvironment,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Pending,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := stor.Applies().CreateWithTasksAndOperations(ctx, apply, nil, []*storage.ApplyOperation{{
		Deployment: apply.Deployment,
		State:      state.ApplyOperation.Pending,
		CreatedAt:  now,
		UpdatedAt:  now,
	}})
	require.NoError(t, err)
	apply.ID = applyID

	// The claim itself succeeds — FindNextApply has no deployment filter, which
	// is exactly why the drive has to be the fail-closed layer.
	claimed, err := stor.Applies().FindNextApply(ctx, "test-operator-"+t.Name())
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)

	driveCtx := storage.WithApplyLease(ctx, claimed.Lease())
	err = client.ResumeApply(driveCtx, claimed)
	require.Error(t, err, "driving a foreign database's apply must fail closed")
	assert.Contains(t, err.Error(), "outside this client's database scope")

	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	err = client.ResumeApplyOperation(driveCtx, claimed, ops[0].ID)
	require.Error(t, err, "the operation drive must refuse a foreign apply the same way")
	assert.Contains(t, err.Error(), "outside this client's database scope")

	err = client.ResumeApplyOperationCutover(driveCtx, claimed, ops[0].ID)
	require.Error(t, err, "the cutover drive must refuse a foreign apply the same way")
	assert.Contains(t, err.Error(), "outside this client's database scope")

	assert.Empty(t, eng.recorded(), "a refused foreign apply must never reach the engine")

	// The claim itself moved the apply to running; the refused drive must not
	// advance it further — it stays active work, re-claimable by an operator
	// whose client matches once this claim's heartbeat goes stale.
	persisted, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.False(t, state.IsTerminalApplyState(persisted.State),
		"a refused foreign apply must not be settled by the wrong client (state %q)", persisted.State)
	assert.Nil(t, persisted.CompletedAt, "a refused foreign apply must not be stamped completed")
}
