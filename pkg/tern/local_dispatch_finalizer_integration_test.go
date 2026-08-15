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

// A group_finalizer dispatch — no target shards, every change VSchema-typed —
// applies one namespace's VSchema after its sibling shard work completes. When
// the stored plan also carries table DDL (the sibling shard work), the data
// plane must create a task-less group_finalizer operation for the dispatched
// namespace, never fall back to the plan's DDL: that fallback would fabricate
// shard-less work tasks a sharded engine rejects, and the VSchema would never
// apply.
func TestLocalClient_VSchemaOnlyDispatchCreatesGroupFinalizer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	// A mixed plan: one namespace with shard work (table DDL) and a VSchema
	// change, mirroring a sharded column add that also hydrates the VSchema.
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-finalizer-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"ks_sharded": {
				Tables: []storage.TableChange{
					{Namespace: "ks_sharded", Table: "mutes", DDL: "ALTER TABLE `mutes` ADD COLUMN `note` varchar(32)", Operation: "alter"},
				},
				Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded": true}`},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)
	plan.ID = planID

	resp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      plan.PlanIdentifier,
		Environment: localClientTestEnvironment,
		DdlChanges: []*ternv1.TableChange{{
			Namespace:  "ks_sharded",
			TableName:  "VSchema: ks_sharded",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA,
		}},
	})
	require.NoError(t, err)
	require.True(t, resp.Accepted, "VSchema-only dispatch was not accepted: %s", resp.ErrorMessage)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, resp.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)

	// The finalizer carries no task rows: the drive reconstructs the VSchema
	// change from the plan. The plan's table DDL must not leak into this apply.
	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	assert.Empty(t, tasks, "a group_finalizer dispatch must not resurrect the plan's table DDL as tasks")

	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, storage.ApplyOperationKindGroupFinalizer, ops[0].OperationKind)
	assert.Equal(t, "ks_sharded/group_finalizer", ops[0].OperationKey)
	assert.Equal(t, state.ApplyOperation.Pending, ops[0].State)
}

// A VSchema-only dispatch naming a namespace whose stored plan carries no
// VSchema artifact has nothing to apply; the dispatch is rejected before any
// apply or operation row is created rather than failing at drive time.
func TestLocalClient_VSchemaOnlyDispatchWithoutArtifactFailsClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-finalizer-noartifact-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"ks_sharded": {
				Tables: []storage.TableChange{
					{Namespace: "ks_sharded", Table: "mutes", DDL: "ALTER TABLE `mutes` ADD COLUMN `note` varchar(32)", Operation: "alter"},
				},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)
	plan.ID = planID

	_, err = client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      plan.PlanIdentifier,
		Environment: localClientTestEnvironment,
		DdlChanges: []*ternv1.TableChange{{
			Namespace:  "ks_sharded",
			TableName:  "VSchema: ks_sharded",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA,
		}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no VSchema artifact")
}
