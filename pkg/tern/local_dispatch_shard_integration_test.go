//go:build integration

package tern

import (
	"database/sql"
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A shard-scoped dispatch — the control plane's per-shard fan-out sending one
// (namespace, shard, table)'s changes to a data plane — tags its drive tasks
// with the target shard and stamps the matching shard operation key on its
// operation row. The operator's whole-apply claim must load those tasks and
// drive them through the engine: the apply reaches completed only after the
// dispatched DDL actually ran on the target, never by completing a task-less
// no-op that leaves the work pending.
func TestLocalClient_ShardScopedDispatchDrivesItsTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTestTables(t, dsn)
	cleanupTasks(t, dsn)

	ctx := t.Context()

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	_, err = db.ExecContext(ctx, "CREATE TABLE users (id INT PRIMARY KEY)")
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	schemaFiles := buildSchemaWithAllTables(t, dsn, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})
	planResp, err := client.Plan(ctx, &ternv1.PlanRequest{
		Type:     "mysql",
		Database: "testdb",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"testdb": {Files: schemaFiles},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, planResp.PlanId)

	applyResp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:       planResp.PlanId,
		Environment:  localClientTestEnvironment,
		TargetShards: []string{"-80"},
		DdlChanges: []*ternv1.TableChange{{
			Namespace:  "testdb",
			TableName:  "users",
			Ddl:        "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
		}},
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted, "shard-scoped dispatch was not accepted: %s", applyResp.ErrorMessage)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, applyResp.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)

	// The dispatch's operation carries the shard operation key that marks its
	// shard-tagged rows as drive tasks, and the whole-apply loader returns them.
	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, "testdb/-80/users", ops[0].OperationKey)
	assert.Equal(t, storage.ApplyOperationKindWork, ops[0].OperationKind)

	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1, "the whole-apply loader must return the shard-scoped drive task")
	assert.Equal(t, "-80", tasks[0].Shard)
	assert.Equal(t, "users", tasks[0].TableName)
	assert.Equal(t, state.Task.Pending, tasks[0].State)

	// The operator claim drives the queued dispatch to completion.
	startTestOperator(t, stor, client)
	waitForApplyComplete(t, client, ctx, applyResp.ApplyId)

	// The dispatched DDL actually ran on the target database.
	var columnCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'testdb' AND TABLE_NAME = 'users' AND COLUMN_NAME = 'email'").Scan(&columnCount)
	require.NoError(t, err)
	assert.Equal(t, 1, columnCount, "the dispatched ALTER must have added the email column")

	// The drive settled the shard-scoped task itself, not just the apply row.
	tasks, err = stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, state.Task.Completed, tasks[0].State)
	assert.Equal(t, "-80", tasks[0].Shard)
}
