//go:build integration

package tern

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/testutil"
)

// This scenario covers a PostgreSQL apply whose plan carries two statements
// for one table, driven as one task per statement, that lost its driver after
// the first statement executed on the target but before that task's outcome
// was recorded. The test seeds that post-crash state directly — both task rows
// stopped, the first statement already on the target — and resumes against a
// real PostgreSQL target. The re-plan still lists the table with only the
// second statement remaining, so the first task must settle as completed
// without being handed to the engine, and the second task must run exactly
// once. Re-running the first statement would fail on the column that already
// exists, so the apply completing is itself evidence of non-re-execution.
func TestLocalClient_ResumeApplyPostgresSettlesLandedStatementWithoutReexecution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const database = "eg_resume_landed"
	_, storageDSN := setupMySQLContainer(t)
	setupStorageSchema(t, storageDSN)
	cleanupTasks(t, storageDSN)

	ctx := t.Context()
	targetDSN, targetDB := testutil.StartPostgres(t, database)
	_, err := targetDB.ExecContext(ctx, "CREATE TABLE public.users (id bigint PRIMARY KEY)")
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, storageDSN)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  database,
		Type:      storage.DatabaseTypePostgres,
		TargetDSN: targetDSN,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	const (
		emailDDL = "ALTER TABLE public.users ADD COLUMN email text"
		nameDDL  = "ALTER TABLE public.users ADD COLUMN name text"
	)
	now := time.Now()
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-pg-landed-%d", now.UnixNano()),
		Database:       database,
		DatabaseType:   storage.DatabaseTypePostgres,
		Deployment:     database,
		Environment:    localClientTestEnvironment,
		CreatedAt:      now,
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY, email text, name text);",
			}},
		},
		Namespaces: map[string]*storage.NamespacePlanData{
			"public": {
				Tables: []storage.TableChange{
					{Namespace: "public", Table: "users", DDL: emailDDL, Operation: "alter"},
					{Namespace: "public", Table: "users", DDL: nameDDL, Operation: "alter"},
				},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)

	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-pg-landed-%d", now.UnixNano()),
		PlanID:          planID,
		Database:        database,
		DatabaseType:    storage.DatabaseTypePostgres,
		Deployment:      database,
		Engine:          storage.EnginePostgres,
		State:           state.Apply.Stopped,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{}),
		Environment:     localClientTestEnvironment,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := stor.Applies().Create(ctx, apply)
	require.NoError(t, err)

	newTask := func(suffix, ddl string) *storage.Task {
		return &storage.Task{
			TaskIdentifier: fmt.Sprintf("task-pg-landed-%s-%d", suffix, now.UnixNano()),
			ApplyID:        applyID,
			PlanID:         planID,
			Database:       database,
			DatabaseType:   storage.DatabaseTypePostgres,
			Engine:         storage.EnginePostgres,
			State:          state.Task.Stopped,
			TableName:      "users",
			Namespace:      "public",
			DDL:            ddl,
			DDLAction:      "alter",
			Options:        storage.MarshalApplyOptions(storage.ApplyOptions{}),
			Environment:    localClientTestEnvironment,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
	}
	emailTask := newTask("email", emailDDL)
	nameTask := newTask("name", nameDDL)
	for _, task := range []*storage.Task{emailTask, nameTask} {
		taskID, err := stor.Tasks().Create(ctx, task)
		require.NoError(t, err)
		task.ID = taskID
	}

	// The first statement reached the target, but the driver died before it
	// could record the task's outcome.
	_, err = targetDB.ExecContext(ctx, emailDDL)
	require.NoError(t, err)

	_, alreadyPending, err := stor.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "integration-test",
	})
	require.NoError(t, err)
	assert.False(t, alreadyPending)

	claimed, err := stor.Applies().ClaimApplyByID(ctx, applyID, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, state.Apply.Stopped, claimed.State)

	require.NoError(t, client.ResumeApply(ctx, claimed))

	storedApply, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	assert.Equal(t, state.Apply.Completed, storedApply.State)
	assert.NotNil(t, storedApply.CompletedAt)

	storedEmail, err := stor.Tasks().Get(ctx, emailTask.TaskIdentifier)
	require.NoError(t, err)
	require.NotNil(t, storedEmail)
	assert.Equal(t, state.Task.Completed, storedEmail.State)
	assert.Equal(t, 100, storedEmail.ProgressPercent)
	assert.NotNil(t, storedEmail.CompletedAt)
	assert.Equal(t, emailDDL, storedEmail.DDL, "a landed task keeps the DDL it was reviewed with")

	storedName, err := stor.Tasks().Get(ctx, nameTask.TaskIdentifier)
	require.NoError(t, err)
	require.NotNil(t, storedName)
	assert.Equal(t, state.Task.Completed, storedName.State)
	assert.NotNil(t, storedName.CompletedAt)

	assert.True(t, postgresColumnExists(t, targetDB, "public", "users", "name"),
		"the remaining task's statement must land on the target")

	logs, err := stor.ApplyLogs().GetByApply(ctx, applyID)
	require.NoError(t, err)
	assert.True(t, hasLogMessageContaining(logs,
		fmt.Sprintf("Task %s already completed (its statement landed before its outcome was recorded)", emailTask.TaskIdentifier)),
		"the landed task's settlement must be visible in the apply log")
	assert.False(t, hasLogMessageContaining(logs, fmt.Sprintf("Task %s resumed", emailTask.TaskIdentifier)),
		"the landed task must never be handed to the engine")
	assert.True(t, hasLogMessageContaining(logs, fmt.Sprintf("Task %s resumed", nameTask.TaskIdentifier)),
		"the remaining task must be handed to the engine")
	assert.False(t, hasLogMessageContaining(logs,
		fmt.Sprintf("Task %s already completed", nameTask.TaskIdentifier)),
		"the remaining task must run, not be settled as already landed")

	pendingStart, err := stor.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.Nil(t, pendingStart)
}

// postgresColumnExists reports whether the column is present on the table in
// the given PostgreSQL schema.
func postgresColumnExists(t *testing.T, db *sql.DB, schemaName, tableName, columnName string) bool {
	t.Helper()
	var count int
	err := db.QueryRowContext(t.Context(), `
		SELECT COUNT(*)
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2 AND column_name = $3`,
		schemaName, tableName, columnName).Scan(&count)
	require.NoError(t, err)
	return count > 0
}
