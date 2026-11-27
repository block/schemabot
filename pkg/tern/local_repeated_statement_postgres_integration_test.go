//go:build integration

package tern

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/testutil"
)

// An owner makes accounts.display_name NOT NULL on a table that already has a
// check under the name the NOT NULL sequence gives its helper, but with a rule
// of its own. The plan drops that check, adds and validates the helper, sets
// NOT NULL, and drops the helper. The two drops are the same statement on one
// table, but each carries the engine's record of the constraint it was
// reviewed against. The apply, opted into the unsafe drop, hands each drop its
// own record, completes every step, and leaves the column NOT NULL with no
// helper behind.
func TestLocalClientPostgresApplyRunsARepeatedStatementWithItsOwnRecord(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const (
		database = "repeated_statement"
		helper   = "accounts_display_name_not_null_check"
	)
	_, storageDSN := setupMySQLContainer(t)
	setupStorageSchema(t, storageDSN)
	cleanupTasks(t, storageDSN)
	targetDSN, targetDB := testutil.StartPostgres(t, database)
	_, err := targetDB.ExecContext(t.Context(), `
		CREATE TABLE public.accounts (id bigint PRIMARY KEY, display_name text);
		INSERT INTO public.accounts VALUES (1, 'Ada'), (2, 'Grace');
		ALTER TABLE public.accounts ADD CONSTRAINT `+helper+` CHECK (display_name <> '')`)
	require.NoError(t, err)

	stor := createStorage(t, storageDSN)
	defer utils.CloseAndLog(stor)
	client, err := NewLocalClient(LocalConfig{
		Database:  database,
		Type:      storage.DatabaseTypePostgres,
		TargetDSN: targetDSN,
	}, stor, slog.Default())
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	plan, err := client.Plan(t.Context(), &ternv1.PlanRequest{
		Database: database,
		Type:     storage.DatabaseTypePostgres,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"public": {Files: map[string]string{"accounts.sql": "CREATE TABLE accounts (id bigint PRIMARY KEY, display_name text NOT NULL);"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	steps := plan.Changes[0].TableChanges
	require.Len(t, steps, 5, "drop the existing check, add the helper, validate, SET NOT NULL, drop the helper")
	assert.Contains(t, steps[0].Ddl, "DROP CONSTRAINT")
	assert.Contains(t, steps[4].Ddl, "DROP CONSTRAINT")

	applyResponse, err := client.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:      plan.PlanId,
		Environment: localClientTestEnvironment,
		DdlChanges:  steps,
		Options:     map[string]string{"allow_unsafe": "true"},
	})
	require.NoError(t, err)
	require.True(t, applyResponse.Accepted, applyResponse.ErrorMessage)
	driveQueuedApply(t, stor, client, applyResponse.ApplyId)

	apply, err := stor.Applies().GetByApplyIdentifier(t.Context(), applyResponse.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.Equal(t, state.Apply.Completed, apply.State, apply.ErrorMessage)
	tasks, err := stor.Tasks().GetByApplyID(t.Context(), apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 5)
	for i, task := range tasks {
		assert.Equal(t, state.Task.Completed, task.State, "%s: %s", task.DDL, task.ErrorMessage)
		assert.Equal(t, i+1, task.PlanChangeOrdinal, task.DDL)
	}

	var notNull bool
	require.NoError(t, targetDB.QueryRowContext(t.Context(),
		`SELECT attnotnull FROM pg_attribute WHERE attrelid = 'public.accounts'::regclass AND attname = 'display_name'`).Scan(&notNull))
	assert.True(t, notNull)
	var helpers int
	require.NoError(t, targetDB.QueryRowContext(t.Context(),
		`SELECT count(*) FROM pg_constraint WHERE conrelid = 'public.accounts'::regclass AND conname = $1`, helper).Scan(&helpers))
	assert.Zero(t, helpers, "the new helper is dropped")
}
