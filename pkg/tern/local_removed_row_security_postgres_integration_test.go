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

// A pull request turns on row security for h_plain with a "read all" policy,
// and SchemaBot applies it. A second pull request reverts the first, so the
// file is the bare table again. The plan for the revert is refused in the
// environment the policy was applied to, rather than reported as no changes
// while the policy stays. Another environment, where SchemaBot never applied
// it, still plans the file as the table definition alone.
func TestLocalClientPostgresPlanRefusesARevertOfAppliedRowSecurity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const database = "removed_row_security"
	_, storageDSN := setupMySQLContainer(t)
	setupStorageSchema(t, storageDSN)
	cleanupTasks(t, storageDSN)
	targetDSN, targetDB := testutil.StartPostgres(t, database)
	_, err := targetDB.ExecContext(t.Context(), `CREATE TABLE public.h_plain (id bigint PRIMARY KEY)`)
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

	planFile := func(environment, file string) (*ternv1.PlanResponse, error) {
		return client.Plan(t.Context(), &ternv1.PlanRequest{
			Database:    database,
			Type:        storage.DatabaseTypePostgres,
			Environment: environment,
			SchemaFiles: map[string]*ternv1.SchemaFiles{"public": {Files: map[string]string{"h_plain.sql": file}}},
		})
	}

	enable, err := planFile(localClientTestEnvironment, `CREATE TABLE h_plain (id bigint PRIMARY KEY);
ALTER TABLE h_plain ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read all" ON h_plain FOR SELECT USING (true);`)
	require.NoError(t, err)
	require.Len(t, enable.Changes, 1)
	applyResponse, err := client.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:      enable.PlanId,
		Environment: localClientTestEnvironment,
		DdlChanges:  enable.Changes[0].TableChanges,
		Options:     map[string]string{"allow_unsafe": "true"},
	})
	require.NoError(t, err)
	require.True(t, applyResponse.Accepted, applyResponse.ErrorMessage)
	driveQueuedApply(t, stor, client, applyResponse.ApplyId)
	apply, err := stor.Applies().GetByApplyIdentifier(t.Context(), applyResponse.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	require.Equal(t, state.Apply.Completed, apply.State, apply.ErrorMessage)

	_, err = planFile(localClientTestEnvironment, `CREATE TABLE h_plain (id bigint PRIMARY KEY);`)
	require.Error(t, err, "a revert of applied row security never plans as no changes")
	assert.Contains(t, err.Error(), `table "h_plain" no longer declares row security, but the schema files SchemaBot last applied did, and the table still has row level security enabled and policy "read all"`)
	assert.Contains(t, err.Error(), `ALTER TABLE "h_plain" DISABLE ROW LEVEL SECURITY;`)

	other, err := planFile("production", `CREATE TABLE h_plain (id bigint PRIMARY KEY);`)
	require.NoError(t, err)
	assert.Empty(t, other.Changes, "an environment SchemaBot never applied the row security to leaves it alone")
}
