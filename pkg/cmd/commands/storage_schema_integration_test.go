//go:build integration

package commands

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/testutil"
)

// The direct path is the one these commands exist for: the server is down —
// possibly down because its own schema bootstrap is failing — so the operator
// points the CLI at the storage database itself. It resolves the target, infers
// the dialect from the DSN's form, runs this binary's own bootstrap and
// attributes the report to this build, none of which the API path exercises.
//
// So it is driven here against a real, empty storage database: the convergence
// has to create the whole schema from nothing, and the plan that follows has to
// agree that it did.
func TestStorageSchemaDirect_ConvergesAnEmptyDatabaseAndReportsItConverged(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "schemabot")
	globals := &Globals{Version: "v1.4.0"}
	require.False(t, testutil.PostgresTableExists(t, db, "public", "applies"),
		"the database starts with no storage schema, so the convergence has to create it")

	apply := &StorageApplyCmd{
		storageSchemaTargetFlags: storageSchemaTargetFlags{DSN: dsn},
		AutoApprove:              true,
	}
	applyOut := captureStdout(func() {
		require.NoError(t, apply.Run(t.Context(), globals))
	})

	assert.True(t, testutil.PostgresTableExists(t, db, "public", "applies"),
		"the convergence ran this binary's own bootstrap against the database the DSN names")
	assert.True(t, testutil.PostgresTableExists(t, db, "public", "settings"))
	assert.Contains(t, applyOut, "Nothing is outstanding.",
		"a convergence that created the whole schema leaves nothing behind")
	assert.Contains(t, applyOut, "the schema embedded in v1.4.0",
		"the CLI answered, so the report names this build as the schema it converged to")

	// The plan is the other half of the direct path, and it takes a named
	// source: the release the deploy is about to roll. The embedded files of
	// this build are what the convergence just ran, so the same files have to
	// report no outstanding statements.
	plan := &StoragePlanCmd{
		storageSchemaTargetFlags: storageSchemaTargetFlags{DSN: dsn},
		storageSchemaSourceFlags: storageSchemaSourceFlags{SchemaDir: filepath.Join("..", "..", "schema", "postgres")},
	}
	planOut := captureStdout(func() {
		require.NoError(t, plan.Run(t.Context(), globals), "exit 0 is how a pre-deploy gate reads 'this storage is ready'")
	})
	assert.Contains(t, planOut, "No schema changes detected.")
	assert.Contains(t, planOut, "Database: schemabot", "the plan names the database the DSN addressed")
	assert.NotContains(t, planOut, "+ applies", "nothing is outstanding against the schema that was just applied")
}

// An attended direct apply previews the convergence before it asks: the plan on
// screen is a real read of the database the DSN names, and the schema it names
// is this build's own, because a convergence has no flag to point it at another
// release's.
func TestStorageSchemaDirect_AttendedApplyPreviewsThisBuildsOwnSchema(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "schemabot")
	answerPrompt(t, "yes")

	apply := &StorageApplyCmd{storageSchemaTargetFlags: storageSchemaTargetFlags{DSN: dsn}}
	out := captureStdout(func() {
		require.NoError(t, apply.Run(t.Context(), &Globals{Version: "v1.4.0"}))
	})

	assert.Contains(t, out, "the schema embedded in v1.4.0",
		"the preview names the release whose schema the convergence is about to run")
	assert.Contains(t, out, "Do you want to apply these changes to schemabot",
		"an attended run asks before it converges, and names what it would converge")
	assert.True(t, testutil.PostgresTableExists(t, db, "public", "applies"),
		"the answered prompt converged the database")
}

// A direct connection reports the database it was pointed at, and the dialect
// comes from the DSN's own form with no flag and no server to ask. A plan is
// read-only, so this runs against a database with no storage schema at all and
// the whole schema reports as outstanding — the state an operator is in when
// the bootstrap that would create it is what is failing.
func TestStorageSchemaDirect_PlanReportsTheWholeSchemaOutstanding(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "schemabot")

	plan := &StoragePlanCmd{
		storageSchemaTargetFlags: storageSchemaTargetFlags{DSN: dsn},
		storageSchemaSourceFlags: storageSchemaSourceFlags{SchemaDir: filepath.Join("..", "..", "schema", "postgres")},
	}
	var err error
	out := captureStdout(func() {
		err = plan.Run(t.Context(), &Globals{Version: "v1.4.0"})
	})

	require.Error(t, err, "outstanding statements exit non-zero so a pre-deploy gate can tell them from a converged database")
	assert.Equal(t, ExitStorageSchemaOutstanding, ExitCodeFor(err),
		"outstanding work has its own status, distinct from a failed read")
	assert.Contains(t, out, "+ applies", "the whole schema is outstanding, table by table")
	assert.Contains(t, out, "Database: schemabot", "the report names the database the DSN addressed")
	assert.Contains(t, out, "PostgreSQL Schema Change Plan",
		"the dialect came from the DSN's own form, with no --dialect and no server to ask")
}
