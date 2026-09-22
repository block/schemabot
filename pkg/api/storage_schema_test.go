package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/schema"
)

// A statement's kind is named in one vocabulary whichever dialect produced it,
// so two deployments' reports read and parse alike.
func TestStorageSchemaOperation(t *testing.T) {
	create, err := storageSchemaOperation(ddl.StatementCreateTable)
	require.NoError(t, err)
	assert.Equal(t, postgresOpCreateTable, create, "both dialects name a new table the same way")

	alter, err := storageSchemaOperation(ddl.StatementAlterTable)
	require.NoError(t, err)
	assert.Equal(t, "alter_table", alter)

	drop, err := storageSchemaOperation(ddl.StatementDropTable)
	require.NoError(t, err)
	assert.Equal(t, "drop_table", drop)
}

// A statement type the storage schema cannot contain is an error, not a label.
// Labelling it would hide a differ result this package does not understand
// inside a report an operator is about to act on.
func TestStorageSchemaOperation_UnexpectedTypeIsAnError(t *testing.T) {
	_, err := storageSchemaOperation(ddl.StatementRenameTable)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected statement type")
	assert.Contains(t, err.Error(), "CREATE TABLE")
}

// The diff and the bootstrap start from one set of option defaults, so a
// report cannot describe a policy the next boot would not apply.
func TestNewEnsureSchemaOptions_Defaults(t *testing.T) {
	defaults := newEnsureSchemaOptions()
	assert.Equal(t, schema.DialectMySQL, defaults.dialect)
	assert.Equal(t, DefaultPostgresStatementTimeout, defaults.postgresStatementTimeout)
	assert.False(t, defaults.allowDestructive, "destructive storage changes are refused unless asked for")

	assert.False(t, defaults.deploymentAllowsDestructive, "and a deployment is not assumed to have permitted them")

	configured := newEnsureSchemaOptions(
		WithDialect(schema.DialectPostgres),
		WithDestructiveSchemaChangePolicy(true, false),
		WithPostgresStatementTimeout(0),
	)
	assert.Equal(t, schema.DialectPostgres, configured.dialect)
	assert.True(t, configured.allowDestructive)
	assert.Zero(t, configured.postgresStatementTimeout, "zero disables the statement budget explicitly")
}

// A caller's opt-in widens the deployment's standing policy for the run it
// asked for, and says nothing about the deployment. The two are tracked apart
// because a report has to be able to state what the next pod to boot does,
// which no per-request flag moves: a convergence run with the flag against a
// deployment that has not configured it leaves surplus state that the
// deployment's own boots still refuse to drop.
func TestWithDestructiveSchemaChangePolicy_RequestWidensOnlyThisRun(t *testing.T) {
	neither := newEnsureSchemaOptions(WithDestructiveSchemaChangePolicy(false, false))
	assert.False(t, neither.allowDestructive)
	assert.False(t, neither.deploymentAllowsDestructive)

	requestOnly := newEnsureSchemaOptions(WithDestructiveSchemaChangePolicy(false, true))
	assert.True(t, requestOnly.allowDestructive, "the opt-in widens what this run may execute")
	assert.False(t, requestOnly.deploymentAllowsDestructive,
		"and leaves the deployment's own policy exactly where it was")

	deploymentOnly := newEnsureSchemaOptions(WithDestructiveSchemaChangePolicy(true, false))
	assert.True(t, deploymentOnly.allowDestructive,
		"a standing policy is never narrowed by a request that did not mention it")
	assert.True(t, deploymentOnly.deploymentAllowsDestructive)

	both := newEnsureSchemaOptions(WithDestructiveSchemaChangePolicy(true, true))
	assert.True(t, both.allowDestructive)
	assert.True(t, both.deploymentAllowsDestructive)
}
