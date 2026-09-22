package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
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

	assert.Equal(t, DestructivePolicyUnknown, defaults.deploymentDestructive,
		"and a caller that named no deployment policy is not assumed to have read one")

	configured := newEnsureSchemaOptions(
		WithDialect(schema.DialectPostgres),
		WithDestructiveSchemaChangePolicy(DestructivePolicyPermits, false),
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
	forbidding := newEnsureSchemaOptions(WithDestructiveSchemaChangePolicy(DestructivePolicyForbids, false))
	assert.False(t, forbidding.allowDestructive)
	assert.Equal(t, DestructivePolicyForbids, forbidding.deploymentDestructive)

	requestOnly := newEnsureSchemaOptions(WithDestructiveSchemaChangePolicy(DestructivePolicyForbids, true))
	assert.True(t, requestOnly.allowDestructive, "the opt-in widens what this run may execute")
	assert.Equal(t, DestructivePolicyForbids, requestOnly.deploymentDestructive,
		"and leaves the deployment's own policy exactly where it was")

	deploymentOnly := newEnsureSchemaOptions(WithDestructiveSchemaChangePolicy(DestructivePolicyPermits, false))
	assert.True(t, deploymentOnly.allowDestructive,
		"a standing policy is never narrowed by a request that did not mention it")
	assert.Equal(t, DestructivePolicyPermits, deploymentOnly.deploymentDestructive)

	both := newEnsureSchemaOptions(WithDestructiveSchemaChangePolicy(DestructivePolicyPermits, true))
	assert.True(t, both.allowDestructive)
	assert.Equal(t, DestructivePolicyPermits, both.deploymentDestructive)
}

// An unread deployment policy permits nothing, so it never widens what runs,
// and it stays distinguishable from a policy that was read and forbids.
//
// The execution half matters because a convergence addressed by DSN must not
// acquire permissions from the fact that nobody could say what it had. The
// reporting half matters because the two answers differ exactly where an
// operator is deciding whether to pre-apply: one deployment keeps what they
// converge, and the other may drop it.
func TestWithDestructiveSchemaChangePolicy_UnknownPermitsNothingAndStaysUnknown(t *testing.T) {
	unknown := newEnsureSchemaOptions(WithDestructiveSchemaChangePolicy(DestructivePolicyUnknown, false))
	assert.False(t, unknown.allowDestructive, "an unread policy grants nothing")
	assert.Equal(t, DestructivePolicyUnknown, unknown.deploymentDestructive)
	assert.NotEqual(t, DestructivePolicyForbids, unknown.deploymentDestructive,
		"not knowing is not the same answer as knowing it forbids")

	widened := newEnsureSchemaOptions(WithDestructiveSchemaChangePolicy(DestructivePolicyUnknown, true))
	assert.True(t, widened.allowDestructive, "--allow-unsafe is still the way to widen it")
	assert.Equal(t, DestructivePolicyUnknown, widened.deploymentDestructive,
		"and widening this run still says nothing about the deployment")

	assert.Equal(t, DestructivePolicyPermits, ConfiguredDestructivePolicy(true))
	assert.Equal(t, DestructivePolicyForbids, ConfiguredDestructivePolicy(false),
		"a config that was read and says no is a real answer, unlike the absence of one")
}

// The report says what a boot does, which on MySQL is the deployment's policy
// and never a default standing in for one that could not be read.
func TestMySQLBootRemovalPolicy(t *testing.T) {
	assert.Equal(t, apitypes.BootRemovalRemoves, mysqlBootRemovalPolicy(DestructivePolicyPermits))
	assert.Equal(t, apitypes.BootRemovalPreserves, mysqlBootRemovalPolicy(DestructivePolicyForbids))
	assert.Equal(t, apitypes.BootRemovalUnknown, mysqlBootRemovalPolicy(DestructivePolicyUnknown))
}
