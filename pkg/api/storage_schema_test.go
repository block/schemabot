package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
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

// A refusal reason says whether the whole statement was refused because its
// clauses could not be partitioned, which is the difference between "these
// clauses are refused" and "none of this ran".
func TestRefusedStorageChange_ReportedReason(t *testing.T) {
	classified := refusedStorageChange{
		change: engine.TableChange{Table: "applies"},
		reason: "DROP COLUMN destroys data",
	}
	assert.Equal(t, "DROP COLUMN destroys data", classified.reportedReason())

	unsplittable := refusedStorageChange{
		change:   engine.TableChange{Table: "applies"},
		reason:   "DROP COLUMN destroys data",
		splitErr: assert.AnError,
	}
	assert.Contains(t, unsplittable.reportedReason(), "DROP COLUMN destroys data")
	assert.Contains(t, unsplittable.reportedReason(), "refused whole")
	assert.Contains(t, unsplittable.reportedReason(), assert.AnError.Error())
}

// The diff and the bootstrap start from one set of option defaults, so a
// report cannot describe a policy the next boot would not apply.
func TestNewEnsureSchemaOptions_Defaults(t *testing.T) {
	defaults := newEnsureSchemaOptions()
	assert.Equal(t, schema.DialectMySQL, defaults.dialect)
	assert.Equal(t, DefaultPostgresStatementTimeout, defaults.postgresStatementTimeout)
	assert.False(t, defaults.allowDestructive, "destructive storage changes are refused unless asked for")

	configured := newEnsureSchemaOptions(
		WithDialect(schema.DialectPostgres),
		WithAllowDestructiveSchemaChanges(true),
		WithPostgresStatementTimeout(0),
	)
	assert.Equal(t, schema.DialectPostgres, configured.dialect)
	assert.True(t, configured.allowDestructive)
	assert.Zero(t, configured.postgresStatementTimeout, "zero disables the statement budget explicitly")
}
