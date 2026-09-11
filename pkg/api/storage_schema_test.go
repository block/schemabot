package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
)

// A report is converged only when nothing at all is outstanding. Refused
// destructive statements and manual entries both mean the database does not
// match this binary's schema, and reporting convergence there would tell an
// operator the opposite of what is true.
func TestStorageSchemaReport_Converged(t *testing.T) {
	statement := StorageSchemaStatement{Table: "applies", Operation: storageSchemaOpAlterTable, DDL: "ALTER TABLE `applies` ADD COLUMN `caller` varchar(255) NOT NULL DEFAULT ''"}

	tests := []struct {
		name      string
		report    StorageSchemaReport
		converged bool
	}{
		{"nothing outstanding", StorageSchemaReport{Database: "schemabot"}, true},
		{"outstanding statement", StorageSchemaReport{Outstanding: []StorageSchemaStatement{statement}}, false},
		{"refused destructive statement", StorageSchemaReport{Destructive: []StorageSchemaStatement{statement}}, false},
		{"destructive statement that would run", StorageSchemaReport{Destructive: []StorageSchemaStatement{statement}, DestructiveAllowed: true}, false},
		{"manual remediation", StorageSchemaReport{Manual: []StorageSchemaStatement{statement}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.converged, tc.report.Converged())
		})
	}
}

// A report crosses the wire to a control plane and back without losing
// anything: the control plane renders what the data plane decided, so a field
// dropped in conversion would be a statement an operator never sees.
func TestStorageSchemaReport_ProtoRoundTrip(t *testing.T) {
	report := &StorageSchemaReport{
		Dialect:  schema.DialectPostgres,
		Database: "schemabot",
		Version:  "v0.1.67",
		Outstanding: []StorageSchemaStatement{{
			Table:     "apply_operations",
			Operation: postgresOpAddColumn,
			DDL:       `ALTER TABLE "apply_operations" ADD COLUMN "operation_kind" varchar(32) NOT NULL DEFAULT 'work'`,
		}},
		Destructive: []StorageSchemaStatement{{
			Table:     "vitess_tasks",
			Operation: storageSchemaOpDropTable,
			DDL:       "DROP TABLE `vitess_tasks`",
			Reason:    "DROP TABLE destroys data",
		}},
		DestructiveAllowed: true,
		Manual: []StorageSchemaStatement{{
			Table:     "checks",
			Operation: postgresOpAddColumn,
			DDL:       `ALTER TABLE "checks" ADD COLUMN "head_sha" varchar(64) NOT NULL`,
			Reason:    "column is NOT NULL without a DEFAULT",
		}},
	}

	round := StorageSchemaReportFromProto(StorageSchemaReportProto(report))
	require.NotNil(t, round)
	assert.Equal(t, report, round)
}

// A nil wire report converts to a nil report rather than to an empty one. An
// RPC that answered with no report at all is a different condition from one
// that reported convergence, and inventing convergence here would turn a
// broken answer into a green light.
func TestStorageSchemaReport_NilProtoIsNotConvergence(t *testing.T) {
	assert.Nil(t, StorageSchemaReportProto(nil))
	assert.Nil(t, StorageSchemaReportFromProto(nil))

	var report *StorageSchemaReport
	assert.Nil(t, report.APIType())
}

// The HTTP shape carries the statements and the convergence verdict, so the
// CLI renders the same answer the differ produced without recomputing it.
func TestStorageSchemaReport_APIType(t *testing.T) {
	report := &StorageSchemaReport{
		Dialect:  schema.DialectMySQL,
		Database: "schemabot_storage",
		Version:  "v0.1.67",
		Destructive: []StorageSchemaStatement{{
			Table:     "newer_release_state",
			Operation: storageSchemaOpDropTable,
			DDL:       "DROP TABLE `newer_release_state`",
			Reason:    "DROP TABLE destroys data",
		}},
	}

	converted := report.APIType()
	require.NotNil(t, converted)
	assert.Equal(t, "mysql", converted.Dialect)
	assert.Equal(t, "schemabot_storage", converted.Database)
	assert.Equal(t, "v0.1.67", converted.Version)
	assert.False(t, converted.Converged, "a refused destructive statement is not convergence")
	assert.False(t, converted.DestructiveAllowed)
	assert.Empty(t, converted.Outstanding)
	require.Len(t, converted.Destructive, 1)
	assert.Equal(t, "newer_release_state", converted.Destructive[0].Table)
	assert.Equal(t, storageSchemaOpDropTable, converted.Destructive[0].Operation)
	assert.Equal(t, "DROP TABLE `newer_release_state`", converted.Destructive[0].DDL)
	assert.Equal(t, "DROP TABLE destroys data", converted.Destructive[0].Reason)
}

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
