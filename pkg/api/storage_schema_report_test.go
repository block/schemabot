package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// A nil report converts to a nil HTTP shape rather than to an empty one, so a
// caller cannot read "no report" as "nothing outstanding".
func TestStorageSchemaReport_NilAPITypeIsNotConvergence(t *testing.T) {
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

// A convergence permitted to run destructive statements runs them, so they
// count as applied. Counting only the outstanding set would log that nothing
// ran after a convergence that dropped surplus state on purpose.
func TestStorageSchemaReportAppliedCount(t *testing.T) {
	planned := &StorageSchemaReport{
		Outstanding: []StorageSchemaStatement{{Table: "applies"}, {Table: "checks"}},
		Destructive: []StorageSchemaStatement{{Table: "stale_state"}},
	}
	assert.Equal(t, 2, planned.appliedCount(), "a refused destructive statement did not run")

	planned.DestructiveAllowed = true
	assert.Equal(t, 3, planned.appliedCount())
}
