package commands

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/glyph"
)

// A storage plan renders through the same templates as every other plan: the
// header box naming the database and the schema it was compared against, the
// per-table change symbols, and the summary line. An operator who can read
// `plan` can read this without learning a second layout.
func TestOutputStorageSchemaPlan_RendersAsAPlan(t *testing.T) {
	report := &apitypes.StorageSchemaReport{
		Dialect:      "mysql",
		Database:     "schemabot",
		Host:         "db-1.example",
		SchemaSource: "the schema files of release v1.4.0 in block/schemabot",
		Outstanding: []apitypes.StorageSchemaStatement{
			{Table: "applies", Operation: "alter_table", DDL: "ALTER TABLE `applies` ADD COLUMN `caller` varchar(255) NOT NULL DEFAULT ''"},
			{Table: "checks", Operation: "create_table", DDL: "CREATE TABLE `checks` (`id` BIGINT UNSIGNED AUTO_INCREMENT, PRIMARY KEY (`id`))"},
		},
	}

	out := captureStdout(func() {
		require.NoError(t, outputStorageSchemaPlan(report, false, storageSchemaPlanHints(report)))
	})

	assert.Contains(t, out, "MySQL Schema Change Plan")
	assert.Contains(t, out, "Database: schemabot on db-1.example")
	assert.Contains(t, out, "Schema: the schema files of release v1.4.0 in block/schemabot")
	assert.Contains(t, out, "+ checks")
	assert.Contains(t, out, "~ applies")
	assert.Contains(t, out, "📋 Plan: 1 table to create, 1 table to alter")
	assert.Contains(t, out, "run that release's binary",
		"the plan names the next step, which is never an apply of a different release's schema")
	assert.NotContains(t, out, "(mysql)",
		"the title already names the family; repeating it in the database line is noise")
}

// A converged storage database says so and stops. There is no section to read
// and no next step, so a hint naming one would be wrong.
func TestOutputStorageSchemaPlan_Converged(t *testing.T) {
	report := &apitypes.StorageSchemaReport{
		Dialect:      "postgres",
		Database:     "schemabot",
		SchemaSource: "the schema embedded in v1.4.0",
		Converged:    true,
	}

	out := captureStdout(func() {
		require.NoError(t, outputStorageSchemaPlan(report, false, storageSchemaPlanHints(report)))
	})

	assert.Contains(t, out, "PostgreSQL Schema Change Plan")
	assert.Contains(t, out, "✓ No schema changes detected.")
	assert.NotContains(t, out, "run that release's binary")
	assert.NotContains(t, out, "📋 Plan:")
}

// A destructive statement carries the glyph its situation earns: attention
// while a plan is only disclosing it, refusal once an apply has declined to run
// it, and escalation when destructive consent is already in effect and the next
// convergence will destroy the state.
func TestOutputStorageSchemaPlan_DestructiveSeverity(t *testing.T) {
	destructive := []apitypes.StorageSchemaStatement{{
		Table:     "stale_state",
		Operation: "drop_table",
		DDL:       "DROP TABLE `stale_state`",
		Reason:    "DROP TABLE destroys data",
	}}

	tests := []struct {
		name    string
		allowed bool
		isApply bool
		glyph   string
		heading string
		runs    bool
	}{
		{name: "a plan discloses", glyph: glyph.Attention, heading: "refused unless destructive storage changes are allowed"},
		{name: "an apply refuses", isApply: true, glyph: glyph.Refused, heading: "the surplus state stays in place"},
		{name: "consent in effect", allowed: true, glyph: glyph.Escalation, heading: "running because destructive storage changes are allowed", runs: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			report := &apitypes.StorageSchemaReport{
				Dialect:            "mysql",
				Database:           "schemabot",
				SchemaSource:       "the schema embedded in v1.4.0",
				Destructive:        destructive,
				DestructiveAllowed: tc.allowed,
			}
			out := captureStdout(func() {
				require.NoError(t, outputStorageSchemaPlan(report, tc.isApply, nil))
			})

			assert.Contains(t, out, tc.glyph+" Destructive changes")
			assert.Contains(t, out, tc.heading)
			assert.Contains(t, out, "1. stale_state: DROP TABLE destroys data")
			if tc.runs {
				assert.Contains(t, out, "- stale_state", "a statement that will run is shown as a change")
				assert.Contains(t, out, "📋 Plan: 1 table to drop")
			} else {
				assert.NotContains(t, out, "📋 Plan:",
					"a refused statement is not something the convergence is about to do")
			}
		})
	}
}

// A manual entry blocks the whole convergence rather than being held back on
// its own, so nothing is summarized as about to run while one is outstanding.
func TestOutputStorageSchemaPlan_ManualBlocksEverything(t *testing.T) {
	report := &apitypes.StorageSchemaReport{
		Dialect:      "postgres",
		Database:     "schemabot",
		Deployment:   "west",
		Environment:  "production",
		SchemaSource: "the schema embedded in v1.4.0",
		Outstanding: []apitypes.StorageSchemaStatement{
			{Table: "applies", Operation: "add_column", DDL: `ALTER TABLE "applies" ADD COLUMN "caller" text`},
		},
		Manual: []apitypes.StorageSchemaStatement{{
			Table:     "checks",
			Operation: "add_column",
			DDL:       `ALTER TABLE "checks" ADD COLUMN "head_sha" varchar(64) NOT NULL`,
			Reason:    "column is NOT NULL without a DEFAULT",
		}},
	}

	out := captureStdout(func() {
		require.NoError(t, outputStorageSchemaPlan(report, false, nil))
	})

	assert.Contains(t, out, "Database: schemabot (deployment west)")
	assert.Contains(t, out, "Production", "the environment heads the section, as it does in every plan")
	assert.Contains(t, out, glyph.Attention+" Needs manual remediation")
	assert.Contains(t, out, "1. checks: column is NOT NULL without a DEFAULT")
	assert.NotContains(t, out, "📋 Plan:",
		"the convergence refuses the whole drift set, so nothing below the manual entry runs either")
}

// A clean convergence states that nothing is left, rather than leaving an
// operator to read the absence of a section as good news.
func TestOutputStorageSchemaConvergence_Clean(t *testing.T) {
	planned := &apitypes.StorageSchemaReport{
		Dialect:      "mysql",
		Database:     "schemabot",
		Host:         "db-1.example",
		SchemaSource: "the schema embedded in v1.4.0",
		Outstanding: []apitypes.StorageSchemaStatement{
			{Table: "applies", Operation: "alter_table", DDL: "ALTER TABLE `applies` ADD COLUMN `caller` varchar(255) NOT NULL DEFAULT ''"},
			{Table: "checks", Operation: "create_table", DDL: "CREATE TABLE `checks` (`id` BIGINT UNSIGNED AUTO_INCREMENT, PRIMARY KEY (`id`))"},
		},
	}
	remaining := &apitypes.StorageSchemaReport{Dialect: "mysql", Database: "schemabot", Host: "db-1.example", Converged: true}

	out := captureStdout(func() {
		require.NoError(t, outputStorageSchemaConvergence(planned, remaining, false))
	})

	assert.Equal(t, "✓ Ran 2 statements against schemabot on db-1.example. Nothing is outstanding.\n\n", out)
}

// An unattended convergence prints the plan it ran, because no preview was
// shown before it and the run has to be self-describing afterwards.
func TestOutputStorageSchemaConvergence_PrintsThePlanItRan(t *testing.T) {
	planned := &apitypes.StorageSchemaReport{
		Dialect:      "mysql",
		Database:     "schemabot",
		Host:         "db-1.example",
		SchemaSource: "the schema embedded in v1.4.0",
		Outstanding: []apitypes.StorageSchemaStatement{
			{Table: "applies", Operation: "alter_table", DDL: "ALTER TABLE `applies` ADD COLUMN `caller` varchar(255) NOT NULL DEFAULT ''"},
		},
	}
	remaining := &apitypes.StorageSchemaReport{Dialect: "mysql", Database: "schemabot", Host: "db-1.example", Converged: true}

	out := captureStdout(func() {
		require.NoError(t, outputStorageSchemaConvergence(planned, remaining, true))
	})

	assert.Contains(t, out, "MySQL Schema Change Apply")
	assert.Contains(t, out, "~ applies")
	assert.Contains(t, out, "✓ Ran 1 statement against schemabot on db-1.example. Nothing is outstanding.")
}

// A convergence that left statements behind prints them with the reason they
// did not run, so the operator's next move is on the screen rather than in the
// documentation.
func TestOutputStorageSchemaConvergence_LeftBehind(t *testing.T) {
	planned := &apitypes.StorageSchemaReport{
		Dialect:      "mysql",
		Database:     "schemabot",
		Host:         "db-1.example",
		SchemaSource: "the schema embedded in v1.4.0",
		Outstanding:  []apitypes.StorageSchemaStatement{{Table: "applies", Operation: "alter_table", DDL: "ALTER TABLE `applies` ADD COLUMN `caller` varchar(255) NOT NULL DEFAULT ''"}},
	}
	remaining := &apitypes.StorageSchemaReport{
		Dialect:  "mysql",
		Database: "schemabot",
		Host:     "db-1.example",
		Destructive: []apitypes.StorageSchemaStatement{{
			Table:     "stale_state",
			Operation: "drop_table",
			DDL:       "DROP TABLE `stale_state`",
			Reason:    "DROP TABLE destroys data",
		}},
	}

	out := captureStdout(func() {
		require.NoError(t, outputStorageSchemaConvergence(planned, remaining, false))
	})

	assert.Contains(t, out, "✓ Ran 1 statement against schemabot on db-1.example.")
	assert.NotContains(t, out, "Nothing is outstanding")
	assert.Contains(t, out, glyph.Refused+" Destructive changes refused")
	assert.Contains(t, out, "1. stale_state: DROP TABLE destroys data")
	assert.Contains(t, out, "--allow-destructive")
}

// The summary line counts what the convergence would actually run. Promising
// tables it will refuse to touch is worse than promising nothing.
func TestStorageSchemaRunnable(t *testing.T) {
	outstanding := []apitypes.StorageSchemaStatement{{Table: "applies", Operation: "alter_table", DDL: "ALTER TABLE `applies` ADD COLUMN `caller` text"}}
	destructive := []apitypes.StorageSchemaStatement{{Table: "stale_state", Operation: "drop_table", DDL: "DROP TABLE `stale_state`", Reason: "DROP TABLE destroys data"}}

	runnable := func(report *apitypes.StorageSchemaReport) int {
		outChanges, err := storageSchemaChanges(report.Outstanding)
		require.NoError(t, err)
		destChanges, err := storageSchemaChanges(report.Destructive)
		require.NoError(t, err)
		return len(storageSchemaRunnable(report, outChanges, destChanges))
	}

	assert.Equal(t, 1, runnable(&apitypes.StorageSchemaReport{Outstanding: outstanding, Destructive: destructive}),
		"a refused destructive statement is not something the convergence will run")
	assert.Equal(t, 2, runnable(&apitypes.StorageSchemaReport{Outstanding: outstanding, Destructive: destructive, DestructiveAllowed: true}))
	assert.Equal(t, 0, runnable(&apitypes.StorageSchemaReport{
		Outstanding: outstanding,
		Destructive: destructive,
		Manual:      []apitypes.StorageSchemaStatement{{Table: "checks", Operation: "add_column", Reason: "column is NOT NULL without a DEFAULT"}},
	}), "a manual entry stops the whole drift set, not just itself")
}

// The report's operation vocabulary maps onto the plan's three change symbols.
// An operation with no mapping is an error: the symbol and the summary both
// come from it, so guessing would misreport what is about to happen to a table.
func TestStorageSchemaChangeType(t *testing.T) {
	for _, tc := range []struct {
		operation string
		want      string
	}{
		{"create_table", "CREATE"},
		{"drop_table", "DROP"},
		{"alter_table", "ALTER"},
		{"add_column", "ALTER"},
		{"create_index", "ALTER"},
		// Spacing and case come from whichever engine wrote the report, so
		// neither decides whether a statement can be rendered.
		{" Alter_Table ", "ALTER"},
	} {
		changeType, err := storageSchemaChangeType(tc.operation)
		require.NoError(t, err, tc.operation)
		assert.Equal(t, tc.want, changeType, tc.operation)
	}

	_, err := storageSchemaChangeType("rename_table")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unexpected storage schema operation "rename_table"`)

	_, err = storageSchemaChanges([]apitypes.StorageSchemaStatement{{Table: "applies", Operation: "create_view"}})
	require.Error(t, err, "an unrenderable statement fails the command instead of printing a wrong symbol")
}

// The title names the family SchemaBot stores its own state on, which is not
// one of the two a schema change plan titles itself with.
func TestStorageSchemaEngineLabel(t *testing.T) {
	assert.Equal(t, "MySQL", storageSchemaEngineLabel("mysql"))
	assert.Equal(t, "PostgreSQL", storageSchemaEngineLabel("postgres"))
	assert.Equal(t, "Storage", storageSchemaEngineLabel(""),
		"a report that names no dialect still gets a title rather than a blank one")
}

// A plan always compares the database against a release the operator named, so
// its next step is never `storage apply`: an apply converges the schema of the
// binary that runs it, which is not necessarily the schema the plan describes.
// The hint names the two ways to converge the named release instead.
func TestStorageSchemaPlanHints(t *testing.T) {
	hints := storageSchemaPlanHints(&apitypes.StorageSchemaReport{
		Database:     "schemabot",
		Host:         "db-1.example",
		Dialect:      "mysql",
		SchemaSource: "the schema files of release v1.4.0",
	})
	require.Len(t, hints, 1)
	assert.Contains(t, hints[0], "schemabot on db-1.example")
	assert.Contains(t, hints[0], "the schema files of release v1.4.0")
	assert.Contains(t, hints[0], "run that release's binary")
	assert.NotContains(t, hints[0], "storage apply")
}

// A report is labelled with the database, the server it is on, its family, and
// the deployment it came from, so an error naming it is unambiguous. The header
// box drops the family and the environment, which it states on their own lines.
func TestStorageSchemaHeaderDatabase(t *testing.T) {
	full := &apitypes.StorageSchemaReport{
		Database: "schemabot", Host: "10.0.0.7", Dialect: "postgres", Deployment: "west", Environment: "production",
	}
	assert.Equal(t, "schemabot on 10.0.0.7 (postgres), deployment west in production", storageSchemaDatabaseLabel(full))
	assert.Equal(t, "schemabot on 10.0.0.7 (deployment west)", storageSchemaHeaderDatabase(full))
	assert.Equal(t, "schemabot (mysql)", storageSchemaDatabaseLabel(&apitypes.StorageSchemaReport{
		Database: "schemabot", Dialect: "mysql",
	}), "a server that reports no name of its own is left out rather than guessed at")
	assert.Equal(t, "schemabot on db-1.example (mysql)", storageSchemaDatabaseLabel(&apitypes.StorageSchemaReport{
		Database: "schemabot", Host: "db-1.example", Dialect: "mysql",
	}))
	assert.Equal(t, "the storage database", storageSchemaDatabaseLabel(&apitypes.StorageSchemaReport{}))
	assert.Equal(t, "the storage database", storageSchemaHeaderDatabase(&apitypes.StorageSchemaReport{}),
		"a report with no database name still reads as a sentence")
}

// Every list entry names what stands in the way of its statement. A report that
// somehow carried no reason still says what the statement does, rather than
// printing a table name against an empty line.
func TestStorageSchemaNotices(t *testing.T) {
	notices := storageSchemaNotices([]apitypes.StorageSchemaStatement{
		{Table: "stale_state", Operation: "drop_table", DDL: "DROP TABLE `stale_state`", Reason: "DROP TABLE destroys data"},
		{Table: "plans", Operation: "drop_table", DDL: "DROP TABLE `plans_old`"},
	})
	require.Len(t, notices, 2)
	assert.Equal(t, "DROP TABLE destroys data", notices[0].Reason)
	assert.Equal(t, "DROP TABLE `plans_old`", notices[1].Reason)
	assert.Empty(t, storageSchemaNotices(nil))
}

// A plan's sections come out in a fixed order — what runs, what is refused,
// what needs a person — so two plans of the same database read the same way.
func TestOutputStorageSchemaPlan_SectionOrder(t *testing.T) {
	report := &apitypes.StorageSchemaReport{
		Dialect:      "mysql",
		Database:     "schemabot",
		SchemaSource: "the schema embedded in v1.4.0",
		Outstanding:  []apitypes.StorageSchemaStatement{{Table: "applies", Operation: "alter_table", DDL: "ALTER TABLE `applies` ADD COLUMN `caller` varchar(255) NOT NULL DEFAULT ''"}},
		Destructive:  []apitypes.StorageSchemaStatement{{Table: "stale_state", Operation: "drop_table", DDL: "DROP TABLE `stale_state`", Reason: "DROP TABLE destroys data"}},
		Manual:       []apitypes.StorageSchemaStatement{{Table: "checks", Operation: "add_column", DDL: "ALTER TABLE `checks` ADD COLUMN `head_sha` varchar(64) NOT NULL", Reason: "column is NOT NULL without a DEFAULT"}},
	}

	out := captureStdout(func() {
		require.NoError(t, outputStorageSchemaPlan(report, false, nil))
	})

	assert.Less(t, strings.Index(out, "~ applies"), strings.Index(out, "Destructive changes"))
	assert.Less(t, strings.Index(out, "Destructive changes"), strings.Index(out, "Needs manual remediation"))
}
