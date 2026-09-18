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
		require.NoError(t, outputStorageSchemaPlan(report, false, "", nil))
	})

	assert.Contains(t, out, "MySQL Schema Change Plan")
	assert.Contains(t, out, "Database: schemabot on db-1.example")
	assert.Contains(t, out, "Schema: the schema files of release v1.4.0 in block/schemabot")
	assert.Contains(t, out, "+ checks")
	assert.Contains(t, out, "~ applies")
	assert.Contains(t, out, "📋 Plan: 1 table to create, 1 table to alter")
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
		require.NoError(t, outputStorageSchemaPlan(report, false, "", []string{"resolve the manual entries above before the next roll"}))
	})

	assert.Contains(t, out, "PostgreSQL Schema Change Plan")
	assert.Contains(t, out, "✓ No schema changes detected.")
	assert.NotContains(t, out, "resolve the manual entries above")
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
		heading string
		rerun   bool
	}{
		{name: "a plan discloses", heading: glyph.Attention + " Unsafe Changes Detected:"},
		{name: "an apply refuses", isApply: true, heading: glyph.Refused + " Apply blocked: 1 unsafe change(s) detected", rerun: true},
		{name: "consent in effect", allowed: true, heading: glyph.Escalation + " Unsafe Changes (destructive storage changes allowed)"},
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
				require.NoError(t, outputStorageSchemaPlan(report, tc.isApply, "storage apply --allow-unsafe", nil))
			})

			assert.Contains(t, out, tc.heading)
			assert.Contains(t, out, "1. stale_state: DROP TABLE destroys data")
			if tc.rerun {
				assert.Contains(t, out, "schemabot storage apply --allow-unsafe",
					"a blocked apply names the command that permits what it refused, carrying this one's target flags")
			}
			// The statement is printed and counted whatever its disposition,
			// the way `plan` prints and counts an unsafe change it will
			// refuse. What runs is what the sections under it say.
			assert.Contains(t, out, "- stale_state")
			assert.Contains(t, out, "📋 Plan: 1 table to drop")
		})
	}
}

// A manual entry is disclosed under the statements rather than in place of
// them: the plan renders the whole difference between the two schemas, and the
// notice says what stands in the way of converging it.
func TestOutputStorageSchemaPlan_ManualIsDisclosedUnderThePlan(t *testing.T) {
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
		require.NoError(t, outputStorageSchemaPlan(report, false, "storage apply --allow-unsafe", nil))
	})

	assert.Contains(t, out, "Database: schemabot (deployment west)")
	assert.Contains(t, out, "Production", "the environment heads the section, as it does in every plan")
	assert.Contains(t, out, "~ applies")
	assert.Contains(t, out, "~ checks", "the statement waiting on the remediation is printed, not withheld")
	assert.Contains(t, out, glyph.Attention+" Needs manual remediation")
	assert.Contains(t, out, "1. checks: column is NOT NULL without a DEFAULT")
	assert.Contains(t, out, "📋 Plan: 2 tables to alter")
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
		require.NoError(t, outputStorageSchemaConvergence(planned, remaining, false, "storage apply --allow-unsafe"))
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
		require.NoError(t, outputStorageSchemaConvergence(planned, remaining, true, "storage apply --allow-unsafe"))
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

	// The re-run command is the one that addressed this storage, target flags
	// and all: it is printed to be copied, and one that named a different
	// database than the refusal is about would be worse than none.
	rerun := "storage apply --deployment shard-a -e production --allow-unsafe"
	out := captureStdout(func() {
		require.NoError(t, outputStorageSchemaConvergence(planned, remaining, false, rerun))
	})

	assert.Contains(t, out, "✓ Ran 1 statement against schemabot on db-1.example.")
	assert.NotContains(t, out, "Nothing is outstanding")
	assert.Contains(t, out, glyph.Refused+" Apply blocked")
	assert.Contains(t, out, "1. stale_state: DROP TABLE destroys data")
	assert.Contains(t, out, "schemabot "+rerun)
}

// A convergence that ran nothing and left everything says so as a refusal. A ✓
// over a run that changed nothing is a success an operator has to disprove from
// the sections below it, and the one mid-incident reads the first line.
//
// Under --yes the plan is printed above the result, and what remains is what was
// planned, so the refusal is reported once rather than as two identical
// sections either side of the result line.
func TestOutputStorageSchemaConvergence_RefusedRunsSayNothingRan(t *testing.T) {
	gated := &apitypes.StorageSchemaReport{
		Dialect:      "postgres",
		Database:     "schemabot",
		Host:         "db-1.example",
		SchemaSource: "the schema embedded in v1.4.0",
		Outstanding: []apitypes.StorageSchemaStatement{
			{Table: "applies", Operation: "add_column", DDL: "ALTER TABLE applies ADD COLUMN caller text"},
		},
		Manual: []apitypes.StorageSchemaStatement{{
			Table:     "checks",
			Operation: "add_column",
			DDL:       "ALTER TABLE checks ADD COLUMN head_sha varchar(64) NOT NULL",
			Reason:    "definition is NOT NULL without a DEFAULT",
		}},
	}

	out := captureStdout(func() {
		require.NoError(t, outputStorageSchemaConvergence(gated, gated, false, "storage apply --allow-unsafe"))
	})
	assert.Contains(t, out, glyph.Refused+" Ran no statements against schemabot on db-1.example; the storage schema is unchanged.")
	assert.NotContains(t, out, "✓ Ran", "nothing ran, so nothing succeeded")
	assert.Equal(t, 1, strings.Count(out, "Needs manual remediation"))

	withPlan := captureStdout(func() {
		require.NoError(t, outputStorageSchemaConvergence(gated, gated, true, "storage apply --allow-unsafe"))
	})
	assert.Contains(t, withPlan, "PostgreSQL Schema Change Apply")
	assert.Contains(t, withPlan, glyph.Refused+" Ran no statements against")
	assert.Equal(t, 1, strings.Count(withPlan, "Needs manual remediation"),
		"the plan above already carries the refusal; repeating it below reports one refusal twice")
}

// A convergence that applied something and refused something reports the
// refusal once too. The count of what ran is the only fact the result line
// adds; the refusal beneath it was already on screen in the plan, and printing
// it again puts "Apply blocked" above a line saying a statement ran.
func TestOutputStorageSchemaConvergence_MixedRunsReportTheRefusalOnce(t *testing.T) {
	refused := apitypes.StorageSchemaStatement{
		Table: "stale_state", Operation: "drop_table", DDL: "DROP TABLE `stale_state`",
		Reason: `Unsafe operation detected: "DROP TABLE ` + "`stale_state`" + `"`,
	}
	planned := &apitypes.StorageSchemaReport{
		Dialect:      "mysql",
		Database:     "schemabot",
		Host:         "db-1.example",
		SchemaSource: "the schema embedded in v1.4.0",
		Outstanding: []apitypes.StorageSchemaStatement{
			{Table: "applies", Operation: "add_column", DDL: "ALTER TABLE `applies` ADD COLUMN `caller` text"},
		},
		Destructive: []apitypes.StorageSchemaStatement{refused},
	}
	remaining := &apitypes.StorageSchemaReport{
		Dialect:      planned.Dialect,
		Database:     planned.Database,
		Host:         planned.Host,
		SchemaSource: planned.SchemaSource,
		Destructive:  []apitypes.StorageSchemaStatement{refused},
	}
	const rerun = "storage apply --allow-unsafe"

	withPlan := captureStdout(func() {
		require.NoError(t, outputStorageSchemaConvergence(planned, remaining, true, rerun))
	})
	assert.Contains(t, withPlan, "✓ Ran 1 statement against schemabot on db-1.example.")
	assert.Equal(t, 1, strings.Count(withPlan, glyph.Refused+" Apply blocked"),
		"the plan above already carries the refusal; repeating it below reports one refusal twice")
	assert.Equal(t, 1, strings.Count(withPlan, "schemabot "+rerun),
		"one refusal means one copyable re-run command")

	// Attended, the plan was shown before the prompt rather than by this
	// function, so what remains is printed here and is the only copy on screen.
	out := captureStdout(func() {
		require.NoError(t, outputStorageSchemaConvergence(planned, remaining, false, rerun))
	})
	assert.Contains(t, out, "✓ Ran 1 statement against")
	assert.Equal(t, 1, strings.Count(out, glyph.Refused+" Apply blocked"))
	assert.Equal(t, 1, strings.Count(out, "schemabot "+rerun))
	assert.Contains(t, lastLineOf(out), "schemabot "+rerun,
		"the command an operator copies is the last thing on screen, not a paragraph above it")
}

// lastLineOf is the final non-empty line of rendered output, which is where a
// copyable command has to land to be the one an operator's eye stops on.
func lastLineOf(out string) string {
	lines := strings.Split(strings.TrimRight(out, "\n \t"), "\n")
	return lines[len(lines)-1]
}

// The summary line counts the difference between the two schemas, whatever
// each statement's disposition — the count a reader checks the printed sections
// against. What will actually run is what the notices under them say.
func TestOutputStorageSchemaPlan_SummaryCountsTheWholeDifference(t *testing.T) {
	report := &apitypes.StorageSchemaReport{
		Dialect:      "mysql",
		Database:     "schemabot",
		SchemaSource: "the schema embedded in v1.4.0",
		Outstanding:  []apitypes.StorageSchemaStatement{{Table: "applies", Operation: "alter_table", DDL: "ALTER TABLE `applies` ADD COLUMN `caller` text"}},
		Destructive:  []apitypes.StorageSchemaStatement{{Table: "stale_state", Operation: "drop_table", DDL: "DROP TABLE `stale_state`", Reason: "DROP TABLE destroys data"}},
		Manual: []apitypes.StorageSchemaStatement{{
			Table: "checks", Operation: "add_column",
			DDL:    "ALTER TABLE `checks` ADD COLUMN `head_sha` varchar(64) NOT NULL",
			Reason: "column is NOT NULL without a DEFAULT",
		}},
	}

	out := captureStdout(func() {
		require.NoError(t, outputStorageSchemaPlan(report, false, "storage apply --allow-unsafe", nil))
	})

	assert.Contains(t, out, "📋 Plan: 2 tables to alter, 1 table to drop")
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

	// A deployment with no environment ends the label rather than trailing a
	// preposition with nothing after it.
	assert.Equal(t, "schemabot (postgres), deployment west", storageSchemaDatabaseLabel(&apitypes.StorageSchemaReport{
		Database: "schemabot", Dialect: "postgres", Deployment: "west",
	}))
}

// Every list entry names what stands in the way of its statement. A statement
// with no reason of its own — a gated one, held only by the remediation below
// it — is named by what it would do, so the entry is a line rather than a table
// name against a blank one or a block of DDL.
func TestStorageSchemaNotices(t *testing.T) {
	notices := storageSchemaNotices([]apitypes.StorageSchemaStatement{
		{Table: "stale_state", Operation: "drop_table", DDL: "DROP TABLE `stale_state`", Reason: "DROP TABLE destroys data"},
		{Table: "plans", Operation: "create_table", DDL: "CREATE TABLE `plans` (\n  `id` bigint\n)"},
	})
	require.Len(t, notices, 2)
	assert.Equal(t, "DROP TABLE destroys data", notices[0].Reason)
	assert.Empty(t, notices[1].Reason)
	assert.Equal(t, "create table", notices[1].ChangeType,
		"a statement with no reason is named by what it does, not by the DDL that does it")
	assert.Empty(t, storageSchemaNotices(nil))
}

// A manual reason is one sentence written for one change, and the statement it
// names is printed as SQL above it. The numbered entry stays a line: a CREATE
// TABLE on one of them would put a whole schema file, newlines and all, inside
// the list the operator is counting through.
func TestOutputStorageSchemaPlan_ManualEntryIsOneLinePerStatement(t *testing.T) {
	report := &apitypes.StorageSchemaReport{
		Dialect:      "postgres",
		Database:     "schemabot",
		SchemaSource: "the schema embedded in v1.4.0",
		Outstanding: []apitypes.StorageSchemaStatement{
			{Table: "plans", Operation: "create_table", DDL: "CREATE TABLE plans (\n  id bigint PRIMARY KEY,\n  created_at timestamptz NOT NULL\n)"},
			{Table: "applies", Operation: "add_column", DDL: "ALTER TABLE applies ADD COLUMN caller text"},
		},
		Manual: []apitypes.StorageSchemaStatement{{
			Table:     "checks",
			Operation: "add_column",
			DDL:       "ALTER TABLE checks ADD COLUMN head_sha varchar(64) NOT NULL",
			Reason:    "definition is NOT NULL without a DEFAULT; add it manually or ship the column with a DEFAULT",
		}},
	}

	out := captureStdout(func() {
		require.NoError(t, outputStorageSchemaPlan(report, false, "storage apply --allow-unsafe", nil))
	})

	assert.Contains(t, out, "+ plans", "every statement in the report is printed as SQL")
	assert.Contains(t, out, "~ checks")

	// Split on its semicolon the reason would read as two separate things to
	// fix, one of them the remedy for the other.
	_, notice, found := strings.Cut(out, "Needs manual remediation")
	require.True(t, found, "the plan discloses the manual entry")
	assert.Contains(t, notice, "1. checks: definition is NOT NULL without a DEFAULT; add it manually or ship the column with a DEFAULT")
	assert.NotContains(t, notice, "2.", "one entry per statement, so the numbering ends where the list does")
	assert.NotContains(t, notice, "CREATE TABLE",
		"the notice names statements; the DDL is in the section above it")
}

// A destructive statement's reason is one finding, however it is punctuated.
// The plan joins every linter that erred on one statement into a single reason,
// so a combined ALTER that both loses data and removes an index carries two
// semicolon-joined messages about the one statement. Numbered as two findings,
// the second would read as a separate statement that was also refused.
func TestOutputStorageSchemaPlan_DestructiveReasonIsOneFinding(t *testing.T) {
	reason := "Unsafe operation detected: \"DROP COLUMN\"; Index \"idx_state\" should be made invisible before dropping to ensure it's not needed"
	report := &apitypes.StorageSchemaReport{
		Dialect:      "mysql",
		Database:     "schemabot",
		SchemaSource: "the schema embedded in v1.4.0",
		Destructive: []apitypes.StorageSchemaStatement{{
			Table:     "applies",
			Operation: "alter_table",
			DDL:       "ALTER TABLE `applies` DROP COLUMN `caller`, DROP INDEX `idx_state`",
			Reason:    reason,
		}},
	}

	out := captureStdout(func() {
		require.NoError(t, outputStorageSchemaPlan(report, true, "storage apply --allow-unsafe", nil))
	})

	assert.Contains(t, out, "1 unsafe change(s) detected", "one statement is one finding")
	assert.Contains(t, out, "  1. applies: "+reason+"\n")
	assert.NotContains(t, out, "2. applies:")
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
	}

	out := captureStdout(func() {
		require.NoError(t, outputStorageSchemaPlan(report, false, "storage apply --allow-unsafe", nil))
	})

	assert.Less(t, strings.Index(out, "~ applies"), strings.Index(out, "Unsafe Changes Detected"))
	assert.Less(t, strings.Index(out, "Unsafe Changes Detected"), strings.Index(out, "📋 Plan:"),
		"a plan discloses before it summarizes, the order `plan` prints them in")

	// An apply's refusal is the exception: it carries the command that permits
	// what was refused, so it goes last, where `apply` prints its own. A
	// summary of tables the run will not touch must not be the final word.
	out = captureStdout(func() {
		require.NoError(t, outputStorageSchemaPlan(report, true, "storage apply --allow-unsafe", nil))
	})

	assert.Less(t, strings.Index(out, "📋 Plan:"), strings.Index(out, "Apply blocked"))
	assert.Less(t, strings.Index(out, "Apply blocked"), strings.Index(out, "schemabot storage apply --allow-unsafe"))
}

// A manual entry outranks the destructive refusal. Both statements are printed
// and counted, but the remedy on screen is the one that unblocks: passing
// --allow-unsafe would permit the DROP and still converge nothing.
func TestOutputStorageSchemaPlan_ManualOutranksTheDestructiveRefusal(t *testing.T) {
	report := &apitypes.StorageSchemaReport{
		Dialect:      "mysql",
		Database:     "schemabot",
		SchemaSource: "the schema embedded in v1.4.0",
		Destructive: []apitypes.StorageSchemaStatement{
			{Table: "stale_state", Operation: "drop_table", DDL: "DROP TABLE `stale_state`", Reason: "DROP TABLE destroys data"},
		},
		Manual: []apitypes.StorageSchemaStatement{{
			Table: "checks", Operation: "add_column",
			DDL:    "ALTER TABLE `checks` ADD COLUMN `head_sha` varchar(64) NOT NULL",
			Reason: "column is NOT NULL without a DEFAULT",
		}},
	}

	for _, isApply := range []bool{false, true} {
		out := captureStdout(func() {
			require.NoError(t, outputStorageSchemaPlan(report, isApply, "storage apply --allow-unsafe", nil))
		})

		assert.Contains(t, out, "- stale_state")
		assert.Contains(t, out, "~ checks")
		assert.Contains(t, out, glyph.Attention+" Unsafe Changes Detected:",
			"the destructive statement is still disclosed as destructive")
		assert.Contains(t, out, "Needs manual remediation")
		assert.NotContains(t, out, "Apply blocked")
		assert.NotContains(t, out, "--allow-unsafe",
			"the flag would permit the DROP and converge nothing, so it is not the remedy to offer")
		assert.Contains(t, out, "📋 Plan: 1 table to alter, 1 table to drop")
	}
}

// Each disposition renders its own SQL section. On MySQL the formatter
// combines a table's alters into one statement, so an ALTER split into a safe
// half and a refused half would be recombined into a statement nothing is
// going to run if the two were rendered together.
func TestOutputStorageSchemaPlan_SplitAlterStaysSplit(t *testing.T) {
	report := &apitypes.StorageSchemaReport{
		Dialect:      "mysql",
		Database:     "schemabot",
		SchemaSource: "the schema embedded in v1.4.0",
		Outstanding:  []apitypes.StorageSchemaStatement{{Table: "applies", Operation: "alter_table", DDL: "ALTER TABLE `applies` ADD COLUMN `caller` varchar(255) NOT NULL DEFAULT ''"}},
		Destructive: []apitypes.StorageSchemaStatement{{
			Table: "applies", Operation: "alter_table",
			DDL:    "ALTER TABLE `applies` DROP COLUMN `legacy_owner`",
			Reason: "DROP COLUMN destroys data",
		}},
	}

	out := captureStdout(func() {
		require.NoError(t, outputStorageSchemaPlan(report, false, "storage apply --allow-unsafe", nil))
	})

	assert.Equal(t, 2, strings.Count(out, "~ applies"),
		"the half that runs and the half that is refused are two statements, and the plan shows two")
	// Keywords are colorized in place, so the assertions match the parts of the
	// statement the formatter leaves alone.
	assert.Contains(t, out, "COLUMN `caller`")
	assert.Contains(t, out, "COLUMN `legacy_owner`")
	assert.Contains(t, out, "📋 Plan: 1 table to alter",
		"two statements against one table are one table in the summary")
}

// A dialect that needs several statements for one table still summarizes in
// tables. PostgreSQL emits an add_column per missing column, so a table short
// two columns must not read as two tables to alter.
func TestOutputStorageSchemaPlan_SummarizesInTables(t *testing.T) {
	report := &apitypes.StorageSchemaReport{
		Dialect:  "postgres",
		Database: "schemabot",
		Outstanding: []apitypes.StorageSchemaStatement{
			{Table: "applies", Operation: "add_column", DDL: "ALTER TABLE applies ADD COLUMN caller text"},
			{Table: "applies", Operation: "add_column", DDL: "ALTER TABLE applies ADD COLUMN driver text"},
			{Table: "applies", Operation: "create_index", DDL: "CREATE INDEX applies_caller ON applies (caller)"},
			{Table: "checks", Operation: "create_table", DDL: "CREATE TABLE checks (id bigint PRIMARY KEY)"},
		},
	}
	out := captureStdout(func() {
		require.NoError(t, outputStorageSchemaPlan(report, false, "storage apply --allow-unsafe", nil))
	})
	assert.Contains(t, out, "📋 Plan: 1 table to create, 1 table to alter")
	assert.Contains(t, out, "COLUMN driver", "every statement is still printed")
}
