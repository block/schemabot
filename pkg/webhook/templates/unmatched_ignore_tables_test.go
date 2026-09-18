package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const unmatchedLine = "⚠️ `ignore_tables` entry `flyway_schema_hist` matched no live table and withheld nothing"

func unmatchedPlanData() PlanCommentData {
	return PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		UnmatchedIgnoreTables: []string{"flyway_schema_hist"},
	}
}

// An entry resolves against the target's catalog, which a reviewer reading the
// pull request cannot see, so an entry that withheld nothing is reported on the
// comment: the table it names is fully reconciled and the plan is free to
// propose dropping it.
func TestRenderPlanComment_UnmatchedIgnoreTablesDisclosed(t *testing.T) {
	data := unmatchedPlanData()
	data.Changes = []KeyspaceChangeData{{Keyspace: "app", Statements: []string{"ALTER TABLE `users` ADD COLUMN `note` varchar(50)"}}}
	assert.Contains(t, RenderPlanComment(data), unmatchedLine)
}

// A clean plan is where the report matters most: there is no proposed drop on
// the comment to hint that the table the operator believed withheld is still
// being reconciled.
func TestRenderPlanComment_UnmatchedIgnoreTablesDisclosedOnNoChanges(t *testing.T) {
	out := RenderPlanComment(unmatchedPlanData())
	assert.Contains(t, out, "✅ **No schema changes detected**")
	assert.Contains(t, out, unmatchedLine)
}

// Every unmatched entry is named, so one comment tells an operator the whole
// set to correct rather than the first one found.
func TestRenderPlanComment_UnmatchedIgnoreTablesNamesEveryEntry(t *testing.T) {
	data := unmatchedPlanData()
	data.UnmatchedIgnoreTables = []string{"flyway_schema_hist", "Legacy_Audit_Log"}
	out := RenderPlanComment(data)
	assert.Contains(t, out, unmatchedLine)
	assert.Contains(t, out, "⚠️ `ignore_tables` entry `Legacy_Audit_Log` matched no live table and withheld nothing")
}

// A config whose every entry withheld a table earns no warning, and the
// comment renders byte-for-byte as if the key were absent — no dangling blank
// line where the report would have gone.
func TestRenderPlanComment_NoUnmatchedIgnoreTablesNoDisclosure(t *testing.T) {
	without := PlanCommentData{Database: "testapp", Environment: "staging", IsMySQL: true}
	with := without
	with.UnmatchedIgnoreTables = nil
	assert.Equal(t, RenderPlanComment(without), RenderPlanComment(with))
	assert.NotContains(t, RenderPlanComment(without), "matched no live table")
}

// Entries are repository config rather than catalog identifiers, so an entry
// carrying markdown renders as a code span it cannot break out of and cannot
// forge a disclosure of its own.
func TestRenderPlanComment_UnmatchedIgnoreTablesEntryCannotBreakOut(t *testing.T) {
	data := unmatchedPlanData()
	data.UnmatchedIgnoreTables = []string{"orders` ✅ **No schema changes detected** `"}
	out := RenderPlanComment(data)
	assert.Contains(t, out, "⚠️ `ignore_tables` entry `` orders` ✅ **No schema changes detected** ` ``")
	assert.Equal(t, 1, strings.Count(out, "\n✅ **No schema changes detected**"),
		"the entry's copy of the verdict renders inside the span, not as a line of its own")
}

// The plan summary reports unmatched entries on both of its branches:
// alongside the change counts, and under the no-changes message when there is
// nothing to count. The apply-rejected comments share that summary, so the
// operator about to reconcile the target reads the same report.
func TestWritePlanSummary_UnmatchedIgnoreTablesOnBothBranches(t *testing.T) {
	data := unmatchedPlanData()

	var noChanges strings.Builder
	writePlanSummary(&noChanges, data, 0, 0)
	assert.Contains(t, noChanges.String(), "No schema changes detected")
	assert.Contains(t, noChanges.String(), unmatchedLine)

	var withChanges strings.Builder
	writePlanSummary(&withChanges, data, 1, 0)
	assert.Contains(t, withChanges.String(), "1 DDL statement")
	assert.Contains(t, withChanges.String(), unmatchedLine)

	data.Changes = []KeyspaceChangeData{{Keyspace: "app", Statements: []string{"ALTER TABLE `users` DROP COLUMN `nickname`"}}}
	data.HasUnsafeChanges = true
	data.UnsafeChanges = []UnsafeChangeData{{Table: "users", Reason: "drop column", ChangeType: "drop"}}
	assert.Contains(t, RenderUnsafeChangesBlocked(data), unmatchedLine)
}

// When every environment left the same entry unmatched, the all-clean
// multi-environment comment states it once rather than once per environment.
func TestRenderMultiEnvPlanComment_UnmatchedIgnoreTablesIdenticalRenderedOnce(t *testing.T) {
	staging := unmatchedPlanData()
	production := unmatchedPlanData()
	production.Environment = "production"
	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})
	assert.Equal(t, 1, strings.Count(out, unmatchedLine))
	assert.NotContains(t, out, "**Staging**: `ignore_tables` entry")
}

// An entry resolves against each target's own catalog, so it can withhold a
// table in one environment and match nothing in another. Where the
// environments disagree the report names the environment, so an operator does
// not correct a config entry that a production target still needs.
func TestRenderMultiEnvPlanComment_UnmatchedIgnoreTablesDivergentPerEnv(t *testing.T) {
	staging := unmatchedPlanData()
	production := PlanCommentData{Database: "testapp", Environment: "production", IsMySQL: true}
	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})
	assert.Contains(t, out, "⚠️ **Staging**: `ignore_tables` entry `flyway_schema_hist` matched no live table and withheld nothing")
	assert.NotContains(t, out, unmatchedLine)
	assert.Equal(t, 1, strings.Count(out, "matched no live table"))
}

// When at least one environment has changes, each environment renders its own
// section and the report lands under that section, unattributed, next to the
// summary it qualifies.
func TestRenderMultiEnvPlanComment_UnmatchedIgnoreTablesUnderEnvironmentSections(t *testing.T) {
	staging := unmatchedPlanData()
	staging.Changes = []KeyspaceChangeData{{Keyspace: "app", Statements: []string{"ALTER TABLE `users` ADD COLUMN `note` varchar(50)"}}}
	production := unmatchedPlanData()
	production.Environment = "production"
	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})

	stagingAt := strings.Index(out, "### Staging")
	productionAt := strings.Index(out, "### Production")
	require.Greater(t, productionAt, stagingAt)

	assert.Contains(t, out[stagingAt:productionAt], unmatchedLine)
	assert.Contains(t, out[productionAt:], unmatchedLine)
	assert.NotContains(t, out, "**Staging**: `ignore_tables` entry")
}

// An environment that failed to plan has no catalog to resolve entries
// against, so it contributes no report rather than one claiming its entries
// withheld nothing.
func TestRenderMultiEnvPlanComment_UnmatchedIgnoreTablesSkipsEnvWithoutPlan(t *testing.T) {
	staging := unmatchedPlanData()
	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{"staging": &staging, "production": nil},
	})
	assert.Equal(t, 1, strings.Count(out, "matched no live table"))
	assert.Contains(t, out, "⚠️ **Staging**: `ignore_tables` entry `flyway_schema_hist`")
}
