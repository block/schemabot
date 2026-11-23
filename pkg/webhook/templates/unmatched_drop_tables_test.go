package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const unmatchedDropLine = "⚠️ `drop_tables` entry `_jobs_olds` names no table on the target, so this plan drops nothing for it. Fix the name, or remove the entry if the table is already gone."

// unmatchedDropPlanData is a MySQL plan for a drop_tables list with one entry
// naming a live leftover, planned as a drop, and one with a typo that names
// no table.
func unmatchedDropPlanData() PlanCommentData {
	return PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true, DatabaseType: "mysql",
		Changes:             []KeyspaceChangeData{{Keyspace: "app", Statements: []string{"DROP TABLE `_jobs_old`"}}},
		UnmatchedDropTables: []string{"_jobs_olds"},
	}
}

// The entry that names no table is reported under the plan summary, and the
// entry that planned a drop is not.
func TestRenderPlanComment_UnmatchedDropTablesEntryReported(t *testing.T) {
	out := RenderPlanComment(unmatchedDropPlanData())
	summaryAt := strings.Index(out, "📋 **Plan**:")
	lineAt := strings.Index(out, unmatchedDropLine+"\n")
	assert.Greater(t, lineAt, summaryAt, "the line follows the plan summary:\n%s", out)
	assert.NotContains(t, out, "entry `_jobs_old` names no table")
}

// A plan whose only entry has a typo is otherwise clean, and that is where the
// line matters most: without it the clean plan reads as if the table were seen
// and kept.
func TestRenderPlanComment_UnmatchedDropTablesEntryReportedOnNoChanges(t *testing.T) {
	data := unmatchedDropPlanData()
	data.Changes = nil
	data.ExemptTables = []ExemptTablesData{{Namespace: "app", Tables: []string{"_jobs_old"}, Reason: "leading underscore"}}
	out := RenderPlanComment(data)
	assert.Contains(t, out, "✅ **No schema changes detected**")
	assert.Contains(t, out, "ℹ️ Ignored tables in namespace `app` (leading underscore): `_jobs_old`\n\n"+unmatchedDropLine+"\n")
}

// An entry wrapped in backticks renders as one balanced code span, so the
// reader sees the backticks that kept the entry from matching.
func TestRenderPlanComment_UnmatchedDropTablesEntryKeepsItsBackticks(t *testing.T) {
	data := unmatchedDropPlanData()
	data.UnmatchedDropTables = []string{"`_jobs_old`"}
	assert.Contains(t, RenderPlanComment(data), "`drop_tables` entry `` `_jobs_old` `` names no table on the target")
}

// When every environment reports the same entry, the all-clean multi-env
// comment states it once; when they differ, each line names its environment.
func TestRenderMultiEnvPlanComment_UnmatchedDropTables(t *testing.T) {
	render := func(production []string) string {
		staging := PlanCommentData{Database: "testapp", Environment: "staging", IsMySQL: true, UnmatchedDropTables: []string{"_jobs_olds"}}
		prod := PlanCommentData{Database: "testapp", Environment: "production", IsMySQL: true, UnmatchedDropTables: production}
		return RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
			Database: "testapp", Environments: []string{"staging", "production"},
			Plans: map[string]*PlanCommentData{"staging": &staging, "production": &prod},
		})
	}

	same := render([]string{"_jobs_olds"})
	assert.Equal(t, 1, strings.Count(same, unmatchedDropLine))

	divergent := render(nil)
	assert.Contains(t, divergent, "⚠️ **Staging**: `drop_tables` entry `_jobs_olds` names no table on the target")
	assert.NotContains(t, divergent, "**Production**: `drop_tables`")
}

// A plan whose entries all matched renders exactly as one without drop_tables.
func TestRenderPlanComment_NoUnmatchedDropTablesNoLine(t *testing.T) {
	data := unmatchedDropPlanData()
	data.UnmatchedDropTables = nil
	assert.NotContains(t, RenderPlanComment(data), "names no table")
}
