package templates

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/presentation"
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
// comment states it once. An entry only one environment reports is not
// stated: the other environment has the table.
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
	assert.Equal(t, 1, strings.Count(same, unmatchedDropLine), same)

	divergent := render(nil)
	assert.NotContains(t, divergent, "names no table", divergent)
}

// Staging has already applied the drop of _jobs_old, so its plan is clean and
// reports the entry as naming no table, while production still has the table
// and plans the drop. The comment shows production's drop and does not tell
// the reader to remove the entry that drop depends on.
func TestRenderMultiEnvPlanComment_UnmatchedDropTablesNotReportedBesideAnotherEnvironmentsDrop(t *testing.T) {
	staging := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true, DatabaseType: "mysql",
		UnmatchedDropTables: []string{"_jobs_old", "_jobs_olds"},
	}
	prod := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true, DatabaseType: "mysql",
		Changes:             []KeyspaceChangeData{{Keyspace: "app", Statements: []string{"DROP TABLE `_jobs_old`"}}},
		UnmatchedDropTables: []string{"_jobs_olds"},
	}
	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{"staging": &staging, "production": &prod},
	})

	assert.Contains(t, out, "DROP TABLE `_jobs_old`", out)
	assert.NotContains(t, out, "entry `_jobs_old` names no table", out)
	assert.Equal(t, 2, strings.Count(out, unmatchedDropLine+"\n"), "the typo every environment reports stays under each section:\n%s", out)
}

// An environment whose plan failed says nothing about its tables, so an entry
// the other environment reports is not stated beside it.
func TestRenderMultiEnvPlanComment_UnmatchedDropTablesNotReportedBesideAFailedPlan(t *testing.T) {
	staging := unmatchedDropPlanData()
	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", Environments: []string{"staging", "production"},
		Plans:  map[string]*PlanCommentData{"staging": &staging},
		Errors: map[string]string{"production": "plan failed; see server logs"},
	})

	assert.Contains(t, out, "DROP TABLE `_jobs_old`", out)
	assert.NotContains(t, out, "names no table", out)
}

// A clean plan whose only disclosure is an entry that names no table, as after
// the drop has landed and nothing else is left on the target, still says so
// under the no-changes message.
func TestRenderPlanComment_UnmatchedDropTablesOnlyDisclosureOnCleanPlan(t *testing.T) {
	data := unmatchedDropPlanData()
	data.Changes = nil
	out := RenderPlanComment(data)
	assert.Contains(t, out, "✅ **No schema changes detected**\n\n"+unmatchedDropLine+"\n", out)
}

// unmatchedDropRolloutDrift is a clean rollout of two independent targets in
// which the primary has neither _jobs_old nor _jobs_olds, while the other
// target still has _jobs_old and plans the drop. The rollout lists only the
// entry no target matched.
func unmatchedDropRolloutDrift() *DeploymentDriftData {
	return &DeploymentDriftData{
		Computed: true, Clean: true, Independent: true,
		Deployments: []DeploymentDriftEntry{
			{Deployment: "primary", Target: "app_1", Primary: true, Class: "planned"},
			{Deployment: "primary", Target: "app_2", Class: "planned"},
		},
		Plans: []DeploymentPlanGroup{
			{Members: []string{"primary/app_1"}, Primary: true},
			{Members: []string{"primary/app_2"}, Changes: []KeyspaceChangeData{{Keyspace: "app", Statements: []string{"DROP TABLE `_jobs_old`"}}}},
		},
		UnmatchedDropTables: []string{"_jobs_olds"},
	}
}

// A rollout plan reports an unmatched drop_tables entry only when no target
// has the table. _jobs_old is missing on the primary target, but the other
// target drops it, so the comment shows that DROP and no line telling the
// reader to remove the entry it depends on. _jobs_olds names no table on
// either target, so it is reported once. This holds whether the comment
// renders every target's plan, the primary's plan alone, or the
// environment's section of a multi-environment comment.
func TestRenderPlanComment_RolloutReportsOnlyEntriesNoTargetMatched(t *testing.T) {
	withTargetPlans := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true, DatabaseType: "mysql",
		UnmatchedDropTables: []string{"_jobs_old", "_jobs_olds"},
		DeploymentDrift:     unmatchedDropRolloutDrift(),
	}
	out := RenderPlanComment(withTargetPlans)
	assert.Contains(t, out, "DROP TABLE `_jobs_old`", out)
	assert.NotContains(t, out, "entry `_jobs_old` names no table", out)
	assert.Equal(t, 1, strings.Count(out, unmatchedDropLine+"\n"), out)

	primaryOnly := withTargetPlans
	primaryOnly.DeploymentDrift = &DeploymentDriftData{
		Computed: true, Clean: true,
		Deployments: []DeploymentDriftEntry{
			{Deployment: "primary", Target: "app_1", Primary: true, Class: "match"},
			{Deployment: "primary", Target: "app_2", Class: "match"},
		},
		UnmatchedDropTables: []string{"_jobs_olds"},
	}
	out = RenderPlanComment(primaryOnly)
	assert.Contains(t, out, "No schema changes detected", out)
	assert.NotContains(t, out, "entry `_jobs_old` names no table", out)
	assert.Equal(t, 1, strings.Count(out, unmatchedDropLine+"\n"), out)

	staging := unmatchedDropPlanData()
	staging.Changes = nil
	staging.UnmatchedDropTables = []string{"_jobs_old", "_jobs_olds"}
	production := withTargetPlans
	out = RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})
	assert.Contains(t, out, "### Production", out)
	assert.NotContains(t, out, "entry `_jobs_old` names no table", out)
	assert.Equal(t, 2, strings.Count(out, unmatchedDropLine+"\n"), "each environment's section reports the entry no target matched:\n%s", out)
}

// A rollout whose list is empty, because a target could not be planned or
// the rollup was not computed, reports nothing, whatever the primary's own
// list says: what the other target holds is unknown.
func TestRenderPlanComment_RolloutWithoutAListReportsNoUnmatchedDropTables(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true, DatabaseType: "mysql",
		UnmatchedDropTables: []string{"_jobs_olds"},
		DeploymentDrift:     &DeploymentDriftData{Computed: false},
	}
	assert.NotContains(t, RenderPlanComment(data), "names no table")
}

// The plan summary carries the line on both of its branches.
func TestWritePlanSummary_UnmatchedDropTablesOnBothBranches(t *testing.T) {
	data := unmatchedDropPlanData()

	var noChanges strings.Builder
	writePlanSummary(&noChanges, data, 0, 0)
	assert.Contains(t, noChanges.String(), "No schema changes detected")
	assert.Contains(t, noChanges.String(), unmatchedDropLine+"\n")

	var withChanges strings.Builder
	writePlanSummary(&withChanges, data, 1, 0)
	assert.Contains(t, withChanges.String(), "**1** table to drop")
	assert.Contains(t, withChanges.String(), unmatchedDropLine+"\n")
}

// A drop_tables list with hundreds of entries that name no table names the
// first ones and counts the rest, so the lines never push the comment past
// GitHub's size limit and the plan above them stays visible.
func TestRenderPlanComment_ManyUnmatchedDropTablesAreCounted(t *testing.T) {
	data := unmatchedDropPlanData()
	data.UnmatchedDropTables = nil
	for i := range 400 {
		data.UnmatchedDropTables = append(data.UnmatchedDropTables, fmt.Sprintf("_jobs_old_%04d", i))
	}
	out := RenderPlanComment(data)

	assert.Contains(t, out, "DROP TABLE `_jobs_old`", "the plan stays visible")
	assert.Equal(t, presentation.UnmatchedDropTablesListLimit, strings.Count(out, "names no table on the target, so this plan drops nothing for it."), out)
	assert.Contains(t, out, "entry `_jobs_old_0019` names no table")
	assert.NotContains(t, out, "_jobs_old_0020")
	assert.Contains(t, out, "⚠️ 380 more `drop_tables` entries name no table on the target, so this plan drops nothing for them. Fix the names, or remove the entries whose tables are already gone.\n", out)
	assert.Less(t, len(out), 20000, "the lines stay a small part of the comment")
}

// A plan whose entries all matched renders exactly as one without drop_tables.
func TestRenderPlanComment_NoUnmatchedDropTablesNoLine(t *testing.T) {
	data := unmatchedDropPlanData()
	data.UnmatchedDropTables = nil
	assert.NotContains(t, RenderPlanComment(data), "names no table")
}
