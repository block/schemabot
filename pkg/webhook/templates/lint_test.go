package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func lintPlanData(violations []LintViolationData) PlanCommentData {
	return PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
		LintViolations: violations,
	}
}

// A short lint list renders inline in the same visual shape as the Issues
// section — counted header, backticked table prefix per item — so the operator
// scans both sections the same way, with quoted identifiers as inline code.
func TestRenderPlanComment_LintInlineBelowFoldThreshold(t *testing.T) {
	plan := RenderPlanComment(lintPlanData([]LintViolationData{
		{Message: `Index "idx_email" should be made invisible before dropping`, Table: "users"},
		{Message: `Using "varchar" as primary key is discouraged`, Table: "sessions"},
	}))

	assert.Contains(t, plan, "💡 **Lint Warnings**: 2 advisory findings")
	assert.Contains(t, plan, "- `users`: Index `idx_email` should be made invisible before dropping")
	assert.Contains(t, plan, "- `sessions`: Using `varchar` as primary key is discouraged")
	assert.NotContains(t, plan, "<summary>💡", "a short list never hides behind a fold")
}

// A long lint list folds into a details block grouped by table, so many
// advisory findings stop dominating the plan comment while the count stays
// visible in the summary line. Groups appear in first-appearance order and
// untabled findings surface first as general notes.
func TestRenderPlanComment_LintFoldsAndGroupsAboveThreshold(t *testing.T) {
	plan := RenderPlanComment(lintPlanData([]LintViolationData{
		{Message: "first users finding", Table: "users"},
		{Message: "first sessions finding", Table: "sessions"},
		{Message: "a general schema note"},
		{Message: "second users finding", Table: "users"},
		{Message: "second sessions finding", Table: "sessions"},
		{Message: "third users finding", Table: "users"},
	}))

	assert.Contains(t, plan, "<summary>💡 <b>Lint Warnings</b>: 6 advisory findings</summary>")
	assert.Contains(t, plan, "**`users`**\n- first users finding\n- second users finding\n- third users finding")
	assert.Contains(t, plan, "**`sessions`**\n- first sessions finding\n- second sessions finding")

	generalIdx := strings.Index(plan, "- a general schema note")
	usersIdx := strings.Index(plan, "**`users`**")
	sessionsIdx := strings.Index(plan, "**`sessions`**")
	assert.Greater(t, generalIdx, -1)
	assert.Less(t, generalIdx, usersIdx, "untabled findings lead the folded list")
	assert.Less(t, usersIdx, sessionsIdx, "table groups keep first-appearance order")
}

// Advice is visible after the folded list, appears only once, and does not
// attach MySQL-specific explanations to other engines or unrelated findings.
func TestRenderPlanComment_PrimaryKeyGuidance(t *testing.T) {
	for _, tc := range []struct {
		name       string
		count      int
		mysql      bool
		primaryKey bool
		locked     bool
		want       bool
	}{
		{"short", 1, true, true, false, true},
		{"folded", 6, true, true, false, true},
		{"fifty findings", 50, true, true, false, true},
		{"issues only", 0, true, true, false, true},
		{"other engine", 1, false, true, false, false},
		{"other rule", 1, true, false, false, false},
		{"locked apply", 1, true, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			warnings := make([]LintViolationData, tc.count)
			for i := range warnings {
				warnings[i] = LintViolationData{Message: "Review this column", Table: "customers"}
				if tc.primaryKey {
					warnings[i].Message = `Primary key column "id" has type "varchar"`
					warnings[i].LinterName = "primary_key"
				}
			}
			data := lintPlanData(warnings)
			data.IsMySQL = tc.mysql
			if tc.primaryKey {
				data.LintRuleNames = []string{"primary_key"}
			}
			data.IsLocked = tc.locked
			if tc.primaryKey {
				data.HasUnsafeChanges = true
				data.UnsafeChanges = []UnsafeChangeData{{Table: "customers", Reason: `[ERROR] primary_key: Primary key column "id" has type "varchar"`}}
			}
			out := RenderPlanComment(data)
			if !tc.want {
				assert.NotContains(t, out, primaryKeyDocURL)
				return
			}
			assert.Equal(t, 1, strings.Count(out, primaryKeyDocURL))
			assert.Contains(t, out, "📖 **Related guidance:** [Primary key tradeoffs]("+primaryKeyDocURL+")")
			if tc.count > lintWarningsFoldThreshold {
				assert.Greater(t, strings.Index(out, primaryKeyDocURL), strings.Index(out, "</details>"))
			}
		})
	}
}
