package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRelatedGuidanceAggregatesRulesAcrossEnvironments(t *testing.T) {
	first := lintPlanData(nil)
	first.LintRuleNames = []string{"rename_column", "unknown_rule"}
	first.HasUnsafeChanges = true
	first.UnsafeChanges = []UnsafeChangeData{{Table: "users", Reason: `[ERROR] rename_column: Column rename detected`}}
	second := lintPlanData(nil)
	for range 50 {
		second.LintRuleNames = append(second.LintRuleNames, "primary_key", "rename_column")
		second.LintViolations = append(second.LintViolations, LintViolationData{
			Table: "customers", LinterName: "primary_key", Message: `Primary key column "id" has type "varchar"`,
		})
	}
	data := MultiEnvPlanCommentData{
		Database: "testapp", IsMySQL: true, DatabaseType: "mysql",
		Environments: []string{"staging", "production"},
		Plans:        map[string]*PlanCommentData{"staging": &first, "production": &second},
	}
	out := RenderMultiEnvPlanComment(data)
	assert.Equal(t, 1, strings.Count(out, "📖 **Related guidance:**"))
	assert.Equal(t, 1, strings.Count(out, primaryKeyDocURL))
	assert.Equal(t, 1, strings.Count(out, "#renaming-a-column-or-table"))
	assert.Contains(t, out, ")\n- [Renaming a column or table](")
	assert.Greater(t, strings.Index(out, "📖 **Related guidance:**"), strings.LastIndex(out, "</details>"))
	assert.NotContains(t, out, "unknown_rule")

	var forward, reverse strings.Builder
	writeRelatedGuidance(&forward, first.disclosesEverySeverity(), second.disclosesEverySeverity())
	writeRelatedGuidance(&reverse, second.disclosesEverySeverity(), first.disclosesEverySeverity())
	assert.Equal(t, forward.String(), reverse.String(), "guide order must not depend on environment order")

	data.Errors = map[string]string{"production": "Plan unavailable"}
	out = RenderMultiEnvPlanComment(data)
	assert.NotContains(t, out, primaryKeyDocURL, "failed environments must not contribute stale guidance")
}

func TestRelatedGuidanceIgnoresUnknownAndLockedFindings(t *testing.T) {
	for _, data := range []PlanCommentData{
		{IsMySQL: true, LintRuleNames: []string{"unknown_rule"}},
		{IsMySQL: true, IsLocked: true, LintRuleNames: []string{"primary_key"}},
		{IsMySQL: false, LintRuleNames: []string{"primary_key"}},
	} {
		var out strings.Builder
		writeRelatedGuidance(&out, data.disclosesEverySeverity())
		assert.Empty(t, out.String())
	}
}

// A comment that shows no unsafe section leaves its error-severity findings
// unshown, so it links no guide for them: a "Related guidance" link with no
// finding above it reads as advice about nothing.
func TestRelatedGuidanceSkipsFindingsTheCommentWithholds(t *testing.T) {
	data := PlanCommentData{
		IsMySQL:        true,
		LintRuleNames:  []string{"primary_key", "rename_column"},
		LintViolations: []LintViolationData{{Table: "customers", LinterName: "rename_column", Message: "Column rename detected"}},
	}

	var withheld strings.Builder
	writeRelatedGuidance(&withheld, data.disclosesNonErrorsOnly())
	assert.Contains(t, withheld.String(), "#renaming-a-column-or-table", "the shown finding keeps its guide")
	assert.NotContains(t, withheld.String(), primaryKeyDocURL, "the withheld finding contributes no link")

	var shown strings.Builder
	writeRelatedGuidance(&shown, data.disclosesEverySeverity())
	assert.Contains(t, shown.String(), primaryKeyDocURL, "a comment that shows every severity links both")
}
