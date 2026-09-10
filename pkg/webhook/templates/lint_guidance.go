package templates

import (
	"fmt"
	"slices"
	"strings"
)

const primaryKeyDocURL = "https://github.com/block/schemabot/blob/main/docs/mysql.md#choosing-a-primary-key"

type lintGuide struct {
	rules     []string
	label     string
	url       string
	mysqlOnly bool
}

// Registry order is display order, independent of finding or environment order.
// Multiple rules can point to a guide; its URL appears only once per comment.
var lintGuides = []lintGuide{
	{rules: []string{"primary_key"}, label: "Choosing a primary key", url: primaryKeyDocURL, mysqlOnly: true},
	{rules: []string{"rename_column"}, label: "Renaming a column or table", url: "https://github.com/block/schemabot/blob/main/docs/pre-merge-workflow.md#renaming-a-column-or-table", mysqlOnly: true},
}

// writeRelatedGuidance collects docs across severities and environments, outside
// the optional findings fold. Unknown rules have no link until registered here.
func writeRelatedGuidance(sb *strings.Builder, plans ...PlanCommentData) {
	seen := make(map[string]bool)
	var links []string
	for _, guide := range lintGuides {
		for _, plan := range plans {
			if plan.IsLocked || (guide.mysqlOnly && !plan.IsMySQL) {
				continue
			}
			matches := slices.ContainsFunc(plan.LintRuleNames, func(rule string) bool {
				return slices.Contains(guide.rules, rule)
			}) || slices.ContainsFunc(plan.LintViolations, func(finding LintViolationData) bool {
				return slices.Contains(guide.rules, finding.LinterName)
			})
			if matches && !seen[guide.url] {
				seen[guide.url] = true
				links = append(links, fmt.Sprintf("[%s](%s)", guide.label, guide.url))
			}
		}
	}
	if len(links) > 0 {
		fmt.Fprintf(sb, "📖 **Related guidance:**\n\n- %s\n\n", strings.Join(links, "\n- "))
	}
}
