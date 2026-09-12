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

// guidanceScope is the lint rules one comment discloses for one plan, with the
// dialect that decides which guides apply. A comment links a guide only for a
// finding it shows, so a reader never meets a link with nothing above it that
// explains why it is there.
type guidanceScope struct {
	rules   []string
	isMySQL bool
}

// disclosesEverySeverity scopes a comment that shows findings of every
// severity: error-severity findings reach it as unsafe changes carrying the
// lint message, and the rest through the lint fold. A locked apply comment
// shows neither, so it discloses nothing.
func (d PlanCommentData) disclosesEverySeverity() guidanceScope {
	if d.IsLocked {
		return guidanceScope{}
	}
	return guidanceScope{rules: d.LintRuleNames, isMySQL: d.IsMySQL}
}

// disclosesNonErrorsOnly scopes a comment that shows the lint fold and no
// unsafe section, which leaves its error-severity findings unshown.
func (d PlanCommentData) disclosesNonErrorsOnly() guidanceScope {
	rules := make([]string, 0, len(d.LintViolations))
	for _, finding := range d.LintViolations {
		rules = append(rules, finding.LinterName)
	}
	return guidanceScope{rules: rules, isMySQL: d.IsMySQL}
}

// writeRelatedGuidance links the docs for the rules its scopes disclose, across
// severities and environments, outside the optional findings fold. Unknown
// rules have no link until registered above.
func writeRelatedGuidance(sb *strings.Builder, scopes ...guidanceScope) {
	seen := make(map[string]bool)
	var links []string
	for _, guide := range lintGuides {
		for _, scope := range scopes {
			if guide.mysqlOnly && !scope.isMySQL {
				continue
			}
			matches := slices.ContainsFunc(scope.rules, func(rule string) bool {
				return slices.Contains(guide.rules, rule)
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
