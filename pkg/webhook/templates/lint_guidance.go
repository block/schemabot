package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/lintguidance"
)

const primaryKeyDocURL = "https://github.com/block/schemabot/blob/main/docs/mysql.md#choosing-a-primary-key"

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
	mapped := make([]lintguidance.Scope, 0, len(scopes))
	for _, scope := range scopes {
		mapped = append(mapped, lintguidance.Scope{Rules: scope.rules, IsMySQL: scope.isMySQL})
	}
	var links []string
	for _, guide := range lintguidance.Guides(mapped...) {
		links = append(links, fmt.Sprintf("[%s](%s)", guide.Label, guide.URL))
	}
	if len(links) > 0 {
		fmt.Fprintf(sb, "📖 **Related guidance:**\n\n- %s\n\n", strings.Join(links, "\n- "))
	}
}
