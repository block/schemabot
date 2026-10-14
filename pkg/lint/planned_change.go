package lint

import (
	"cmp"
	"slices"
	"strings"

	spiritlint "github.com/block/spirit/pkg/lint"

	"github.com/block/schemabot/pkg/engine"
)

// unsafeReasonSeparator joins the messages of a table's error-severity
// violations into its unsafe reason. ui.LintReasons splits on it to render
// each message on its own line.
const unsafeReasonSeparator = "; "

// separatorInFinding stands in for the separator inside one finding's message,
// so reading the reason back splits it into exactly the findings it was built
// from: a message containing the separator would otherwise read back as two
// findings. The non-breaking space renders the same as the space it replaces.
const separatorInFinding = ";\u00a0"

// unsafeFinding is one error-severity violation as an unsafe reason counts it:
// its message and the column it names. The same message raised on two columns
// is two findings, so a plan that gains the second column's finding is not
// taken for the plan the operator consented to with only the first.
type unsafeFinding struct {
	message string
	column  string
}

func compareUnsafeFindings(a, b unsafeFinding) int {
	return cmp.Or(strings.Compare(a.message, b.message), strings.Compare(a.column, b.column))
}

// PlannedChangeViolations converts the violations Spirit raised on one planned
// change, at every severity, carrying the column a violation names.
func PlannedChangeViolations(pc spiritlint.PlannedChange) []engine.LintViolation {
	violations := make([]engine.LintViolation, 0, len(pc.Violations))
	for _, v := range pc.Violations {
		lv := engine.LintViolation{
			Table:    pc.TableName,
			Message:  v.Message,
			Severity: strings.ToLower(v.Severity.String()),
		}
		if v.Linter != nil {
			lv.Linter = v.Linter.Name()
		}
		if v.Location != nil && v.Location.Column != nil {
			lv.Column = *v.Location.Column
		}
		violations = append(violations, lv)
	}
	return violations
}

// PlannedChangeUnsafeReason returns the reason a planned change needs the
// unsafe opt-in, and whether it needs it: the change does exactly when Spirit
// raised an error-severity violation on it.
//
// The reason is the set of the error findings, sorted and joined, one message
// per finding. Spirit reports a table's violations in no fixed order, so two
// targets with the same findings can receive them in different orders.
// Building the reason from the set gives both the same reason, which is what
// lets a rollout's targets be recognized as carrying the same unsafe change. A
// violation Spirit reports twice for the same column is one finding.
func PlannedChangeUnsafeReason(pc spiritlint.PlannedChange) (string, bool) {
	errs := pc.Errors()
	if len(errs) == 0 {
		return "", false
	}
	findings := make([]unsafeFinding, len(errs))
	for i, v := range errs {
		findings[i] = unsafeFinding{message: strings.ReplaceAll(v.Message, unsafeReasonSeparator, separatorInFinding)}
		if v.Location != nil && v.Location.Column != nil {
			findings[i].column = *v.Location.Column
		}
	}
	slices.SortFunc(findings, compareUnsafeFindings)
	findings = slices.Compact(findings)
	msgs := make([]string, len(findings))
	for i, f := range findings {
		msgs[i] = f.message
	}
	return strings.Join(msgs, unsafeReasonSeparator), true
}
