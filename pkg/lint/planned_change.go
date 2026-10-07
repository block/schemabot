package lint

import (
	"slices"
	"strings"

	spiritlint "github.com/block/spirit/pkg/lint"

	"github.com/block/schemabot/pkg/engine"
)

// unsafeReasonSeparator joins the messages of a table's error-severity
// violations into its unsafe reason. ui.LintReasons splits on it to render
// each message on its own line.
const unsafeReasonSeparator = "; "

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
// The reason is the set of the error messages, sorted and joined. Spirit
// reports a table's violations in no fixed order, so two targets with the same
// findings can receive them in different orders. Building the reason from the
// set gives both the same reason, which is what lets a rollout's targets be
// recognized as carrying the same unsafe change.
func PlannedChangeUnsafeReason(pc spiritlint.PlannedChange) (string, bool) {
	errs := pc.Errors()
	if len(errs) == 0 {
		return "", false
	}
	msgs := make([]string, len(errs))
	for i, v := range errs {
		msgs[i] = v.Message
	}
	slices.Sort(msgs)
	return strings.Join(slices.Compact(msgs), unsafeReasonSeparator), true
}
