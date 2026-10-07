package lint

import (
	"testing"

	spiritlint "github.com/block/spirit/pkg/lint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

func violationOn(column string, severity spiritlint.Severity) spiritlint.Violation {
	return spiritlint.Violation{
		Message:  "Column `" + column + "` uses `TIMESTAMP`",
		Severity: severity,
		Location: &spiritlint.Location{Table: "bikes", Column: &column},
	}
}

// A planned change's unsafe reason is built from the set of its error
// messages, so the same findings reported in any order give the same reason.
// Only error-severity findings make a change unsafe.
func TestPlannedChangeUnsafeReason(t *testing.T) {
	change := func(violations ...spiritlint.Violation) spiritlint.PlannedChange {
		return spiritlint.PlannedChange{TableName: "bikes", Violations: violations}
	}
	createdAt := violationOn("created_at", spiritlint.SeverityError)
	updatedAt := violationOn("updated_at", spiritlint.SeverityError)
	const want = "Column `created_at` uses `TIMESTAMP`; Column `updated_at` uses `TIMESTAMP`"

	reason, unsafe := PlannedChangeUnsafeReason(change(createdAt, updatedAt))
	require.True(t, unsafe)
	assert.Equal(t, want, reason)

	reason, unsafe = PlannedChangeUnsafeReason(change(updatedAt, createdAt))
	require.True(t, unsafe)
	assert.Equal(t, want, reason, "the same findings in another order")

	reason, unsafe = PlannedChangeUnsafeReason(change(createdAt, updatedAt, createdAt))
	require.True(t, unsafe)
	assert.Equal(t, want, reason, "a finding reported twice is listed once")

	reason, unsafe = PlannedChangeUnsafeReason(change(violationOn("created_at", spiritlint.SeverityWarning)))
	assert.False(t, unsafe, "a warning alone needs no opt-in")
	assert.Empty(t, reason)
}

// Every violation on a planned change is converted, at its own severity, with
// the column it names.
func TestPlannedChangeViolations(t *testing.T) {
	pc := spiritlint.PlannedChange{TableName: "bikes", Violations: []spiritlint.Violation{
		violationOn("created_at", spiritlint.SeverityError),
		{Message: "Table `bikes` has no secondary index", Severity: spiritlint.SeverityWarning, Location: &spiritlint.Location{Table: "bikes"}},
	}}

	assert.Equal(t, []engine.LintViolation{
		{Table: "bikes", Column: "created_at", Message: "Column `created_at` uses `TIMESTAMP`", Severity: "error"},
		{Table: "bikes", Message: "Table `bikes` has no secondary index", Severity: "warning"},
	}, PlannedChangeViolations(pc))
}
