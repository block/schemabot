package lint

import (
	"testing"

	spiritlint "github.com/block/spirit/pkg/lint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/ui"
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

	sameMessageOn := func(column string) spiritlint.Violation {
		return spiritlint.Violation{Message: "Column uses `TIMESTAMP`", Severity: spiritlint.SeverityError, Location: &spiritlint.Location{Table: "bikes", Column: &column}}
	}
	reason, unsafe = PlannedChangeUnsafeReason(change(sameMessageOn("created_at"), sameMessageOn("updated_at"), sameMessageOn("created_at")))
	require.True(t, unsafe)
	assert.Equal(t, "Column uses `TIMESTAMP`; Column uses `TIMESTAMP`", reason,
		"the same message on two columns is two findings, and on one column twice is one")

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

// A message that contains the separator reads back from the reason as the one
// finding it is, so it cannot pair with another finding to look like a
// different set.
func TestPlannedChangeUnsafeReasonKeepsAFindingWhole(t *testing.T) {
	const withSeparator = "`id` is an auto-increment column; capacity cannot be checked"
	pc := spiritlint.PlannedChange{TableName: "bikes", Violations: []spiritlint.Violation{
		{Message: withSeparator, Severity: spiritlint.SeverityError},
		violationOn("created_at", spiritlint.SeverityError),
	}}

	reason, unsafe := PlannedChangeUnsafeReason(pc)
	require.True(t, unsafe)
	assert.Equal(t, []string{
		"Column `created_at` uses `TIMESTAMP`",
		"`id` is an auto-increment column;\u00a0capacity cannot be checked",
	}, ui.LintReasons(reason))
}
