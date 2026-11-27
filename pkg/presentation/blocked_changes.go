package presentation

// BlockedChangesHeading heads the CLI plan's and apply's list of changes an
// engine refuses to run.
const BlockedChangesHeading = "Cannot apply"

// BlockedChangesRemedy closes every list of changes an engine refuses to run
// with what the operator does next. The CLI plan and the PR plan comment both
// end their list with it, so the two surfaces give one answer.
const BlockedChangesRemedy = "An apply will fail on these statements. " + BlockedChangesFix

// RollbackBlockedChangesHeading heads a rollback's list of refused changes.
const RollbackBlockedChangesHeading = "Cannot roll back"

// RollbackBlockedChangesRemedy closes a rollback's list of refused changes.
// A rollback plans its own reversal each time it runs, so the next step is to
// run the rollback again rather than to plan.
const RollbackBlockedChangesRemedy = "A rollback will fail on these statements. Each reason names its fix: make it and run the rollback again, or contact your SchemaBot operators for help."

// BlockedChangesFix is the next step for a refused change. The remedy differs
// by cause (a rewrite, a grant, a smaller table), so each reason states its
// own and this line only says to act on it. An apply refusal ends with it
// alone, since the refusal has already said the apply will not run.
const BlockedChangesFix = "Each reason names its fix: make it and plan again, or contact your SchemaBot operators for help."
