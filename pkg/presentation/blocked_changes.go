package presentation

// BlockedChangesRemedy closes every list of changes an engine refuses to run
// with what the operator does next. The CLI plan and the PR plan comment both
// end their list with it, so the two surfaces give one answer.
const BlockedChangesRemedy = "An apply will fail on these statements. " + BlockedChangesFix

// BlockedChangesFix is the next step for a refused change. The remedy differs
// by cause (a rewrite, a grant, a smaller table), so each reason states its
// own and this line only says to act on it. An apply refusal ends with it
// alone, since the refusal has already said the apply will not run.
const BlockedChangesFix = "Each reason names its fix: make it and plan again, or contact your SchemaBot operators for help."
