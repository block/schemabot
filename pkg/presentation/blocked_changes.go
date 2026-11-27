package presentation

// BlockedChangesRemedy closes every list of changes an engine refuses to run
// with what the operator does next. The CLI plan and the PR plan comment both
// end their list with it, so the two surfaces give one answer.
const BlockedChangesRemedy = "An apply will fail on these statements. Fix what each reason names — rewrite an unsupported change, or provision the stated access — or contact your SchemaBot operators for help."
