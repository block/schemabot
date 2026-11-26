package engine

// AuthoredText lets a renderer outside this package recognize an OperatorMessage
// through api.AuthoredError. It is SchemaBot's own message, and the cause stays
// out of it.
func (e *OperatorMessage) AuthoredText() (string, bool) { return e.Message, false }

// AuthoredText lets a renderer outside this package recognize a refusal through
// api.AuthoredError. It is the refusal's own text, which an engine composes
// from the request's schema files and the statements in them, so it is flagged
// as quoting the repository.
func (e *PlanRefusedError) AuthoredText() (string, bool) { return e.Err.Error(), true }
