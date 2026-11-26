package engine

// AuthoredText lets a renderer outside this package recognize an OperatorMessage
// through api.AuthoredError. It is SchemaBot's own message, and the cause stays
// out of it.
func (e *OperatorMessage) AuthoredText() (string, bool) { return e.Message, false }

// AuthoredText lets a renderer outside this package recognize a refusal through
// api.AuthoredError. A refusal that carries an OperatorMessage gives up that
// message alone, without the wrap around it or the message's cause. Any other
// refusal is the text an engine composed from the request's schema files and
// the statements in them, so it is flagged as quoting the repository.
func (e *PlanRefusedError) AuthoredText() (string, bool) {
	if message, ok := OperatorMessageOf(e.Err); ok {
		return message, false
	}
	return e.Err.Error(), true
}
