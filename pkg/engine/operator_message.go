package engine

import (
	"errors"
	"fmt"
)

// OperatorMessage marks an error whose text SchemaBot wrote for an operator to
// read, so a renderer may publish it on a pull request.
//
// Marking is the whole point, and it is deliberately explicit. Almost every
// error an engine catches came from the target, its driver, or a library, and
// that text quotes statements, names hosts, and carries rows out of the
// customer's table. A renderer therefore treats an unmarked error as text it
// cannot show — which means a failure path added later renders as a generic
// reason until someone decides its message is safe to publish. Forgetting to
// mark costs detail; the arrangement that fails the other way costs a leak.
type OperatorMessage struct {
	// Message is SchemaBot's own words, safe to render.
	Message string
	// Err is the cause. It stays out of the rendered message and is what a
	// server-side log records.
	Err error
}

// Error carries the cause as well, so a log of this error keeps the detail the
// rendered message deliberately leaves out.
func (e *OperatorMessage) Error() string {
	if e.Err == nil {
		return e.Message
	}
	return fmt.Sprintf("%s: %v", e.Message, e.Err)
}

func (e *OperatorMessage) Unwrap() error { return e.Err }

// OperatorErrorf wraps cause with a message SchemaBot wrote. cause may be nil
// when the condition is one SchemaBot detected itself rather than caught.
//
// The format string and its arguments must be SchemaBot's own words and
// identifiers an operator already has — a table name from the plan, a
// configured timeout. Never format a driver message, a target's error, or a
// value read off the target into it; wrap those as cause instead.
//
// A sentence a library authored about the requested change may be interpolated,
// but only after reading what that library actually puts in it. The bar is the
// same one this type exists to hold: it must carry nothing beyond the statement
// the pull request already shows. Text that reaches the library from the target
// does not qualify, however the library words it.
func OperatorErrorf(cause error, format string, args ...any) error {
	return &OperatorMessage{Message: fmt.Sprintf(format, args...), Err: cause}
}

// OperatorMessageOf returns the message an operator may be shown, and whether
// the error carried one at all.
func OperatorMessageOf(err error) (string, bool) {
	var marked *OperatorMessage
	if !errors.As(err, &marked) {
		return "", false
	}
	return marked.Message, true
}
