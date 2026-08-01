// Package panicsafe converts panics at background-work boundaries into errors.
//
// SchemaBot's control plane multiplexes many independent units of work —
// operator drives, reconciliation loops, observer pollers — onto one process.
// A panic in any one of them would otherwise kill the process, and because
// crashed work is re-claimed after restart, a single poisoned row could
// crash-loop every replica. Call is the containment boundary: it turns a panic
// into an *Error that the caller routes through its normal failure handling
// (log, metric, state transition) so one bad unit of work degrades only
// itself.
package panicsafe

import (
	"fmt"
	"runtime/debug"
)

// Error is a recovered panic carrying the panic value and the goroutine stack
// captured at the recovery point. Callers detect a contained panic with
// errors.As and log Value and Stack so the fault is triageable from logs
// alone.
type Error struct {
	// Value is the value the panicking code passed to panic().
	Value any
	// Stack is the goroutine stack captured where the panic was recovered.
	Stack []byte
}

func (e *Error) Error() string {
	return fmt.Sprintf("panic: %v", e.Value)
}

// Call invokes fn, converting a panic into an *Error. A nil return means fn
// completed normally; a non-nil return is either fn's own error or, when
// errors.As matches *Error, a contained panic.
func Call(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &Error{Value: r, Stack: debug.Stack()}
		}
	}()
	return fn()
}
