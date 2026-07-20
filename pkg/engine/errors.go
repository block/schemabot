package engine

import (
	"errors"
	"fmt"
	"strings"
)

// PermanentError wraps an error to indicate it should not be retried.
// Engines return this when the error is permanent.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// NewPermanentError wraps err as a permanent error.
func NewPermanentError(msg string, args ...any) error {
	return &PermanentError{Err: fmt.Errorf(msg, args...)}
}

// NotReadyError wraps an error to indicate the engine's backend was not yet
// ready to accept the operation and is expected to accept it once it catches
// up. Polling drives should treat this as an expected condition and reattempt
// on a later tick rather than reporting the attempt as a failure.
type NotReadyError struct {
	Err error
}

func (e *NotReadyError) Error() string { return e.Err.Error() }
func (e *NotReadyError) Unwrap() error { return e.Err }

// NewNotReadyError wraps err as a not-ready error.
func NewNotReadyError(msg string, args ...any) error {
	return &NotReadyError{Err: fmt.Errorf(msg, args...)}
}

// IsNotReady reports whether err indicates the engine's backend was not yet
// ready to accept the operation.
func IsNotReady(err error) bool {
	if err == nil {
		return false
	}
	var notReady *NotReadyError
	return errors.As(err, &notReady)
}

// IsRetryable returns true if the error should be retried by the operator.
// All errors are retryable by default, and engines explicitly wrap only
// permanent errors with PermanentError.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var permanent *PermanentError
	return !errors.As(err, &permanent)
}

// IsTransientTransportError reports whether err matches common transport
// failures that often resolve on a later attempt.
func IsTransientTransportError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "Too many requests")
}
