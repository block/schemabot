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

// AlreadyCompletedError wraps an error to indicate the engine's backend
// rejected a control operation because the schema change had already completed
// before the operation arrived. The backend's terminal outcome is
// authoritative and retrying can never succeed, so callers should reconcile
// stored state to the completed outcome instead of retrying — leaving the
// operation pending would re-run a rejection forever against a change that has
// already landed.
type AlreadyCompletedError struct {
	Err error
}

func (e *AlreadyCompletedError) Error() string { return e.Err.Error() }
func (e *AlreadyCompletedError) Unwrap() error { return e.Err }

// NewAlreadyCompletedError wraps err as an already-completed error.
func NewAlreadyCompletedError(msg string, args ...any) error {
	return &AlreadyCompletedError{Err: fmt.Errorf(msg, args...)}
}

// IsAlreadyCompleted reports whether err indicates the engine's backend
// rejected a control operation because the schema change had already
// completed.
func IsAlreadyCompleted(err error) bool {
	if err == nil {
		return false
	}
	var alreadyCompleted *AlreadyCompletedError
	return errors.As(err, &alreadyCompleted)
}

// IsRetryable returns true if the error should be retried by the operator.
// All errors are retryable by default, and engines explicitly wrap only
// permanent errors with PermanentError.
//
// AlreadyCompletedError deliberately stays retryable here even though the
// rejected operation can never succeed: callers treat a non-retryable error as
// a permanent failure, and recording a failure for a schema change that
// already landed would misrepresent the target. Paths that can receive an
// already-completed rejection must reconcile to the completed outcome via
// IsAlreadyCompleted instead; anywhere that doesn't, retrying keeps the stored
// state honest until a drive that reconciles picks it up.
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
