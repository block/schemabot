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

// UnsupportedOperationError wraps an error to indicate the engine cannot
// perform the requested control operation for its database type — ever, not
// just for this schema change or this moment. Retrying can never succeed, so
// a caller consuming a durable control request should resolve the request
// terminally with the engine's reason instead of retrying, leaving the
// underlying schema change untouched to settle on its own.
type UnsupportedOperationError struct {
	Err error
}

func (e *UnsupportedOperationError) Error() string { return e.Err.Error() }
func (e *UnsupportedOperationError) Unwrap() error { return e.Err }

// NewUnsupportedOperationError wraps err as an unsupported-operation error.
// The message reaches operator-facing surfaces verbatim, so with no args it
// is used as-is rather than interpreted as a format string — a literal `%`
// in a decline reason must never render as a corrupted fmt verb.
func NewUnsupportedOperationError(msg string, args ...any) error {
	if len(args) == 0 {
		return &UnsupportedOperationError{Err: errors.New(msg)}
	}
	return &UnsupportedOperationError{Err: fmt.Errorf(msg, args...)}
}

// AsUnsupportedOperation extracts the unsupported-operation decline from
// err's tree, reporting whether one is present. Callers that only need the
// boolean use IsUnsupportedOperation.
func AsUnsupportedOperation(err error) (*UnsupportedOperationError, bool) {
	var unsupported *UnsupportedOperationError
	if errors.As(err, &unsupported) {
		return unsupported, true
	}
	return nil, false
}

// IsUnsupportedOperation reports whether err indicates the engine cannot
// perform the requested control operation for its database type.
func IsUnsupportedOperation(err error) bool {
	_, ok := AsUnsupportedOperation(err)
	return ok
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
//
// UnsupportedOperationError stays retryable for the same reason: the schema
// change itself is not failed — only the control operation is undeliverable —
// so classifying it permanent here would let a generic failure path record a
// healthy change as failed. Paths that can receive an unsupported rejection
// must resolve the control request terminally via IsUnsupportedOperation
// instead.
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
