package engine

import (
	"errors"
	"fmt"
	"strings"
)

// ErrTargetHeld reports that an engine could not start a schema change because
// another run already holds the target, such as a run an earlier driver of the
// same apply started and has not yet brought down. The refusal is not a failure
// of the schema change: the work can start once the holder releases the target,
// and retrying sooner is only refused again.
var ErrTargetHeld = errors.New("target is held by another run of a schema change")

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

// SettledOutcomeError wraps an error to indicate the engine refused a control
// operation because the schema change had already settled on an outcome of its
// own, such as failed or cancelled, that the operation must not replace. It is
// a refusal for this change at this moment, not a gap in what the engine
// supports. Retrying can never succeed, because the outcome will not change
// back, so a caller consuming a durable control request should resolve the
// request terminally with the engine's reason and keep driving, so the drive
// records the outcome the engine reports. A completed change has its own type,
// AlreadyCompletedError, because its caller reconciles to completed instead.
type SettledOutcomeError struct {
	Err error
}

func (e *SettledOutcomeError) Error() string { return e.Err.Error() }
func (e *SettledOutcomeError) Unwrap() error { return e.Err }

// NewSettledOutcomeError wraps err as a settled-outcome refusal.
func NewSettledOutcomeError(msg string, args ...any) error {
	return &SettledOutcomeError{Err: fmt.Errorf(msg, args...)}
}

// AsSettledOutcome extracts the settled-outcome refusal from err's tree,
// reporting whether one is present.
func AsSettledOutcome(err error) (*SettledOutcomeError, bool) {
	var settled *SettledOutcomeError
	if errors.As(err, &settled) {
		return settled, true
	}
	return nil, false
}

// IsSettledOutcome reports whether err indicates the engine refused a control
// operation because the schema change had already settled on its own outcome.
func IsSettledOutcome(err error) bool {
	_, ok := AsSettledOutcome(err)
	return ok
}

// UnsupportedOperationError wraps an error to indicate the engine declines
// the requested control operation deterministically: it will refuse it for
// every schema change on its database type, not just for this one or this
// moment, so retrying through the engine can never succeed. A caller
// consuming a durable control request should resolve the request terminally
// with the engine's reason instead of retrying, leaving the underlying
// schema change untouched to settle on its own. The decline speaks for the
// engine, not the database: the operation may still be possible out of band
// at the database itself, and the decline reason should tell the operator
// how when it is.
type UnsupportedOperationError struct {
	Err error
}

func (e *UnsupportedOperationError) Error() string { return e.Err.Error() }
func (e *UnsupportedOperationError) Unwrap() error { return e.Err }

// NewUnsupportedOperationError builds a typed unsupported-operation decline
// from msg, optionally treating it as a format string when args are given.
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
//
// SettledOutcomeError stays retryable for a different reason: the change has
// settled, but its outcome is the engine's to report through progress, and it
// is not always a failure. Classifying the refusal permanent would let a
// generic failure path record failed over a change that was cancelled, or
// replace the engine's own failure reason with the refusal's. Paths that can
// receive a settled-outcome refusal must resolve the control request terminally
// via IsSettledOutcome and let the drive record the engine's outcome.
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
