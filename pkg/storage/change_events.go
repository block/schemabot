package storage

import (
	"context"
	"sync/atomic"

	"github.com/block/schemabot/pkg/state"
)

// OperationChangeRecorder is implemented by stores configured to observe
// operation execution. Engines call it at the dispatch boundary.
type OperationChangeRecorder interface {
	ObserveOperationDispatch(ctx context.Context, apply *Apply)
}

type operationChangeAttemptKey struct{}

// OperationChangeAttempt is the attempt identity captured when a claim commits.
type OperationChangeAttempt struct {
	OperationID int64
	Deployment  string
	Number      int
	Starts      bool
	Retry       bool
	dispatched  atomic.Bool
	finished    atomic.Bool
}

// ObserveDispatch records the first local engine handoff for this drive.
func (a *OperationChangeAttempt) ObserveDispatch() bool {
	return a.dispatched.CompareAndSwap(false, true)
}

// ObservedDispatch reports whether this drive reached the local engine.
func (a *OperationChangeAttempt) ObservedDispatch() bool {
	return a.dispatched.Load()
}

// ObserveFinish lets a drive offer at most one outcome for the attempt.
func (a *OperationChangeAttempt) ObserveFinish() bool {
	return a.finished.CompareAndSwap(false, true)
}

// WithOperationChangeAttempt carries claim-time identity through the drive.
func WithOperationChangeAttempt(ctx context.Context, op *ApplyOperation) context.Context {
	return context.WithValue(ctx, operationChangeAttemptKey{}, &OperationChangeAttempt{
		OperationID: op.ID, Deployment: op.Deployment, Number: op.Attempt + 1, Starts: op.StartsAttempt,
		Retry: op.StartsAttempt && state.IsState(op.State, state.ApplyOperation.FailedRetryable),
	})
}

// OperationChangeAttemptFromContext returns nil for unscoped writes.
func OperationChangeAttemptFromContext(ctx context.Context) *OperationChangeAttempt {
	attempt, _ := ctx.Value(operationChangeAttemptKey{}).(*OperationChangeAttempt)
	return attempt
}
