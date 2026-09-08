package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/block/pg-sprite/pkg/progress"

	"github.com/block/schemabot/pkg/engine"
)

// Cancel signals the backend only for this apply's active concurrent index
// build. The durable control request remains the source of operator intent;
// cancelRequested only distinguishes its executor exit from an out-of-band
// backend cancellation (CO-1, CO-2).
func (e *Engine) Cancel(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	key := ""
	if req != nil {
		key = progressIdentity(req.ResumeState)
	}

	e.mu.Lock()
	tracked := e.progress[key]
	if tracked == nil || tracked.result.State != engine.StateRunning {
		e.mu.Unlock()
		return nil, engine.NewUnsupportedOperationError("cancel is supported for PostgreSQL concurrent index builds only: no running concurrent index build is tracked for this apply")
	}
	tracker := tracked.tracker
	e.mu.Unlock()

	snapshot, err := tracker.Progress(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect PostgreSQL apply %q before cancel: %w", key, err)
	}
	if snapshot.Detail.Operation != progress.OperationConcurrentIndex || !snapshot.Detail.Active {
		return nil, engine.NewUnsupportedOperationError("cancel is supported for PostgreSQL concurrent index builds only: the active step is %q", snapshot.Detail.Operation)
	}

	e.mu.Lock()
	tracked.cancelRequested = true
	e.mu.Unlock()
	if err := tracker.CancelBuild(ctx); err != nil {
		e.mu.Lock()
		tracked.cancelRequested = false
		e.mu.Unlock()
		switch {
		case errors.Is(err, progress.ErrNoActiveBuild), errors.Is(err, progress.ErrBuildNotRunning):
			return nil, engine.NewAlreadyCompletedError("cancel arrived after the PostgreSQL concurrent index build step finished")
		case errors.Is(err, progress.ErrBuildUnobservable):
			return nil, engine.NewUnsupportedOperationError("cancel PostgreSQL concurrent index build refused: grant the engine role pg_signal_backend so it can observe and signal the build backend")
		default:
			return nil, fmt.Errorf("cancel PostgreSQL concurrent index build for apply %q: %w", key, err)
		}
	}

	return &engine.ControlResult{Accepted: true, Message: "Concurrent index build cancellation requested"}, nil
}

func (e *Engine) consumeCancelRequested(key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	tracked := e.progress[key]
	if tracked == nil || !tracked.cancelRequested {
		return false
	}
	tracked.cancelRequested = false
	return true
}
