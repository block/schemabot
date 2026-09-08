package postgres

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/block/pg-sprite/pkg/progress"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

type fakeBuildTracker struct {
	snapshot    progress.Snapshot
	progressErr error
	cancelErr   error
	cancelled   bool
}

func (f *fakeBuildTracker) Progress(context.Context) (progress.Snapshot, error) {
	return f.snapshot, f.progressErr
}

func (f *fakeBuildTracker) CancelBuild(context.Context) error {
	f.cancelled = true
	return f.cancelErr
}

func trackedCancelEngine(tracker buildTracker) *Engine {
	return &Engine{progress: map[string]*trackedApply{
		"apply-1": {result: &engine.ProgressResult{State: engine.StateRunning}, tracker: tracker, logger: slog.New(slog.DiscardHandler)},
	}}
}

func concurrentSnapshot() progress.Snapshot {
	return progress.Snapshot{Detail: progress.Detail{Operation: progress.OperationConcurrentIndex, Active: true}}
}

func cancelRequest() *engine.ControlRequest {
	return &engine.ControlRequest{ResumeState: &engine.ResumeState{MigrationContext: "apply-1"}}
}

func TestCancelSignalsActiveConcurrentIndexBuild(t *testing.T) {
	tracker := &fakeBuildTracker{snapshot: concurrentSnapshot()}
	eng := trackedCancelEngine(tracker)

	result, err := eng.Cancel(t.Context(), cancelRequest())
	require.NoError(t, err)
	assert.True(t, result.Accepted)
	assert.True(t, tracker.cancelled)
	assert.True(t, eng.progress["apply-1"].cancelRequested)
}

func TestCancelRefusesNonConcurrentStep(t *testing.T) {
	tracker := &fakeBuildTracker{snapshot: progress.Snapshot{Detail: progress.Detail{Operation: progress.OperationBrief, Active: true}}}
	eng := trackedCancelEngine(tracker)

	result, err := eng.Cancel(t.Context(), cancelRequest())

	assert.Nil(t, result)
	require.Error(t, err)
	assert.True(t, engine.IsUnsupportedOperation(err))
	assert.False(t, tracker.cancelled)
}

// TestCancelProceedsWhenProgressReadFails covers a cancel issued while the
// tracker's server-side progress read is failing: the last-known snapshot still
// identifies an active concurrent index build, so the cancel reaches the build
// instead of being declined for a transient read failure.
func TestCancelProceedsWhenProgressReadFails(t *testing.T) {
	tracker := &fakeBuildTracker{snapshot: concurrentSnapshot(), progressErr: errors.New("progress view unreadable")}
	eng := trackedCancelEngine(tracker)

	result, err := eng.Cancel(t.Context(), cancelRequest())
	require.NoError(t, err)
	assert.True(t, result.Accepted)
	assert.True(t, tracker.cancelled)
	assert.True(t, eng.progress["apply-1"].cancelRequested)
}

// TestCancelMapsTrackerSentinels pins how each pg-sprite cancel sentinel
// resolves. A build that has already finished declines the cancel as
// unsupported rather than reporting the apply complete: only the build step is
// over, and the apply is still running toward that build's verdict. An
// unobservable build declines with the privilege remedy.
func TestCancelMapsTrackerSentinels(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		contains string
	}{
		{name: "no active build", err: progress.ErrNoActiveBuild, contains: "the apply continues to the build's verdict"},
		{name: "build not running", err: progress.ErrBuildNotRunning, contains: "the apply continues to the build's verdict"},
		{name: "build unobservable", err: progress.ErrBuildUnobservable, contains: "pg_signal_backend"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tracker := &fakeBuildTracker{snapshot: concurrentSnapshot(), cancelErr: tc.err}
			eng := trackedCancelEngine(tracker)

			result, err := eng.Cancel(t.Context(), cancelRequest())

			assert.Nil(t, result)
			require.Error(t, err)
			assert.True(t, engine.IsUnsupportedOperation(err))
			assert.False(t, engine.IsAlreadyCompleted(err))
			assert.Contains(t, err.Error(), tc.contains)
			assert.False(t, eng.progress["apply-1"].cancelRequested)
		})
	}
}

func TestCancelWrapsUnexpectedTrackerError(t *testing.T) {
	tracker := &fakeBuildTracker{snapshot: concurrentSnapshot(), cancelErr: errors.New("connection lost")}
	eng := trackedCancelEngine(tracker)

	_, err := eng.Cancel(t.Context(), cancelRequest())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cancel PostgreSQL concurrent index build for apply \"apply-1\"")
}
