package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/block/pg-sprite/pkg/progress"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

type fakeBuildTracker struct {
	snapshot  progress.Snapshot
	cancelErr error
	cancelled bool
}

func (f *fakeBuildTracker) Progress(context.Context) (progress.Snapshot, error) {
	return f.snapshot, nil
}

func (f *fakeBuildTracker) CancelBuild(context.Context) error {
	f.cancelled = true
	return f.cancelErr
}

func trackedCancelEngine(tracker buildTracker) *Engine {
	return &Engine{progress: map[string]*trackedApply{
		"apply-1": {result: &engine.ProgressResult{State: engine.StateRunning}, tracker: tracker},
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

func TestCancelMapsTrackerSentinels(t *testing.T) {
	tests := []struct {
		name            string
		err             error
		alreadyFinished bool
		unsupported     bool
	}{
		{name: "no active build", err: progress.ErrNoActiveBuild, alreadyFinished: true},
		{name: "build not running", err: progress.ErrBuildNotRunning, alreadyFinished: true},
		{name: "build unobservable", err: progress.ErrBuildUnobservable, unsupported: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tracker := &fakeBuildTracker{snapshot: concurrentSnapshot(), cancelErr: tc.err}
			eng := trackedCancelEngine(tracker)

			result, err := eng.Cancel(t.Context(), cancelRequest())

			assert.Nil(t, result)
			require.Error(t, err)
			assert.Equal(t, tc.alreadyFinished, engine.IsAlreadyCompleted(err))
			assert.Equal(t, tc.unsupported, engine.IsUnsupportedOperation(err))
			if tc.unsupported {
				assert.Contains(t, err.Error(), "pg_signal_backend")
			}
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
