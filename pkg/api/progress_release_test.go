package api

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// resolveReleaseLatch enriches the progress response with the apply-level
// release latch so the CLI/TUI render a released pause as running degraded
// rather than paused. It reads the latch only for a pause rollout and fails
// closed on an unreleased or absent latch.
func TestResolveReleaseLatch(t *testing.T) {
	apply := &storage.Apply{ID: 5, ApplyIdentifier: "apply-rel", Environment: "staging"}
	pauseOps := []*storage.ApplyOperation{
		{ID: 1, State: state.ApplyOperation.Failed, OnFailure: storage.OnFailurePause},
		{ID: 2, State: state.ApplyOperation.Pending, OnFailure: storage.OnFailurePause},
	}

	t.Run("released latch reports released for the projected apply", func(t *testing.T) {
		control := &releaseLatchControlStore{release: &storage.ApplyControlRequest{
			Operation: storage.ControlOperationRelease, Status: storage.ControlRequestCompleted,
		}}
		svc := newStopReconcileTestService(nil, nil, control)
		assert.True(t, svc.resolveReleaseLatch(t.Context(), apply, pauseOps))
		assert.True(t, control.queriedRelease)
		assert.Equal(t, apply.ID, control.queriedApply)
	})

	t.Run("unreleased pause stays held", func(t *testing.T) {
		control := &releaseLatchControlStore{release: nil}
		svc := newStopReconcileTestService(nil, nil, control)
		assert.False(t, svc.resolveReleaseLatch(t.Context(), apply, pauseOps))
	})

	t.Run("no pause operation never reads the latch", func(t *testing.T) {
		control := &releaseLatchControlStore{release: &storage.ApplyControlRequest{
			Operation: storage.ControlOperationRelease, Status: storage.ControlRequestCompleted,
		}}
		svc := newStopReconcileTestService(nil, nil, control)
		haltOps := []*storage.ApplyOperation{{ID: 1, State: state.ApplyOperation.Failed}}
		assert.False(t, svc.resolveReleaseLatch(t.Context(), apply, haltOps))
		assert.False(t, control.queriedRelease)
	})
}
