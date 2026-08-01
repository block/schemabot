package tern

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/storage"
)

func newVolumeConvergenceClient(eng *fakeControlEngine) *LocalClient {
	return &LocalClient{
		config:       LocalConfig{Type: storage.DatabaseTypeMySQL, Database: "testdb"},
		spiritEngine: eng,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func volumeConvergenceApply(volume int32) *storage.Apply {
	return &storage.Apply{
		ApplyIdentifier: "apply-volume-convergence",
		Database:        "testdb",
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{Volume: int(volume)}),
	}
}

// Volume tunes row-copy throughput, which only exists for an ALTER's row copy.
// On-start convergence must retune only tasks that carry copy work: an engine
// volume retune restarts the running schema change, and restarting a create or
// drop task's statement phase interrupts a statement that has no copy to tune.
func TestConvergeTaskVolumeToStoredLevel_RetunesOnlyRowCopyTasks(t *testing.T) {
	tests := []struct {
		ddlAction   string
		wantRetuned bool
	}{
		{ddlAction: "alter", wantRetuned: true},
		{ddlAction: "ALTER", wantRetuned: true},
		{ddlAction: "create", wantRetuned: false},
		{ddlAction: "drop", wantRetuned: false},
	}
	for _, tt := range tests {
		t.Run(tt.ddlAction, func(t *testing.T) {
			eng := &fakeControlEngine{}
			c := newVolumeConvergenceClient(eng)
			task := &storage.Task{TaskIdentifier: "task-" + tt.ddlAction, Database: "testdb", DDLAction: tt.ddlAction}

			c.convergeTaskVolumeToStoredLevel(t.Context(), volumeConvergenceApply(6), task, nil, nil)

			if tt.wantRetuned {
				assert.Equal(t, 1, eng.volumeCount, "a task with row-copy work converges to the stored volume level")
			} else {
				assert.Equal(t, 0, eng.volumeCount, "a task without row-copy work must not be retuned")
			}
		})
	}
}

// Without a stored volume level there is nothing to converge to, so no task is
// retuned regardless of its operation.
func TestConvergeTaskVolumeToStoredLevel_NoStoredLevelSkipsRetune(t *testing.T) {
	eng := &fakeControlEngine{}
	c := newVolumeConvergenceClient(eng)
	task := &storage.Task{TaskIdentifier: "task-alter", Database: "testdb", DDLAction: "alter"}

	c.convergeTaskVolumeToStoredLevel(t.Context(), volumeConvergenceApply(0), task, nil, nil)

	assert.Equal(t, 0, eng.volumeCount, "no stored volume level means no retune")
}
