package spirit

import (
	"testing"

	"github.com/block/spirit/pkg/status"
	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/engine"
)

// TestProgressState verifies that a progress poll reports the tracked state
// for every terminal outcome regardless of what a lingering runner's Spirit
// status says, and that Spirit's status only refines non-terminal states
// (sentinel wait for a deferred cutover, volume restarts).
func TestProgressState(t *testing.T) {
	tests := []struct {
		name        string
		rm          *runningMigration
		spiritState status.State
		want        engine.State
	}{
		{
			name: "completed is never downgraded by sentinel wait",
			rm: &runningMigration{
				state:        engine.StateCompleted,
				deferCutover: true,
			},
			spiritState: status.WaitingOnSentinelTable,
			want:        engine.StateCompleted,
		},
		{
			name: "completed is not re-derived from a closing runner",
			rm: &runningMigration{
				state: engine.StateCompleted,
			},
			spiritState: status.Close,
			want:        engine.StateCompleted,
		},
		{
			name: "failed is never downgraded by sentinel wait",
			rm: &runningMigration{
				state:        engine.StateFailed,
				deferCutover: true,
			},
			spiritState: status.WaitingOnSentinelTable,
			want:        engine.StateFailed,
		},
		{
			name: "stopped stays stopped while the runner tears down",
			rm: &runningMigration{
				state:        engine.StateStopped,
				deferCutover: true,
			},
			spiritState: status.WaitingOnSentinelTable,
			want:        engine.StateStopped,
		},
		{
			name: "closing runner does not imply completion",
			rm: &runningMigration{
				state: engine.StateRunning,
			},
			spiritState: status.Close,
			want:        engine.StateRunning,
		},
		{
			name: "sentinel wait surfaces deferred cutover",
			rm: &runningMigration{
				state:        engine.StateRunning,
				deferCutover: true,
			},
			spiritState: status.WaitingOnSentinelTable,
			want:        engine.StateWaitingForCutover,
		},
		{
			name: "sentinel wait without deferred cutover stays running",
			rm: &runningMigration{
				state: engine.StateRunning,
			},
			spiritState: status.WaitingOnSentinelTable,
			want:        engine.StateRunning,
		},
		{
			name: "volume restart reports stopped state as running",
			rm: &runningMigration{
				state:                   engine.StateStopped,
				volumeRestartInProgress: true,
			},
			spiritState: status.Close,
			want:        engine.StateRunning,
		},
		{
			name: "volume restart still surfaces deferred cutover",
			rm: &runningMigration{
				state:                   engine.StateStopped,
				volumeRestartInProgress: true,
				deferCutover:            true,
			},
			spiritState: status.WaitingOnSentinelTable,
			want:        engine.StateWaitingForCutover,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, progressState(tt.rm, tt.spiritState))
		})
	}
}
