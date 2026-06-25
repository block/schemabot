package spirit

import (
	"testing"
	"time"

	"github.com/block/spirit/pkg/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// TestBuildSpiritTableProgress verifies that Spirit's per-table progress is
// mapped into engine TableProgress, and that the runner's single row-copy ETA
// is surfaced on tables still copying only once it is ready: a still-measuring
// or essentially-done estimate is not yet a number, so those tables (and any
// already-complete table) report no ETA rather than a misleading value.
func TestBuildSpiritTableProgress(t *testing.T) {
	ddlByTable := map[string]string{"users": "ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255)"}
	tableNamespace := map[string]string{"users": "app"}

	t.Run("ready ETA surfaces on copying tables, not completed ones", func(t *testing.T) {
		prog := status.Progress{
			ETA: status.ETA{State: status.ETAReady, Duration: 90 * time.Second},
			Tables: []status.TableProgress{
				{TableName: "users", RowsCopied: 45000, RowsTotal: 100000},
				{TableName: "orders", RowsCopied: 1000, RowsTotal: 1000, IsComplete: true},
			},
		}

		got := buildSpiritTableProgress(prog, "copyRows", ddlByTable, tableNamespace)
		require.Len(t, got, 2)

		users := got[0]
		assert.Equal(t, "users", users.Table)
		assert.Equal(t, "app", users.Namespace)
		assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255)", users.DDL)
		assert.Equal(t, "copyRows", users.State)
		assert.Equal(t, int64(45000), users.RowsCopied)
		assert.Equal(t, int64(100000), users.RowsTotal)
		assert.Equal(t, 45, users.Progress)
		assert.Equal(t, "45000/100000 45% copyRows", users.ProgressDetail)
		assert.Equal(t, int64(90), users.ETASeconds)

		orders := got[1]
		assert.Equal(t, "completed", orders.State)
		assert.Equal(t, 100, orders.Progress)
		assert.Equal(t, int64(0), orders.ETASeconds, "completed table carries no ETA")
	})

	t.Run("ready ETA is withheld from a table without an established total", func(t *testing.T) {
		prog := status.Progress{
			ETA:    status.ETA{State: status.ETAReady, Duration: 90 * time.Second},
			Tables: []status.TableProgress{{TableName: "users", RowsCopied: 0, RowsTotal: 0}},
		}
		got := buildSpiritTableProgress(prog, "copyRows", ddlByTable, tableNamespace)
		require.Len(t, got, 1)
		assert.Equal(t, int64(0), got[0].ETASeconds, "no row total means no ETA")
		assert.Equal(t, 0, got[0].Progress)
		assert.Empty(t, got[0].ProgressDetail)
	})

	notReady := []struct {
		name string
		eta  status.ETA
	}{
		{"measuring carries no ETA", status.ETA{State: status.ETAMeasuring}},
		{"due carries no ETA", status.ETA{State: status.ETADue}},
		{"none carries no ETA", status.ETA{State: status.ETANone}},
	}
	for _, tt := range notReady {
		t.Run(tt.name, func(t *testing.T) {
			prog := status.Progress{
				ETA:    tt.eta,
				Tables: []status.TableProgress{{TableName: "users", RowsCopied: 45000, RowsTotal: 100000}},
			}
			got := buildSpiritTableProgress(prog, "copyRows", ddlByTable, tableNamespace)
			require.Len(t, got, 1)
			assert.Equal(t, int64(0), got[0].ETASeconds)
		})
	}
}
