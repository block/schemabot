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
// (sentinel wait for a deferred cutover).
func TestProgressState(t *testing.T) {
	tests := []struct {
		name        string
		rm          *runningSchemaChange
		spiritState status.State
		want        engine.State
	}{
		{
			name: "completed is never downgraded by sentinel wait",
			rm: &runningSchemaChange{
				state:        engine.StateCompleted,
				deferCutover: true,
			},
			spiritState: status.WaitingOnSentinelTable,
			want:        engine.StateCompleted,
		},
		{
			name: "completed is not re-derived from a closing runner",
			rm: &runningSchemaChange{
				state: engine.StateCompleted,
			},
			spiritState: status.Close,
			want:        engine.StateCompleted,
		},
		{
			name: "failed is never downgraded by sentinel wait",
			rm: &runningSchemaChange{
				state:        engine.StateFailed,
				deferCutover: true,
			},
			spiritState: status.WaitingOnSentinelTable,
			want:        engine.StateFailed,
		},
		{
			name: "stopped stays stopped while the runner tears down",
			rm: &runningSchemaChange{
				state:        engine.StateStopped,
				deferCutover: true,
			},
			spiritState: status.WaitingOnSentinelTable,
			want:        engine.StateStopped,
		},
		{
			name: "cancelled stays cancelled while the runner tears down",
			rm: &runningSchemaChange{
				state:        engine.StateCancelled,
				deferCutover: true,
			},
			spiritState: status.WaitingOnSentinelTable,
			want:        engine.StateCancelled,
		},
		{
			name: "closing runner does not imply completion",
			rm: &runningSchemaChange{
				state: engine.StateRunning,
			},
			spiritState: status.Close,
			want:        engine.StateRunning,
		},
		{
			name: "sentinel wait surfaces deferred cutover",
			rm: &runningSchemaChange{
				state:        engine.StateRunning,
				deferCutover: true,
			},
			spiritState: status.WaitingOnSentinelTable,
			want:        engine.StateWaitingForCutover,
		},
		{
			name: "sentinel wait without deferred cutover stays running",
			rm: &runningSchemaChange{
				state: engine.StateRunning,
			},
			spiritState: status.WaitingOnSentinelTable,
			want:        engine.StateRunning,
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

		got := buildSpiritTableProgress(prog, status.CopyRows, ddlByTable, tableNamespace)
		require.Len(t, got, 2)

		users := got[0]
		assert.Equal(t, "users", users.Table)
		assert.Equal(t, "app", users.Namespace)
		assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255)", users.DDL)
		assert.Equal(t, "copyRows", users.State)
		assert.Equal(t, int64(45000), users.RowsCopied)
		assert.Equal(t, int64(100000), users.RowsTotal)
		assert.Equal(t, 45, users.Progress)
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
		got := buildSpiritTableProgress(prog, status.CopyRows, ddlByTable, tableNamespace)
		require.Len(t, got, 1)
		assert.Equal(t, int64(0), got[0].ETASeconds, "no row total means no ETA")
		assert.Equal(t, 0, got[0].Progress)
	})

	// A completed copy is a count, not an estimate: the reported total is
	// reconciled to the copied rows whether the statistics estimate landed
	// high or low, so the table shows a consistent 100% with matching rows.
	t.Run("completed copy reconciles the estimated total to the copied count", func(t *testing.T) {
		prog := status.Progress{
			Tables: []status.TableProgress{
				{TableName: "users", RowsCopied: 3261100506, RowsTotal: 3291032158, IsComplete: true},
				{TableName: "orders", RowsCopied: 1200, RowsTotal: 1000, IsComplete: true},
			},
		}
		got := buildSpiritTableProgress(prog, status.CopyRows, ddlByTable, tableNamespace)
		require.Len(t, got, 2)

		estimateHigh := got[0]
		assert.Equal(t, int64(3261100506), estimateHigh.RowsCopied)
		assert.Equal(t, int64(3261100506), estimateHigh.RowsTotal)
		assert.Equal(t, 100, estimateHigh.Progress)

		estimateLow := got[1]
		assert.Equal(t, int64(1200), estimateLow.RowsCopied)
		assert.Equal(t, int64(1200), estimateLow.RowsTotal)
		assert.Equal(t, 100, estimateLow.Progress)
	})

	// During Spirit's post-copy phases every table copy is already complete,
	// so the runner phase is the table's state: consumers surface "applying
	// accumulated changes" or "verifying" instead of a serene completed bar.
	t.Run("post-copy runner phases surface as the table state", func(t *testing.T) {
		phases := []status.State{
			status.ApplyChangeset, status.RestoreSecondaryIndexes, status.AnalyzeTable,
			status.Checksum, status.PostChecksum, status.CutOver,
		}
		for _, phase := range phases {
			prog := status.Progress{
				Tables: []status.TableProgress{
					{TableName: "users", RowsCopied: 1000, RowsTotal: 1000, IsComplete: true},
				},
			}
			got := buildSpiritTableProgress(prog, phase, ddlByTable, tableNamespace)
			require.Len(t, got, 1)
			assert.Equal(t, phase.String(), got[0].State, "phase %s should surface per table", phase)
			assert.Equal(t, 100, got[0].Progress)
		}
	})

	t.Run("waiting and teardown runner states report completed tables as completed", func(t *testing.T) {
		for _, phase := range []status.State{status.WaitingOnSentinelTable, status.ReverseWindow, status.Close} {
			prog := status.Progress{
				Tables: []status.TableProgress{
					{TableName: "users", RowsCopied: 1000, RowsTotal: 1000, IsComplete: true},
				},
			}
			got := buildSpiritTableProgress(prog, phase, ddlByTable, tableNamespace)
			require.Len(t, got, 1)
			assert.Equal(t, "completed", got[0].State, "state %s is reported at the apply level, not per table", phase)
		}
	})

	// Spirit's checksum estimate is runner-wide and only populated during the
	// verify phase, when every table copy is complete — it must reach
	// completed tables or it would never render at all.
	t.Run("checksum progress reaches completed tables", func(t *testing.T) {
		prog := status.Progress{
			Checksum: status.ChecksumProgress{RowsChecked: 250, RowsTotal: 1000},
			Tables: []status.TableProgress{
				{TableName: "users", RowsCopied: 1000, RowsTotal: 1000, IsComplete: true},
			},
		}
		got := buildSpiritTableProgress(prog, status.Checksum, ddlByTable, tableNamespace)
		require.Len(t, got, 1)
		assert.Equal(t, "checksum", got[0].State)
		assert.Equal(t, int64(250), got[0].ChecksumRowsChecked)
		assert.Equal(t, int64(1000), got[0].ChecksumRowsTotal)
	})

	// Spirit's throttle status is runner-wide but only meaningful to tables
	// participating in the paced phase: a table still copying, or every table
	// during the checksum verify. A table whose copy finished while others
	// still copy must not render as paused by their throttling.
	t.Run("throttle reaches copying tables, not completed ones", func(t *testing.T) {
		prog := status.Progress{
			Throttle: status.ThrottleStatus{Throttled: true, Reason: "replica-lag 12s > 10s"},
			Tables: []status.TableProgress{
				{TableName: "users", RowsCopied: 45000, RowsTotal: 100000},
				{TableName: "orders", RowsCopied: 1000, RowsTotal: 1000, IsComplete: true},
			},
		}
		got := buildSpiritTableProgress(prog, status.CopyRows, ddlByTable, tableNamespace)
		require.Len(t, got, 2)

		copying := got[0]
		assert.True(t, copying.Throttled)
		assert.Equal(t, "replica-lag 12s > 10s", copying.ThrottleReason)

		completed := got[1]
		assert.False(t, completed.Throttled, "a finished copy is not paused by another table's throttle")
		assert.Empty(t, completed.ThrottleReason)
	})

	// The reason travels only with the flag: an unthrottled table carries no
	// reason, even if the runner reports leftover reason text.
	t.Run("a reason without the throttled flag is not stamped", func(t *testing.T) {
		prog := status.Progress{
			Throttle: status.ThrottleStatus{Throttled: false, Reason: "replica-lag 2s > 10s"},
			Tables:   []status.TableProgress{{TableName: "users", RowsCopied: 45000, RowsTotal: 100000}},
		}
		got := buildSpiritTableProgress(prog, status.CopyRows, ddlByTable, tableNamespace)
		require.Len(t, got, 1)
		assert.False(t, got[0].Throttled)
		assert.Empty(t, got[0].ThrottleReason)
	})

	// The checksum verify runs only after every copy completes, so a throttled
	// verify is stamped on the completed tables — otherwise it would never
	// surface at all.
	t.Run("throttle reaches completed tables during checksum", func(t *testing.T) {
		prog := status.Progress{
			Throttle: status.ThrottleStatus{Throttled: true, Reason: "threads-running 130 > 128"},
			Tables: []status.TableProgress{
				{TableName: "users", RowsCopied: 1000, RowsTotal: 1000, IsComplete: true},
			},
		}
		got := buildSpiritTableProgress(prog, status.Checksum, ddlByTable, tableNamespace)
		require.Len(t, got, 1)
		assert.True(t, got[0].Throttled)
		assert.Equal(t, "threads-running 130 > 128", got[0].ThrottleReason)
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
			got := buildSpiritTableProgress(prog, status.CopyRows, ddlByTable, tableNamespace)
			require.Len(t, got, 1)
			assert.Equal(t, int64(0), got[0].ETASeconds)
		})
	}
}

// Spirit runs the schema change in a goroutine of this process and publishes
// the tracked state before Apply returns, so it declares its work registration
// synchronous. A driver reads that declaration to decide whether a pending
// progress report about work it believes is in flight is conclusive.
func TestRegistersWorkSynchronously(t *testing.T) {
	eng := New(Config{})

	assert.True(t, eng.RegistersWorkSynchronously(),
		"Spirit publishes the tracked schema change before Apply returns")
	assert.True(t, engine.RegistersWorkSynchronously(eng),
		"the package helper resolves Spirit's declaration")
}

// An engine that tracks no schema change — a fresh process whose predecessor
// held the work, or one that has released its last outcome — must answer a
// progress poll with pending and no error. Control resolution probes exactly
// such an engine to decide whether work it can no longer reach is still live,
// and pending is the only answer that lets a stop or cancel settle durably.
// Any other state, or an error, reads as unresolved work and keeps the control
// request retrying against an engine that will never run it.
func TestIdleEngineReportsPending(t *testing.T) {
	eng := New(Config{})

	result := pollProgress(t, eng)

	assert.Equal(t, engine.StatePending, result.State,
		"an engine tracking no schema change reports the idle sentinel control resolution reads")
	assert.Equal(t, "No active schema change", result.Message)
}
