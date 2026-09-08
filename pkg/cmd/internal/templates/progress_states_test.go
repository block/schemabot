package templates

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLabel_PlanetScalePhases(t *testing.T) {
	assert.Equal(t, "Preparing branch", state.Label(state.Apply.PreparingBranch))
	assert.Equal(t, "Applying changes to branch", state.Label(state.Apply.ApplyingBranchChanges))
	assert.Equal(t, "Validating branch", state.Label(state.Apply.ValidatingBranch))
	assert.Equal(t, "Creating deploy request", state.Label(state.Apply.CreatingDeployRequest))
	assert.Equal(t, "Validating deploy request", state.Label(state.Apply.ValidatingDeployRequest))
	assert.Equal(t, "Cancelled", state.Label(state.Apply.Cancelled))
	assert.Equal(t, "Retrying", state.Label(state.Apply.FailedRetryable))
}

func TestFormatProgressState_PlanetScalePhases(t *testing.T) {
	assert.Contains(t, FormatProgressState(state.Apply.PreparingBranch), "Preparing branch")
	assert.Contains(t, FormatProgressState(state.Apply.ApplyingBranchChanges), "Applying changes to branch")
	assert.Contains(t, FormatProgressState(state.Apply.ValidatingBranch), "Validating branch")
	assert.Contains(t, FormatProgressState(state.Apply.CreatingDeployRequest), "Creating deploy request")
	assert.Contains(t, FormatProgressState(state.Apply.ValidatingDeployRequest), "Validating deploy request")
	assert.Contains(t, FormatProgressState(state.Apply.Cancelled), "Cancelled")
	assert.Contains(t, FormatProgressState(state.Apply.FailedRetryable), "Retrying")
	assert.Contains(t, FormatProgressState(state.Apply.Recovering), "Recovering")
	assert.Contains(t, FormatProgressState(state.Apply.RunningDegraded), "Running (degraded)")
}

func TestWriteStatusListHasMoreFooter(t *testing.T) {
	output := captureStdout(t, func() {
		WriteStatusList(StatusListData{
			ActiveCount: 0,
			Limit:       20,
			MaxLimit:    1000,
			HasMore:     true,
			Applies: []ActiveApplyData{
				{
					ApplyID:     "apply-example",
					Database:    "orders",
					Environment: "staging",
					State:       state.Apply.Completed,
					StartedAt:   "2026-05-28T12:00:00Z",
					CompletedAt: "2026-05-28T12:00:02Z",
					Caller:      "cli",
				},
			},
		})
	})

	assert.Contains(t, output, "Recent schema changes")
	assert.Contains(t, output, "apply-example")
	assert.Contains(t, output, "STARTED")
	assert.NotContains(t, output, "DURATION")
	assert.Contains(t, output, "Showing the 20 most recent schema changes. Use --limit N to show more.")
	assert.Contains(t, output, "Use 'schemabot status <apply_id>' to view details")
}

func TestWriteStatusListHasMoreFooterAtMaxLimit(t *testing.T) {
	output := captureStdout(t, func() {
		WriteStatusList(StatusListData{
			ActiveCount: 0,
			Limit:       1000,
			MaxLimit:    1000,
			HasMore:     true,
			Applies: []ActiveApplyData{
				{
					ApplyID:     "apply-example",
					Database:    "orders",
					Environment: "staging",
					State:       state.Apply.Completed,
					StartedAt:   "2026-05-28T12:00:00Z",
					CompletedAt: "2026-05-28T12:00:02Z",
					Caller:      "cli",
				},
			},
		})
	})

	assert.Contains(t, output, "Showing the 1000 most recent schema changes. This server caps status history at 1000.")
	assert.NotContains(t, output, "Use --limit N to show more.")
}

func TestWriteStatusListStateSummary(t *testing.T) {
	output := captureStdout(t, func() {
		WriteStatusList(StatusListData{
			ActiveCount: 1,
			Limit:       20,
			StateCounts: map[string]int{
				state.Apply.Completed: 12,
				state.Apply.Failed:    2,
				state.Apply.Running:   1,
			},
			Applies: []ActiveApplyData{
				{
					ApplyID:     "apply-example",
					Database:    "orders",
					Environment: "staging",
					State:       state.Apply.Running,
					StartedAt:   "2026-05-28T12:00:00Z",
					Caller:      "cli",
				},
			},
		})
	})

	assert.Contains(t, output, "15 total: 12 Completed · 2 Failed · 1 Running")
}

func TestWriteStatusListStateFilterSuffix(t *testing.T) {
	output := captureStdout(t, func() {
		WriteStatusList(StatusListData{
			ActiveCount: 0,
			Limit:       20,
			StateFilter: state.Apply.FailedRetryable,
			Last:        "6h",
			Applies: []ActiveApplyData{
				{
					ApplyID:     "apply-example",
					Database:    "orders",
					Environment: "staging",
					State:       state.Apply.FailedRetryable,
					StartedAt:   "2026-05-28T12:00:00Z",
					Caller:      "cli",
				},
			},
		})
	})

	assert.Contains(t, output, "(in state Retrying, updated in the last 6h)")
}

func TestWriteStatusListEmptyWithStateFilter(t *testing.T) {
	output := captureStdout(t, func() {
		WriteStatusList(StatusListData{
			Limit:       20,
			StateFilter: state.Apply.Running,
		})
	})

	assert.Contains(t, output, "No recent schema changes in state Running")
}

func TestWriteStatusListNoStateSummaryWhenEmpty(t *testing.T) {
	output := captureStdout(t, func() {
		WriteStatusList(StatusListData{
			ActiveCount: 0,
			Limit:       20,
			Applies: []ActiveApplyData{
				{
					ApplyID:     "apply-example",
					Database:    "orders",
					Environment: "staging",
					State:       state.Apply.Completed,
					StartedAt:   "2026-05-28T12:00:00Z",
					CompletedAt: "2026-05-28T12:00:02Z",
					Caller:      "cli",
				},
			},
		})
	})

	assert.NotContains(t, output, "total:")
}

func TestWriteStatusListExternalID(t *testing.T) {
	output := captureStdout(t, func() {
		WriteStatusList(StatusListData{
			ActiveCount:    0,
			Limit:          20,
			MaxLimit:       1000,
			HasMore:        false,
			ShowExternalID: true,
			Applies: []ActiveApplyData{
				{
					ApplyID:     "apply-complete",
					ExternalID:  "external-123",
					Database:    "orders",
					Environment: "staging",
					State:       state.Apply.Completed,
					StartedAt:   "2026-05-28T12:00:00Z",
					CompletedAt: "2026-05-28T12:00:02Z",
					Caller:      "cli",
				},
			},
		})
	})

	assert.Contains(t, output, "EXTERNAL ID")
	assert.Contains(t, output, "external-123")
	assert.Contains(t, output, "apply-complete")
}

// An operator who asked for the external-id column gets it even when no apply
// on the page recorded a remote id: an all-dash column positively answers
// "nothing recorded", where a missing column would be indistinguishable from
// the flag doing nothing.
func TestWriteStatusListExternalIDColumnRendersWithoutValues(t *testing.T) {
	output := captureStdout(t, func() {
		WriteStatusList(StatusListData{
			ActiveCount:    0,
			Limit:          20,
			MaxLimit:       1000,
			ShowExternalID: true,
			Applies: []ActiveApplyData{
				{
					ApplyID:     "apply-local",
					Database:    "orders",
					Environment: "staging",
					State:       state.Apply.Completed,
					StartedAt:   "2026-05-28T12:00:00Z",
					CompletedAt: "2026-05-28T12:00:02Z",
					Caller:      "cli",
				},
			},
		})
	})

	assert.Contains(t, output, "EXTERNAL ID", "the requested column renders even with nothing recorded")
	assert.Contains(t, output, "apply-local  -", "a row with no remote id shows a dash in the column")
}

// On a mixed unfiltered page the DEPLOYMENT column is retained for the rows
// that carry one, and a row without a deployment shows a dash rather than
// blank padding, so the gap reads as "none recorded" instead of an alignment
// artifact.
func TestWriteStatusListMixedDeploymentRowsShowDash(t *testing.T) {
	output := captureStdout(t, func() {
		WriteStatusList(StatusListData{
			ActiveCount: 0,
			Limit:       20,
			MaxLimit:    1000,
			Applies: []ActiveApplyData{
				{
					ApplyID:     "apply-deployed",
					Database:    "orders",
					Environment: "staging",
					Deployment:  "deploy-a",
					State:       state.Apply.Completed,
					StartedAt:   "2026-05-28T12:00:00Z",
					CompletedAt: "2026-05-28T12:00:02Z",
					Caller:      "cli",
				},
				{
					ApplyID:     "apply-local",
					Database:    "orders",
					Environment: "staging",
					State:       state.Apply.Completed,
					StartedAt:   "2026-05-28T12:01:00Z",
					CompletedAt: "2026-05-28T12:01:02Z",
					Caller:      "cli",
				},
			},
		})
	})

	assert.Contains(t, output, "DEPLOYMENT", "one populated row retains the column for the page")
	assert.Contains(t, output, "deploy-a")
	assert.Contains(t, output, "staging  -", "a deployment-less row shows a dash in the retained column")
}

// A deployment-filtered list names each remote handle in its own column, the
// way the detail views do: the deployment's shared data-plane apply id and the
// per-operation remote row id. APPLY ID stays the control-plane id the status
// drill-down resolves, and DEPLOYMENT is dropped because every row repeats it
// back to the operator who named it.
func TestWriteStatusListDeploymentNamesBothRemoteHandles(t *testing.T) {
	output := captureStdout(t, func() {
		WriteStatusList(StatusListData{
			ActiveCount:    1,
			Limit:          20,
			MaxLimit:       1000,
			ShowExternalID: true,
			Deployment:     "deploy-a",
			Applies: []ActiveApplyData{
				{
					ApplyID:             "apply-running",
					ExternalID:          "apply-remote-a",
					ExternalOperationID: "remote-operation-a",
					Database:            "orders",
					Environment:         "staging",
					Deployment:          "deploy-a",
					State:               state.Apply.Running,
					StartedAt:           "2026-05-28T12:00:00Z",
					Caller:              "cli",
				},
			},
		})
	})

	assert.Contains(t, output, "EXTERNAL APPLY ID")
	assert.Contains(t, output, "EXTERNAL OP ID")
	assert.Contains(t, output, "apply-remote-a")
	assert.Contains(t, output, "remote-operation-a")
	assert.Contains(t, output, "apply-running",
		"APPLY ID stays the control-plane id the status drill-down resolves")
	assert.NotContains(t, output, "DEPLOYMENT",
		"a list filtered to one deployment repeats it on every row, so the column carries nothing")
	assert.Contains(t, output, "Use 'schemabot status <apply_id>' to view details",
		"every id in the APPLY ID column feeds the drill-down, so the footer needs no qualifier")
}

// A deployment that drives its applies locally records no remote handles, so
// the remote-id columns are left out rather than rendered as a column of
// dashes. The same holds for an apply not yet dispatched to a data plane.
func TestWriteStatusListDeploymentOmitsUnpopulatedRemoteColumns(t *testing.T) {
	output := captureStdout(t, func() {
		WriteStatusList(StatusListData{
			ActiveCount:    1,
			Limit:          20,
			MaxLimit:       1000,
			ShowExternalID: true,
			Deployment:     "deploy-a",
			Applies: []ActiveApplyData{
				{
					ApplyID:     "apply-pending",
					Database:    "orders",
					Environment: "staging",
					Deployment:  "deploy-a",
					State:       state.Apply.Pending,
					Caller:      "cli",
				},
			},
		})
	})

	assert.Contains(t, output, "apply-pending")
	assert.NotContains(t, output, "EXTERNAL APPLY ID")
	assert.NotContains(t, output, "EXTERNAL OP ID")
}

// A deployment whose operations fold into one shared data-plane apply has no
// per-operation remote row id, so only the shared handle gets a column.
func TestWriteStatusListDeploymentOmitsOperationColumnWhenOnlyTheSharedApplyIsRecorded(t *testing.T) {
	output := captureStdout(t, func() {
		WriteStatusList(StatusListData{
			ActiveCount:    1,
			Limit:          20,
			MaxLimit:       1000,
			ShowExternalID: true,
			Deployment:     "deploy-a",
			Applies: []ActiveApplyData{
				{
					ApplyID:     "apply-sharded",
					ExternalID:  "apply-remote-shared",
					Database:    "inventory",
					Environment: "staging",
					Deployment:  "deploy-a",
					State:       state.Apply.Running,
					StartedAt:   "2026-05-28T12:00:00Z",
					Caller:      "cli",
				},
			},
		})
	})

	assert.Contains(t, output, "EXTERNAL APPLY ID")
	assert.Contains(t, output, "apply-remote-shared")
	assert.NotContains(t, output, "EXTERNAL OP ID")
}

func TestWriteStatusListFailedOnly(t *testing.T) {
	output := captureStdout(t, func() {
		WriteStatusList(StatusListData{
			Limit:          20,
			MaxLimit:       1000,
			FailuresOnly:   true,
			ShowExternalID: true,
			Applies: []ActiveApplyData{
				{
					ApplyID:      "apply-failed",
					ExternalID:   "external-failed",
					Database:     "payments",
					Environment:  "staging",
					State:        state.Apply.Failed,
					StartedAt:    "2026-05-28T11:00:00Z",
					CompletedAt:  "2026-05-28T11:00:03Z",
					Caller:       "github:alice",
					ErrorMessage: "failed to apply schema change\nbecause duplicate column name 'status'",
				},
				{
					ApplyID:      "apply-failed-pr",
					ExternalID:   "external-failed-pr",
					Database:     "billing",
					Environment:  "production",
					State:        state.Apply.Failed,
					StartedAt:    "2026-05-28T12:00:00Z",
					CompletedAt:  "2026-05-28T12:00:03Z",
					Caller:       "github:alice@acme/pay#77",
					ErrorMessage: "cutover failed",
				},
			},
		})
	})

	assert.Contains(t, output, "Recent failed schema changes")
	assert.Contains(t, output, "payments staging: Failed (github:alice; external_id=external-failed) [2026-05-28 11:00:03 UTC]")
	assert.Contains(t, output, "billing production: Failed (https://github.com/acme/pay/pull/77; external_id=external-failed-pr) [2026-05-28 12:00:03 UTC]")
	assert.Contains(t, output, "failed to apply schema change because duplicate column name 'status'")
	assert.Contains(t, output, "schemabot status apply-failed")
	assert.NotContains(t, output, "APPLY ID")
	assert.NotContains(t, output, "REASON")
	assert.NotContains(t, output, "Use 'schemabot status <apply_id>' to view details")
}

// TestWriteDatabaseHistoryTable pins the exact bytes of the history table: a
// bold title, a dimmed header row, one row per apply with every cell padded
// to its column's widest value and the state cell wrapped in the state's
// color (the separator after it stays uncolored), and the detail hint footer.
// A state the CLI does not recognize renders uncolored, an apply that never
// recorded timestamps shows dashes for STARTED and DURATION, and an apply
// that recorded neither state nor caller shows a dash in every empty cell —
// the same "none recorded" rendering the status list uses.
func TestWriteDatabaseHistoryTable(t *testing.T) {
	now := time.Date(2026, 1, 15, 14, 30, 0, 0, time.UTC)
	prevNow := nowFunc
	prevUINow := ui.NowFunc
	nowFunc = func() time.Time { return now }
	ui.NowFunc = func() time.Time { return now }
	t.Cleanup(func() {
		nowFunc = prevNow
		ui.NowFunc = prevUINow
	})

	output := captureStdout(t, func() {
		WriteDatabaseHistory(DatabaseHistoryData{
			Database: "orders-db",
			Applies: []ApplyHistoryData{
				{ApplyID: "apply_abc123", Environment: "staging", State: state.Apply.Completed, Caller: "cli:jdoe@host", StartedAt: "2026-01-15T13:30:00Z", CompletedAt: "2026-01-15T13:45:00Z"},
				{ApplyID: "apply_def456789", Environment: "production", State: state.Apply.Failed, Caller: "github:acme/shop#42", StartedAt: "2026-01-15T08:00:00Z", CompletedAt: "2026-01-15T08:30:00Z"},
				{ApplyID: "apply_ghi", Environment: "staging", State: state.Apply.Running, Caller: "cli:ops@host", StartedAt: "2026-01-15T14:00:00Z"},
				{ApplyID: "apply_unknown", Environment: "staging", State: "SOME_NEW_STATE", Caller: "cli:ops@host"},
				{ApplyID: "apply_bare", Environment: "staging"},
			},
		})
	})

	expected := strings.Join([]string{
		ANSIBold + "Schema change history for orders-db" + ANSIReset,
		"",
		"  " + ANSIDim + "APPLY ID         ENV         STATE           STARTED         DURATION  SOURCE" + ANSIReset,
		"  apply_abc123     staging     " + ANSIGreen + "Completed     " + ANSIReset + "  1 hour ago      15m       cli:jdoe",
		"  apply_def456789  production  " + ANSIRed + "Failed        " + ANSIReset + "  6 hours ago     30m       github:acme/shop#42",
		"  apply_ghi        staging     " + ANSICyan + "Running       " + ANSIReset + "  30 minutes ago  30m       cli:ops",
		"  apply_unknown    staging     SOME_NEW_STATE  -               -         cli:ops",
		"  apply_bare       staging     -               -               -         -",
		"",
		ANSIDim + "Use 'schemabot status <apply_id>' to view details" + ANSIReset,
		"",
	}, "\n")
	assert.Equal(t, expected, output)
}

// TestWriteDatabaseHistoryEmpty pins the dimmed one-line message a database
// with no recorded schema changes renders instead of a table.
func TestWriteDatabaseHistoryEmpty(t *testing.T) {
	output := captureStdout(t, func() {
		WriteDatabaseHistory(DatabaseHistoryData{Database: "new-db"})
	})
	assert.Equal(t, ANSIDim+"No schema changes found for database 'new-db'"+ANSIReset+"\n", output)
}

// TestWriteStatusListColoredStateCellBytes pins the colored STATE cell's
// exact bytes: the color escape wraps the padded cell and closes before the
// two-space separator, so the separator between columns is never colored —
// the same composition the history table renders.
func TestWriteStatusListColoredStateCellBytes(t *testing.T) {
	output := captureStdout(t, func() {
		WriteStatusList(StatusListData{
			ActiveCount: 1,
			Limit:       20,
			MaxLimit:    1000,
			Applies: []ActiveApplyData{
				{ApplyID: "apply-run", Database: "orders", Environment: "staging", State: state.Apply.Running, StartedAt: "2026-05-28T12:00:00Z", Caller: "cli"},
				{ApplyID: "apply-done", Database: "orders", Environment: "staging", State: state.Apply.Completed, StartedAt: "2026-05-28T12:00:00Z", CompletedAt: "2026-05-28T12:00:02Z", Caller: "cli"},
			},
		})
	})

	assert.Contains(t, output, ANSICyan+"Running  "+ANSIReset+"  ",
		"a state narrower than its column is padded inside the escape, with the separator outside")
	assert.Contains(t, output, ANSIGreen+"Completed"+ANSIReset+"  ",
		"the column-width state closes its escape before the separator")
}

// TestWriteDatabaseHistoryAlignsMultiByteCells pins that column widths count
// terminal cells, not bytes: a CJK environment name occupies two cells per
// rune but three bytes, so byte-counted padding would push every column to
// its right out of line on one row and pad the ASCII rows too wide.
func TestWriteDatabaseHistoryAlignsMultiByteCells(t *testing.T) {
	output := captureStdout(t, func() {
		WriteDatabaseHistory(DatabaseHistoryData{
			Database: "orders-db",
			Applies: []ApplyHistoryData{
				{ApplyID: "apply_wide", Environment: "生産環境", State: state.Apply.Completed, Caller: "cli:jdoe"},
				{ApplyID: "apply_ascii", Environment: "production", State: state.Apply.Failed, Caller: "cli:ops"},
			},
		})
	})

	assert.Contains(t, output, "  apply_wide   生産環境    "+ANSIGreen+"Completed"+ANSIReset+"  ",
		"an eight-cell CJK name in a ten-cell column gets two cells of padding")
	assert.Contains(t, output, "  apply_ascii  production  "+ANSIRed+"Failed   "+ANSIReset+"  ",
		"the ASCII row pads to the same visible width, not to the CJK value's byte length")
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	original := os.Stdout
	read, write, err := os.Pipe()
	require.NoError(t, err)
	defer func() {
		os.Stdout = original
	}()

	os.Stdout = write
	fn()
	require.NoError(t, write.Close())

	output, err := io.ReadAll(read)
	require.NoError(t, err)
	require.NoError(t, read.Close())

	return string(output)
}

func TestProgressSymbol(t *testing.T) {
	assert.Equal(t, "+ ", progressSymbol("create"))
	assert.Equal(t, "- ", progressSymbol("drop"))
	assert.Equal(t, "~ ", progressSymbol("alter"))
	assert.Equal(t, "~ ", progressSymbol(""))
}

func TestFormatTableProgress_ChangeTypeSymbol(t *testing.T) {
	for _, tt := range []struct {
		changeType string
		symbol     string
	}{
		{"create", "+"},
		{"drop", "-"},
		{"alter", "~"},
	} {
		tp := TableProgress{
			TableName:  "users",
			ChangeType: tt.changeType,
			Status:     state.Apply.Completed,
		}
		output := FormatTableProgress(tp)
		assert.Contains(t, output, tt.symbol+" users:", "expected %q symbol for %s", tt.symbol, tt.changeType)
	}
}

// A running table whose row total is known but with nothing copied yet (the
// VReplication / Spirit ramp-up window) shows a starting indicator and the row
// total, not a 0% bar that reads as stuck. Holds whether or not per-shard data
// has arrived.
func TestFormatTableProgress_StartingCopy(t *testing.T) {
	for _, tt := range []struct {
		name   string
		shards []ShardProgress
	}{
		{name: "no shard data yet"},
		{name: "sharded", shards: []ShardProgress{
			{Shard: "-80", Status: state.Task.Running, RowsTotal: 70_000_000},
			{Shard: "80-", Status: state.Task.Running, RowsTotal: 74_484_274},
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			output := FormatTableProgress(TableProgress{
				TableName: "customers", ChangeType: "alter", Status: state.Apply.Running,
				DDL:        "ALTER TABLE `customers` ADD INDEX `idx_updated_at`(`updated_at`)",
				RowsCopied: 0, RowsTotal: 144_484_274, PercentComplete: 0,
				Shards: tt.shards,
			})
			// The table line shows the starting indicator and total, never a
			// bare 0% bar. (Per-shard detail lines may still show 0% — that is
			// the separate per-shard rendering path.)
			assert.Contains(t, output, "customers: ⏳ Starting copy...")
			assert.Contains(t, output, "Rows: 0 / 144,484,274")
			assert.NotContains(t, output, "customers: "+ui.ProgressBarRowCopy(0))
		})
	}
}

// A table applying its accumulated changes names the catch-up phase rather
// than rendering a bare full bar — its copy is done but the engine is still
// draining the changes that piled up on the source, which can run for hours on
// a busy table.
func TestFormatTableProgress_CatchingUp(t *testing.T) {
	output := FormatTableProgress(TableProgress{
		TableName: "orders", ChangeType: "alter", Status: state.Task.CatchingUp,
		RowsCopied: 1466232, RowsTotal: 1466232, PercentComplete: 100,
	})
	assert.Contains(t, output, "orders: ")
	assert.Contains(t, output, "⏩ Catching up on accumulated changes...")
	assert.Contains(t, output, "Rows copied: 1,466,232")
}

// A table draining the changes that accumulated during the verify names the
// post-checksum phase — never an indeterminate checksum that already finished.
func TestFormatTableProgress_PostChecksum(t *testing.T) {
	output := FormatTableProgress(TableProgress{
		TableName: "orders", ChangeType: "alter", Status: state.Task.PostChecksum,
		RowsCopied: 1466232, RowsTotal: 1466232, PercentComplete: 100,
	})
	assert.Contains(t, output, "orders: ")
	assert.Contains(t, output, "⏩ Data verified, applying final changes...")
	assert.Contains(t, output, "Rows copied: 1,466,232")
	assert.NotContains(t, output, "Checksumming to verify data")
}

// A checksumming table renders its verify progress rather than a row-copy
// percent — its copy is done and the engine is now verifying the data, which
// can run for hours on a large table.
func TestFormatTableProgress_Checksumming(t *testing.T) {
	// Before Spirit reports a total, the verify shows an indeterminate label.
	measuring := FormatTableProgress(TableProgress{
		TableName: "orders", ChangeType: "alter", Status: state.Task.Checksumming,
	})
	assert.Contains(t, measuring, "orders: ")
	assert.Contains(t, measuring, "🔍 Checksumming to verify data...")

	// Once a total is known, it shows how far the verify has progressed.
	withProgress := FormatTableProgress(TableProgress{
		TableName: "orders", ChangeType: "alter", Status: state.Task.Checksumming,
		ChecksumRowsChecked: 321450, ChecksumRowsTotal: 1466232,
	})
	assert.Contains(t, withProgress, "🔍 Checksumming to verify data (21.92%)")
	assert.Contains(t, withProgress, "Rows verified: 321,450 / 1,466,232")

	// A verify that has only just begun still renders as visibly started: the
	// bar shows a first segment, and the percent shows the true sub-1% fraction
	// (floored away from 0.00%) instead of an empty "not started" display while
	// verification is actively running.
	justStarted := FormatTableProgress(TableProgress{
		TableName: "orders", ChangeType: "alter", Status: state.Task.Checksumming,
		ChecksumRowsChecked: 1, ChecksumRowsTotal: 1000000,
	})
	assert.Contains(t, justStarted, "🔍 Checksumming to verify data (0.01%)")
	assert.Contains(t, justStarted, "🟦")
}

// A table slowed by the engine's throttler carries a "(throttled)" annotation
// on its header line with the trigger explained in a dimmed tooltip, so a slow
// progress bar reads as deliberate backpressure (e.g. thread pressure) rather
// than a hang. The annotation renders for the active copy and checksum phases
// only — a throttled flag on a terminal table would be stale. A reason with an
// unrecognized signal still renders raw, with no tip attached.
func TestFormatTableProgress_Throttled(t *testing.T) {
	copying := FormatTableProgress(TableProgress{
		TableName: "orders", ChangeType: "alter", Status: state.Apply.Running,
		RowsCopied: 45000, RowsTotal: 100000, PercentComplete: 45,
		Throttled: true, ThrottleReason: "redo-aware 4 > 3",
	})
	assert.Contains(t, copying, "45.00% (throttled)",
		"the annotation lands on the header line next to the percent")
	assert.Contains(t, copying, "ℹ️ Throttled: redo-aware 4 > 3 · backing off while the database's active threads exceed its budget")

	noReason := FormatTableProgress(TableProgress{
		TableName: "orders", ChangeType: "alter", Status: state.Apply.Running,
		RowsCopied: 45000, RowsTotal: 100000, PercentComplete: 45,
		Throttled: true,
	})
	assert.Contains(t, noReason, "45.00% (throttled)")
	assert.NotContains(t, noReason, "ℹ️ Throttled", "no tooltip without a reason")

	unknownSignal := FormatTableProgress(TableProgress{
		TableName: "orders", ChangeType: "alter", Status: state.Apply.Running,
		RowsCopied: 45000, RowsTotal: 100000, PercentComplete: 45,
		Throttled: true, ThrottleReason: "disk-usage 95% > 90%",
	})
	assert.Contains(t, unknownSignal, "ℹ️ Throttled: disk-usage 95% > 90%",
		"an unrecognized signal still surfaces its raw reason")
	assert.NotContains(t, unknownSignal, "·", "no tip separator without a recognized tip")

	checksumming := FormatTableProgress(TableProgress{
		TableName: "orders", ChangeType: "alter", Status: state.Task.Checksumming,
		ChecksumRowsChecked: 321450, ChecksumRowsTotal: 1466232,
		Throttled: true, ThrottleReason: "threads-running 21 > 18",
	})
	assert.Contains(t, checksumming, "🔍 Checksumming to verify data (21.92%) (throttled)")
	assert.Contains(t, checksumming, "ℹ️ Throttled: threads-running 21 > 18 · backing off while the database's active threads exceed its budget")

	notThrottled := FormatTableProgress(TableProgress{
		TableName: "orders", ChangeType: "alter", Status: state.Apply.Running,
		RowsCopied: 45000, RowsTotal: 100000, PercentComplete: 45,
	})
	assert.NotContains(t, notThrottled, "(throttled)")
	assert.NotContains(t, notThrottled, "Throttled")

	completed := FormatTableProgress(TableProgress{
		TableName: "orders", ChangeType: "alter", Status: state.Apply.Completed,
		RowsCopied: 100000, RowsTotal: 100000, PercentComplete: 100,
		Throttled: true, ThrottleReason: "replica-lag 12s > 10s",
	})
	assert.NotContains(t, completed, "Throttled", "a terminal table never renders a stale throttle flag")
}

func TestFormatTableProgress_InstantDDL(t *testing.T) {
	tp := TableProgress{
		TableName:  "users",
		ChangeType: "alter",
		Status:     state.Apply.Running,
		IsInstant:  true,
	}
	output := FormatTableProgress(tp)
	assert.Contains(t, output, "Applying instantly...")

	tp.Status = state.Apply.Completed
	output = FormatTableProgress(tp)
	assert.Contains(t, output, "Applied instantly")
}

// An instant-flagged table still renders its phase states — cutover, revert
// window, waiting — and its shard breakdown; the instant label describes only
// the generic in-progress moment, never a phase the operator is watching for.
func TestFormatTableProgress_InstantTableShowsPhaseStates(t *testing.T) {
	tp := TableProgress{
		TableName:  "users",
		ChangeType: "drop",
		DDL:        "DROP TABLE `users`;",
		Status:     state.Apply.CuttingOver,
		IsInstant:  true,
		Shards: []ShardProgress{
			{Shard: "-80", Status: state.Task.CuttingOver},
			{Shard: "80-", Status: state.Task.WaitingForCutover},
		},
	}
	output := FormatTableProgress(tp)
	assert.Contains(t, output, "Applying...")
	assert.NotContains(t, output, "Applying instantly")
	assert.Contains(t, output, "Shards: 2")

	tp.Status = state.Apply.RevertWindow
	output = FormatTableProgress(tp)
	assert.Contains(t, output, "Complete (revert window open)")
	assert.NotContains(t, output, "Applying instantly")

	tp.Status = state.Task.WaitingForCutover
	output = FormatTableProgress(tp)
	assert.Contains(t, output, "Waiting for cutover")
	assert.NotContains(t, output, "Applying instantly")
}

// CREATE and DROP are not instant DDL even when the engine flags them as
// skipping the row copy: they keep the generic applying/complete labels, while
// an instant ALTER keeps its lightning label.
func TestFormatTableProgress_InstantLabelIsAlterOnly(t *testing.T) {
	for _, changeType := range []string{"create", "drop"} {
		tp := TableProgress{
			TableName:  "users",
			ChangeType: changeType,
			Status:     state.Apply.Running,
			IsInstant:  true,
		}
		output := FormatTableProgress(tp)
		assert.Contains(t, output, "Applying...", "%s renders the generic applying label", changeType)
		assert.NotContains(t, output, "Applying instantly", "%s is not instant DDL", changeType)

		tp.Status = state.Apply.Completed
		output = FormatTableProgress(tp)
		assert.Contains(t, output, "✓ Complete", "%s completes with the generic label", changeType)
		assert.NotContains(t, output, "Applied instantly", "%s is not instant DDL", changeType)
	}
}

// An engine can flag a whole apply instant while one of its ALTERs still row
// copies (the flag is apply-scoped, the copy is table-scoped). The copying
// table must show its real progress — the instant label is reserved for
// tables with no row copy to report.
func TestFormatTableProgress_InstantAlterRendering(t *testing.T) {
	copying := TableProgress{
		TableName:       "users",
		ChangeType:      "alter",
		DDL:             "ALTER TABLE `users` ADD COLUMN `email` varchar(255);",
		Status:          state.Apply.Running,
		IsInstant:       true,
		RowsCopied:      250,
		RowsTotal:       1000,
		PercentComplete: 25,
	}
	output := FormatTableProgress(copying)
	assert.Contains(t, output, "25.00%", "a copying instant-flagged ALTER shows its real percent")
	assert.NotContains(t, output, "Applying instantly", "the instant label must not mask copy progress")

	instant := TableProgress{
		TableName:  "users",
		ChangeType: "alter",
		Status:     state.Apply.Running,
		IsInstant:  true,
	}
	output = FormatTableProgress(instant)
	assert.Contains(t, output, "Applying instantly...", "an instant ALTER with no row copy keeps the instant label")

	plain := instant
	plain.IsInstant = false
	output = FormatTableProgress(plain)
	assert.Contains(t, output, "Running...", "a non-instant ALTER with no row data gets the generic label")
	assert.NotContains(t, output, "Applying instantly", "the instant label requires the engine flag")
}

func TestFormatTableProgress_CreateDropLabels(t *testing.T) {
	for _, changeType := range []string{"create", "drop"} {
		tp := TableProgress{
			TableName:  "users",
			ChangeType: changeType,
			Status:     state.Apply.Running,
		}
		output := FormatTableProgress(tp)
		assert.Contains(t, output, "Applying...", "%s should show 'Applying...'", changeType)
	}

	tp := TableProgress{
		TableName:  "users",
		ChangeType: "alter",
		Status:     state.Apply.CuttingOver,
	}
	output := FormatTableProgress(tp)
	assert.Contains(t, output, "Cutting over...")

	tp.Status = state.Apply.Recovering
	tp.PercentComplete = 45
	output = FormatTableProgress(tp)
	assert.Contains(t, output, "Recovering state...")
	assert.Contains(t, output, ui.ProgressBarRowCopy(45))
	assert.NotContains(t, output, ui.ProgressBarRowCopy(100))

	// Once row counts arrive, the displayed percent is computed from them
	// rather than the engine's stale whole-number percent.
	tp.RowsCopied = 420
	tp.RowsTotal = 1000
	tp.ETASeconds = 120
	output = FormatTableProgress(tp)
	assert.Contains(t, output, "Row copy in progress (42.00%)")
	assert.Contains(t, output, "Rows: 420 / 1,000 · ETA: 2m")
	assert.NotContains(t, output, "Recovering state...")
}

// A copy that has begun but not yet reached 1% shows the true fraction
// computed from the row counts, with the bar's first segment lit so the
// operator sees both that copying started and how little of a huge table has
// actually copied.
func TestFormatTableProgress_SubPercentRowCopyShowsFraction(t *testing.T) {
	tp := TableProgress{
		TableName:       "orders",
		ChangeType:      "alter",
		Status:          state.Apply.Running,
		RowsCopied:      3_000,
		RowsTotal:       1_604_159,
		PercentComplete: 0,
	}

	output := FormatTableProgress(tp)

	assert.Contains(t, output, "orders: "+ui.ProgressBarRowCopy(1)+" 0.19%")
	assert.Contains(t, output, "Rows: 3,000 / 1,604,159")
	assert.NotContains(t, output, " 0%")
}

// A row copy renders its ETA from the structured field (the same source and
// FormatETA the PR comment uses), so the two surfaces show an identical
// "Rows … · ETA …" line.
func TestFormatTableProgress_RowCopyShowsStructuredETA(t *testing.T) {
	tp := TableProgress{
		TableName:       "users",
		ChangeType:      "alter",
		Status:          state.Apply.Running,
		RowsCopied:      45_000,
		RowsTotal:       100_000,
		PercentComplete: 45,
		ETASeconds:      340,
	}

	output := FormatTableProgress(tp)

	assert.Contains(t, output, "Rows: 45,000 / 100,000 · ETA: 5m 40s")
}

func TestFormatTableProgress_FailedRetryableKeepsProgress(t *testing.T) {
	t.Run("with progress", func(t *testing.T) {
		tp := TableProgress{
			TableName:       "users",
			ChangeType:      "alter",
			Status:          state.Apply.FailedRetryable,
			PercentComplete: 45,
		}

		output := FormatTableProgress(tp)
		assert.Contains(t, output, ui.ProgressBar(45, ui.ColorYellow)+" Retrying")
	})

	t.Run("without progress", func(t *testing.T) {
		tp := TableProgress{
			TableName:  "users",
			ChangeType: "alter",
			Status:     state.Apply.FailedRetryable,
		}

		output := FormatTableProgress(tp)
		assert.Contains(t, output, "users: Retrying")
		assert.NotContains(t, output, ui.ColorYellow)
	})
}

func TestFormatTableProgress_EstimateExceeded(t *testing.T) {
	tp := TableProgress{
		TableName:       "users",
		ChangeType:      "alter",
		Status:          state.Apply.Running,
		RowsCopied:      145000,
		RowsTotal:       100000,
		PercentComplete: 145,
	}

	output := FormatTableProgress(tp)
	assert.Contains(t, output, ui.ProgressBarActivity()+" Finalizing copy")
	assert.Contains(t, output, "Rows copied: 145,000 so far")
	assert.Contains(t, output, ui.EstimateExceededTooltip)
	assert.NotContains(t, output, "145%")
	assert.NotContains(t, output, "100%")
	assert.NotContains(t, output, "100,000 / 100,000")
}

func TestFormatVSchemaStatus(t *testing.T) {
	// No VSchema change → nothing rendered.
	assert.Empty(t, FormatVSchemaStatus(nil))

	// A single keyspace renders its name, status, and diff.
	single := FormatVSchemaStatus([]apitypes.VSchemaChange{
		{Namespace: "commerce", Status: "applied", Diff: "--- a/commerce.json\n+++ b/commerce.json\n+  \"new_table\": {}"},
	})
	assert.Contains(t, single, "VSchema (commerce)")
	assert.Contains(t, single, "Applied")
	assert.Contains(t, single, "new_table")

	// Multiple keyspaces each render independently, with their own status.
	multi := FormatVSchemaStatus([]apitypes.VSchemaChange{
		{Namespace: "commerce", Status: "applied", Diff: `+ "lookup": {}`},
		{Namespace: "commerce_sharded", Status: "applying", Diff: `+ "xxhash": {}`},
	})
	assert.Contains(t, multi, "VSchema (commerce): Applied")
	assert.Contains(t, multi, "VSchema (commerce_sharded): Applying...")
}

func TestStateColorFunc_PlanetScalePhases(t *testing.T) {
	for _, s := range []string{
		state.Apply.PreparingBranch,
		state.Apply.ApplyingBranchChanges,
		state.Apply.ValidatingBranch,
		state.Apply.CreatingDeployRequest,
		state.Apply.ValidatingDeployRequest,
		state.Apply.Recovering,
		state.Apply.Cancelled,
		state.Apply.RunningDegraded,
	} {
		fn := stateColorFunc(s)
		assert.NotNil(t, fn, "expected color function for state %q", s)
	}
}

// Red means "something broke, go fix it" — an operator scanning a status list
// should be able to trust that red marks a real failure. Operator-initiated
// terminal states (stopped, cancelled, reverted) are deliberate outcomes and
// render orange instead, so a routine revert never reads as a failure.
func TestStateColorsReserveRedForFailure(t *testing.T) {
	allStates := []string{
		state.Apply.Pending,
		state.Apply.Running,
		state.Apply.RunningDegraded,
		state.Apply.WaitingForDeploy,
		state.Apply.WaitingForCutover,
		state.Apply.Recovering,
		state.Apply.CuttingOver,
		state.Apply.RevertWindow,
		state.Apply.SkippingRevert,
		state.Apply.Reverting,
		state.Apply.Completed,
		state.Apply.Failed,
		state.Apply.FailedRetryable,
		state.Apply.Stopped,
		state.Apply.Cancelled,
		state.Apply.Reverted,
		state.Apply.PreparingBranch,
		state.Apply.ApplyingBranchChanges,
		state.Apply.ValidatingBranch,
		state.Apply.CreatingDeployRequest,
		state.Apply.ValidatingDeployRequest,
	}
	for _, s := range allStates {
		if fn := stateColorFunc(s); fn != nil {
			colored := fn(state.Label(s))
			if s == state.Apply.Failed {
				assert.Contains(t, colored, ANSIRed, "Failed must render red")
			} else {
				assert.NotContains(t, colored, ANSIRed, "state %q must not render red — red is reserved for Failed", s)
			}
		}
		formatted := FormatProgressState(s)
		if s == state.Apply.Failed {
			assert.Contains(t, formatted, ANSIRed, "FormatProgressState(Failed) must render red")
		} else {
			assert.NotContains(t, formatted, ANSIRed, "FormatProgressState(%q) must not render red", s)
		}
	}

	for _, s := range []string{state.Apply.Stopped, state.Apply.Cancelled, state.Apply.Reverted} {
		fn := stateColorFunc(s)
		require.NotNil(t, fn, "expected color function for state %q", s)
		assert.Contains(t, fn(state.Label(s)), ANSIOrange, "operator-halted state %q must render orange", s)
	}
}

// A reverted table shows a terminal orange bar with the revert label; a
// cancelled mid-copy table shows its progress in orange. Neither uses the red
// failure bar, which is reserved for tables that actually failed.
func TestFormatTableProgressOperatorHaltedBars(t *testing.T) {
	reverted := FormatTableProgress(TableProgress{
		TableName:  "users",
		ChangeType: "alter",
		Status:     state.Apply.Reverted,
		DDL:        "ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255)",
	})
	assert.Contains(t, reverted, "↩️ Reverted")
	assert.Contains(t, reverted, ui.ColorOrange)
	assert.NotContains(t, reverted, ui.ColorRed)

	cancelled := FormatTableProgress(TableProgress{
		TableName:       "orders",
		ChangeType:      "alter",
		Status:          state.Apply.Cancelled,
		PercentComplete: 30,
		RowsCopied:      300,
		RowsTotal:       1000,
	})
	assert.Contains(t, cancelled, "🚫 Cancelled at 30.00%")
	assert.Contains(t, cancelled, ui.ColorOrange)
	assert.NotContains(t, cancelled, ui.ColorRed)

	failed := FormatTableProgress(TableProgress{
		TableName:       "payments",
		ChangeType:      "alter",
		Status:          state.Apply.Failed,
		PercentComplete: 30,
		RowsCopied:      300,
		RowsTotal:       1000,
	})
	assert.Contains(t, failed, "❌ Failed")
	assert.Contains(t, failed, ui.ColorRed)
}
