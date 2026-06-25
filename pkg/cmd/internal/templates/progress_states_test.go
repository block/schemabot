package templates

import (
	"io"
	"os"
	"testing"

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
			},
		})
	})

	assert.Contains(t, output, "Recent failed schema changes")
	assert.Contains(t, output, "payments staging: Failed (github:alice; external_id=external-failed) [2026-05-28 11:00:03 UTC]")
	assert.Contains(t, output, "failed to apply schema change because duplicate column name 'status'")
	assert.Contains(t, output, "schemabot status apply-failed")
	assert.NotContains(t, output, "APPLY ID")
	assert.NotContains(t, output, "REASON")
	assert.NotContains(t, output, "Use 'schemabot status <apply_id>' to view details")
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
	assert.Contains(t, withProgress, "🔍 Checksumming to verify data (21%)")
	assert.Contains(t, withProgress, "Rows verified: 321,450 / 1,466,232")
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

	tp.RowsCopied = 420
	tp.RowsTotal = 1000
	tp.ETASeconds = 120
	output = FormatTableProgress(tp)
	assert.Contains(t, output, "Row copy in progress (45%)")
	assert.Contains(t, output, "Rows: 420 / 1,000 · ETA: 2m")
	assert.NotContains(t, output, "Recovering state...")
}

func TestFormatTableProgress_RowCopyDisplaysOnePercentAfterCopyStarts(t *testing.T) {
	tp := TableProgress{
		TableName:       "orders",
		ChangeType:      "alter",
		Status:          state.Apply.Running,
		RowsCopied:      3_000,
		RowsTotal:       1_604_159,
		PercentComplete: 0,
	}

	output := FormatTableProgress(tp)

	assert.Contains(t, output, "orders: "+ui.ProgressBarRowCopy(1)+" 1%")
	assert.Contains(t, output, "Rows: 3,000 / 1,604,159")
	assert.NotContains(t, output, " 0%")
}

// A Spirit row-copy reports its detail string and a structured ETA. The CLI
// renders the ETA from the structured field (the same source and FormatETA the
// PR comment uses), so the two surfaces show an identical "Rows … · ETA …" line
// even though the detail string itself no longer carries the ETA.
func TestFormatTableProgress_RowCopyShowsStructuredETA(t *testing.T) {
	tp := TableProgress{
		TableName:       "users",
		ChangeType:      "alter",
		Status:          state.Apply.Running,
		RowsCopied:      45_000,
		RowsTotal:       100_000,
		PercentComplete: 45,
		ETASeconds:      340,
		ProgressDetail:  "45000/100000 45% copyRows",
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
	t.Run("structured progress", func(t *testing.T) {
		tp := TableProgress{
			TableName:       "users",
			ChangeType:      "alter",
			Status:          state.Apply.Running,
			RowsCopied:      145000,
			RowsTotal:       100000,
			PercentComplete: 145,
		}

		output := FormatTableProgress(tp)
		assert.Contains(t, output, ui.ProgressBarActivity()+" Active")
		assert.Contains(t, output, "Rows copied: 145,000 so far")
		assert.Contains(t, output, ui.EstimateExceededTooltip)
		assert.NotContains(t, output, "145%")
		assert.NotContains(t, output, "100%")
		assert.NotContains(t, output, "100,000 / 100,000")
	})

	t.Run("parsed Spirit progress", func(t *testing.T) {
		tp := TableProgress{
			TableName:      "users",
			ChangeType:     "alter",
			Status:         state.Apply.Running,
			ProgressDetail: "145000/100000 100% copyRows ETA TBD",
		}

		output := FormatTableProgress(tp)
		assert.Contains(t, output, ui.ProgressBarActivity()+" Active")
		assert.Contains(t, output, "Rows copied: 145,000 so far")
		assert.NotContains(t, output, "100%")
	})
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
