package presentation

import (
	"fmt"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// CopyStillAhead reports whether a target whose engine has reported no row
// total will still copy the table, so the plan's row estimate may stand in for
// the rows it is about to report. A target that is queued, waiting for its
// deploy, copying or recovering has not reported its copy yet. A target past
// its copy (catching up, checksumming, waiting for or in its cutover,
// reverting) copied a table that had no rows, so its zero is the engine's
// answer and an estimate would count work that is already done.
func CopyStillAhead(status string) bool {
	return state.IsState(status,
		state.Task.Pending,
		state.Task.WaitingForDeploy,
		state.Task.Running,
		state.Task.Recovering)
}

// PlannedRowsSpan names the targets a planned row count is summed across, for
// the rows line of a table queued on several: " across 3 targets". It counts
// only the targets that run the line's change, which can be a subset of the
// rollout, so it never says "all"; the heading above the line states the
// coverage. A count for one target reads " on 1 target".
func PlannedRowsSpan(targets int) string {
	if targets == 1 {
		return " on " + TargetNoun.Count(targets)
	}
	return " across " + TargetNoun.Count(targets)
}

// PlannedRowsText is the rows line of a table no engine has reported on yet:
// "Rows: ~1.2M · ~3.0 GB", the plan's row estimate and size, both approximate,
// so the work ahead is visible before the copy starts. span names the targets
// the estimate is summed across (PlannedRowsSpan), or is empty for one. When
// the plan sized the table but could not count its rows, as for a PostgreSQL
// table that has never been analyzed, the line gives the size alone, "Size:
// ~3.0 GB", rather than reporting a row count it does not have. It is empty
// when the plan had neither. The CLI and the PR comment each add their own
// line prefix and newline.
func PlannedRowsText(estimatedRows, estimatedBytes *int64, span string) string {
	if estimatedRows != nil {
		return fmt.Sprintf("Rows: %s%s%s", ui.FormatApproxRows(*estimatedRows), span, ui.FormatTableSizeClause(estimatedBytes))
	}
	if estimatedBytes != nil {
		return fmt.Sprintf("Size: %s%s", ui.FormatApproxBytes(*estimatedBytes), span)
	}
	return ""
}

// TargetsTableSizeClause is a table's planned size on the rows line of a
// table rolled up across targets: " · ~575 GB". The size is every target's,
// so beside rows that leave some targets out (partial) it names the full span
// rather than reading as the reporting targets' size. Empty when the plan
// did not size the table on every target.
func TargetsTableSizeClause(estimatedBytes *int64, partial bool, targets int) string {
	clause := ui.FormatTableSizeClause(estimatedBytes)
	if clause == "" || !partial {
		return clause
	}
	return fmt.Sprintf("%s across all %d targets", clause, targets)
}
