package ui

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine/postgres/indexphase"
)

// FormatBuildWork formats the detail line of a running concurrent index build,
// the line both surfaces write under the table's statement where a row copy
// writes its rows. It returns an empty string for other operations, for a
// phase the engine does not know, or when the phase's counters are not yet
// published.
//
// The line describes the current server phase only, reading the one counter
// pair indexphase says that phase owns: a scan reports its blocks under the
// phase's label, the tuple load reports its tuples, a wait reports the
// outstanding lockers, and a sort — which counts nothing — reports what it is
// doing in the table's prose so the line does not vanish mid-build. The other counters are a
// completed phase's values that PostgreSQL carries forward, and are not shown.
// Only fixed prose from the phase table and formatted integers reach the
// output; the server's own text never does.
func FormatBuildWork(work apitypes.BuildWork) string {
	if !work.IsConcurrentIndexBuild() {
		return ""
	}
	phase, ok := indexphase.Lookup(work.ServerPhase)
	if !ok {
		return ""
	}
	parts := make([]string, 0, 2)
	if line := formatPhaseWork(phase, work); line != "" {
		parts = append(parts, line)
	}
	if work.Attempt > 1 {
		parts = append(parts, fmt.Sprintf("attempt %d", work.Attempt))
	}
	return CapitalizeFirst(strings.Join(parts, " · "))
}

func formatPhaseWork(phase indexphase.Phase, work apitypes.BuildWork) string {
	switch phase.Counter {
	case indexphase.Lockers:
		if work.LockersTotal > 0 {
			outstanding := max(work.LockersTotal-work.LockersDone, 0)
			return fmt.Sprintf("waiting on %s of %s lockers", FormatNumber(outstanding), FormatNumber(work.LockersTotal))
		}
	case indexphase.Blocks:
		return formatBlocks(phase.Label, work)
	case indexphase.Tuples:
		return formatTuples(phase.Label, work)
	case indexphase.BlocksOrTuples:
		if work.BlocksTotal > 0 {
			return formatBlocks(phase.Label, work)
		}
		return formatTuples(phase.Label, work)
	case indexphase.None:
		if phase.Label != "" {
			return phase.Label + ": " + phase.Detail
		}
	}
	return ""
}

// formatBlocks and formatTuples show the phase's raw counters without a
// percent of their own: the progress bar carries the whole-build percent, and a
// second, phase-local percent beside it would read as a contradiction. A done
// count past its total, which the server can report as the scan finishes, is
// shown as the total.
func formatBlocks(label string, work apitypes.BuildWork) string {
	if work.BlocksTotal == 0 {
		return ""
	}
	return fmt.Sprintf("%s: %s of %s blocks", label, FormatNumber(min(work.BlocksDone, work.BlocksTotal)), FormatNumber(work.BlocksTotal))
}

func formatTuples(label string, work apitypes.BuildWork) string {
	if work.TuplesTotal == 0 {
		return ""
	}
	return fmt.Sprintf("%s: %s of %s tuples", label, FormatNumber(min(work.TuplesDone, work.TuplesTotal)), FormatNumber(work.TuplesTotal))
}

// IndexBuildProgress renders the progress of a running concurrent index build:
// a bar and the engine's whole-build percent, which counts every phase of the
// build rather than only the current one. Before the build reports any
// progress it shows a starting indicator instead, because a 0% bar reads as
// stuck, the same way a row copy that has not started does. The PR comment and
// the CLI both render the build through this, so they show the same number.
func IndexBuildProgress(percent int) string {
	pct := ClampPercent(percent)
	if pct == 0 {
		return "⏳ Starting index build..."
	}
	return fmt.Sprintf("%s %d%%", ProgressBarRowCopy(pct), pct)
}
