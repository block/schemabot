package ui

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine/postgres/indexphase"
)

// FormatBuildWork formats one plain-prose line of concurrent index build work.
// It returns an empty string for other operations, for a phase the engine does
// not know, or when the phase's counters are not yet published.
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
	return strings.Join(parts, " · ")
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

func formatBlocks(label string, work apitypes.BuildWork) string {
	if work.BlocksTotal == 0 {
		return ""
	}
	pct := ClampPercent(int(work.BlocksDone * 100 / work.BlocksTotal))
	return fmt.Sprintf("%s: %d%% of blocks (%s/%s)", label, pct, FormatNumber(work.BlocksDone), FormatNumber(work.BlocksTotal))
}

func formatTuples(label string, work apitypes.BuildWork) string {
	if work.TuplesTotal == 0 {
		return ""
	}
	return fmt.Sprintf("%s: %s/%s tuples", label, FormatNumber(work.TuplesDone), FormatNumber(work.TuplesTotal))
}
