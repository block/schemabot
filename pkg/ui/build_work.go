package ui

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
)

// FormatBuildWork formats one plain-prose line of concurrent index build work.
// It returns an empty string for other operations or when no work is available.
//
// The line describes the current server phase only. PostgreSQL carries a
// completed scan's block and tuple counters into the locker-wait phase that
// follows it, so while the server waits on lockers those counters are stale
// and only the outstanding lockers are shown.
func FormatBuildWork(work apitypes.BuildWork) string {
	if !work.IsConcurrentIndexBuild() {
		return ""
	}
	parts := make([]string, 0, 3)
	if work.WaitingOnLockers() {
		if work.LockersTotal > 0 {
			outstanding := max(work.LockersTotal-work.LockersDone, 0)
			parts = append(parts, fmt.Sprintf("waiting on %s of %s lockers", FormatNumber(outstanding), FormatNumber(work.LockersTotal)))
		}
	} else {
		if work.BlocksTotal > 0 {
			pct := ClampPercent(int(work.BlocksDone * 100 / work.BlocksTotal))
			parts = append(parts, fmt.Sprintf("building index: %d%% of blocks (%s/%s)", pct, FormatNumber(work.BlocksDone), FormatNumber(work.BlocksTotal)))
		}
		if work.TuplesTotal > 0 {
			parts = append(parts, fmt.Sprintf("%s/%s tuples", FormatNumber(work.TuplesDone), FormatNumber(work.TuplesTotal)))
		}
	}
	if work.Attempt > 1 {
		parts = append(parts, fmt.Sprintf("attempt %d", work.Attempt))
	}
	return strings.Join(parts, " · ")
}
