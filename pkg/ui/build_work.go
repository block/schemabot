package ui

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
)

// FormatBuildWork formats one plain-prose line of concurrent index build work.
// It returns an empty string for other operations or when no work is available.
func FormatBuildWork(work apitypes.BuildWork) string {
	if !work.IsConcurrentIndexBuild() {
		return ""
	}
	parts := make([]string, 0, 4)
	if work.BlocksTotal > 0 {
		pct := ClampPercent(int(work.BlocksDone * 100 / work.BlocksTotal))
		parts = append(parts, fmt.Sprintf("building index: %d%% of blocks (%s/%s)", pct, FormatNumber(work.BlocksDone), FormatNumber(work.BlocksTotal)))
	}
	if work.TuplesTotal > 0 {
		parts = append(parts, fmt.Sprintf("%s/%s tuples", FormatNumber(work.TuplesDone), FormatNumber(work.TuplesTotal)))
	}
	if work.WaitingOnLockers() && work.LockersTotal > 0 {
		outstanding := max(work.LockersTotal-work.LockersDone, 0)
		parts = append(parts, fmt.Sprintf("waiting on %s of %s lockers", FormatNumber(outstanding), FormatNumber(work.LockersTotal)))
	}
	if work.Attempt > 1 {
		parts = append(parts, fmt.Sprintf("attempt %d", work.Attempt))
	}
	return strings.Join(parts, " · ")
}
