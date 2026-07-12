package templates

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRenderRecentFailureLogs verifies the recent-logs section appended to a
// failed apply's summary folds the entries into a details block and formats
// each line like the CLI logs output: UTC timestamp, bracketed level tag,
// message, and state transition when set.
func TestRenderRecentFailureLogs(t *testing.T) {
	at := time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC)
	rendered := RenderRecentFailureLogs([]LogEntryData{
		{CreatedAt: at, Level: "info", Message: "Apply claimed by driver", OldState: "queued", NewState: "running"},
		{CreatedAt: at.Add(3 * time.Second), Level: "warn", Message: "Copy throttled by replication lag"},
		{CreatedAt: at.Add(9 * time.Second), Level: "error", Message: "Lost MySQL connection; retrying"},
	})

	assert.Contains(t, rendered, "<details>")
	assert.Contains(t, rendered, "<summary>Recent logs (3)</summary>")
	assert.Contains(t, rendered, "```text")
	assert.Contains(t, rendered, "2026-07-12 16:32:01 UTC [INF] Apply claimed by driver [queued -> running]")
	assert.Contains(t, rendered, "2026-07-12 16:32:04 UTC [WRN] Copy throttled by replication lag")
	assert.Contains(t, rendered, "2026-07-12 16:32:10 UTC [ERR] Lost MySQL connection; retrying")
	assert.NotContains(t, rendered, "omitted")
}

// TestRenderRecentFailureLogsEmpty verifies an apply with no log entries adds
// nothing to the summary — no empty details block.
func TestRenderRecentFailureLogsEmpty(t *testing.T) {
	assert.Empty(t, RenderRecentFailureLogs(nil))
}

// TestRenderRecentFailureLogsTrimsToSizeBudget verifies that when the rendered
// log block would blow GitHub's comment size limit, the earliest lines are
// dropped, the newest are kept, and the fold says how many were omitted.
func TestRenderRecentFailureLogsTrimsToSizeBudget(t *testing.T) {
	at := time.Date(2026, 7, 12, 16, 0, 0, 0, time.UTC)
	entries := make([]LogEntryData, 100)
	for i := range entries {
		entries[i] = LogEntryData{
			CreatedAt: at.Add(time.Duration(i) * time.Second),
			Level:     "info",
			Message:   strings.Repeat("x", 1000) + " #" + time.Duration(i).String(),
		}
	}
	rendered := RenderRecentFailureLogs(entries)

	require.Less(t, len(rendered), 65536, "rendered section must leave room inside GitHub's size limit")
	assert.Contains(t, rendered, "<summary>Recent logs (100)</summary>")
	assert.Contains(t, rendered, "earlier entries omitted")
	assert.NotContains(t, rendered, "16:00:00 UTC", "earliest entry is dropped first")
	assert.Contains(t, rendered, "16:01:39 UTC", "newest entry always survives")
}
