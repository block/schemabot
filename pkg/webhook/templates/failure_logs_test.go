package templates

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRenderRecentFailureLogs verifies the logs section appended to a failed
// apply's summary folds the entries into a details block and formats each
// line like the CLI logs output: UTC timestamp, bracketed level tag, message,
// and state transition when set. A complete history is labeled "Show logs" —
// no entries were left out, so the fold must not suggest a subset.
func TestRenderRecentFailureLogs(t *testing.T) {
	at := time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC)
	rendered := RenderRecentFailureLogs([]LogEntryData{
		{CreatedAt: at, Level: "info", Message: "Apply claimed by driver", OldState: "queued", NewState: "running"},
		{CreatedAt: at.Add(3 * time.Second), Level: "warn", Message: "Copy throttled by replication lag"},
		{CreatedAt: at.Add(9 * time.Second), Level: "error", Message: "Lost MySQL connection; retrying"},
	}, GitHubIssueCommentMaxChars, false)

	assert.Contains(t, rendered, "<details>")
	assert.Contains(t, rendered, "<summary>Show logs (3 entries)</summary>")
	assert.NotContains(t, rendered, "Show recent logs")
	assert.Contains(t, rendered, "```text")
	assert.Contains(t, rendered, "2026-07-12 16:32:01 UTC [INF] Apply claimed by driver [queued -> running]")
	assert.Contains(t, rendered, "2026-07-12 16:32:04 UTC [WRN] Copy throttled by replication lag")
	assert.Contains(t, rendered, "2026-07-12 16:32:10 UTC [ERR] Lost MySQL connection; retrying")
	assert.NotContains(t, rendered, "omitted")
}

// TestRenderRecentFailureLogsTailLabel verifies that when older entries exist
// beyond the loaded tail, the fold is labeled "Show recent logs" so the
// operator knows they are seeing a subset, not the full history.
func TestRenderRecentFailureLogsTailLabel(t *testing.T) {
	at := time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC)
	rendered := RenderRecentFailureLogs([]LogEntryData{
		{CreatedAt: at, Level: "error", Message: "Apply failed", OldState: "running", NewState: "failed"},
	}, GitHubIssueCommentMaxChars, true)

	assert.Contains(t, rendered, "<summary>Show recent logs (1 entry)</summary>")
}

// TestRenderRecentFailureLogsEmpty verifies an apply with no log entries adds
// nothing to the summary — no empty details block.
func TestRenderRecentFailureLogsEmpty(t *testing.T) {
	assert.Empty(t, RenderRecentFailureLogs(nil, GitHubIssueCommentMaxChars, false))
}

// TestRenderRecentFailureLogsSanitizesUntrustedText verifies engine-supplied
// log text cannot break out of the fenced code block: newlines collapse to
// spaces so every entry stays on one line, and backtick fences are split so
// the rest of the comment cannot be reinterpreted as markup.
func TestRenderRecentFailureLogsSanitizesUntrustedText(t *testing.T) {
	at := time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC)
	rendered := RenderRecentFailureLogs([]LogEntryData{
		{CreatedAt: at, Level: "error", Message: "line one\r\nline two\nline three"},
		{CreatedAt: at.Add(time.Second), Level: "error", Message: "fence breakout ```\n# not a heading"},
		{CreatedAt: at.Add(2 * time.Second), Level: "error", Message: "long run `````x"},
	}, GitHubIssueCommentMaxChars, false)

	assert.Contains(t, rendered, "[ERR] line one line two line three")
	assert.Contains(t, rendered, "[ERR] fence breakout `` ` # not a heading")
	assert.Equal(t, 2, strings.Count(rendered, "```"), "only the section's own fence markers survive")
	assert.Equal(t, strings.Index(rendered, "```text"), strings.Index(rendered, "```"), "first fence marker is the section's opener")
}

// TestRenderRecentFailureLogsTrimsToSizeBudget verifies that when the rendered
// log block would blow GitHub's comment size limit, the earliest lines are
// dropped, the newest are kept, the fold says how many were omitted, and the
// label flips to "Show recent logs" because a subset is shown.
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
	rendered := RenderRecentFailureLogs(entries, GitHubIssueCommentMaxChars, false)

	require.Less(t, len(rendered), 65536, "rendered section must leave room inside GitHub's size limit")
	assert.Contains(t, rendered, "<summary>Show recent logs (")
	assert.Contains(t, rendered, "earlier entries omitted")
	assert.NotContains(t, rendered, "16:00:00 UTC", "earliest entry is dropped first")
	assert.Contains(t, rendered, "16:01:39 UTC", "newest entry always survives")
}

// TestRenderRecentFailureLogsShrinksToAvailableRoom verifies a large summary
// body shrinks the section: with less room available than the default cap, the
// section trims to what fits so appending it never pushes the assembled
// comment over GitHub's size limit.
func TestRenderRecentFailureLogsShrinksToAvailableRoom(t *testing.T) {
	at := time.Date(2026, 7, 12, 16, 0, 0, 0, time.UTC)
	entries := make([]LogEntryData, 20)
	for i := range entries {
		entries[i] = LogEntryData{
			CreatedAt: at.Add(time.Duration(i) * time.Second),
			Level:     "info",
			Message:   strings.Repeat("x", 200) + " #" + time.Duration(i).String(),
		}
	}
	available := 2000
	rendered := RenderRecentFailureLogs(entries, available, false)

	require.NotEmpty(t, rendered)
	assert.LessOrEqual(t, len(rendered), available, "the section must fit in the room the summary body leaves")
	assert.Contains(t, rendered, "earlier entries omitted")
	assert.Contains(t, rendered, "16:00:19 UTC", "newest entry always survives")
	assert.NotContains(t, rendered, "16:00:00 UTC", "earliest entry is dropped first")
}

// TestRenderRecentFailureLogsSkipsWhenNoRoom verifies that a summary body
// leaving no meaningful room under the comment size limit drops the section
// entirely — the summary must still post, and a fold too small to carry a log
// line is noise.
func TestRenderRecentFailureLogsSkipsWhenNoRoom(t *testing.T) {
	entries := []LogEntryData{
		{CreatedAt: time.Date(2026, 7, 12, 16, 0, 0, 0, time.UTC), Level: "error", Message: "Apply failed"},
	}
	assert.Empty(t, RenderRecentFailureLogs(entries, 0, false))
	assert.Empty(t, RenderRecentFailureLogs(entries, -500, false))
	assert.Empty(t, RenderRecentFailureLogs(entries, MinFailureLogsSectionChars-1, false))
}

// TestRenderRecentFailureLogsTruncatesSingleOversizedLine verifies one
// enormous engine error message cannot blow the budget on its own: the sole
// surviving line is truncated to fit rather than carried oversize.
func TestRenderRecentFailureLogsTruncatesSingleOversizedLine(t *testing.T) {
	entries := []LogEntryData{
		{
			CreatedAt: time.Date(2026, 7, 12, 16, 0, 0, 0, time.UTC),
			Level:     "error",
			Message:   "Apply failed: " + strings.Repeat("é", GitHubIssueCommentMaxChars),
		},
	}
	available := 4000
	rendered := RenderRecentFailureLogs(entries, available, false)

	require.NotEmpty(t, rendered)
	assert.LessOrEqual(t, len(rendered), available, "a single oversized line must be truncated to the budget")
	assert.Contains(t, rendered, "Apply failed: ")
	assert.Contains(t, rendered, "…", "the truncated line ends with an ellipsis")
	assert.True(t, strings.HasSuffix(strings.TrimSpace(rendered), "</details>"), "the fold still closes cleanly")
}
