package templates

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func engineLogTime() time.Time {
	return time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC)
}

// The engine-logs fold carries the engine's own account of a failure into the
// PR, formatted like the CLI logs output so the two surfaces read the same.
// A single data plane needs no group label: there is nothing to tell apart.
func TestRenderEngineFailureLogs(t *testing.T) {
	at := engineLogTime()
	rendered := RenderEngineFailureLogs([]EngineLogSourceData{{
		Deployment: "region-a",
		Entries: []LogEntryData{
			{CreatedAt: at, Level: "info", Message: "[orders] copy starting: 1466232 rows estimated"},
			{CreatedAt: at.Add(9 * time.Second), Level: "warn", Message: "[orders] unsafe warning 1265: Data truncated for column 'nickname' at row 1"},
		},
	}}, GitHubIssueCommentMaxChars)

	assert.Contains(t, rendered, "<details>")
	assert.Contains(t, rendered, "<summary>Show engine logs (2 entries)</summary>")
	assert.Contains(t, rendered, "```text")
	assert.Contains(t, rendered, "2026-07-12 16:32:01 UTC [INF] [orders] copy starting: 1466232 rows estimated")
	assert.Contains(t, rendered, "2026-07-12 16:32:10 UTC [WRN] [orders] unsafe warning 1265: Data truncated for column 'nickname' at row 1")
	assert.NotContains(t, rendered, "== deployment:", "one data plane needs no group label")
	assert.NotContains(t, rendered, "omitted")
}

// A source that knows it is showing a tail says so, so the fold never reads
// as the engine's complete account when the line that explains the failure
// may have scrolled past the window.
func TestRenderEngineFailureLogsTailLabel(t *testing.T) {
	rendered := RenderEngineFailureLogs([]EngineLogSourceData{{
		Deployment: "region-a",
		Entries:    []LogEntryData{{CreatedAt: engineLogTime(), Level: "error", Message: "[orders] aborting"}},
		HasOlder:   true,
	}}, GitHubIssueCommentMaxChars)

	assert.Contains(t, rendered, "<summary>Show recent engine logs (1 entry)</summary>")
}

// An apply with no data plane produces no sources, and an apply whose data
// planes said nothing produces sources with no entries. Neither may turn into
// an empty fold: the summary of an apply that never reached an engine must
// read exactly as it did before this fold existed.
func TestRenderEngineFailureLogsOmitsEmptyFold(t *testing.T) {
	assert.Empty(t, RenderEngineFailureLogs(nil, GitHubIssueCommentMaxChars))
	assert.Empty(t, RenderEngineFailureLogs([]EngineLogSourceData{{Deployment: "region-a"}}, GitHubIssueCommentMaxChars))
}

// A fan-out labels each data plane's lines, so an operator reading two groups
// knows which one raised the warning. The groups stay separate rather than
// being merged by timestamp: their clocks are independent, and an interleaved
// stream would assert an ordering across machines that nothing establishes.
func TestRenderEngineFailureLogsLabelsEachDataPlane(t *testing.T) {
	at := engineLogTime()
	rendered := RenderEngineFailureLogs([]EngineLogSourceData{
		{Deployment: "region-a", Entries: []LogEntryData{{CreatedAt: at.Add(9 * time.Second), Level: "warn", Message: "[orders] unsafe warning 1265"}}},
		{Deployment: "region-b", Entries: []LogEntryData{{CreatedAt: at, Level: "info", Message: "[orders] cutover complete"}}},
	}, GitHubIssueCommentMaxChars)

	assert.Contains(t, rendered, "<summary>Show engine logs (2 entries)</summary>")
	regionA := strings.Index(rendered, "== deployment: region-a ==")
	regionB := strings.Index(rendered, "== deployment: region-b ==")
	require.Positive(t, regionA)
	require.Positive(t, regionB)
	assert.Less(t, regionA, strings.Index(rendered, "[orders] unsafe warning 1265"))
	assert.Less(t, strings.Index(rendered, "[orders] unsafe warning 1265"), regionB,
		"each data plane's lines stay under their own label rather than merging into one stream")
}

// Engine log text is the target's words, forwarded by a data plane: it can
// carry newlines that would break the one-line-per-entry format, a fence that
// would let the rest of the comment render as markup, and connection endpoints
// the error block above already redacts. None of it may reach the PR raw.
func TestRenderEngineFailureLogsSanitizesUntrustedText(t *testing.T) {
	rendered := RenderEngineFailureLogs([]EngineLogSourceData{{
		Deployment: "region-a",
		Entries: []LogEntryData{
			{CreatedAt: engineLogTime(), Level: "error", Message: "line one\r\nline two\n```\n**bold**"},
			{CreatedAt: engineLogTime(), Level: "error", Message: "dial tcp 10.1.2.3:3306: connect: connection refused"},
		},
	}}, GitHubIssueCommentMaxChars)

	assert.NotContains(t, rendered, "line one\r\nline two")
	assert.Contains(t, rendered, "line one line two")
	assert.Equal(t, 2, strings.Count(rendered, "```"), "the fold's own fence is the only fence in it")
	assert.NotContains(t, rendered, "10.1.2.3:3306")
}

// A hostile deployment name shares the fence with the engine's lines, so it
// is sanitized and clamped like any other text in it: a name must not be able
// to close the fence or crowd out the lines the label introduces.
func TestRenderEngineFailureLogsSanitizesTheGroupLabel(t *testing.T) {
	rendered := RenderEngineFailureLogs([]EngineLogSourceData{
		{Deployment: "region-a\n```\n**bold**", Entries: []LogEntryData{{CreatedAt: engineLogTime(), Level: "info", Message: "one"}}},
		{Deployment: strings.Repeat("z", 400), Entries: []LogEntryData{{CreatedAt: engineLogTime(), Level: "info", Message: "two"}}},
	}, GitHubIssueCommentMaxChars)

	assert.Equal(t, 2, strings.Count(rendered, "```"))
	assert.NotContains(t, rendered, strings.Repeat("z", maxDeploymentLabelChars+1))
}

// The fold spends only the room the rest of the comment left it, whatever the
// engine wrote. A block that overran the budget would push the summary past
// GitHub's comment cap, and the comment would not post at all — so an
// oversized engine log costs its earliest lines, never the summary.
func TestRenderEngineFailureLogsStaysWithinBudget(t *testing.T) {
	at := engineLogTime()
	var entries []LogEntryData
	for i := range 400 {
		entries = append(entries, LogEntryData{CreatedAt: at.Add(time.Duration(i) * time.Second), Level: "info", Message: strings.Repeat("x", 200)})
	}
	const available = 4096
	rendered := RenderEngineFailureLogs([]EngineLogSourceData{{Deployment: "region-a", Entries: entries}}, available)

	require.NotEmpty(t, rendered)
	assert.LessOrEqual(t, len(rendered), available)
	assert.Contains(t, rendered, "Show recent engine logs")
	assert.Contains(t, rendered, "earlier entries omitted to fit the comment size limit")
	assert.Contains(t, rendered, formatLogEntryLine(entries[len(entries)-1]), "the newest line always survives")
}

// Each data plane gets its own share of the budget, so the first source read
// cannot spend the room the others needed and leave a fan-out looking like a
// single-region failure.
func TestRenderEngineFailureLogsSharesTheBudgetAcrossDataPlanes(t *testing.T) {
	at := engineLogTime()
	source := func(deployment, marker string) EngineLogSourceData {
		var entries []LogEntryData
		for i := range 200 {
			entries = append(entries, LogEntryData{CreatedAt: at.Add(time.Duration(i) * time.Second), Level: "info", Message: marker + strings.Repeat("x", 200)})
		}
		return EngineLogSourceData{Deployment: deployment, Entries: entries}
	}
	const available = 4096
	rendered := RenderEngineFailureLogs([]EngineLogSourceData{source("region-a", "A:"), source("region-b", "B:")}, available)

	assert.LessOrEqual(t, len(rendered), available)
	assert.Contains(t, rendered, "A:")
	assert.Contains(t, rendered, "B:")
}

// Below the minimum worth rendering, the fold is dropped whole rather than
// emitted as a truncated stub: a summary that already fills the comment
// budget must still post.
func TestRenderEngineFailureLogsDroppedWhenNoRoomRemains(t *testing.T) {
	rendered := RenderEngineFailureLogs([]EngineLogSourceData{{
		Deployment: "region-a",
		Entries:    []LogEntryData{{CreatedAt: engineLogTime(), Level: "error", Message: "[orders] aborting"}},
	}}, MinFailureLogsSectionChars-1)

	assert.Empty(t, rendered)
}
