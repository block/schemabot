package templates

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRenderFailureLogs verifies the logs section appended to a failed
// apply's summary folds the entries into a details block and formats each
// line like the CLI logs output: UTC timestamp, bracketed level tag, message,
// and state transition when set. A complete history is labeled "Show logs" —
// no entries were left out, so the fold must not suggest a subset.
func TestRenderFailureLogs(t *testing.T) {
	at := time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC)
	rendered := RenderFailureLogs([]LogGroupData{{Entries: []LogEntryData{
		{CreatedAt: at, Level: "info", Message: "Apply claimed by driver", OldState: "queued", NewState: "running"},
		{CreatedAt: at.Add(3 * time.Second), Level: "warn", Message: "Copy throttled by replication lag"},
		{CreatedAt: at.Add(9 * time.Second), Level: "error", Message: "Lost MySQL connection; retrying"},
	}, HasOlder: false}}, GitHubIssueCommentMaxChars)

	assert.Contains(t, rendered, "<details>")
	assert.Contains(t, rendered, "<summary>Show logs (3 entries)</summary>")
	assert.NotContains(t, rendered, "Show recent logs")
	assert.Contains(t, rendered, "```text")
	assert.Contains(t, rendered, "2026-07-12 16:32:01 UTC [INF] Apply claimed by driver [queued -> running]")
	assert.Contains(t, rendered, "2026-07-12 16:32:04 UTC [WRN] Copy throttled by replication lag")
	assert.Contains(t, rendered, "2026-07-12 16:32:10 UTC [ERR] Lost MySQL connection; retrying")
	assert.NotContains(t, rendered, "omitted")
}

// TestRenderFailureLogsTailLabel verifies that when older entries exist
// beyond the loaded tail, the fold is labeled "Show recent logs" so the
// operator knows they are seeing a subset, not the full history.
func TestRenderFailureLogsTailLabel(t *testing.T) {
	at := time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC)
	rendered := RenderFailureLogs([]LogGroupData{{Entries: []LogEntryData{
		{CreatedAt: at, Level: "error", Message: "Apply failed", OldState: "running", NewState: "failed"},
	}, HasOlder: true}}, GitHubIssueCommentMaxChars)

	assert.Contains(t, rendered, "<summary>Show recent logs (1 entry)</summary>")
}

// TestRenderFailureLogsEmpty verifies an apply with no log entries adds
// nothing to the summary — no empty details block.
func TestRenderFailureLogsEmpty(t *testing.T) {
	assert.Empty(t, RenderFailureLogs(nil, GitHubIssueCommentMaxChars))
}

// TestRenderFailureLogsSanitizesUntrustedText verifies engine-supplied
// log text cannot break out of the fenced code block: newlines collapse to
// spaces so every entry stays on one line, and backtick fences are split so
// the rest of the comment cannot be reinterpreted as markup.
func TestRenderFailureLogsSanitizesUntrustedText(t *testing.T) {
	at := time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC)
	rendered := RenderFailureLogs([]LogGroupData{{Entries: []LogEntryData{
		{CreatedAt: at, Level: "error", Message: "line one\r\nline two\nline three"},
		{CreatedAt: at.Add(time.Second), Level: "error", Message: "fence breakout ```\n# not a heading"},
		{CreatedAt: at.Add(2 * time.Second), Level: "error", Message: "long run `````x"},
	}, HasOlder: false}}, GitHubIssueCommentMaxChars)

	assert.Contains(t, rendered, "[ERR] line one line two line three")
	assert.Contains(t, rendered, "[ERR] fence breakout `` ` # not a heading")
	assert.Equal(t, 2, strings.Count(rendered, "```"), "only the section's own fence markers survive")
	assert.Equal(t, strings.Index(rendered, "```text"), strings.Index(rendered, "```"), "first fence marker is the section's opener")
}

// TestRenderFailureLogsTrimsToSizeBudget verifies that when the rendered
// log block would blow GitHub's comment size limit, the earliest lines are
// dropped, the newest are kept, the fold says how many were omitted, and the
// label flips to "Show recent logs" because a subset is shown.
func TestRenderFailureLogsTrimsToSizeBudget(t *testing.T) {
	at := time.Date(2026, 7, 12, 16, 0, 0, 0, time.UTC)
	entries := make([]LogEntryData, 100)
	for i := range entries {
		entries[i] = LogEntryData{
			CreatedAt: at.Add(time.Duration(i) * time.Second),
			Level:     "info",
			Message:   strings.Repeat("x", 1000) + " #" + time.Duration(i).String(),
		}
	}
	rendered := RenderFailureLogs([]LogGroupData{{Entries: entries, HasOlder: false}}, GitHubIssueCommentMaxChars)

	require.Less(t, len(rendered), 65536, "rendered section must leave room inside GitHub's size limit")
	assert.Contains(t, rendered, "<summary>Show recent logs (")
	assert.Contains(t, rendered, "earlier entries omitted")
	assert.NotContains(t, rendered, "16:00:00 UTC", "earliest entry is dropped first")
	assert.Contains(t, rendered, "16:01:39 UTC", "newest entry always survives")
}

// TestRenderFailureLogsShrinksToAvailableRoom verifies a large summary
// body shrinks the section: with less room available than the default cap, the
// section trims to what fits so appending it never pushes the assembled
// comment over GitHub's size limit.
func TestRenderFailureLogsShrinksToAvailableRoom(t *testing.T) {
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
	rendered := RenderFailureLogs([]LogGroupData{{Entries: entries, HasOlder: false}}, available)

	require.NotEmpty(t, rendered)
	assert.LessOrEqual(t, len(rendered), available, "the section must fit in the room the summary body leaves")
	assert.Contains(t, rendered, "earlier entries omitted")
	assert.Contains(t, rendered, "16:00:19 UTC", "newest entry always survives")
	assert.NotContains(t, rendered, "16:00:00 UTC", "earliest entry is dropped first")
}

// TestRenderFailureLogsSkipsWhenNoRoom verifies that a summary body
// leaving no meaningful room under the comment size limit drops the section
// entirely — the summary must still post, and a fold too small to carry a log
// line is noise.
func TestRenderFailureLogsSkipsWhenNoRoom(t *testing.T) {
	entries := []LogEntryData{
		{CreatedAt: time.Date(2026, 7, 12, 16, 0, 0, 0, time.UTC), Level: "error", Message: "Apply failed"},
	}
	assert.Empty(t, RenderFailureLogs([]LogGroupData{{Entries: entries, HasOlder: false}}, 0))
	assert.Empty(t, RenderFailureLogs([]LogGroupData{{Entries: entries, HasOlder: false}}, -500))
	assert.Empty(t, RenderFailureLogs([]LogGroupData{{Entries: entries, HasOlder: false}}, MinFailureLogsSectionChars-1))
}

// TestRenderFailureLogsTruncatesSingleOversizedLine verifies one
// enormous engine error message cannot blow the budget on its own: the sole
// surviving line is truncated to fit rather than carried oversize.
func TestRenderFailureLogsTruncatesSingleOversizedLine(t *testing.T) {
	entries := []LogEntryData{
		{
			CreatedAt: time.Date(2026, 7, 12, 16, 0, 0, 0, time.UTC),
			Level:     "error",
			Message:   "Apply failed: " + strings.Repeat("é", GitHubIssueCommentMaxChars),
		},
	}
	available := 4000
	rendered := RenderFailureLogs([]LogGroupData{{Entries: entries, HasOlder: false}}, available)

	require.NotEmpty(t, rendered)
	assert.LessOrEqual(t, len(rendered), available, "a single oversized line must be truncated to the budget")
	assert.Contains(t, rendered, "Apply failed: ")
	assert.Contains(t, rendered, "…", "the truncated line ends with an ellipsis")
	assert.True(t, strings.HasSuffix(strings.TrimSpace(rendered), "</details>"), "the fold still closes cleanly")
}

// Engine log lines rendered in the failure fold carry the same raw dial
// errors the error block above redacts, so the fold redacts endpoints too —
// otherwise it would reveal exactly what the error block hides.
func TestRenderFailureLogsRedactsEndpoints(t *testing.T) {
	at := time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC)
	rendered := RenderFailureLogs([]LogGroupData{{Entries: []LogEntryData{
		{CreatedAt: at, Level: "error", Message: "dial tcp db-primary.internal:3306: connection refused"},
	}, HasOlder: false}}, GitHubIssueCommentMaxChars)

	assert.NotContains(t, rendered, "db-primary.internal", "internal endpoints are redacted")
	assert.Contains(t, rendered, "[ERR] dial tcp [endpoint redacted]: connection refused")
}

func TestRenderFailureLogsRedactsTabSeparatedPostgresIdentity(t *testing.T) {
	at := time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC)
	rendered := RenderFailureLogs([]LogGroupData{{Entries: []LogEntryData{
		{CreatedAt: at, Level: "error", Message: "\tALTER TABLE database\t\"orders\" DROP COLUMN legacy (SQLSTATE 42704)"},
	}, HasOlder: false}}, GitHubIssueCommentMaxChars)

	assert.Contains(t, rendered, "[ERR] \tALTER TABLE database \"[endpoint redacted]\" DROP COLUMN legacy (SQLSTATE 42704)")
	assert.NotContains(t, rendered, "orders")
}

// Engine log lines can carry ANSI escape sequences and bidi override
// characters that browsers still apply inside a fenced block, letting a log
// line recolor or visually reorder the text an operator reads during a
// failure. The fold strips them the same way the error block above does.
func TestRenderFailureLogsStripsControlCharacters(t *testing.T) {
	at := time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC)
	rendered := RenderFailureLogs([]LogGroupData{{Entries: []LogEntryData{
		{CreatedAt: at, Level: "error", Message: "red\x1b[31malert re\u202enamed.txt\u202c"},
	}, HasOlder: false}}, GitHubIssueCommentMaxChars)

	assert.Contains(t, rendered, "[ERR] redalert renamed.txt")
	assert.NotContains(t, rendered, "\x1b", "ANSI escapes are stripped")
	assert.NotContains(t, rendered, "\u202e", "bidi overrides are stripped")
}

// TestSanitizeLogTextControlCharCannotFormFence verifies that stripping a
// control character between backticks cannot join them into a fence marker:
// control characters are removed before the fence check, so the joined run
// is still split.
func TestSanitizeLogTextControlCharCannotFormFence(t *testing.T) {
	got := sanitizeLogText("``\x01`")
	assert.NotContains(t, got, "```")
	assert.Equal(t, "`` `", got)
}

// A failed apply that ran on a data plane has two accounts of itself, and the
// fold carries both under their own headings. They are not interleaved by
// timestamp: each account is written by a different process against a
// different clock, so a merged stream would claim an ordering between them
// that nothing establishes.
func TestRenderFailureLogsGroupsEveryAccountInOneFold(t *testing.T) {
	at := time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC)
	rendered := RenderFailureLogs([]LogGroupData{
		{Label: "apply logs", Entries: []LogEntryData{
			{CreatedAt: at.Add(20 * time.Second), Level: "error", Message: "Apply failed", OldState: "running", NewState: "failed"},
		}},
		{Label: "engine logs: region-a", Entries: []LogEntryData{
			{CreatedAt: at, Level: "warn", Message: "[orders] unsafe warning 1265"},
		}},
	}, GitHubIssueCommentMaxChars)

	assert.Contains(t, rendered, "<summary>Show logs (2 entries)</summary>")
	assert.Equal(t, 2, strings.Count(rendered, "```"), "one fold, one fence")
	applyHeading := strings.Index(rendered, "== apply logs ==")
	engineHeading := strings.Index(rendered, "== engine logs: region-a ==")
	require.Positive(t, applyHeading)
	require.Positive(t, engineHeading)
	assert.Less(t, applyHeading, strings.Index(rendered, "Apply failed"))
	assert.Less(t, strings.Index(rendered, "Apply failed"), engineHeading,
		"each account's lines stay under its own heading rather than merging into one stream")
	assert.Less(t, engineHeading, strings.Index(rendered, "[orders] unsafe warning 1265"))
}

// An apply with only one account needs no heading: there is nothing to tell
// apart, and naming the single group would read as though something else were
// missing from the fold.
func TestRenderFailureLogsOmitsHeadingsForOneGroup(t *testing.T) {
	rendered := RenderFailureLogs([]LogGroupData{
		{Label: "apply logs", Entries: []LogEntryData{
			{CreatedAt: time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC), Level: "error", Message: "Apply failed"},
		}},
	}, GitHubIssueCommentMaxChars)

	assert.Contains(t, rendered, "<summary>Show logs (1 entry)</summary>")
	assert.NotContains(t, rendered, "==")
}

// A group that carried no line is dropped rather than rendered as an empty
// headed block, and a fold left with nothing renders as nothing at all.
func TestRenderFailureLogsDropsEmptyGroups(t *testing.T) {
	rendered := RenderFailureLogs([]LogGroupData{
		{Label: "apply logs", Entries: []LogEntryData{
			{CreatedAt: time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC), Level: "error", Message: "Apply failed"},
		}},
		{Label: "engine logs: region-a"},
	}, GitHubIssueCommentMaxChars)

	assert.NotContains(t, rendered, "region-a")
	assert.NotContains(t, rendered, "==", "the surviving group is the only one, so it needs no heading")
	assert.Empty(t, RenderFailureLogs([]LogGroupData{{Label: "engine logs: region-a"}}, GitHubIssueCommentMaxChars))
}

// Each group gets its own share of the room, so the first account rendered
// cannot spend what the others needed and leave a fan-out reading like a
// single-region failure.
func TestRenderFailureLogsSharesTheBudgetAcrossGroups(t *testing.T) {
	at := time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC)
	group := func(label, marker string) LogGroupData {
		var entries []LogEntryData
		for i := range 200 {
			entries = append(entries, LogEntryData{CreatedAt: at.Add(time.Duration(i) * time.Second), Level: "info", Message: marker + strings.Repeat("x", 200)})
		}
		return LogGroupData{Label: label, Entries: entries}
	}
	const available = 4096
	rendered := RenderFailureLogs([]LogGroupData{group("apply logs", "A:"), group("engine logs: region-a", "B:")}, available)

	assert.LessOrEqual(t, len(rendered), available)
	assert.Contains(t, rendered, "A:")
	assert.Contains(t, rendered, "B:")
	assert.Contains(t, rendered, "earlier entries omitted to fit the comment size limit")
}

// The fold never overruns the room it was given, however many accounts an
// apply fanned out across. Headings and truncation markers have a cost of
// their own, so a budget too small to give every group a renderable share
// keeps the groups it can and says how many it dropped — a section that
// overran its budget would push the comment past GitHub's cap and cost the
// operator the summary itself.
func TestRenderFailureLogsStaysWithinBudgetAcrossManyGroups(t *testing.T) {
	at := time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC)
	var groups []LogGroupData
	for i := range 12 {
		groups = append(groups, LogGroupData{
			Label:   "engine logs: " + strings.Repeat("r", 60) + string(rune('a'+i)),
			Entries: []LogEntryData{{CreatedAt: at, Level: "info", Message: strings.Repeat("x", 300)}},
		})
	}
	const available = MinFailureLogsSectionChars

	rendered := RenderFailureLogs(groups, available)

	assert.LessOrEqual(t, len(rendered), available)
	if rendered != "" {
		assert.Contains(t, rendered, "omitted to fit the comment size limit")
	}
}

// A hostile level is untrusted for the same reasons a message is: it comes
// from the same row, and for a remotely driven apply it crosses the same data
// plane boundary. It shares the fence with every other line, so it must not be
// able to close it or to inject markup into the comment around it.
func TestRenderFailureLogsSanitizesTheLevel(t *testing.T) {
	rendered := RenderFailureLogs([]LogGroupData{{Entries: []LogEntryData{
		{CreatedAt: time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC), Level: "warn\n```\n**bold**", Message: "[orders] copy starting"},
	}}}, GitHubIssueCommentMaxChars)

	assert.Equal(t, 2, strings.Count(rendered, "```"), "the fold's own fence is the only fence in it")
	assert.NotContains(t, rendered, "**bold**")
	assert.Contains(t, rendered, "[orders] copy starting")
}

// A hostile heading shares the fence with the lines it introduces, so it is
// sanitized and clamped like any other text in it.
func TestRenderFailureLogsSanitizesGroupHeadings(t *testing.T) {
	at := time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC)
	rendered := RenderFailureLogs([]LogGroupData{
		{Label: "engine logs: region-a\n```\n**bold**", Entries: []LogEntryData{{CreatedAt: at, Level: "info", Message: "one"}}},
		{Label: strings.Repeat("z", 400), Entries: []LogEntryData{{CreatedAt: at, Level: "info", Message: "two"}}},
	}, GitHubIssueCommentMaxChars)

	assert.Equal(t, 2, strings.Count(rendered, "```"))
	assert.NotContains(t, rendered, strings.Repeat("z", MaxGroupLabelChars+1))
}

// A heading exists to tell two accounts apart, so the clamp that keeps it
// inside the budget must not be the thing that makes them identical. Names
// that identify a deployment or a target share long prefixes and differ at
// the tail, so the middle goes rather than the end.
func TestElideMiddleKeepsTheEndThatDistinguishes(t *testing.T) {
	const max = MaxGroupLabelPartChars
	third := ElideMiddle("payments-production-shard-003", max)
	fourth := ElideMiddle("payments-production-shard-004", max)

	assert.LessOrEqual(t, len(third), max)
	assert.LessOrEqual(t, len(fourth), max)
	assert.NotEqual(t, third, fourth, "two targets that differ only at the tail must not clamp to one name")
	assert.True(t, strings.HasSuffix(third, "003"))
	assert.True(t, strings.HasSuffix(fourth, "004"))
	assert.Equal(t, "shard-a", ElideMiddle("shard-a", max), "a name that fits is left alone")

	for _, name := range []string{"ααααααααααααα", "shard-ααααα-003", strings.Repeat("é", 40)} {
		for budget := 1; budget <= max; budget++ {
			elided := ElideMiddle(name, budget)
			assert.LessOrEqual(t, len(elided), budget, "%q at %d", name, budget)
			assert.True(t, utf8.ValidString(elided), "a clamp never splits a rune: %q at %d", name, budget)
		}
	}
}

// The section must never exceed the room it was given, whatever shape the
// groups take: the comment it is appended to is already sized against
// GitHub's cap, and a section that overruns costs the operator the terminal
// summary itself. The budget has to cover more than the lines — the headings,
// the blank line between each pair of groups, and the omission note, all of
// which grow with the number of groups — so this sweeps the shapes rather
// than checking one.
func TestRenderFailureLogsNeverExceedsItsBudget(t *testing.T) {
	at := time.Date(2026, 7, 12, 16, 32, 1, 0, time.UTC)
	for _, groupCount := range []int{1, 2, 3, 8, 40, 300} {
		for _, labelLen := range []int{0, 13, 200} {
			for _, entries := range []int{1, 7} {
				for _, messageLen := range []int{0, 40, 300} {
					for _, available := range []int{MinFailureLogsSectionChars, 600, 4096, GitHubIssueCommentMaxChars} {
						groups := make([]LogGroupData, groupCount)
						for i := range groups {
							lines := make([]LogEntryData, entries)
							for j := range lines {
								lines[j] = LogEntryData{CreatedAt: at, Level: "info", Message: strings.Repeat("x", messageLen)}
							}
							groups[i] = LogGroupData{
								Label:    strings.Repeat("r", labelLen) + string(rune('a'+i%26)),
								Entries:  lines,
								HasOlder: i%2 == 0,
							}
						}
						rendered := RenderFailureLogs(groups, available)
						require.LessOrEqual(t, len(rendered), available,
							"groups=%d label=%d entries=%d message=%d available=%d",
							groupCount, labelLen, entries, messageLen, available)
					}
				}
			}
		}
	}
}
