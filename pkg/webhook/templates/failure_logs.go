package templates

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// LogEntryData is one apply log line rendered into the recent-logs section of
// a failed apply's summary comment.
type LogEntryData struct {
	// CreatedAt is when the entry was written; rendered in UTC because PR
	// comments are read across timezones.
	CreatedAt time.Time
	// Level is the log level: "debug", "info", "warn", "error".
	Level string
	// Message is the human-readable log message.
	Message string
	// OldState and NewState carry the transition for state-change entries;
	// both empty for plain messages.
	OldState string
	NewState string
}

// GitHubIssueCommentMaxChars is GitHub's hard cap on an issue comment body —
// a larger body is rejected outright, so everything rendered into a comment
// must fit under it. This is the only size limit on the logs section: the
// fold spends whatever room the rest of the comment leaves, and when the
// block would exceed that, the earliest lines are dropped — the entries
// closest to the failure are what an operator reading the PR needs.
const GitHubIssueCommentMaxChars = 65536

// MinRenderedLogLineChars is the smallest a rendered log line can be: the UTC
// timestamp and the bracketed level tag with their separating spaces, plus the
// joining newline — the degenerate case of an empty level and empty message.
// Callers use it to bound how many entries could ever fit in a fold — loading
// more than budget / MinRenderedLogLineChars entries can never add a rendered
// line.
const MinRenderedLogLineChars = len("2006-01-02 15:04:05 UTC [] ") + 1

// sectionChromeChars reserves room within the budget for the section's own
// markup: the details/summary fold, the omitted-entries note, and the code
// fences.
const sectionChromeChars = 256

// MinFailureLogsSectionChars is the smallest budget worth rendering for —
// below this, not even one truncated log line would convey anything useful, so
// the section is dropped entirely. Callers can pre-check their available room
// against it to skip loading entries that could never render.
const MinFailureLogsSectionChars = 512

// RenderRecentFailureLogs renders the collapsed logs section appended to a
// failed apply's summary comment, formatted like the CLI logs output
// (timestamp, level tag, message, state transition). The fold is labeled
// "Show logs" when it carries the apply's complete log history and "Show
// recent logs" when it is a tail — hasOlder reports that entries older than
// entries[0] exist but were not loaded. The section spends at most available characters —
// the room the rest of the comment leaves under GitHub's size limit, so a
// large summary body shrinks the fold instead of pushing the comment over the
// limit. Returns "" when there are no entries or no meaningful room, so the
// summary renders unchanged.
func RenderRecentFailureLogs(entries []LogEntryData, available int, hasOlder bool) string {
	if len(entries) == 0 {
		return ""
	}
	if available < MinFailureLogsSectionChars {
		return ""
	}
	budget := available

	lines := make([]string, len(entries))
	for i, entry := range entries {
		lines[i] = formatLogEntryLine(entry)
	}
	lines, omitted := trimLogLinesToBudget(lines, budget-sectionChromeChars)

	label := "Show logs"
	if hasOlder || omitted > 0 {
		label = "Show recent logs"
	}
	noun := "entries"
	if len(lines) == 1 {
		noun = "entry"
	}
	section := fmt.Sprintf("\n<details>\n<summary>%s (%d %s)</summary>\n\n", label, len(lines), noun)
	if omitted > 0 {
		note := fmt.Sprintf("_%d earlier entries omitted to fit the comment size limit", omitted)
		if hasOlder {
			note += " (older entries also exist)"
		}
		section += note + "._\n\n"
	}
	section += "```text\n" + strings.Join(lines, "\n") + "\n```\n\n</details>\n"
	return section
}

// formatLogEntryLine renders one log entry in the CLI logs format, minus the
// terminal colors: `2026-07-12 16:32:01 UTC [INF] message [running -> stopped]`.
func formatLogEntryLine(entry LogEntryData) string {
	line := fmt.Sprintf("%s %s %s",
		entry.CreatedAt.UTC().Format("2006-01-02 15:04:05 UTC"),
		LogLevelTag(entry.Level),
		sanitizeLogText(entry.Message))
	if entry.OldState != "" && entry.NewState != "" {
		line += fmt.Sprintf(" [%s -> %s]", sanitizeLogText(entry.OldState), sanitizeLogText(entry.NewState))
	}
	return line
}

// sanitizeLogText makes untrusted log text safe inside the section's fenced
// code block. Engine log messages pass through verbatim, so they can carry
// newlines (which would break the one-line-per-entry format and the size
// accounting) or a ``` sequence (which would close the fence and let the rest
// of the text render as comment markup). Newlines collapse to spaces; backtick
// runs are split with a space until no fence marker remains.
func sanitizeLogText(text string) string {
	text = strings.NewReplacer("\r\n", " ", "\n", " ", "\r", " ").Replace(text)
	for strings.Contains(text, "```") {
		text = strings.ReplaceAll(text, "```", "`` `")
	}
	return text
}

// LogLevelTag returns the bracketed apply-log level indicator without colors:
// [ERR], [WRN], [INF], [DBG], or [LEVEL] for anything else. It is the single
// source of the tag text — the CLI logs output wraps it in ANSI colors, and
// the failed-summary fold renders it bare — so the two surfaces cannot drift.
func LogLevelTag(level string) string {
	switch strings.ToLower(level) {
	case "error":
		return "[ERR]"
	case "warn":
		return "[WRN]"
	case "info":
		return "[INF]"
	case "debug":
		return "[DBG]"
	default:
		return "[" + strings.ToUpper(level) + "]"
	}
}

// trimLogLinesToBudget drops the earliest lines until the joined block fits
// the budget, returning the kept lines and how many were dropped. The newest
// lines always survive; when the last remaining line alone exceeds the budget
// (a single enormous engine error message), it is truncated to fit rather than
// carried oversize — the block must never exceed the budget.
func trimLogLinesToBudget(lines []string, budget int) ([]string, int) {
	total := 0
	for _, line := range lines {
		total += len(line) + 1
	}
	omitted := 0
	for total > budget && len(lines) > 1 {
		total -= len(lines[0]) + 1
		lines = lines[1:]
		omitted++
	}
	if len(lines) == 1 && total > budget {
		lines[0] = truncateToBytes(lines[0], budget-len("…")-1) + "…"
	}
	return lines, omitted
}

// truncateToBytes cuts text to at most maxBytes without splitting a UTF-8
// rune, so the truncated line stays valid text.
func truncateToBytes(text string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(text) <= maxBytes {
		return text
	}
	cut := maxBytes
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut]
}

// sampleFailureLogEntries returns the log entries shared by the failed-summary
// previews: the tail of an apply that failed mid-copy, ending with the given
// failure so the fold reads coherently with the preview's error block.
func sampleFailureLogEntries(failedTable, failureMessage string) []LogEntryData {
	start := sampleTime().Add(-8 * time.Minute)
	return []LogEntryData{
		{CreatedAt: start, Level: "info", Message: "Apply claimed by driver", OldState: "queued", NewState: "running"},
		{CreatedAt: start.Add(15 * time.Second), Level: "info", Message: "Task started: schema change on `" + failedTable + "`"},
		{CreatedAt: start.Add(3 * time.Minute), Level: "warn", Message: "Copy throttled by replication lag (1.2s)"},
		{CreatedAt: start.Add(6 * time.Minute), Level: "error", Message: "Task failed: " + failureMessage},
		{CreatedAt: start.Add(7 * time.Minute), Level: "error", Message: "Apply failed", OldState: "running", NewState: "failed"},
	}
}
