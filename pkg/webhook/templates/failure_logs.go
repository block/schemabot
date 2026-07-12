package templates

import (
	"fmt"
	"strings"
	"time"
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

// maxFailureLogsSectionChars caps the rendered log block so the summary
// comment stays under GitHub's comment size limit with headroom for the
// summary body and fold markup. When the block would exceed it, the earliest
// lines are dropped — the entries closest to the failure are what an operator
// reading the PR needs.
const maxFailureLogsSectionChars = 50000

// RenderRecentFailureLogs renders the collapsed recent-logs section appended
// to a failed apply's summary comment, formatted like the CLI logs output
// (timestamp, level tag, message, state transition). Returns "" when there are
// no entries so the summary renders unchanged.
func RenderRecentFailureLogs(entries []LogEntryData) string {
	if len(entries) == 0 {
		return ""
	}

	lines := make([]string, len(entries))
	for i, entry := range entries {
		lines[i] = formatLogEntryLine(entry)
	}
	lines, omitted := trimLogLinesToBudget(lines, maxFailureLogsSectionChars)

	section := fmt.Sprintf("\n<details>\n<summary>Recent logs (%d)</summary>\n\n", len(entries))
	if omitted > 0 {
		section += fmt.Sprintf("_%d earlier entries omitted to fit the comment size limit._\n\n", omitted)
	}
	section += "```text\n" + strings.Join(lines, "\n") + "\n```\n\n</details>\n"
	return section
}

// formatLogEntryLine renders one log entry in the CLI logs format, minus the
// terminal colors: `2026-07-12 16:32:01 UTC [INF] message [running -> stopped]`.
func formatLogEntryLine(entry LogEntryData) string {
	line := fmt.Sprintf("%s %s %s",
		entry.CreatedAt.UTC().Format("2006-01-02 15:04:05 UTC"),
		logLevelTag(entry.Level),
		entry.Message)
	if entry.OldState != "" && entry.NewState != "" {
		line += fmt.Sprintf(" [%s -> %s]", entry.OldState, entry.NewState)
	}
	return line
}

// logLevelTag returns the CLI's bracketed level indicator without colors.
func logLevelTag(level string) string {
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
// lines always survive; a single oversized line is kept rather than rendering
// an empty block.
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
	return lines, omitted
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
