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

// LogGroupData is one account of an apply inside the logs fold: SchemaBot's
// own log for it, or one data plane's engine lines for it. Groups are rendered
// one after another under their own headings rather than interleaved by
// timestamp. Each account is written by a different process against a
// different clock, so a merged stream would assert an ordering between them
// that nothing establishes — an engine line could sort above the transition
// that started it, and read as though the engine ran first.
type LogGroupData struct {
	// Label names the account inside the fenced block, without the surrounding
	// markers: "apply logs", "engine logs: region-a". It renders only when the
	// fold carries more than one group, so the common single-account fold
	// reads as a plain log.
	Label string
	// Entries are the group's lines, oldest first.
	Entries []LogEntryData
	// HasOlder reports that the group has lines older than Entries, so the
	// fold can say it shows a tail rather than the whole account.
	HasOlder bool
}

// maxGroupLabelChars bounds what one group heading costs the budget. A label
// longer than this is clamped rather than allowed to eat the lines it exists
// to introduce.
const maxGroupLabelChars = 64

// RenderFailureLogs renders the collapsed logs section appended to a failed
// apply's summary comment, formatted like the CLI logs output (timestamp,
// level tag, message, state transition). Every account of the apply shares the
// one fold: SchemaBot's log for it, and the engine's own lines from each data
// plane that ran it, each under its own heading. One fold means an operator
// opens one thing to triage a failure, and headings keep each account's
// ordering its own.
//
// The fold is labeled "Show logs" when it carries every line of every group
// and "Show recent logs" when any group is a tail — a group's HasOlder reports
// that lines older than its first exist but were not loaded. The section
// spends at most available characters, the room the rest of the comment leaves
// under GitHub's size limit, so a large summary body shrinks the fold instead
// of pushing the comment over the limit. Returns "" when no group carried a
// line or there is no meaningful room, so the summary renders unchanged.
func RenderFailureLogs(groups []LogGroupData, available int) string {
	groups = groupsWithEntries(groups)
	if len(groups) == 0 {
		return ""
	}
	if available < MinFailureLogsSectionChars {
		return ""
	}
	groups, headings, budget, droppedGroups := groupsWithinBudget(groups, available)
	if len(groups) == 0 {
		return ""
	}
	// Every group gets an equal share of what is left, so the first account
	// rendered cannot spend the room the others needed and leave a fan-out
	// reading like a single-region failure.
	share := budget / len(groups)

	var blocks []string
	total := 0
	omitted := 0
	hasOlder := droppedGroups > 0
	for i, group := range groups {
		lines := make([]string, len(group.Entries))
		for j, entry := range group.Entries {
			lines[j] = formatLogEntryLine(entry)
		}
		lines, dropped := trimLogLinesToBudget(lines, share)
		omitted += dropped
		hasOlder = hasOlder || group.HasOlder
		total += len(lines)
		if headings != nil {
			lines = append([]string{headings[i]}, lines...)
		}
		blocks = append(blocks, strings.Join(lines, "\n"))
	}

	label := "Show logs"
	if hasOlder || omitted > 0 {
		label = "Show recent logs"
	}
	noun := "entries"
	if total == 1 {
		noun = "entry"
	}
	section := fmt.Sprintf("\n<details>\n<summary>%s (%d %s)</summary>\n\n", label, total, noun)
	if note := omissionNote(omitted, droppedGroups, hasOlder); note != "" {
		section += note
	}
	section += "```text\n" + strings.Join(blocks, "\n\n") + "\n```\n\n</details>\n"
	return section
}

// groupsWithinBudget decides how many groups the fold can carry and what the
// headings cost it. Headings render only when more than one group survives, so
// a fold that ends up carrying one account reads as a plain log.
//
// A fold too small to give every group a renderable share keeps the groups it
// can and reports the rest as dropped, rather than emitting a heading and an
// ellipsis for each: the headings and truncation markers alone can outgrow the
// room the whole section was given, and a section that overruns its budget
// pushes the comment past GitHub's cap and costs the operator the summary
// itself. The earliest groups are kept, so what survives is the apply's own
// account before any data plane's.
func groupsWithinBudget(groups []LogGroupData, available int) (kept []LogGroupData, headings []string, budget int, dropped int) {
	for n := len(groups); n > 0; n-- {
		kept = groups[:n]
		budget = available - sectionChromeChars
		headings = nil
		if n > 1 {
			headings = make([]string, n)
			for i, group := range kept {
				headings[i] = groupHeading(group.Label)
				// The heading and the newline joining it to the lines below
				// come out of the budget before it is shared, so a headed fold
				// cannot overrun the room a bare one would have fitted in.
				budget -= len(headings[i]) + 1
			}
		}
		if budget/n >= MinRenderedLogLineChars {
			return kept, headings, budget, len(groups) - n
		}
	}
	return nil, nil, 0, len(groups)
}

// groupHeading introduces one account's lines inside the fenced block. The
// label is sanitized like any other text in the fence: it is assembled from
// server configuration rather than from a log line, but it shares the fence
// with text the engine wrote and nothing in it should be able to close it.
func groupHeading(label string) string {
	return "== " + truncateToBytes(sanitizeLogText(label), maxGroupLabelChars) + " =="
}

// groupsWithEntries drops the groups that carried no line, so an account that
// answered with nothing does not turn into an empty headed block.
func groupsWithEntries(groups []LogGroupData) []LogGroupData {
	kept := make([]LogGroupData, 0, len(groups))
	for _, group := range groups {
		if len(group.Entries) > 0 {
			kept = append(kept, group)
		}
	}
	return kept
}

// omissionNote says what the fold left out, so a reader never takes a trimmed
// fold for the complete account. It covers both ways the section sheds
// content under a tight budget: lines dropped from the front of a group, and
// whole groups that could not be given a renderable share.
func omissionNote(omitted, droppedGroups int, hasOlder bool) string {
	var parts []string
	if omitted > 0 {
		parts = append(parts, fmt.Sprintf("%d earlier entries omitted", omitted))
	}
	if droppedGroups > 0 {
		noun := "sources"
		if droppedGroups == 1 {
			noun = "source"
		}
		parts = append(parts, fmt.Sprintf("%d %s omitted", droppedGroups, noun))
	}
	if len(parts) == 0 {
		return ""
	}
	note := "_" + strings.Join(parts, " and ") + " to fit the comment size limit"
	if hasOlder {
		note += " (older entries also exist)"
	}
	return note + "._\n\n"
}

// formatLogEntryLine renders one log entry in the CLI logs format, minus the
// terminal colors: `2026-07-12 16:32:01 UTC [INF] message [running -> stopped]`.
func formatLogEntryLine(entry LogEntryData) string {
	line := fmt.Sprintf("%s %s %s",
		entry.CreatedAt.UTC().Format("2006-01-02 15:04:05 UTC"),
		LogLevelTag(sanitizeLogLevel(entry.Level)),
		sanitizeLogText(entry.Message))
	if entry.OldState != "" && entry.NewState != "" {
		line += fmt.Sprintf(" [%s -> %s]", sanitizeLogText(entry.OldState), sanitizeLogText(entry.NewState))
	}
	return line
}

// sanitizeLogText makes untrusted log text safe inside the section's fenced
// code block. Engine log messages pass through verbatim, so they can carry
// newlines (which would break the one-line-per-entry format and the size
// accounting), a ``` sequence (which would close the fence and let the rest
// of the text render as comment markup), ANSI or bidi control characters
// (which visually reorder or recolor the rendered text), or connection
// endpoints (which leak internal infrastructure the error block above
// already redacts). Newlines collapse to spaces; control characters are
// stripped before the fence check so that removing them cannot join
// backticks into a new fence; backtick runs are then split with a space
// until no fence marker remains; endpoints are redacted with the same rules
// as the comment error sanitizer.
func sanitizeLogText(text string) string {
	text = strings.NewReplacer("\r\n", " ", "\n", " ", "\r", " ").Replace(text)
	text = stripControlText(text)
	for strings.Contains(text, "```") {
		text = strings.ReplaceAll(text, "```", "`` `")
	}
	text = redactConnectionDetails(text)
	return text
}

// maxLogLevelChars bounds an unrecognized level's contribution to a rendered
// line. A level is a short word; anything longer is a value that does not
// belong in the column and must not crowd out the message beside it.
const maxLogLevelChars = 16

// sanitizeLogLevel makes an unrecognized level safe to render in the level
// column. A level arrives from the same storage as the message and, for a
// remotely driven apply, crosses the same data-plane boundary, so it is
// untrusted for the same reasons: a newline would break the one-line-per-entry
// format and a backtick run would close the fence the level sits inside. The
// recognized levels map to fixed tags and never reach this, so the cost lands
// only on a level nothing authored.
func sanitizeLogLevel(level string) string {
	return truncateToBytes(sanitizeLogText(level), maxLogLevelChars)
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

// sampleRemoteFailureLogEntries returns the control-plane tail of an apply a
// data plane drove: the same lifecycle as sampleFailureLogEntries, minus the
// engine lines, which for a remote drive land in the data plane's storage and
// reach the PR through the engine-logs fold instead. The failure it reports is
// the sentence SchemaBot wrote for the target's error code, because the
// target's own words never leave the server log.
func sampleRemoteFailureLogEntries(failedTable, failureReason string) []LogEntryData {
	start := sampleTime().Add(-8 * time.Minute)
	return []LogEntryData{
		{CreatedAt: start, Level: "info", Message: "Apply dispatched to data plane", OldState: "queued", NewState: "running"},
		{CreatedAt: start.Add(20 * time.Second), Level: "info", Message: "Task started: schema change on `" + failedTable + "`"},
		{CreatedAt: start.Add(6 * time.Minute), Level: "error", Message: "Apply failed: " + failureReason, OldState: "running", NewState: "failed"},
	}
}

// sampleEngineFailureLogEntries returns the engine's own account of the same
// failure: the copy it started, the warning the target raised on a row it
// could not convert, and the abort that followed.
func sampleEngineFailureLogEntries(failedTable, failedColumn string) []LogEntryData {
	start := sampleTime().Add(-8 * time.Minute)
	prefix := "[" + failedTable + "] "
	return []LogEntryData{
		{CreatedAt: start.Add(25 * time.Second), Level: "info", Message: prefix + "copy starting: 1466232 rows estimated, 4 threads"},
		{CreatedAt: start.Add(2 * time.Minute), Level: "info", Message: prefix + "copy progress: 12.4% 181812/1466232 rows, eta 21m"},
		{CreatedAt: start.Add(5 * time.Minute), Level: "info", Message: prefix + "copy progress: 30.0% 439870/1466232 rows, eta 14m"},
		{CreatedAt: start.Add(5*time.Minute + 40*time.Second), Level: "warn", Message: prefix + "unsafe warning 1265: Data truncated for column '" + failedColumn + "' at row 1"},
		{CreatedAt: start.Add(5*time.Minute + 41*time.Second), Level: "error", Message: prefix + "aborting: the copy would change values that are already in the table"},
	}
}
