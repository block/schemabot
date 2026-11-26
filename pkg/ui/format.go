// Package ui provides shared formatting helpers for CLI and PR comment template rendering.
package ui

import (
	"fmt"
	"math"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/block/schemabot/pkg/state"
)

// FormatNumber formats an integer with comma-separated thousands. The sign of
// a negative number stays in front of the digits rather than being grouped
// with them.
// Example: 1234567 → "1,234,567", -123456 → "-123,456"
func FormatNumber(n int64) string {
	s := fmt.Sprintf("%d", n)
	sign := ""
	if n < 0 {
		sign, s = "-", s[1:]
	}
	if len(s) <= 3 {
		return sign + s
	}

	result := []byte(sign)
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}

// FormatBytesBinary formats a byte count with binary units (KiB/MiB), one
// decimal place above the byte range. Storage engines report sizes in bytes,
// and an operator reading a table's footprint wants the magnitude, not the
// digits. Use this for a measured allocation figure; use FormatApproxBytes for
// an estimate, which renders decimal units — the two disagree by 7% at a
// gibibyte and diverge further up the scale.
// Example: 1536 → "1.5 KiB", 1048576 → "1.0 MiB"
func FormatBytesBinary(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	value := float64(b)
	for _, suffix := range []string{"KiB", "MiB", "GiB", "TiB", "PiB"} {
		value /= unit
		if value < unit {
			return fmt.Sprintf("%.1f %s", value, suffix)
		}
	}
	return fmt.Sprintf("%.1f EiB", value/unit)
}

// FormatApproxRows renders an approximate row-count estimate compactly with a
// leading tilde: 842 → "~842", 15_200 → "~15.2k", 2_340_000 → "~2.3M",
// 5_100_000_000 → "~5.1B". Row estimates come from engine statistics and are
// never exact, so the tilde is part of the format. Each unit's threshold sits
// where the one-decimal rendering would round to 1000 of the smaller unit, so
// a value rolls over to "~1M" rather than rendering as "~1000k".
func FormatApproxRows(n int64) string {
	if n < 0 {
		n = 0
	}
	switch {
	case n >= 999_950_000_000:
		return "~" + trimTrailingZero(float64(n)/1e12) + "T"
	case n >= 999_950_000:
		return "~" + trimTrailingZero(float64(n)/1e9) + "B"
	case n >= 999_950:
		return "~" + trimTrailingZero(float64(n)/1e6) + "M"
	case n >= 1_000:
		return "~" + trimTrailingZero(float64(n)/1e3) + "k"
	default:
		return fmt.Sprintf("~%d", n)
	}
}

// trimTrailingZero renders a scaled magnitude with one decimal, dropping a
// trailing ".0" so round values stay short ("2.3", "12").
func trimTrailingZero(v float64) string {
	return strings.TrimSuffix(fmt.Sprintf("%.1f", v), ".0")
}

// oneDecimal renders a scaled magnitude with one decimal below 100 ("4.0",
// "48.2") and none from 100 up ("186"), where the integer already carries
// three significant figures. A size below 100 keeps its ".0" so a value that
// happens to be round reads at the same precision as its neighbours instead
// of looking rounded off.
func oneDecimal(v float64) string {
	if v >= 99.95 {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.1f", v)
}

// FormatApproxBytes renders an approximate byte-size estimate compactly with a
// leading tilde, decimal units, and one decimal: 812 → "~812 B",
// 4_000_000_000 → "~4.0 GB", 48_200_000_000 → "~48.2 GB".
// Byte estimates come from engine statistics and are never exact, so the tilde
// is part of the format. Decimal units (not binary) because the value is an
// order-of-magnitude signal, not an allocation figure — a measured allocation
// belongs in FormatBytesBinary instead. Each unit's threshold
// sits where the integer rendering would round to 1000 of the smaller unit,
// so a value rolls over to "~1.0 GB" rather than rendering as "~1000 MB".
func FormatApproxBytes(b int64) string {
	if b < 0 {
		b = 0
	}
	switch {
	case b >= 999_500_000_000_000:
		return "~" + oneDecimal(float64(b)/1e15) + " PB"
	case b >= 999_500_000_000:
		return "~" + oneDecimal(float64(b)/1e12) + " TB"
	case b >= 999_500_000:
		return "~" + oneDecimal(float64(b)/1e9) + " GB"
	case b >= 999_500:
		return "~" + oneDecimal(float64(b)/1e6) + " MB"
	case b >= 1_000:
		return "~" + oneDecimal(float64(b)/1e3) + " KB"
	default:
		return fmt.Sprintf("~%d B", b)
	}
}

// FormatTableSizeClause renders the " · ~23.4 GB" clause a copy progress line
// carries after its row counts: the table's on-disk size when it was planned,
// so the operator sees the scale of the copy beside how far it has come. It is
// the whole table's size, not a measure of bytes copied. Empty when no
// estimate is known.
func FormatTableSizeClause(estimatedBytes *int64) string {
	if estimatedBytes == nil {
		return ""
	}
	return " \u00b7 " + FormatApproxBytes(*estimatedBytes)
}

// FormatShardedTableSizeClause is a sharded table's planned size beside row
// figures that cover only some of its shards: the size is the whole table's,
// so it names the shards it spans rather than reading as theirs. plannedShards
// is the plan's count, or zero when it recorded none.
func FormatShardedTableSizeClause(estimatedBytes *int64, plannedShards int) string {
	if estimatedBytes == nil {
		return ""
	}
	if plannedShards > 0 {
		return fmt.Sprintf("%s across all %d shards", FormatTableSizeClause(estimatedBytes), plannedShards)
	}
	return FormatTableSizeClause(estimatedBytes) + " across all shards"
}

// VSchemaStatusLabel maps an engine's vschema_status display value to a human
// label, shared by the CLI progress view and the PR comment so both surfaces
// describe VSchema application identically.
func VSchemaStatusLabel(status string) string {
	switch status {
	case "applying":
		return "Applying..."
	case "applied":
		return "Applied"
	case "failed":
		return "Failed"
	case "cancelled":
		return "Cancelled"
	case "stopped":
		return "Stopped"
	case "":
		return "Pending"
	default:
		return status
	}
}

// FormatETA formats a number of seconds, such as an ETA or the time left in a
// window, the same way as FormatHumanDuration, so every duration reads alike.
// Examples: 45 → "45s", 195 → "3m 15s", 1800 → "30m", 90060 → "1d 1h"
func FormatETA(seconds int64) string {
	return FormatHumanDuration(time.Duration(seconds) * time.Second)
}

// MaxStatementRunes bounds an engine-reported statement rendered as a
// progress position so a pathological DDL text cannot flood a terminal line
// or a PR comment. Both surfaces share the bound so the operator reads the
// same excerpt in the CLI and on the PR.
const MaxStatementRunes = 160

// ClampStatement folds an engine-supplied statement onto one bounded line:
// every run of whitespace or control characters — newlines, tabs, ANSI escape
// bytes — collapses to a single space, and text past MaxStatementRunes is cut
// with an ellipsis so the result is at most MaxStatementRunes runes. The
// statement is untrusted input as far as rendering is concerned, so this is
// the single place its line and width discipline live for both the CLI and
// the PR comment.
func ClampStatement(text string) string {
	text = strings.Join(strings.FieldsFunc(text, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}), " ")
	runes := []rune(text)
	if len(runes) <= MaxStatementRunes {
		return text
	}
	return string(runes[:MaxStatementRunes-1]) + "…"
}

// ClampRows returns rows clamped to total for display purposes.
// Spirit can report rows copied > rows total when rows are inserted during copy.
func ClampRows(copied, total int64) int64 {
	if total > 0 && copied > total {
		return total
	}
	return copied
}

// CopyFinishedShortOfEstimate reports whether the engine says a table's copy
// is done while the rows it copied fall short of the estimated total, as when
// the estimate ran high or the copy skipped rows. An engine's percent reaches
// 100 short of the total only when the engine itself declares the copy
// finished, since a percent read from the rows stays below 100 until the rows
// reach the total.
func CopyFinishedShortOfEstimate(pct int, copied, total int64) bool {
	return pct >= 100 && copied >= 0 && copied < total
}

// CopyCoveredRows is how many rows of its total a copy accounts for: the
// rows it copied, clamped to the total, or the whole total once the engine
// reports the copy finished short of it. Percents and sums across tables,
// shards and targets read a copy through this, so a finished copy never
// counts as a fraction of its estimate.
func CopyCoveredRows(pct int, copied, total int64) int64 {
	if CopyFinishedShortOfEstimate(pct, copied, total) {
		return total
	}
	return ClampRows(copied, total)
}

// EstimateExceeded reports whether the row copy has exceeded its estimated total.
func EstimateExceeded(copied, total int64) bool {
	return total > 0 && copied > total
}

// ClampPercent returns a percentage clamped to [0, 100].
func ClampPercent(pct int) int {
	if pct > 100 {
		return 100
	}
	if pct < 0 {
		return 0
	}
	return pct
}

// RowCopyDisplayPercent returns the whole-number percentage for row-copy
// progress bars and threshold comparisons. A non-zero copied row count means
// copying has begun, so it reports at least 1% even when integer progress
// rounds down to 0%. Textual percents render through FormatRowCopyPercent,
// which shows the exact row-derived fraction instead of the bump.
func RowCopyDisplayPercent(pct int, rowsCopied int64) int {
	displayPercent := ClampPercent(pct)
	if displayPercent == 0 && rowsCopied > 0 {
		return 1
	}
	return displayPercent
}

// RecoveringCopyingRows reports whether a table that is recovering after a
// restart is still copying rows, so its progress reads as a row copy rather
// than a bare "recovering". It is when the engine has a row total and the copy
// has not reached 100%. The CLI and the PR comment both render a recovering
// table by this test.
func RecoveringCopyingRows(rowsTotal int64, percentComplete int) bool {
	return rowsTotal > 0 && percentComplete < 100
}

// ChecksumDisplayPercent returns the whole-number percentage for a checksum's
// progress bar from the rows it has verified of its total. It rounds down, so
// the bar fills only once every row is verified and never reads as finished
// beside a percent that says otherwise, and like RowCopyDisplayPercent it
// reports at least 1% once verification has begun. The percent beside the bar
// renders through FormatRowCopyPercent from the same counts. With no total it
// reports 0.
func ChecksumDisplayPercent(rowsChecked, rowsTotal int64) int {
	if rowsTotal <= 0 {
		return 0
	}
	return RowCopyDisplayPercent(int(rowsChecked*100/rowsTotal), rowsChecked)
}

// RowCopyFraction returns row-copy progress as a float percent in [0, 100],
// preferring the exact fraction computed from the row counts over the
// engine's whole-number percent, which has already lost the precision. With
// no row counts to compute from, it falls back to the clamped whole-number
// percent. Renderers comparing tables or shards by progress should compare
// this value so the selection agrees with what FormatRowCopyPercent displays.
func RowCopyFraction(pct int, rowsCopied, rowsTotal int64) float64 {
	rowsCopied = CopyCoveredRows(pct, rowsCopied, rowsTotal)
	if rowsCopied > 0 && rowsTotal > 0 {
		return math.Min(float64(rowsCopied)/float64(rowsTotal)*100, 100)
	}
	return float64(ClampPercent(pct))
}

// FormatRowCopyPercent renders row-copy progress as a percent string at its
// true precision. When the row counts are known the percent is recomputed
// from them and rendered with two decimals (e.g. "0.03%", "45.37%"), so
// progress on a huge table reads as the fraction it is instead of a rounded
// whole number. The rendering is bounded on both ends by what the counts say:
// floored at 0.01% so an in-flight copy never reads as 0.00%, and capped at
// 99.99% while copied rows still trail the total, so a copy never reads as
// finished before it is — an operator deciding whether to keep waiting takes
// "100.00%" as done. A copy the engine reports finished short of its total
// reads 100.00%, since its rows can no longer measure it. Without row counts it falls back to the engine's
// whole-number percent, or "<1%" when copying has begun but there is no
// total to compute a fraction from.
func FormatRowCopyPercent(pct int, rowsCopied, rowsTotal int64) string {
	rowsCopied = CopyCoveredRows(pct, rowsCopied, rowsTotal)
	if rowsCopied > 0 && rowsTotal > 0 {
		frac := math.Max(RowCopyFraction(pct, rowsCopied, rowsTotal), 0.01)
		if rowsCopied < rowsTotal {
			frac = math.Min(frac, 99.99)
		}
		return fmt.Sprintf("%.2f%%", frac)
	}
	display := ClampPercent(pct)
	if display == 0 && rowsCopied > 0 {
		return "<1%"
	}
	return fmt.Sprintf("%d%%", display)
}

// NowFunc returns the current time. Override in previews for deterministic output.
var NowFunc = time.Now

// FormatTimeAgo formats a time as a relative string like "5 minutes ago".
func FormatTimeAgo(t time.Time) string {
	d := NowFunc().Sub(t)
	if d < time.Minute {
		return "just now"
	}
	if d < time.Hour {
		mins := int(d.Minutes())
		if mins == 1 {
			return "1 minute ago"
		}
		return fmt.Sprintf("%d minutes ago", mins)
	}
	if d < 24*time.Hour {
		hours := int(d.Hours())
		if hours == 1 {
			return "1 hour ago"
		}
		return fmt.Sprintf("%d hours ago", hours)
	}
	days := int(d.Hours() / 24)
	if days == 1 {
		return "1 day ago"
	}
	return fmt.Sprintf("%d days ago", days)
}

// FormatHumanDuration formats a duration the one way SchemaBot shows every
// duration, in the CLI, the watch view, and the PR comment: the two largest
// non-zero units, with a zero unit left out. A duration under a second, zero,
// or negative reads "< 1s".
// Examples: 500ms → "< 1s", 45s → "45s", 90s → "1m 30s", 30m → "30m",
// 25h1m → "1d 1h", 24h30m → "1d 30m", 8d3h → "1w 1d", 7d3h → "1w 3h"
func FormatHumanDuration(d time.Duration) string {
	if d < time.Second {
		return "< 1s"
	}
	seconds := int64(d / time.Second)
	units := []struct {
		suffix  string
		seconds int64
	}{
		{"w", 7 * 24 * 3600},
		{"d", 24 * 3600},
		{"h", 3600},
		{"m", 60},
		{"s", 1},
	}
	var parts []string
	for _, unit := range units {
		count := seconds / unit.seconds
		seconds %= unit.seconds
		if count == 0 {
			continue
		}
		parts = append(parts, fmt.Sprintf("%d%s", count, unit.suffix))
		if len(parts) == 2 {
			break
		}
	}
	return strings.Join(parts, " ")
}

// CapitalizeFirst capitalizes the first letter of a string.
func CapitalizeFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// Pluralize returns singular+"s" when count != 1.
func Pluralize(singular string, count int) string {
	if count == 1 {
		return singular
	}
	return singular + "s"
}

// PluralizeLabel returns singular or plural label based on count.
func PluralizeLabel(singular, plural string, count int) string {
	if count == 1 {
		return singular
	}
	return plural
}

// haltedTaskStates is the order a listing leads with the tables that stopped
// short of completing. It is the same precedence a table's status is rolled up
// across targets by (presentation.RollupTaskStatus), so the two cannot name a
// different state first.
var haltedTaskStates = state.HaltedTaskStates()

// TableStatePriority returns a sort key for table display ordering.
// Lower values sort first: a failure or halt leads, so an operator scanning a
// long list sees it before any queued table, then work in progress, then
// queued tables, and settled tables last. The halted states lead in the order
// of state.HaltedTaskStates. Sorts that use the key must be stable, so tables
// of one rank keep their order.
// Uses Task states (not Apply states) since tables are per-table tasks.
// Used by both CLI (watch TUI) and PR comment rendering for consistent ordering.
func TableStatePriority(taskState string) int {
	if rank := slices.Index(haltedTaskStates, taskState); rank >= 0 {
		return rank
	}
	// The ranks below follow the halted ones, however many there are.
	after := len(haltedTaskStates)
	switch taskState {
	case state.Task.Running, state.Task.CatchingUp, state.Task.Checksumming, state.Task.PostChecksum, state.Task.CuttingOver:
		return after // active
	case state.Task.WaitingForCutover, state.Task.Recovering:
		return after + 1
	case state.Task.Pending:
		return after + 2
	case state.Task.Completed, state.Task.Reverted:
		return after + 3 // settled — bottom
	default:
		return after + 2
	}
}

// LintReasons splits an engine-reported unsafe reason into its individual
// lint violations. Engines join a table's violations with "; ", so renderers
// use this to give each violation its own line instead of one run-on string.
// Each returned message has its severity prefix stripped; empty segments are
// dropped.
func LintReasons(reason string) []string {
	var reasons []string
	for seg := range strings.SplitSeq(reason, "; ") {
		if cleaned := cleanSingleLintReason(seg); cleaned != "" {
			reasons = append(reasons, cleaned)
		}
	}
	return reasons
}

func cleanSingleLintReason(reason string) string {
	for _, prefix := range []string{"[ERROR] ", "[WARNING] ", "[INFO] "} {
		if strings.HasPrefix(reason, prefix) {
			reason = reason[len(prefix):]
			if idx := strings.Index(reason, ": "); idx != -1 {
				reason = reason[idx+2:]
			}
			break
		}
	}
	return reason
}

// Lint and safety messages quote SQL identifiers and types with double
// quotes ("idx_category", "varchar"). The token pattern is restricted to
// identifier and type characters — including the comma and single quote that
// parameterised and enumerated types carry ("decimal(10,2)",
// "enum('active','archived')") — so prose in quotes ("should not be
// dropped") is left alone. Two quoted shapes carry spaces and are still
// unambiguously SQL: an operation or statement fragment led by a DDL keyword
// ("DROP TABLE `t`", "TRUNCATE PARTITION"), and a type or key part followed
// by attribute words ("int(11) unsigned", "created_at DESC").
var (
	doubleQuotedIdentifier = regexp.MustCompile(`"([A-Za-z0-9_$.(),']+)"`)
	quotedSQLFragment      = regexp.MustCompile(`"((?:ALTER|CREATE|DROP|RENAME|TRUNCATE|DISCARD|COALESCE)\b[^"]*)"`)
	quotedTokenWithSuffix  = regexp.MustCompile(`"([A-Za-z0-9_$.(),']+(?: (?:unsigned|signed|zerofill|precision|varying|DESC))+)"`)
)

// codeSpan wraps content in a markdown inline code span. Content that itself
// contains backticks (SQL fragments quote identifiers with them, doubling any
// backtick embedded in a name) needs a delimiter run longer than the longest
// backtick run in the content, plus padding spaces, or the span would close
// mid-content.
func codeSpan(content string) string {
	if !strings.Contains(content, "`") {
		return "`" + content + "`"
	}
	delimiter := strings.Repeat("`", longestBacktickRun(content)+1)
	return delimiter + " " + content + " " + delimiter
}

// longestBacktickRun returns the length of the longest consecutive run of
// backticks in s.
func longestBacktickRun(s string) int {
	longest, run := 0, 0
	for _, r := range s {
		if r == '`' {
			run++
			longest = max(longest, run)
			continue
		}
		run = 0
	}
	return longest
}

// CodeQuoteIdentifiers rewrites quoted SQL identifiers, types, and operation
// fragments in a human-authored message to markdown inline code, so index,
// column, and type names read as code on markdown surfaces. Best-effort
// display formatting: tokens that don't look like SQL keep their original
// quotes.
func CodeQuoteIdentifiers(message string) string {
	message = quotedSQLFragment.ReplaceAllStringFunc(message, func(quoted string) string {
		return codeSpan(quoted[1 : len(quoted)-1])
	})
	message = doubleQuotedIdentifier.ReplaceAllString(message, "`$1`")
	return quotedTokenWithSuffix.ReplaceAllString(message, "`$1`")
}
