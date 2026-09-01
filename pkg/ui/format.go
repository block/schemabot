// Package ui provides shared formatting helpers for CLI and PR comment template rendering.
package ui

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/state"
)

// FormatNumber formats an integer with comma-separated thousands.
// Example: 1234567 → "1,234,567"
func FormatNumber(n int64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}

	var result []byte
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

// FormatApproxBytes renders an approximate byte-size estimate compactly with a
// leading tilde and decimal units: 812 → "~812 B", 48_200_000_000 → "~48.2 GB".
// Byte estimates come from engine statistics and are never exact, so the tilde
// is part of the format. Decimal units (not binary) because the value is an
// order-of-magnitude signal, not an allocation figure — a measured allocation
// belongs in FormatBytesBinary instead. Each unit's threshold
// sits where the one-decimal rendering would round to 1000 of the smaller
// unit, so a value rolls over to "~1 GB" rather than rendering as "~1000 MB".
func FormatApproxBytes(b int64) string {
	if b < 0 {
		b = 0
	}
	switch {
	case b >= 999_950_000_000_000:
		return "~" + trimTrailingZero(float64(b)/1e15) + " PB"
	case b >= 999_950_000_000:
		return "~" + trimTrailingZero(float64(b)/1e12) + " TB"
	case b >= 999_950_000:
		return "~" + trimTrailingZero(float64(b)/1e9) + " GB"
	case b >= 999_950:
		return "~" + trimTrailingZero(float64(b)/1e6) + " MB"
	case b >= 1_000:
		return "~" + trimTrailingZero(float64(b)/1e3) + " KB"
	default:
		return fmt.Sprintf("~%d B", b)
	}
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

// FormatETA formats a duration in seconds as a human-readable string, using the
// two largest non-zero units so a long copy stays readable.
// Examples: 45 → "45s", 195 → "3m 15s", 3700 → "1h 1m", 180000 → "2d 2h"
func FormatETA(seconds int64) string {
	const (
		minute = 60
		hour   = 60 * minute
		day    = 24 * hour
	)
	switch {
	case seconds < minute:
		return fmt.Sprintf("%ds", seconds)
	case seconds < hour:
		return fmt.Sprintf("%dm %ds", seconds/minute, seconds%minute)
	case seconds < day:
		return fmt.Sprintf("%dh %dm", seconds/hour, (seconds%hour)/minute)
	default:
		return fmt.Sprintf("%dd %dh", seconds/day, (seconds%day)/hour)
	}
}

// ClampRows returns rows clamped to total for display purposes.
// Spirit can report rows copied > rows total when rows are inserted during copy.
func ClampRows(copied, total int64) int64 {
	if total > 0 && copied > total {
		return total
	}
	return copied
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
// which shows the real sub-1% fraction instead of the bump.
func RowCopyDisplayPercent(pct int, rowsCopied int64) int {
	displayPercent := ClampPercent(pct)
	if displayPercent == 0 && rowsCopied > 0 {
		return 1
	}
	return displayPercent
}

// FormatRowCopyPercent renders row-copy progress as a percent string. Whole
// percents render as integers; a copy that has begun but not yet reached 1%
// shows two decimals computed from the row counts (e.g. "0.03%"), so early
// progress on a huge table reads as the small fraction it is instead of a
// rounded-up 1%. The fraction is clamped away from "0.00%" (copying has
// begun) and away from "1.00%" (the engine's whole-number percent still says
// 0). Without a row total to compute a fraction from, it falls back to "<1%".
func FormatRowCopyPercent(pct int, rowsCopied, rowsTotal int64) string {
	display := ClampPercent(pct)
	if display > 0 || rowsCopied <= 0 {
		return fmt.Sprintf("%d%%", display)
	}
	if rowsTotal <= 0 {
		return "<1%"
	}
	frac := float64(rowsCopied) / float64(rowsTotal) * 100
	frac = math.Min(math.Max(frac, 0.01), 0.99)
	return fmt.Sprintf("%.2f%%", frac)
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

// FormatHumanDuration formats a duration in a human-readable way.
// Examples: 500ms → "< 1s", 45s → "45s", 90s → "1m 30s", 2h → "2h"
func FormatHumanDuration(d time.Duration) string {
	if d < time.Second {
		return "< 1s"
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		mins := int(d.Minutes())
		secs := int(d.Seconds()) % 60
		if secs == 0 {
			return fmt.Sprintf("%dm", mins)
		}
		return fmt.Sprintf("%dm %ds", mins, secs)
	}
	totalHours := int(d.Hours())
	if totalHours < 24 {
		mins := int(d.Minutes()) % 60
		if mins == 0 {
			return fmt.Sprintf("%dh", totalHours)
		}
		return fmt.Sprintf("%dh %dm", totalHours, mins)
	}
	days := totalHours / 24
	remainHours := totalHours % 24
	if days < 7 {
		if remainHours == 0 {
			return fmt.Sprintf("%dd", days)
		}
		return fmt.Sprintf("%dd %dh", days, remainHours)
	}
	weeks := days / 7
	remainDays := days % 7
	if remainDays == 0 {
		return fmt.Sprintf("%dw", weeks)
	}
	return fmt.Sprintf("%dw %dd", weeks, remainDays)
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

// TableStatePriority returns a sort key for table display ordering.
// Lower values sort first: active/running on top, terminal on bottom.
// Uses Task states (not Apply states) since tables are per-table tasks.
// Used by both CLI (watch TUI) and PR comment rendering for consistent ordering.
func TableStatePriority(taskState string) int {
	switch taskState {
	case state.Task.Running, state.Task.CatchingUp, state.Task.Checksumming, state.Task.PostChecksum, state.Task.CuttingOver:
		return 0 // active — top
	case state.Task.WaitingForCutover, state.Task.Recovering, state.Task.FailedRetryable:
		return 1
	case state.Task.Pending:
		return 2
	case state.Task.Failed, state.Task.Stopped:
		return 3
	case state.Task.Completed, state.Task.Cancelled, state.Task.Reverted:
		return 4 // terminal — bottom
	default:
		return 2
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
