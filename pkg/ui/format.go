// Package ui provides shared formatting helpers for CLI and PR comment template rendering.
package ui

import (
	"fmt"
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

// FormatETA formats a duration in seconds as a human-readable string.
// Examples: 45 → "45s", 195 → "3m 15s", 3700 → "1h 1m"
func FormatETA(seconds int64) string {
	if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	}
	if seconds < 3600 {
		return fmt.Sprintf("%dm %ds", seconds/60, seconds%60)
	}
	return fmt.Sprintf("%dh %dm", seconds/3600, (seconds%3600)/60)
}

// ClampRows returns rows clamped to total for display purposes.
// Spirit can report rows copied > rows total when rows are inserted during copy.
func ClampRows(copied, total int64) int64 {
	if total > 0 && copied > total {
		return total
	}
	return copied
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
	case state.Task.Running, state.Task.CuttingOver:
		return 0 // active — top
	case state.Task.WaitingForCutover:
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

// CleanLintReason strips severity prefixes like "[ERROR] linter_name:" from
// Spirit's raw lint violation strings for cleaner display. Handles multiple
// violations joined by "; " by cleaning each segment individually.
func CleanLintReason(reason string) string {
	segments := strings.Split(reason, "; ")
	for i, seg := range segments {
		segments[i] = cleanSingleLintReason(seg)
	}
	return strings.Join(segments, "; ")
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
