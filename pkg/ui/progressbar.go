// Package ui provides shared UI rendering helpers used by both CLI and PR comment templates.
package ui

import "strings"

// Progress bar colors (emoji).
const (
	ColorBlue   = "🟦" // In progress (copying rows)
	ColorYellow = "🟨" // Waiting for cutover
	ColorGreen  = "🟩" // Complete
	ColorOrange = "🟧" // Stopped
	ColorRed    = "🟥" // Failed
	ColorEmpty  = "⬜" // Unfilled
)

const barWidth = 20

// ProgressBar creates a visual progress bar using the given color emoji.
func ProgressBar(percent int, color string) string {
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}
	filled := percent / 5
	if percent > 0 && filled == 0 {
		filled = 1
	}
	if filled > barWidth {
		filled = barWidth
	}
	empty := barWidth - filled
	return strings.Repeat(color, filled) + strings.Repeat(ColorEmpty, empty)
}

// ProgressBarRowCopy creates a blue progress bar for row copy in progress.
func ProgressBarRowCopy(percent int) string {
	return ProgressBar(percent, ColorBlue)
}

// ProgressBarComplete creates a full green progress bar (100% done).
func ProgressBarComplete() string {
	return ProgressBar(100, ColorGreen)
}

// ProgressBarWaitingCutover creates a full yellow progress bar for waiting for cutover.
func ProgressBarWaitingCutover() string {
	return ProgressBar(100, ColorYellow)
}

// ProgressBarStopped creates an orange progress bar for stopped mid-apply.
func ProgressBarStopped(percent int) string {
	return ProgressBar(percent, ColorOrange)
}

// ProgressBarFailed creates a red progress bar for failed applies.
func ProgressBarFailed(percent int) string {
	return ProgressBar(percent, ColorRed)
}
