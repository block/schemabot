package templates

import (
	"fmt"
	"strings"
)

// BoxRow represents a labeled row in a key-value box.
type BoxRow struct {
	Label string
	Value string
}

// WriteBox draws a bordered key-value box to stdout.
// Each row shows "Label:  Value" with auto-sized column widths.
// The optional colorFn, if provided, is applied to the value of the row
// whose label matches colorLabel.
func WriteBox(rows []BoxRow, colorLabel string, colorFn func(string) string) {
	maxLabelLen := 0
	maxValueLen := 0
	for _, r := range rows {
		if len(r.Label) > maxLabelLen {
			maxLabelLen = len(r.Label)
		}
		if len(r.Value) > maxValueLen {
			maxValueLen = len(r.Value)
		}
	}
	innerWidth := 2 + (maxLabelLen + 1) + 2 + maxValueLen + 2

	fmt.Printf("┌%s┐\n", strings.Repeat("─", innerWidth))
	for _, r := range rows {
		padded := fmt.Sprintf("%-*s", maxValueLen, r.Value)
		if r.Label == colorLabel && colorFn != nil {
			padded = colorFn(padded)
		}
		fmt.Printf("│  %-*s  %s  │\n", maxLabelLen+1, r.Label+":", padded)
	}
	fmt.Printf("└%s┘\n", strings.Repeat("─", innerWidth))
}
