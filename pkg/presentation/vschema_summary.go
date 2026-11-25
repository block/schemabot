package presentation

import "fmt"

// maxVSchemaSummaryLines bounds how many lines of a VSchema change's
// description a surface lists before folding the rest into a count, so a
// keyspace's first VSchema, which adds every table at once, stays readable.
const maxVSchemaSummaryLines = 20

// VSchemaSummaryLines returns the lines of a VSchema change's description a
// surface lists: every line when they fit, otherwise the first ones followed
// by a line counting the rest. The CLI and the PR comment both list these, so
// a long change is cut at the same place on each.
func VSchemaSummaryLines(summary []string) []string {
	if len(summary) <= maxVSchemaSummaryLines {
		return summary
	}
	shown := maxVSchemaSummaryLines - 1
	lines := append([]string(nil), summary[:shown]...)
	return append(lines, fmt.Sprintf("and %d more changes, shown in the diff", len(summary)-shown))
}
