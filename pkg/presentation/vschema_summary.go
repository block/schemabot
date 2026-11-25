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
	shown, omitted := VSchemaSummaryCut(summary)
	if omitted == 0 {
		return shown
	}
	return append(append([]string(nil), shown...), VSchemaMoreChangesLine(omitted))
}

// VSchemaSummaryCut returns the lines of a VSchema change's description a
// surface lists and how many lines it folds into a count after them. It is
// VSchemaSummaryLines without the count line, for a surface that may have to
// cut the list shorter still and count what it leaves out itself.
func VSchemaSummaryCut(summary []string) (shown []string, omitted int) {
	if len(summary) <= maxVSchemaSummaryLines {
		return summary, 0
	}
	kept := maxVSchemaSummaryLines - 1
	return summary[:kept], len(summary) - kept
}

// VSchemaMoreChangesLine is the line that counts the n description lines a
// surface leaves out. It points at the VSchema file's diff rather than the
// diff the surface renders, since a long description usually comes with a
// diff the surface has had to cut too.
func VSchemaMoreChangesLine(n int) string {
	return fmt.Sprintf("and %d more changes; the VSchema file's diff has them all", n)
}
