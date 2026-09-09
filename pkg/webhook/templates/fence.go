package templates

import "strings"

// minimumFenceLength is the shortest backtick run CommonMark accepts as a
// code fence.
const minimumFenceLength = 3

// maxCommentDDLLen bounds one rendered DDL block — the statement text and both
// fence lines together — so a single statement cannot carry a comment past
// GitHub's size limit. The fence grows with the longest backtick run in the
// statement, so text made mostly of backticks renders at three times its own
// length; budgeting the whole block rather than the statement keeps that
// growth inside the limit.
const maxCommentDDLLen = 32768

// ddlTruncatedMarker follows a DDL block that was cut to fit the budget, so an
// operator knows the statement shown is incomplete and where the rest lives.
const ddlTruncatedMarker = "_DDL truncated to fit GitHub's comment size limit; the schema files in this PR carry the full statement._\n"

// sqlFenceOverhead is the byte count of a block's fixed text beyond the two
// fence runs and the content: the "sql" info string, the newline after each
// fence, and the newline that terminates content when it lacks one.
const sqlFenceOverhead = len("sql\n") + len("\n") + len("\n")

// writeSQLFencedBlock writes content as a sql code block whose fence is longer
// than any backtick run inside the content, so a DDL statement that carries
// its own backtick run cannot close the block early and inject markdown into
// the surrounding comment. The closing fence always matches the opening one.
// Content that would render past maxCommentDDLLen is cut and the block is
// followed by a visible marker.
func writeSQLFencedBlock(sb *strings.Builder, content string) {
	content, truncated := fitSQLBlock(content, maxCommentDDLLen)
	fence := strings.Repeat("`", sqlFenceLength(content))

	sb.WriteString(fence)
	sb.WriteString("sql\n")
	if content != "" {
		sb.WriteString(content)
		if !strings.HasSuffix(content, "\n") {
			sb.WriteString("\n")
		}
	}
	sb.WriteString(fence)
	sb.WriteString("\n")
	if truncated {
		sb.WriteString(ddlTruncatedMarker)
	}
}

// sqlFenceLength returns the fence length that no backtick run inside content
// can match.
func sqlFenceLength(content string) int {
	return max(maxBacktickRun(content)+1, minimumFenceLength)
}

// sqlBlockSize is the byte count of the block writeSQLFencedBlock renders for
// content: the content, both fence runs, and the fixed text around them.
func sqlBlockSize(content string) int {
	return len(content) + 2*sqlFenceLength(content) + sqlFenceOverhead
}

// fitSQLBlock returns the longest prefix of content whose rendered block fits
// within budget bytes, reporting whether anything was cut. A longer prefix
// never renders smaller — it has more bytes and its longest backtick run can
// only grow — so the fitting prefix length is found by binary search.
func fitSQLBlock(content string, budget int) (string, bool) {
	if sqlBlockSize(content) <= budget {
		return content, false
	}
	fits, tooLong := 0, len(content)
	for fits < tooLong {
		mid := (fits + tooLong + 1) / 2
		if sqlBlockSize(truncateToBytes(content, mid)) <= budget {
			fits = mid
		} else {
			tooLong = mid - 1
		}
	}
	return truncateToBytes(content, fits), true
}

// maxBacktickRun returns the length of the longest run of consecutive
// backticks in content, or zero when it has none.
func maxBacktickRun(content string) int {
	longest := 0
	current := 0
	for _, char := range content {
		if char == '`' {
			current++
			longest = max(longest, current)
		} else {
			current = 0
		}
	}
	return longest
}

// inlineCode renders an identifier the PR author chose — a table, namespace,
// keyspace, or check name — as a markdown code span the identifier cannot
// break out of. Control and format characters are removed and whitespace runs
// collapse to one space, so the name stays on one line and cannot start a
// fence or a heading of its own; the delimiter is a backtick run longer than
// any run inside the name, padded with a space on each side when the name
// contains a backtick so it never touches the delimiter.
func inlineCode(name string) string {
	flat := strings.Join(strings.Fields(stripControlText(name)), " ")
	delimiter := strings.Repeat("`", maxBacktickRun(flat)+1)
	if strings.ContainsRune(flat, '`') {
		flat = " " + flat + " "
	}
	return delimiter + flat + delimiter
}

// inlineCodeCell is inlineCode for a table cell: the cell separator is
// escaped, which GFM honours inside a code span, so the name cannot split the
// row and still reads as written.
func inlineCodeCell(name string) string {
	return strings.ReplaceAll(inlineCode(name), "|", `\|`)
}
