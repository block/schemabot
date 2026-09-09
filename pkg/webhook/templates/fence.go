package templates

import "strings"

// minimumFenceLength is the shortest backtick run CommonMark accepts as a
// code fence.
const minimumFenceLength = 3

// writeSQLFencedBlock writes content as a sql code block whose fence is longer
// than any backtick run inside the content, so a DDL statement that carries
// its own backtick run cannot close the block early and inject markdown into
// the surrounding comment. The closing fence always matches the opening one.
func writeSQLFencedBlock(sb *strings.Builder, content string) {
	fence := strings.Repeat("`", max(maxBacktickRun(content)+1, minimumFenceLength))

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
