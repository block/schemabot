package templates

import (
	"math"
	"strings"
)

// minimumFenceLength is the shortest backtick run CommonMark accepts as a
// code fence.
const minimumFenceLength = 3

// CommentChromeHeadroom reserves room under GitHub's comment size cap for
// markup added to the body after rendering (the support-channel footer) plus
// margin, so an assembled comment never lands exactly at the limit. Config
// validation caps the support-channel name and URL lengths so the rendered
// footer always fits inside this reservation.
const CommentChromeHeadroom = 1024

// commentBodyLimit is the most a rendered comment body may hold: GitHub's cap
// less the headroom kept for what the poster appends.
const commentBodyLimit = GitHubIssueCommentMaxChars - CommentChromeHeadroom

// applyCommentAppendReserve is the room an apply comment leaves for the
// sections the poster appends after rendering: the rejected-command notice on
// any apply comment and, on a failed apply's summary, the recent-logs fold.
// The logs are what an operator triaging from the PR reads first, so the DDL
// yields this much to them rather than filling the comment on its own.
const applyCommentAppendReserve = 16384

// commentFitPasses bounds the render passes a comment may take to fit under
// its limit. The first pass offers the DDL every byte the limit allows; when
// the rest of the comment turns out to need some of that room, the second
// pass cuts the DDL budget by the overshoot, and the marker allowance charged
// on that pass makes a third one unnecessary unless a comment renders more
// DDL blocks than it announced.
const commentFitPasses = 3

// ddlTruncatedMarker follows a DDL block that was cut to fit the budget, so an
// operator knows the statement shown is incomplete and where to look instead.
const ddlTruncatedMarker = "_DDL truncated to fit GitHub's comment size limit; the desired schema is in this PR's schema files._\n"

// fenceOverhead is the byte count of a block's fixed text beyond the two fence
// runs and the content: the info string, the newline after each fence, and the
// newline that terminates content when it lacks one.
func fenceOverhead(info string) int {
	return len(info) + len("\n") + len("\n") + len("\n")
}

// ddlBlockBudget shares one comment's DDL budget across the blocks the comment
// renders. Each block may use an equal share of what remains, so a short
// statement leaves its unused share to the blocks after it, while no sequence
// of long statements can carry the comment past the budget. A block rendered
// beyond the count the budget was opened with draws on whatever remains.
type ddlBlockBudget struct {
	remaining  int
	blocksLeft int
}

// newDDLBlockBudget opens the per-comment DDL budget for a comment about to
// render the given number of DDL blocks into at most limit bytes of DDL.
func newDDLBlockBudget(blocks, limit int) *ddlBlockBudget {
	return &ddlBlockBudget{remaining: limit, blocksLeft: blocks}
}

// newUnboundedDDLBudget opens a budget no DDL can exhaust, for a render that is
// compared rather than posted. Two sections cut to fit a comment share a prefix
// wherever their statements do, so comparing the cut renders would call plans
// identical that differ only past the cut; comparing the whole renders cannot.
func newUnboundedDDLBudget(blocks int) *ddlBlockBudget {
	return newDDLBlockBudget(blocks, math.MaxInt)
}

// renderWithinCommentLimit renders a comment so its DDL takes every byte the
// rest of the comment leaves under GitHub's size cap, less reserve bytes kept
// for sections the poster appends afterwards. The first pass offers the DDL
// the whole limit; a comment whose other sections fit alongside is done. When
// the body overshoots, the DDL budget is cut by the overshoot plus a
// truncation marker for every block not yet carrying one, so the next pass
// fits. The render callback must produce the same non-DDL text on every pass:
// only the budget it is handed changes.
func renderWithinCommentLimit(blocks, reserve int, render func(*ddlBlockBudget) string) string {
	limit := commentBodyLimit - reserve
	ddlLimit := limit
	var body string
	for range commentFitPasses {
		budget := newDDLBlockBudget(blocks, ddlLimit)
		body = render(budget)
		over := len(body) - limit
		if over <= 0 {
			return body
		}
		spent := ddlLimit - budget.remaining
		unmarked := max(blocks-strings.Count(body, ddlTruncatedMarker), 0)
		ddlLimit = max(spent-over-unmarked*len(ddlTruncatedMarker), 0)
	}
	return body
}

// take returns the share the next block may render into; the caller reports
// what the block actually used through spend.
func (b *ddlBlockBudget) take() int {
	if b.blocksLeft <= 1 {
		b.blocksLeft = 0
		return b.remaining
	}
	share := b.remaining / b.blocksLeft
	b.blocksLeft--
	return share
}

// spend charges a rendered block's size against what remains.
func (b *ddlBlockBudget) spend(size int) {
	b.remaining = max(b.remaining-size, 0)
}

// writeSQLFencedBlock writes content as a sql code block whose fence is longer
// than any backtick run inside the content, so a DDL statement that carries
// its own backtick run cannot close the block early and inject markdown into
// the surrounding comment. The closing fence always matches the opening one.
// Content that would render past the block's share of budget is cut and the
// block is followed by a visible marker.
func writeSQLFencedBlock(sb *strings.Builder, content string, budget *ddlBlockBudget) {
	writeSQLFencedBlocks(sb, []string{content}, budget)
}

// sqlBlockSeparator is the blank line between consecutive sql code blocks that
// share one section, so each block renders as its own fence.
const sqlBlockSeparator = "\n"

// writeSQLFencedBlocks writes each content as its own sql code block, the
// blocks together drawing one share of the budget: the section is charged for
// its fences and separators like any other DDL it renders, so a section split
// into blocks shows a little less DDL than it would as one block and never
// more than its share. Blocks render whole, in order, while the share holds
// them; the first block the share cannot hold is cut to the room left and
// followed by the truncation marker, and the blocks after it are left out —
// the marker says where the rest lives. When the share runs out at a block
// boundary, so that not one byte of the next block would show, the marker
// follows the last whole block directly rather than an empty fence.
func writeSQLFencedBlocks(sb *strings.Builder, contents []string, budget *ddlBlockBudget) {
	share := budget.take()
	spent := 0
	for i, content := range contents {
		room := share - spent
		if i > 0 {
			room -= len(sqlBlockSeparator)
		}
		content, truncated := fitSQLBlock(content, room)
		if i > 0 {
			if truncated && content == "" {
				sb.WriteString(ddlTruncatedMarker)
				break
			}
			sb.WriteString(sqlBlockSeparator)
			spent += len(sqlBlockSeparator)
		}
		spent += sqlBlockSize(content)
		writeFencedBlock(sb, "sql", content)
		if truncated {
			sb.WriteString(ddlTruncatedMarker)
			break
		}
	}
	budget.spend(spent)
}

// writeFencedBlock writes content inside a code fence carrying info as its
// info string, sized so no backtick run inside content can close it early.
func writeFencedBlock(sb *strings.Builder, info, content string) {
	fence := strings.Repeat("`", fenceLength(content))
	sb.WriteString(fence)
	sb.WriteString(info)
	sb.WriteString("\n")
	if content != "" {
		sb.WriteString(content)
		if !strings.HasSuffix(content, "\n") {
			sb.WriteString("\n")
		}
	}
	sb.WriteString(fence)
	sb.WriteString("\n")
}

// fenceLength returns the fence length that no backtick run inside content
// can match.
func fenceLength(content string) int {
	return max(maxBacktickRun(content)+1, minimumFenceLength)
}

// sqlBlockSize is the byte count of the block writeSQLFencedBlock renders for
// content: the content, both fence runs, and the fixed text around them.
func sqlBlockSize(content string) int {
	return len(content) + 2*fenceLength(content) + fenceOverhead("sql")
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
// contains a backtick so it never touches the delimiter. An empty name renders
// as a span holding one space rather than an unbalanced pair of backticks.
func inlineCode(name string) string {
	flat := flattenIdentifier(name)
	if flat == "" {
		// An empty span has no delimiters to balance, so a lone pair of
		// backticks would read as an unmatched run; a single space keeps the
		// span visible and closed.
		flat = " "
	}
	delimiter := strings.Repeat("`", maxBacktickRun(flat)+1)
	if strings.ContainsRune(flat, '`') {
		flat = " " + flat + " "
	}
	return delimiter + flat + delimiter
}

// flattenIdentifier keeps an identifier the PR author chose on one line:
// control and format characters are removed and whitespace runs, line breaks
// included, collapse to one space.
func flattenIdentifier(name string) string {
	return strings.Join(strings.Fields(stripControlText(name)), " ")
}

// inlineCodeCell is inlineCode for a table cell: the cell separator is
// escaped, which GFM honours inside a code span, so the name cannot split the
// row and still reads as written.
func inlineCodeCell(name string) string {
	return strings.ReplaceAll(inlineCode(name), "|", `\|`)
}
