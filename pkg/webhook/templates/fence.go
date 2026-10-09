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
// pass cuts the DDL budget by the overshoot and by the marker of every block
// the first pass left uncut, so the second pass fits and the third is only a
// bound.
const commentFitPasses = 3

// ddlTruncatedMarker follows a DDL block that was cut to fit the budget, so an
// operator knows the statement shown is incomplete and where to look instead.
// It is the marker for DDL whose stored plan is not known; DDL from a stored
// plan names the command that prints that plan instead (planPointer).
const ddlTruncatedMarker = "_DDL truncated to fit GitHub's comment size limit; the desired schema is in this PR's schema files._\n"

// planPointer follows a DDL block that was cut to fit the budget when the DDL
// comes from a stored plan. The schema files hold the desired schema, not the
// statements the plan would run, so a reader who needs the statements the
// comment could not show is pointed at the stored plan ref that holds them,
// described as plan ("plan", "staging plan for this target"), with notes
// saying who else runs the same DDL. The command is labelled as a CLI command
// because every other schemabot command a comment names is a PR-comment
// command, and this one is not.
func planPointer(plan string, ref storedPlanRef, notes []string) string {
	marker := "_DDL truncated to fit GitHub's comment size limit; the full " + plan + " is available from the CLI with " + ref.listCommand()
	if len(notes) > 0 {
		marker += " (" + strings.Join(notes, "; ") + ")"
	}
	return marker + "._\n"
}

// sameEnvironmentDDLNote says which environments a shared section's plan also
// stands for.
func sameEnvironmentDDLNote(environments []string) string {
	matching := make([]string, 0, len(environments))
	for _, env := range environments {
		matching = append(matching, flattenIdentifier(env))
	}
	return joinWithAnd(matching) + " " + runVerb(len(matching)) + " the same DDL"
}

// joinWithAnd joins items as prose: "a", "a and b", "a, b and c".
func joinWithAnd(items []string) string {
	if len(items) <= 1 {
		return strings.Join(items, "")
	}
	return strings.Join(items[:len(items)-1], ", ") + " and " + items[len(items)-1]
}

// storedPlanRef names a stored plan the way the CLI reaches it: the tool name
// the deployment's operators run the CLI as, the environment the plan was made
// for, and the plan's identifier. An empty id means the DDL has no stored plan.
type storedPlanRef struct {
	cliName     string
	environment string
	id          string
}

// listCommand is the CLI command that prints the stored plan in full, scoped
// to the plan's environment so a wrapper that routes by environment reaches
// the server that stored it.
func (p storedPlanRef) listCommand() string {
	return inlineCode(cliCommand(p.cliName, "list-plans "+environmentFlag(p.environment)+" "+p.id))
}

// runVerb agrees "run" with the number of environments it follows.
func runVerb(subjects int) string {
	if subjects == 1 {
		return "runs"
	}
	return "run"
}

// fenceOverhead is the byte count of a block's fixed text beyond the two fence
// runs and the content: the info string, the newline after each fence, and the
// newline writeFencedBlock adds to content that lacks one. It is exact rather
// than an upper bound, because the fit loop cuts the DDL by the overshoot
// measured against what the budget was charged, and a block charged more than
// it wrote would let the next pass overshoot by the difference.
func fenceOverhead(info, content string) int {
	overhead := len(info) + len("\n") + len("\n")
	if content != "" && !strings.HasSuffix(content, "\n") {
		overhead += len("\n")
	}
	return overhead
}

// ddlBlockBudget shares one comment's DDL budget across the blocks the comment
// renders. Each block may use an equal share of what remains, so a short
// statement leaves its unused share to the blocks after it, while no sequence
// of long statements can carry the comment past the budget. A block rendered
// beyond the count the budget was opened with draws on whatever remains.
type ddlBlockBudget struct {
	remaining  int
	blocksLeft int

	// marker is what follows a block cut to fit, set by the section rendering
	// the DDL through pointAt. Empty means ddlTruncatedMarker.
	marker string
	// uncutMarkerBytes is the length of the marker each block taken so far
	// would carry, less the markers the render wrote: the room a later pass
	// must reserve so every block it cuts has room for its own section's
	// marker, and no more than that.
	uncutMarkerBytes int

	// sharedBy lists the environments a section rendered once on behalf of
	// several is shared by, the first being the one whose plan it renders, so
	// a pointer marker in it says whose stored plan it names. Empty outside
	// such a section.
	sharedBy []string

	// scope qualifies the plan a pointer marker names when the DDL is one
	// target group's plan rather than the whole plan, so the reader knows
	// the stored plan holds only those targets' statements. Empty otherwise.
	scope string
	// groupNote follows the command in a pointer marker under a target group
	// of several members, whose stored plan is its first member's, saying the
	// rest of the group runs the same DDL. Empty otherwise.
	groupNote string

	// countsMembersOnly makes a table's member listing keep its heading, which
	// counts every member by state, and leave out the line per member. It is
	// set on a pass after the rest of the comment alone ran past the limit:
	// the per-member lines are the part of it that grows with the apply.
	countsMembersOnly bool
}

// listsMembers reports whether a table's member listing names its members
// one per line, or only counts them.
func (b *ddlBlockBudget) listsMembers() bool {
	return b == nil || !b.countsMembersOnly
}

// newDDLBlockBudget opens the per-comment DDL budget for a comment about to
// render the given number of DDL blocks into at most limit bytes of DDL.
func newDDLBlockBudget(blocks, limit int) *ddlBlockBudget {
	return &ddlBlockBudget{remaining: limit, blocksLeft: blocks}
}

// pointAt makes a block cut from here on name the stored plan, until the
// returned restore runs. A plan with no id means the DDL has no stored plan to
// point at, and a cut block keeps ddlTruncatedMarker. The marker is built here,
// and a block is charged the length of the same text when it is taken, so a
// cli name or environment of any length is reserved exactly.
func (b *ddlBlockBudget) pointAt(plan storedPlanRef) (restore func()) {
	previous := b.marker
	b.marker = ""
	if plan.id != "" {
		b.marker = b.pointerMarker(plan)
	}
	return func() { b.marker = previous }
}

// pointerMarker is the marker naming the stored plan, saying whose plan it is
// when the section is shared by several environments or renders a target group
// of several members.
func (b *ddlBlockBudget) pointerMarker(plan storedPlanRef) string {
	description := "plan"
	var notes []string
	if b.groupNote != "" {
		notes = append(notes, b.groupNote)
	}
	if len(b.sharedBy) > 1 {
		description = flattenIdentifier(b.sharedBy[0]) + " plan"
		notes = append(notes, sameEnvironmentDDLNote(b.sharedBy[1:]))
	}
	return planPointer(description+b.scope, plan, notes)
}

// forTargetGroup marks the DDL rendered from here on as the plan of a target
// group with the given members, naming a stored plan that covers only the
// group's targets, until the returned restore runs.
func (b *ddlBlockBudget) forTargetGroup(members []string) (restore func()) {
	return b.scopePlan(targetGroupPlanScope(members))
}

// forNamedTargets marks the DDL rendered from here on as one table's DDL on the
// targets named above it, naming the first one's stored plan, until the
// returned restore runs.
func (b *ddlBlockBudget) forNamedTargets(members []string) (restore func()) {
	scope, note := targetGroupPlanScope(members)
	if note != "" {
		note = "every target named above runs the same DDL"
	}
	return b.scopePlan(scope, note)
}

// forSoleTargetGroup marks the DDL rendered from here on as the plan of the
// only group of targets a deployment's reporting targets form, rendered with
// no heading naming its members, until the returned restore runs. unreported
// counts the deployment's targets that have not reported, which are not known
// to run the group's DDL.
func (b *ddlBlockBudget) forSoleTargetGroup(members []string, unreported int) (restore func()) {
	return b.scopePlan(soleTargetGroupPlanScope(members, unreported))
}

// scopePlan sets the scope and group note a pointer marker carries until the
// returned restore runs.
func (b *ddlBlockBudget) scopePlan(scope, note string) (restore func()) {
	previousScope, previousNote := b.scope, b.groupNote
	b.scope, b.groupNote = scope, note
	return func() { b.scope, b.groupNote = previousScope, previousNote }
}

// targetGroupPlanScope is how a pointer marker under a target group's DDL
// qualifies the plan it names. A group's stored plan is its first member's, so
// under a group of several the marker names that member and says the rest of
// the group runs the same DDL, rather than calling one member's plan the
// group's.
func targetGroupPlanScope(members []string) (scope, note string) {
	if len(members) <= 1 {
		return " for this target", ""
	}
	return " for " + inlineCode(members[0]), "every target in this group runs the same DDL"
}

// soleTargetGroupPlanScope is how a pointer marker qualifies the plan it names
// under a deployment whose reporting targets all run one change, where no
// heading names the group. The marker names the target whose stored plan it
// is, and says the others run the same DDL only of the targets known to: a
// target that has not reported is not known to run it.
func soleTargetGroupPlanScope(members []string, unreported int) (scope, note string) {
	if len(members) == 0 {
		return "", ""
	}
	scope = " for " + inlineCode(members[0])
	switch {
	case len(members) == 1:
		return scope, ""
	case unreported > 0:
		return scope, "every target that has reported runs the same DDL"
	default:
		return scope, "every target runs the same DDL"
	}
}

// shareAcross marks the DDL rendered from here on as one section shared by
// environments, rendered from the first one's plan, until the returned restore
// runs.
func (b *ddlBlockBudget) shareAcross(environments []string) (restore func()) {
	previous := b.sharedBy
	b.sharedBy = environments
	return func() { b.sharedBy = previous }
}

// truncationMarker is the marker a block cut now is followed by.
func (b *ddlBlockBudget) truncationMarker() string {
	if b.marker == "" {
		return ddlTruncatedMarker
	}
	return b.marker
}

// writeTruncationMarker writes the marker for a block cut to fit, releasing the
// room take reserved for it.
func (b *ddlBlockBudget) writeTruncationMarker(sb *strings.Builder) {
	marker := b.truncationMarker()
	sb.WriteString(marker)
	b.uncutMarkerBytes -= len(marker)
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
// the body overshoots, the DDL budget is cut by the overshoot plus, for every
// block not yet carrying a truncation marker, the marker its own section
// writes, so the next pass fits. When even an empty DDL budget would leave the
// body over the limit, the next pass instead lists each table's members by
// their counts alone and offers the DDL the whole limit again, and the pass
// after it cuts the DDL to whatever room that leaves. The render callback must
// produce the same non-DDL text and take the same blocks in the same sections
// on every pass: only the budget it is handed changes.
func renderWithinCommentLimit(blocks, reserve int, render func(*ddlBlockBudget) string) string {
	limit := commentBodyLimit - reserve
	ddlLimit := limit
	countsMembersOnly := false
	var body string
	for range commentFitPasses {
		budget := newDDLBlockBudget(blocks, ddlLimit)
		budget.countsMembersOnly = countsMembersOnly
		body = render(budget)
		over := len(body) - limit
		if over <= 0 {
			return body
		}
		spent := ddlLimit - budget.remaining
		next := spent - over - budget.uncutMarkerBytes
		if next < 0 && !countsMembersOnly {
			countsMembersOnly, ddlLimit = true, limit
			continue
		}
		ddlLimit = max(next, 0)
	}
	return body
}

// take returns the share the next block may render into; the caller reports
// what the block actually used through spend. The block's own section's
// marker is reserved until the block writes it, so a later pass that cuts
// the block has room for exactly that marker.
func (b *ddlBlockBudget) take() int {
	b.uncutMarkerBytes += len(b.truncationMarker())
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
// block is followed by a visible marker naming where the rest lives.
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
// follows the last whole block directly rather than an empty fence; a share
// with no room for one byte of the first block writes the marker alone, so
// the section never renders a fence its share cannot hold.
func writeSQLFencedBlocks(sb *strings.Builder, contents []string, budget *ddlBlockBudget) {
	share := budget.take()
	spent := 0
	for i, content := range contents {
		room := share - spent
		if i > 0 {
			room -= len(sqlBlockSeparator)
		}
		content, truncated := fitSQLBlock(content, room)
		if truncated && content == "" {
			budget.writeTruncationMarker(sb)
			break
		}
		if i > 0 {
			sb.WriteString(sqlBlockSeparator)
			spent += len(sqlBlockSeparator)
		}
		spent += sqlBlockSize(content)
		writeFencedBlock(sb, "sql", content)
		if truncated {
			budget.writeTruncationMarker(sb)
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
	return len(content) + 2*fenceLength(content) + fenceOverhead("sql", content)
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
