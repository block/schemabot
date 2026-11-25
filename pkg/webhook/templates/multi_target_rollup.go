package templates

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// failedTargetRowLimit bounds the failed-target table so a deployment whose
// every target failed still fits one comment; the <summary> counts carry the total.
const failedTargetRowLimit = 20

// targetTableLine is one line of a target rollup: a table and the DDL that
// changes it, its progress on each target that runs that DDL, and those
// targets as member indexes and as names.
type targetTableLine struct {
	table   TableProgressData
	cells   []TableProgressData
	members []int
	targets []string
	// listed and listedTargets are every target the line speaks for, in the
	// rollout's order: the targets that report the table, and the targets
	// with no table progress yet when the line speaks for them too.
	listed        []TableProgressData
	listedTargets []string
	rank          int
}

// writeTargetRollup writes a multi-target deployment's body the way the
// sharded apply comment writes a keyspace: one line per table and DDL across
// the targets, each DDL once, the targets named above it when it runs on only
// some of them, and a row per failed target. Its size grows with distinct
// changes and failures, not with the number of targets. Targets that already
// had the change ran nothing, so the rollup neither names nor counts them.
func writeTargetRollup(sb *strings.Builder, data MultiDeploymentApplyData, g presentation.Group, budget *ddlBlockBudget) {
	lines := targetTableLines(data, g)
	silent := unreportedTargets(data, g)
	// A target that already had the change ran nothing and never reports
	// details, so a deployment where every target had it is not waiting on any.
	if len(lines) == 0 && silent > 0 {
		sb.WriteString("_No details available yet._\n")
	}
	// A target that has not reported is not known to run any one change, so
	// when the reporting targets diverge each line counts only its own
	// targets and the silent ones are counted once, for the deployment.
	lineSilent := silent
	if targetsDiverge(data, g) {
		lineSilent = 0
	}
	for li := range lines {
		lines[li].listed, lines[li].listedTargets = lineTargets(data, g, lines[li], lineSilent > 0)
	}
	changing := changingMembers(data.Model, g)
	settled := data.Model.RolloutSettled(g)
	resuming := state.IsState(data.Model.State, state.Apply.Resuming)
	rankTargetTableLines(lines)
	for _, line := range lines {
		first := memberDetail(data.Details, line.members[0])
		dialect := dialectForEngine(first.Engine, data.ApplyID)
		// A line that leaves out some changing targets names the ones it runs
		// on above its DDL, so the DDL never reads as running everywhere.
		subset := len(line.targets)+lineSilent < changing
		// The line's DDL is its first target's, so a cut block names that
		// target's stored plan and says which other targets run the same.
		restoreScope := planScopeForLine(budget, line.targets, subset, silent)
		restorePlan := budget.pointAt(first.storedPlan())
		writeDDL := func() { writeTargetLineDDL(sb, dialect, line, subset, changing, budget) }
		if coverage, ok := lineAwaitsResume(line, resuming); ok {
			// The stopped progress is from before the stop, and whether the
			// copy continues from its checkpoint is not known until a target
			// reports it, so the line says the start is under way, as the
			// single-deployment comment does (renderResumingTable).
			fmt.Fprintf(sb, "**%s**: \U0001f504 Resuming…%s\n", inlineCode(line.table.TableName), coverage)
			writeDDL()
		} else {
			strip := targetStrip(line.listed, line.listedTargets)
			writeTargetTableLine(sb, line.table.TableName, line.listed, strip, settled, lineRunsIndexBuild(data, line), budget, writeDDL)
		}
		sb.WriteString("\n")
		restorePlan()
		restoreScope()
	}
	writeFailedTargets(sb, data.Model, g)
}

// anyTargetThrottled reports whether the engine's throttler is holding back
// any target's run of the table, so the rolled-up headline says so, as the
// CLI's does.
func anyTargetThrottled(cells []TableProgressData) bool {
	for _, c := range cells {
		if c.Throttled {
			return true
		}
	}
	return false
}

// lineRunsIndexBuild reports whether a target running the line's table is in
// a concurrent index build, read from that target's own operation, since an
// operation's build work says nothing about its siblings.
func lineRunsIndexBuild(data MultiDeploymentApplyData, line targetTableLine) bool {
	for k, i := range line.members {
		detail := memberDetail(data.Details, i)
		if detail != nil && detail.BuildWork.IsConcurrentIndexBuild() && state.IsState(line.cells[k].Status, state.Task.Running) {
			return true
		}
	}
	return false
}

// lineAwaitsResume reports whether a table line of a resuming apply is one the
// start has yet to pick back up on any of its targets
// (presentation.AwaitingResume), and the coverage its line keeps for the
// targets that finished or are still queued.
func lineAwaitsResume(line targetTableLine, resuming bool) (string, bool) {
	if !resuming {
		return "", false
	}
	statuses := make([]string, len(line.listed))
	for i, c := range line.listed {
		statuses[i] = c.Status
	}
	return presentation.AwaitingResume(statuses)
}

// writeTargetLineDDL writes a table line's DDL under its headline, headed by
// the targets that run it when they are only some of the deployment's.
func writeTargetLineDDL(sb *strings.Builder, dialect schema.Dialect, line targetTableLine, subset bool, changing int, budget *ddlBlockBudget) {
	if line.table.DDL == "" {
		return
	}
	if !subset {
		writeDDLLine(sb, dialect, line.table.DDL, budget)
		return
	}
	if len(line.targets) <= shardNamesInlineLimit {
		fmt.Fprintf(sb, "\n**%s**\n", planGroupList(targetNoun, line.targets, changing))
	} else {
		// A wide subset's names collapse under its count rather than drop, so
		// the heading still says which targets run this DDL.
		fmt.Fprintf(sb, "\n<details>\n<summary><b>%s</b></summary>\n\n%s\n\n</details>\n\n",
			presentation.CoveragePhrase(targetNoun, len(line.targets), changing), strings.Join(inlineCodeList(line.targets), ", "))
	}
	writeSQLFencedBlock(sb, ddl.FormatDDLForDialect(dialect, line.table.DDL), budget)
}

// planScopeForLine scopes the pointer a cut block on one table line carries.
// When the line names its targets the marker speaks for those; otherwise it
// names its plan's target and speaks only for the targets that have reported.
func planScopeForLine(budget *ddlBlockBudget, targets []string, subset bool, unreported int) (restore func()) {
	if subset {
		return budget.forNamedTargets(targets)
	}
	return budget.forSoleTargetGroup(targets, unreported)
}

// unreportedTargets counts the targets with work that have no table progress
// to show yet, so the table lines do not read as covering the whole
// deployment. A target that already had the change ran nothing, so it has
// nothing to report and is not counted.
func unreportedTargets(data MultiDeploymentApplyData, g presentation.Group) int {
	n := 0
	for _, i := range g.Members {
		if targetUnreported(data, i) {
			n++
		}
	}
	return n
}

// targetUnreported reports whether member i has work but no table progress to
// show yet. A target that already had the change ran nothing, so it has
// nothing to report.
func targetUnreported(data MultiDeploymentApplyData, i int) bool {
	if data.Model.Deployments[i].AlreadyApplied() {
		return false
	}
	detail := memberDetail(data.Details, i)
	return detail == nil || len(detail.Tables) == 0
}

// targetStrip is a table line's targets, each with its progress on the table,
// for the per-target strip under the line.
func targetStrip(cells []TableProgressData, targets []string) []ShardProgressData {
	parts := targetParts(cells)
	strip := make([]ShardProgressData, len(cells))
	for i, c := range cells {
		strip[i] = ShardProgressData{
			Shard: targets[i], Status: c.Status, PercentComplete: c.PercentComplete, RowsCopied: c.RowsCopied, RowsTotal: c.RowsTotal,
			ChecksumRowsChecked: c.ChecksumRowsChecked, ChecksumRowsTotal: c.ChecksumRowsTotal,
			PlannedRows: parts[i].PlannedRows,
		}
	}
	return strip
}

// lineTargets is every target a table line speaks for, in the rollout's
// order, each with its progress on the table. With includeUnreported, a target
// that has reported no table progress is among them at where its operation
// stands (presentation.UnreportedTargetStatus): queued while it is still to
// run, otherwise its own halt or outcome, and no progress on the table.
func lineTargets(data MultiDeploymentApplyData, g presentation.Group, line targetTableLine, includeUnreported bool) ([]TableProgressData, []string) {
	cells := make([]TableProgressData, 0, len(g.Members))
	targets := make([]string, 0, len(g.Members))
	for _, i := range g.Members {
		if k := slices.Index(line.members, i); k >= 0 {
			cells = append(cells, line.cells[k])
			targets = append(targets, line.targets[k])
			continue
		}
		if includeUnreported && targetUnreported(data, i) {
			status := presentation.UnreportedTargetStatus(data.Model.Deployments[i].State)
			cells = append(cells, TableProgressData{Status: presentation.RolloutTableStatus(data.Model.State, status)})
			targets = append(targets, memberTarget(data.Model.Deployments[i]))
		}
	}
	return cells, targets
}

// targetTableLines is a deployment's table lines, one per table and DDL across
// the targets that report table detail, each with its progress on every
// target that runs it. Lines come in plan order: the first reporting target's
// tables, then any table only a later target runs. A table changed by two
// statements, or by different DDL on different targets, gets a line per
// statement. A target with no table detail yet runs no line: missing detail
// is not a different change.
func targetTableLines(data MultiDeploymentApplyData, g presentation.Group) []targetTableLine {
	var lines []targetTableLine
	byKey := make(map[string]int)
	for _, i := range g.Members {
		detail := memberDetail(data.Details, i)
		if detail == nil {
			continue
		}
		for _, t := range detail.Tables {
			t.Status = presentation.RolloutTableStatus(data.Model.State, t.Status)
			key := tableChangeKey(t)
			li, seen := byKey[key]
			if !seen {
				li = len(lines)
				byKey[key] = li
				lines = append(lines, targetTableLine{table: t})
			}
			lines[li].cells = append(lines[li].cells, t)
			lines[li].members = append(lines[li].members, i)
			lines[li].targets = append(lines[li].targets, memberTarget(data.Model.Deployments[i]))
		}
	}
	return lines
}

// targetsDiverge reports whether the targets that report table detail run
// different changes, so a target that has not reported is not known to run
// any one of them.
func targetsDiverge(data MultiDeploymentApplyData, g presentation.Group) bool {
	signature := ""
	seen := false
	for _, i := range g.Members {
		detail := memberDetail(data.Details, i)
		if detail == nil || len(detail.Tables) == 0 {
			continue
		}
		s := tableChangeSignature(detail.Tables)
		if seen && s != signature {
			return true
		}
		signature, seen = s, true
	}
	return false
}

// tableChangeKey keys one table change by its namespace, table and DDL.
func tableChangeKey(t TableProgressData) string {
	return t.Namespace + "\x00" + t.TableName + "\x00" + t.DDL
}

// tableChangeSignature keys the change a target runs by its tables and their
// DDL, independent of the order its tasks were listed in.
func tableChangeSignature(tables []TableProgressData) string {
	parts := make([]string, len(tables))
	for i, t := range tables {
		parts[i] = tableChangeKey(t)
	}
	slices.Sort(parts)
	return strings.Join(parts, "\x01")
}

// rankTargetTableLines orders a rollup's table lines by
// presentation.TableChangeOrder, ranked across every target each line speaks
// for, so the table being copied leads, one no target has started sits below
// one already finished on some, and a table its targets change with different
// statements lists them together.
func rankTargetTableLines(lines []targetTableLine) {
	for li := range lines {
		statuses := make([]string, len(lines[li].listed))
		for i, c := range lines[li].listed {
			statuses[i] = c.Status
		}
		lines[li].rank = presentation.TableRolloutRank(statuses)
	}
	order := presentation.TableChangeOrder(len(lines),
		func(i int) string { return lines[i].table.Namespace + "\x00" + lines[i].table.TableName },
		func(i int) int { return lines[i].rank })
	sorted := make([]targetTableLine, len(lines))
	for i, j := range order {
		sorted[i] = lines[j]
	}
	copy(lines, sorted)
}

// writeTargetTableLine writes one table's line across the targets that run it.
// Until every target completes, the line leads with the table's bar across
// all of its targets (presentation.PercentAcrossTargets, keepsTargetProgressBar),
// whatever phase each target is in, and the rows line names its coverage only
// when a target is left out of the sum. A target that has not reported its
// copy yet but carries the plan's row estimate is counted by that estimate,
// which makes the total approximate. Until every target still to copy
// reports, the ETA (the slowest target's) is a floor. Failed targets are
// counted rather than summed, since their rows are not progressing, and so are
// retrying targets. A completed target has reported even when it had no rows
// to copy. Once no target is working on the table, the line names where the
// table settled. cells is every target the line speaks for (lineTargets),
// including those with no progress reported yet, which widen the row
// denominator and make the ETA a floor. writeDDL writes the table's DDL
// directly under the headline, and while the table is in flight strip lists
// each target's state under the rows, the way the sharded comment lays out a
// table and its shards. Once the rollout has settled, a target still pending
// on the table never starts it, so the line says not started rather than
// queued. A running index build has no rows to sum, so while indexBuild is set
// its line leads with the build's percent across its targets instead.
func writeTargetTableLine(sb *strings.Builder, table string, cells []TableProgressData, strip []ShardProgressData, settled, indexBuild bool, budget *ddlBlockBudget, writeDDL func()) {
	var done, queued, failed, retrying int
	// cancelled counts the targets where a cancel stopped the table part-way
	// through, and neverStarted the targets where it was cancelled before it
	// started.
	var cancelled, neverStarted int
	var running int
	statuses := make([]string, len(cells))
	for i, c := range cells {
		statuses[i] = c.Status
		switch state.NormalizeTaskStatus(c.Status) {
		case state.Task.Completed, state.Task.RevertWindow:
			// A target in its revert window has completed the change.
			done++
		case state.Task.Pending:
			queued++
		case state.Task.Failed:
			failed++
		case state.Task.FailedRetryable:
			retrying++
		case state.Task.Cancelled:
			if tableBegan(c) {
				cancelled++
			} else {
				neverStarted++
			}
		case state.Task.Running:
			running++
		}
	}
	parts := targetParts(cells)
	rows := presentation.SumTargetRows(parts)
	name := inlineCode(table)
	pending := queued
	pendingWord := presentation.PendingWord(settled)
	coverage := presentation.TargetCoverage(done, running, pending, failed, retrying, presentation.RunningCopying, pendingWord)
	status := presentation.RollupTaskStatus(statuses)
	if indexBuild && running > 0 {
		// The percent across targets is a whole-number mean of each target's
		// whole percent, so it has no decimals to show.
		fmt.Fprintf(sb, "**%s**: %s%s\n", name, ui.IndexBuildProgress(presentation.IndexBuildPercentAcrossTargets(parts), apitypes.BuildWork{}), ui.ThrottledSuffix(anyTargetThrottled(cells)))
		writeDDL()
		writeMemberList(sb, presentation.TargetNoun, strip, pendingWord, presentation.RunningApplying, budget)
		return
	}
	if keepsTargetProgressBar(cells, status, done, pending, settled) {
		percent := presentation.PercentAcrossTargets(parts, 0)
		// With the rows to copy known, the bar leads from the copy's start,
		// even at 0%, so the size of the work shows before any row moves.
		if pct := ui.RowCopyDisplayPercent(percent, rows.Copied); pct > 0 || rows.Total > 0 {
			// The list under the rows says where each target stands, so the
			// headline carries only the table's progress across all of them,
			// the way a sharded table's does.
			fmt.Fprintf(sb, "**%s**: %s %s%s\n", name, ui.ProgressBarRowCopy(pct),
				presentation.AcrossTargetsPercent(pct, rows.Exact(len(cells)), rows.Copied, rows.Total), ui.ThrottledSuffix(anyTargetThrottled(cells)))
			writeDDL()
			if line := rows.RowsLine(len(cells), targetsTableBytes(cells)); line != "" {
				sb.WriteString("- " + line + "\n")
			}
			writeMemberList(sb, presentation.TargetNoun, strip, pendingWord, presentation.RunningCopying, budget)
			return
		}
	}
	if partlyCompleted(status, done, pending) {
		// Complete on some targets and queued on the rest: the change is live
		// where it completed, so the line leads with that, not with Queued.
		fmt.Fprintf(sb, "**%s**: %s on %d of %d targets%s\n", name, shardedTableStatusPhrase(state.Task.Completed), done, len(cells), presentation.TargetCoverage(0, 0, pending, 0, 0, presentation.RunningCopying, pendingWord))
		writeDDL()
		return
	}
	if status == state.Task.Completed {
		fmt.Fprintf(sb, "**%s**: %s (%s)\n", name, shardedTableStatusPhrase(status), targetNoun.Count(len(cells)))
		writeDDL()
		return
	}
	phrase := shardedTableStatusPhrase(status)
	if status == state.Task.Pending && settled {
		// No target ran the table, so there is nothing to count.
		phrase, coverage = "⊘ Not started", ""
	}
	lists := listsTargets(strip)
	if status == state.Task.Stopped && stoppedBeforeAnyTargetBegan(cells) {
		// Every target would only repeat that it stopped, so the line says it
		// once, the way the CLI's progress view does.
		phrase, coverage, lists = "⏹️ Stopped (not started)", "", false
	}
	if status == state.Task.Cancelled {
		phrase, coverage = cancelledTargetsLine(cancelledTargets{
			done: done, cancelled: cancelled, neverStarted: neverStarted, pending: pending, targets: len(cells),
		}, settled, lists)
	}
	if lists {
		// The list counts the targets, so the headline does not repeat them.
		coverage = ""
	}
	fmt.Fprintf(sb, "**%s**: %s%s\n", name, phrase, coverage)
	writeDDL()
	if status == state.Task.Pending && !settled {
		writePlannedRows(sb, targetsPlannedRows(cells), targetsTableBytes(cells), presentation.PlannedRowsSpan(len(cells)))
	}
	if lists {
		writeMemberList(sb, presentation.TargetNoun, strip, pendingWord, presentation.RunningCopying, budget)
	}
}

// stoppedBeforeAnyTargetBegan reports whether a table stopped on every
// target before any of them began it (presentation.EveryTargetStopped).
func stoppedBeforeAnyTargetBegan(cells []TableProgressData) bool {
	statuses := make([]string, len(cells))
	for i, c := range cells {
		statuses[i] = c.Status
	}
	return presentation.EveryTargetStopped(statuses) && !anyTargetBegan(cells)
}

// anyTargetBegan reports whether any target completed a table or began
// working on it, so a halt there leaves the change partly or wholly live.
func anyTargetBegan(cells []TableProgressData) bool {
	return slices.ContainsFunc(cells, func(c TableProgressData) bool {
		return tableBegan(c) || state.IsState(c.Status, state.Task.Completed, state.Task.RevertWindow)
	})
}

// cancelledTargets counts where a table stands on each of its targets when
// the cancel reached it: done completed it first, cancelled were stopped
// part-way through it, neverStarted were cancelled before they started it,
// and pending have not started it and were not cancelled.
type cancelledTargets struct {
	done, cancelled, neverStarted, pending, targets int
}

// cancelledTargetsLine is the phrase and coverage of a table line whose
// rolled-up status is cancelled. The line says the table was not started only
// when no target began it and none is still to run it; otherwise it counts the
// targets the cancel stopped, unless the list under the line names them.
//
// A target that was not cancelled can still be queued to run the table: a
// failure cancels only the tasks queued behind it on its own target, and the
// other targets carry on. Until the rollout settles those targets are counted
// as queued, apart from the ones that will never begin the table.
func cancelledTargetsLine(t cancelledTargets, settled, lists bool) (string, string) {
	notStarted, queued := t.neverStarted, 0
	if settled {
		notStarted += t.pending
	} else {
		queued = t.pending
	}
	if t.done == 0 && t.cancelled == 0 && queued == 0 {
		return shardedTableStatusPhrase(state.Task.Cancelled), ""
	}
	phrase := "⊘ Cancelled"
	if t.cancelled > 0 && !lists {
		phrase += fmt.Sprintf(" on %d of %d targets", t.cancelled, t.targets)
	}
	coverage := presentation.TargetCoverage(t.done, 0, queued, 0, 0, presentation.RunningCopying, presentation.PendingQueued)
	if notStarted > 0 {
		separator := ", "
		if coverage == "" {
			separator = " · "
		}
		coverage += fmt.Sprintf("%s%d %s", separator, notStarted, presentation.PendingNotStarted)
	}
	return phrase, coverage
}

// keepsTargetProgressBar reports whether a table line across a rollout's
// targets leads with its progress bar rather than a phase: while targets are
// working on the table in different phases, and while a rollout that can
// still run its queued targets has completed the table on others. One
// target's phase never speaks for siblings in another, so the bar holds until
// every target completes and the list under it says where each one stands.
// When every target is in the same phase past the copy, that phase speaks for
// the table (presentation.OnePhaseSpeaksForParts), and so does a revert on any
// target (presentation.RevertSpeaksForParts).
func keepsTargetProgressBar(cells []TableProgressData, status string, done, pending int, settled bool) bool {
	statuses := make([]string, len(cells))
	for i, c := range cells {
		statuses[i] = c.Status
	}
	if presentation.RevertSpeaksForParts(statuses) {
		return false
	}
	if slices.ContainsFunc(cells, func(c TableProgressData) bool { return state.IsInFlightTaskState(c.Status) }) {
		if !presentation.OnePhaseSpeaksForParts(statuses) {
			return true
		}
	}
	return !settled && partlyCompleted(status, done, pending)
}

// listsTargets reports whether a table line lists its targets one per line
// under it (writeMemberList, presentation.ListsTargets), decided by where each
// target stands rather than by the table's rolled-up status.
func listsTargets(strip []ShardProgressData) bool {
	if len(strip) <= 1 {
		return false
	}
	statuses := make([]string, len(strip))
	for i, s := range strip {
		statuses[i] = s.Status
	}
	return presentation.ListsTargets(statuses)
}

// partlyCompleted reports whether a rolled-up table has completed on some of
// its targets and is queued on the rest, with nothing else in between. A target
// in its revert window has completed, so a table in its revert window on some
// targets and queued on the rest is partly completed too; once every target
// is in its revert window, the line keeps the revert window's wording. queued
// counts the targets with no progress reported, so a table complete on every
// target that reported is partly completed while others have not.
func partlyCompleted(status string, done, queued int) bool {
	if done == 0 || queued == 0 {
		return false
	}
	return status == state.Task.Pending || status == state.Task.RevertWindow || status == state.Task.Completed
}

// targetParts is a table's progress on each target that runs it, as the
// shared part rules read it. A target whose copy is still ahead and has
// reported no row total carries the plan's estimate in its place.
func targetParts(cells []TableProgressData) []presentation.Part {
	parts := make([]presentation.Part, len(cells))
	for i, c := range cells {
		parts[i] = presentation.Part{Status: c.Status, PercentComplete: c.PercentComplete, RowsCopied: c.RowsCopied, RowsTotal: c.RowsTotal, ETASeconds: c.ETASeconds}
		if c.RowsTotal == 0 && presentation.CopyStillAhead(c.Status) {
			parts[i].PlannedRows = c.EstimatedRows
		}
	}
	return parts
}

// targetsPlannedRows totals a table's planned rows across the targets that run
// it, under the same every-target rule as targetsTableBytes.
func targetsPlannedRows(cells []TableProgressData) *int64 {
	if len(cells) == 0 {
		return nil
	}
	var total int64
	for _, c := range cells {
		if c.EstimatedRows == nil {
			return nil
		}
		total += *c.EstimatedRows
	}
	return &total
}

// targetsTableBytes totals a table's planned size across the targets that run
// it, since each target copies its own data. It is nil unless every one of
// those targets carries an estimate: a total that left some out would
// understate the table, and a target that has reported nothing has none.
func targetsTableBytes(cells []TableProgressData) *int64 {
	if len(cells) == 0 {
		return nil
	}
	var total int64
	for _, c := range cells {
		if c.EstimatedBytes == nil {
			return nil
		}
		total += *c.EstimatedBytes
	}
	return &total
}

// writeFailedTargets names each failed or retrying target and its error, the
// way the sharded comment names a failed shard. A target stopped at the
// cutover for another's failure is not listed or counted as failed.
func writeFailedTargets(sb *strings.Builder, model presentation.Apply, g presentation.Group) {
	var failed []presentation.Deployment
	for _, i := range g.Members {
		if d := model.Deployments[i]; d.FailedOrRetrying() {
			failed = append(failed, d)
		}
	}
	if len(failed) == 0 {
		return
	}
	sb.WriteString("\n| Target | Status |\n| --- | --- |\n")
	for _, d := range failed[:min(len(failed), failedTargetRowLimit)] {
		cell := shardStatusCell(ShardStatus{Emoji: d.Emoji, Label: d.Label, State: d.State, Error: d.Error})
		fmt.Fprintf(sb, "| %s | %s |\n", inlineCodeCell(memberTarget(d)), cell)
	}
	if more := len(failed) - failedTargetRowLimit; more > 0 {
		fmt.Fprintf(sb, "\n…and %d more failed %s.\n", more, targetNoun.Label(more))
	}
}

// memberTarget is a member's target, or its name when it carries none.
func memberTarget(d presentation.Deployment) string {
	if d.Target == "" {
		return d.Name
	}
	return d.Target
}
