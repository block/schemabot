package templates

import (
	"fmt"
	"slices"
	"strings"

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
		strip := targetStrip(line.listed, line.listedTargets)
		writeTargetTableLine(sb, line.table.TableName, line.listed, strip, settled, budget, func() {
			writeTargetLineDDL(sb, dialect, line, subset, changing, budget)
		})
		sb.WriteString("\n")
		restorePlan()
		restoreScope()
	}
	writeFailedTargets(sb, data.Model, g)
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
	strip := make([]ShardProgressData, len(cells))
	for i, c := range cells {
		strip[i] = ShardProgressData{
			Shard: targets[i], Status: c.Status, PercentComplete: c.PercentComplete, RowsCopied: c.RowsCopied, RowsTotal: c.RowsTotal,
			ChecksumRowsChecked: c.ChecksumRowsChecked, ChecksumRowsTotal: c.ChecksumRowsTotal,
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
			cells = append(cells, TableProgressData{Status: presentation.UnreportedTargetStatus(data.Model.Deployments[i].State)})
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
// presentation.TableRolloutRank across every target each line speaks for, so
// the table being copied leads and one no target has started sits below one
// already finished on some. Lines of equal rank keep plan order.
func rankTargetTableLines(lines []targetTableLine) {
	for li := range lines {
		statuses := make([]string, len(lines[li].listed))
		for i, c := range lines[li].listed {
			statuses[i] = c.Status
		}
		lines[li].rank = presentation.TableRolloutRank(statuses)
	}
	slices.SortStableFunc(lines, func(a, b targetTableLine) int { return a.rank - b.rank })
}

// writeTargetTableLine writes one table's line across the targets that run it.
// Until every target completes, the line leads with the table's bar across
// all of its targets (presentation.PercentAcrossTargets, keepsTargetProgressBar),
// whatever phase each target is in, and the rows line names its coverage only
// when a target is left out of the sum. Until every target still to copy
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
// queued.
func writeTargetTableLine(sb *strings.Builder, table string, cells []TableProgressData, strip []ShardProgressData, settled bool, budget *ddlBlockBudget, writeDDL func()) {
	var done, queued, failed, retrying, reporting, unreported int
	var copied, total, eta int64
	var running int
	for _, c := range cells {
		status := state.NormalizeTaskStatus(c.Status)
		switch status {
		case state.Task.Completed, state.Task.RevertWindow:
			// A target in its revert window has completed the change.
			done++
			if c.RowsTotal == 0 {
				reporting++
				continue
			}
		case state.Task.Pending:
			queued++
		case state.Task.Failed:
			failed++
			continue
		case state.Task.FailedRetryable:
			retrying++
			continue
		case state.Task.Stopped, state.Task.Cancelled, state.Task.Reverted:
			continue
		case state.Task.Running:
			running++
			eta = max(eta, c.ETASeconds)
		}
		if c.RowsTotal == 0 {
			unreported++
			continue
		}
		reporting++
		copied += ui.ClampRows(c.RowsCopied, c.RowsTotal)
		total += c.RowsTotal
	}
	name := inlineCode(table)
	pending := queued
	pendingWord := presentation.PendingWord(settled)
	coverage := presentation.TargetCoverage(done, running, pending, failed, retrying, pendingWord)
	status := rollupTaskStatus(cells)
	if keepsTargetProgressBar(cells, status, done, pending, settled) {
		percent := presentation.PercentAcrossTargets(targetParts(cells), 0)
		if pct := ui.RowCopyDisplayPercent(percent, copied); pct > 0 {
			// The list under the rows says where each target stands, so the
			// headline carries only the table's progress across all of them,
			// the way a sharded table's does.
			fmt.Fprintf(sb, "**%s**: %s %d%%\n", name, ui.ProgressBarRowCopy(pct), pct)
			writeDDL()
			if total > 0 {
				line := fmt.Sprintf("- Rows: %s / %s", ui.FormatNumber(copied), ui.FormatNumber(total))
				partial := reporting < len(cells)
				if partial {
					line += fmt.Sprintf(" across %d of %d targets", reporting, len(cells))
				}
				// The planned size is every target's, including those left out of
				// the rows, so beside partial rows it names the full span rather
				// than reading as the reporting targets' size.
				if size := targetsTableBytes(cells); size != nil {
					line += ui.FormatTableSizeClause(size)
					if partial {
						line += fmt.Sprintf(" across all %d targets", len(cells))
					}
				}
				if eta > 0 {
					// A target still to start, or working without its rows
					// reported yet, can only add time.
					floor := ""
					if queued+unreported > 0 {
						floor = "≥ "
					}
					line += " · ETA: " + floor + ui.FormatETA(eta)
				}
				sb.WriteString(line + "\n")
			}
			writeMemberList(sb, presentation.TargetNoun, strip, pendingWord, budget)
			return
		}
	}
	if partlyCompleted(status, done, pending) {
		// Complete on some targets and queued on the rest: the change is live
		// where it completed, so the line leads with that, not with Queued.
		fmt.Fprintf(sb, "**%s**: %s on %d of %d targets%s\n", name, shardedTableStatusPhrase(state.Task.Completed), done, len(cells), presentation.TargetCoverage(0, 0, pending, 0, 0, pendingWord))
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
	if status == state.Task.Cancelled && done > 0 {
		// The pure-cancelled parenthetical ("not started") would be false:
		// the change is live on the completed targets.
		phrase = "⊘ Cancelled"
	}
	lists := listsTargets(strip)
	if lists {
		// The list counts the targets, so the headline does not repeat them.
		coverage = ""
	}
	fmt.Fprintf(sb, "**%s**: %s%s\n", name, phrase, coverage)
	writeDDL()
	if lists {
		writeMemberList(sb, presentation.TargetNoun, strip, pendingWord, budget)
	}
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
// shared part rules read it.
func targetParts(cells []TableProgressData) []presentation.Part {
	parts := make([]presentation.Part, len(cells))
	for i, c := range cells {
		parts[i] = presentation.Part{Status: c.Status, PercentComplete: c.PercentComplete, RowsCopied: c.RowsCopied, RowsTotal: c.RowsTotal}
	}
	return parts
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

// rollupTaskStatus is a table's status across targets: a failure or halt
// first, then any target still moving, then queued, then complete.
func rollupTaskStatus(cells []TableProgressData) string {
	statuses := make([]string, len(cells))
	for i, c := range cells {
		statuses[i] = state.NormalizeTaskStatus(c.Status)
	}
	for _, halting := range []string{state.Task.Failed, state.Task.FailedRetryable, state.Task.Stopped, state.Task.Cancelled} {
		if slices.Contains(statuses, halting) {
			return halting
		}
	}
	for _, s := range statuses {
		if s != state.Task.Pending && s != state.Task.Completed {
			return s
		}
	}
	if slices.Contains(statuses, state.Task.Pending) {
		return state.Task.Pending
	}
	return state.Task.Completed
}

// writeFailedTargets names each failed or retrying target and its error, the
// way the sharded comment names a failed shard.
func writeFailedTargets(sb *strings.Builder, model presentation.Apply, g presentation.Group) {
	var failed []presentation.Deployment
	for _, i := range g.Members {
		if d := model.Deployments[i]; isShardFailureState(d.State) {
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
