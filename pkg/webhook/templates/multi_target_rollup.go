package templates

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// runningTargetNameLimit bounds the running-target names a table's line lists.
const runningTargetNameLimit = 10

// failedTargetRowLimit bounds the failed-target table so a deployment whose
// every target failed still fits one comment; the <summary> counts carry the total.
const failedTargetRowLimit = 20

// targetWork is the targets of a deployment that run the same change, and the
// first target's tables, which carry the DDL they share.
type targetWork struct {
	members []int
	tables  []TableProgressData
}

// writeTargetRollup writes a multi-target deployment's body the way the
// sharded apply comment writes a keyspace: one line per table across the
// targets, each change's DDL once, a heading per group of targets when they
// diverge, and a row per failed target. Its size grows with distinct changes
// and failures, not with the number of targets.
func writeTargetRollup(sb *strings.Builder, data MultiDeploymentApplyData, g presentation.Group, budget *ddlBlockBudget) {
	work := targetWorkGroups(data, g)
	if len(work) == 0 {
		sb.WriteString("_No details available yet._\n")
	}
	silent := unreportedTargets(data, g)
	// A target that has not reported is not known to run any one group's
	// change, so when the groups diverge each line counts only its own
	// targets and the silent ones are counted once, for the deployment.
	lineSilent := silent
	if len(work) > 1 {
		lineSilent = 0
	}
	for _, w := range work {
		if len(work) > 1 {
			writeTargetGroupHeading(sb, "####", targetNames(data.Model, w.members), len(g.Members))
		}
		first := memberDetail(data.Details, w.members[0])
		dialect := dialectForEngine(first.Engine, data.ApplyID)
		// The group's DDL is its first target's, so a cut block names that
		// target's stored plan and says which other targets run the same.
		restoreGroup := planScopeForWork(budget, targetNames(data.Model, w.members), len(work), silent)
		restorePlan := budget.pointAt(first.storedPlan())
		for _, t := range w.tables {
			cells, targets := tableAcrossTargets(data, w.members, t)
			writeTargetTableLine(sb, t.TableName, cells, targets, lineSilent)
			writeDDLLine(sb, dialect, t.DDL, budget)
			sb.WriteString("\n")
		}
		restorePlan()
		restoreGroup()
	}
	if len(work) > 0 && silent > 0 {
		fmt.Fprintf(sb, "_%d of %d targets have not reported progress yet._\n", silent, len(g.Members))
	}
	writeFailedTargets(sb, data.Model, g)
}

// planScopeForWork scopes the pointer a cut block in one work group carries.
// Under a group heading the marker speaks for the targets the heading names;
// a sole group has no heading, so the marker names its plan's target and
// speaks only for the targets that have reported.
func planScopeForWork(budget *ddlBlockBudget, members []string, groups, unreported int) (restore func()) {
	if groups > 1 {
		return budget.forTargetGroup(members)
	}
	return budget.forSoleTargetGroup(members, unreported)
}

// unreportedTargets counts the targets with no table progress to show yet, so
// the table lines do not read as covering the whole deployment.
func unreportedTargets(data MultiDeploymentApplyData, g presentation.Group) int {
	n := 0
	for _, i := range g.Members {
		if detail := memberDetail(data.Details, i); detail == nil || len(detail.Tables) == 0 {
			n++
		}
	}
	return n
}

// targetWorkGroups partitions a deployment's targets by the change they run.
// A target with no table detail yet joins no group: missing detail is not a
// different change, and setting it apart would show a false divergence.
func targetWorkGroups(data MultiDeploymentApplyData, g presentation.Group) []targetWork {
	var groups []targetWork
	bySignature := make(map[string]int)
	for _, i := range g.Members {
		detail := memberDetail(data.Details, i)
		if detail == nil || len(detail.Tables) == 0 {
			continue
		}
		signature := tableChangeSignature(detail.Tables)
		gi, seen := bySignature[signature]
		if !seen {
			gi = len(groups)
			bySignature[signature] = gi
			groups = append(groups, targetWork{tables: detail.Tables})
		}
		groups[gi].members = append(groups[gi].members, i)
	}
	return groups
}

// tableChangeSignature keys the change a target runs by its tables and their
// DDL, independent of the order its tasks were listed in.
func tableChangeSignature(tables []TableProgressData) string {
	parts := make([]string, len(tables))
	for i, t := range tables {
		parts[i] = t.Namespace + "\x00" + t.TableName + "\x00" + t.DDL
	}
	slices.Sort(parts)
	return strings.Join(parts, "\x01")
}

// tableAcrossTargets returns table's progress on each of members, and the
// target each cell belongs to. A cell matches on its DDL as well as its table,
// so a table changed by two statements gets a line per statement.
func tableAcrossTargets(data MultiDeploymentApplyData, members []int, table TableProgressData) ([]TableProgressData, []string) {
	cells := make([]TableProgressData, 0, len(members))
	targets := make([]string, 0, len(members))
	for _, i := range members {
		for _, t := range memberDetail(data.Details, i).Tables {
			if t.Namespace == table.Namespace && t.TableName == table.TableName && t.DDL == table.DDL {
				cells = append(cells, t)
				targets = append(targets, memberTarget(data.Model.Deployments[i]))
				break
			}
		}
	}
	return cells, targets
}

// writeTargetTableLine writes one table's line across the targets that run it.
// While any target copies, the bar sums the rows of targets copying or done,
// and the rows line names its coverage only when a target is left out of the
// sum; until every target still to copy reports, the ETA (the slowest
// target's) is a floor. Failed targets are counted rather than summed, since
// their rows are not progressing, and so are retrying targets. A completed
// target has reported even when it had no rows to copy. The running targets
// are named unless every target is running. With nothing copying, the line
// names the table's phase. silent is the targets with no progress reported at
// all that this line speaks for; they widen the row denominator and make the
// ETA a floor.
func writeTargetTableLine(sb *strings.Builder, table string, cells []TableProgressData, targets []string, silent int) {
	var done, queued, failed, retrying, reporting, unreported int
	var copied, total, eta int64
	var running []string
	for i, c := range cells {
		status := state.NormalizeTaskStatus(c.Status)
		switch status {
		case state.Task.Completed:
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
		case state.Task.Stopped, state.Task.Cancelled:
			continue
		case state.Task.Running:
			running = append(running, targets[i])
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
	coverage := targetCoverage(done, len(running), queued, failed, retrying)
	if len(running) > 0 && total > 0 {
		if pct := ui.RowCopyDisplayPercent(int(copied*100/total), copied); pct > 0 {
			fmt.Fprintf(sb, "**%s**: %s %d%%%s\n", name, ui.ProgressBarRowCopy(pct), pct, coverage)
			line := fmt.Sprintf("- Rows: %s / %s", ui.FormatNumber(copied), ui.FormatNumber(total))
			partial := reporting < len(cells)+silent
			if partial {
				line += fmt.Sprintf(" across %d of %d targets", reporting, len(cells)+silent)
			}
			// The planned size is every target's, including those left out of
			// the rows, so beside partial rows it names the full span rather
			// than reading as the reporting targets' size.
			if size := targetsTableBytes(cells, silent); size != nil {
				line += ui.FormatTableSizeClause(size)
				if partial {
					line += fmt.Sprintf(" across all %d targets", len(cells))
				}
			}
			if eta > 0 {
				floor := ""
				if unreported+silent > 0 {
					floor = "≥ "
				}
				line += " · ETA: " + floor + ui.FormatETA(eta)
			}
			sb.WriteString(line + "\n")
			writeRunningTargets(sb, running, len(cells))
			return
		}
	}
	status := rollupTaskStatus(cells)
	if status == state.Task.Completed {
		fmt.Fprintf(sb, "**%s**: %s (%d targets)\n", name, shardedTableStatusPhrase(status), len(cells))
		return
	}
	phrase := shardedTableStatusPhrase(status)
	if status == state.Task.Cancelled && done > 0 {
		// The pure-cancelled parenthetical ("not started") would be false:
		// the change is live on the completed targets.
		phrase = "⊘ Cancelled"
	}
	fmt.Fprintf(sb, "**%s**: %s%s\n", name, phrase, coverage)
}

// targetsTableBytes totals a table's planned size across the targets that run
// it, since each target copies its own data. It is nil unless every one of
// those targets carries an estimate: a total that left some out would
// understate the table, and silent targets have reported nothing at all.
func targetsTableBytes(cells []TableProgressData, silent int) *int64 {
	if silent > 0 || len(cells) == 0 {
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

// targetCoverage is the " · 40 complete, 4 running, 19 queued, 1 failed,
// 1 retrying" suffix of a table's line, naming only the states some target is
// in. Queued targets are waiting on the apply's driver cap or on their turn in
// order.
func targetCoverage(done, running, queued, failed, retrying int) string {
	var parts []string
	if done > 0 {
		parts = append(parts, fmt.Sprintf("%d complete", done))
	}
	if running > 0 {
		parts = append(parts, fmt.Sprintf("%d running", running))
	}
	if queued > 0 {
		parts = append(parts, fmt.Sprintf("%d queued", queued))
	}
	if failed > 0 {
		parts = append(parts, fmt.Sprintf("%d failed", failed))
	}
	if retrying > 0 {
		parts = append(parts, fmt.Sprintf("%d retrying", retrying))
	}
	if len(parts) == 0 {
		return ""
	}
	return " · " + strings.Join(parts, ", ")
}

// writeRunningTargets names the targets copying the table, unless all of them
// are, where the count alone already says which.
func writeRunningTargets(sb *strings.Builder, running []string, targets int) {
	if len(running) == 0 || len(running) == targets {
		return
	}
	names := make([]string, 0, runningTargetNameLimit)
	for _, r := range running[:min(len(running), runningTargetNameLimit)] {
		names = append(names, inlineCode(r))
	}
	line := "- Running: " + strings.Join(names, ", ")
	if more := len(running) - runningTargetNameLimit; more > 0 {
		line += fmt.Sprintf(", and %d more", more)
	}
	sb.WriteString(line + "\n")
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
		fmt.Fprintf(sb, "\n…and %d more failed targets.\n", more)
	}
}

// targetNames is each member's target, in the order given.
func targetNames(model presentation.Apply, members []int) []string {
	names := make([]string, len(members))
	for j, i := range members {
		names[j] = memberTarget(model.Deployments[i])
	}
	return names
}

// memberTarget is a member's target, or its name when it carries none.
func memberTarget(d presentation.Deployment) string {
	if d.Target == "" {
		return d.Name
	}
	return d.Target
}
