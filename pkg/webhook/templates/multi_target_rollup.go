package templates

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

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
// targets, each change's DDL once, a "what applies where" split when targets
// diverge, and a row per failed target. Its size grows with distinct changes
// and failures, not with the number of targets.
func writeTargetRollup(sb *strings.Builder, data MultiDeploymentApplyData, g presentation.Group, budget *ddlBlockBudget) {
	work := targetWorkGroups(data, g)
	if len(work) == 0 {
		sb.WriteString("_No details available yet._\n")
	}
	if len(work) > 1 {
		sb.WriteString("Targets diverge — what applies where:\n\n")
	}
	for _, w := range work {
		if len(work) > 1 {
			writeGroupHeading(sb, targetNoun, targetNames(data.Model, w.members), len(g.Members))
		}
		dialect := dialectForEngine(memberDetail(data.Details, w.members[0]).Engine, data.ApplyID)
		for _, t := range w.tables {
			writeTargetTableLine(sb, t.TableName, tableAcrossTargets(data, w.members, t))
			writeDDLLine(sb, dialect, t.DDL, budget)
			sb.WriteString("\n")
		}
	}
	writeFailedTargets(sb, data.Model, g)
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

// tableAcrossTargets returns table's progress on each of members.
func tableAcrossTargets(data MultiDeploymentApplyData, members []int, table TableProgressData) []TableProgressData {
	cells := make([]TableProgressData, 0, len(members))
	for _, i := range members {
		for _, t := range memberDetail(data.Details, i).Tables {
			if t.Namespace == table.Namespace && t.TableName == table.TableName {
				cells = append(cells, t)
				break
			}
		}
	}
	return cells
}

// writeTargetTableLine writes one table's line across the targets that run it.
// While any target copies, the bar sums the rows of targets copying or done;
// until every target still to copy reports, the ETA (the slowest target's) is
// a floor. Failed targets are counted rather than summed, since their rows are
// not progressing. With nothing copying, the line names the table's phase.
func writeTargetTableLine(sb *strings.Builder, table string, cells []TableProgressData) {
	var done, failed, reporting, unreported int
	var copied, total, eta int64
	copying := false
	for _, c := range cells {
		status := state.NormalizeTaskStatus(c.Status)
		switch status {
		case state.Task.Completed:
			done++
		case state.Task.Failed, state.Task.FailedRetryable:
			failed++
			continue
		case state.Task.Stopped, state.Task.Cancelled:
			continue
		case state.Task.Running:
			copying = true
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
	coverage := fmt.Sprintf("%d of %d targets complete", done, len(cells))
	if failed > 0 {
		coverage += fmt.Sprintf(", %d failed", failed)
	}
	if copying && total > 0 {
		if pct := ui.RowCopyDisplayPercent(int(copied*100/total), copied); pct > 0 {
			fmt.Fprintf(sb, "**%s**: %s %d%% · %s\n", name, ui.ProgressBarRowCopy(pct), pct, coverage)
			line := fmt.Sprintf("- Rows: %s / %s across %d of %d targets", ui.FormatNumber(copied), ui.FormatNumber(total), reporting, len(cells))
			if eta > 0 {
				floor := ""
				if unreported > 0 {
					floor = "≥ "
				}
				line += " · ETA: " + floor + ui.FormatETA(eta)
			}
			sb.WriteString(line + "\n")
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
	fmt.Fprintf(sb, "**%s**: %s · %s\n", name, phrase, coverage)
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
