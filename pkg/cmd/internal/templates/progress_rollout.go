package templates

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// rolloutAttentionLimit bounds the targets a deployment's attention list
// names, so a deployment whose every target failed stays one screen; the
// deployment's counts carry the total.
const rolloutAttentionLimit = 20

// RolloutView is what a multi-deployment apply's progress is rendered from.
// The progress output and the watch TUI both render through it, so a rollout
// reads the same on either.
type RolloutView struct {
	ApplyID     string
	Environment string
	Engine      string
	// Operations are the apply's operations in resolved order. Model is
	// derived from them, and Model.Deployments[i] projects Operations[i].
	Operations []ProgressOperation
	Model      presentation.Apply
	Tables     []TableProgress
	// SetupPhase hides table progress while the apply is still in an engine
	// setup phase, where every table reads as queued.
	SetupPhase bool
}

// RolloutCountsUnit is what the rollout's status counts count: targets once
// any deployment addresses several, deployments otherwise.
func RolloutCountsUnit(groups []presentation.Group) string {
	for _, g := range groups {
		if len(g.Members) > 1 {
			return "Targets"
		}
	}
	return "Deployments"
}

// FormatStateCounts joins a status histogram into "40 completed · 3 running".
func FormatStateCounts(counts []presentation.StateCount) string {
	parts := make([]string, 0, len(counts))
	for _, count := range counts {
		parts = append(parts, fmt.Sprintf("%d %s", count.Count, count.Label))
	}
	return strings.Join(parts, " · ")
}

// targetWork is the targets of a deployment that run the same change, and the
// first target's tables, which carry the DDL they share.
type targetWork struct {
	members []int
	tables  []TableProgress
}

// FormatTargetRollup renders a deployment that addresses several targets as
// one section, the way a sharded table rolls up its shards: one block per
// table across the targets with each change's DDL once, a "what applies
// where" split when targets run different changes, and the targets that need
// an operator. Its size grows with distinct changes and failures, not with
// the number of targets.
func FormatTargetRollup(v RolloutView, g presentation.Group) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s — %s (%d targets)\n", g.Lead.Emoji, g.Deployment, FormatStateCounts(g.Counts), len(g.Members))
	if !v.SetupPhase {
		writeTargetTables(&b, v, g)
	}
	writeTargetAttention(&b, v, g)
	// Table blocks carry their own trailing blank line, so the section is
	// trimmed to end on exactly one, whichever block closes it.
	return strings.TrimRight(b.String(), "\n") + "\n\n"
}

// writeTargetTables writes each table's progress across the targets that run
// it. A target that has reported no table progress joins no group: missing
// detail is not a different change, and setting it apart would show a false
// divergence, so such targets are counted once below instead.
func writeTargetTables(b *strings.Builder, v RolloutView, g presentation.Group) {
	var work []targetWork
	bySignature := make(map[string]int)
	silent := 0
	for _, i := range g.Members {
		d := v.Model.Deployments[i]
		tables := activeTablesForMember(v.Tables, d.Deployment, d.Target)
		if len(tables) == 0 {
			silent++
			continue
		}
		signature := tableChangeSignature(tables)
		wi, seen := bySignature[signature]
		if !seen {
			wi = len(work)
			bySignature[signature] = wi
			sortActiveTables(tables)
			work = append(work, targetWork{tables: tables})
		}
		work[wi].members = append(work[wi].members, i)
	}
	if len(work) > 1 {
		b.WriteString("\n")
	}
	for _, w := range work {
		if len(work) > 1 {
			b.WriteString(FormatRolloutGroupHeading(presentation.TargetNoun, rolloutMemberNames(v.Model, w.members), len(g.Members), false))
		} else {
			b.WriteString("\n")
		}
		for _, t := range w.tables {
			b.WriteString(FormatTableProgress(tableAcrossTargets(v, w.members, t)))
			b.WriteString("\n")
		}
	}
	if silent > 0 {
		fmt.Fprintf(b, "  %s%d of %d targets have not reported progress yet.%s\n", ANSIDim, silent, len(g.Members), ANSIReset)
	}
}

// tableChangeSignature keys the change a target runs by its tables and their
// DDL, independent of the order its tasks were listed in.
func tableChangeSignature(tables []TableProgress) string {
	parts := make([]string, len(tables))
	for i, t := range tables {
		parts[i] = t.Namespace + "\x00" + t.TableName + "\x00" + t.DDL
	}
	slices.Sort(parts)
	return strings.Join(parts, "\x01")
}

// tableAcrossTargets rolls table up across members: one entry per target,
// in the rollout's order, and the rows and ETA the engines reported for the
// targets copying or done. A target's entry matches on DDL as well as table,
// so a table changed by two statements rolls up once per statement.
func tableAcrossTargets(v RolloutView, members []int, table TableProgress) TableProgress {
	rolled := TableProgress{
		TableName:     table.TableName,
		Namespace:     table.Namespace,
		Dialect:       table.Dialect,
		ChangeType:    table.ChangeType,
		DDL:           table.DDL,
		IsInstant:     table.IsInstant,
		AcrossTargets: true,
	}
	statuses := make([]string, 0, len(members))
	for _, i := range members {
		d := v.Model.Deployments[i]
		for _, t := range activeTablesForMember(v.Tables, d.Deployment, d.Target) {
			if t.Namespace != table.Namespace || t.TableName != table.TableName || t.DDL != table.DDL {
				continue
			}
			status := state.NormalizeTaskStatus(t.Status)
			statuses = append(statuses, status)
			rolled.Shards = append(rolled.Shards, ShardProgress{
				Shard:           memberTargetName(d),
				Status:          status,
				RowsCopied:      t.RowsCopied,
				RowsTotal:       t.RowsTotal,
				ETASeconds:      t.ETASeconds,
				PercentComplete: t.PercentComplete,
			})
			if status == state.Task.Running || status == state.Task.Completed {
				rolled.RowsCopied += t.RowsCopied
				rolled.RowsTotal += t.RowsTotal
				rolled.ChecksumRowsChecked += t.ChecksumRowsChecked
				rolled.ChecksumRowsTotal += t.ChecksumRowsTotal
				rolled.ETASeconds = max(rolled.ETASeconds, t.ETASeconds)
			}
			if t.Throttled && !rolled.Throttled {
				rolled.Throttled, rolled.ThrottleReason = true, t.ThrottleReason
			}
			break
		}
	}
	rolled.Status = rollupTaskStatus(statuses)
	if rolled.RowsTotal > 0 {
		rolled.PercentComplete = int(min(rolled.RowsCopied, rolled.RowsTotal) * 100 / rolled.RowsTotal)
	}
	return rolled
}

// rollupTaskStatus is a table's status across targets. While any target is
// copying the table reads as copying, so the bar keeps showing the rows still
// moving; the targets that failed are named in the per-target lines and the
// attention list beneath it. Otherwise a failure or halt comes first, then
// any target still moving, then queued, then complete.
func rollupTaskStatus(statuses []string) string {
	if slices.Contains(statuses, state.Task.Running) {
		return state.Task.Running
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

// writeTargetAttention names each failed or retrying target with its error
// and the data-plane apply to look at, the way a sharded apply names a failed
// shard.
func writeTargetAttention(b *strings.Builder, v RolloutView, g presentation.Group) {
	var attention []int
	for _, i := range g.Members {
		if isTargetFailureState(v.Model.Deployments[i].State) {
			attention = append(attention, i)
		}
	}
	if len(attention) == 0 {
		return
	}
	fmt.Fprintf(b, "\n  %sTargets needing attention:%s\n", ANSIBold, ANSIReset)
	for _, i := range attention[:min(len(attention), rolloutAttentionLimit)] {
		d := v.Model.Deployments[i]
		line := fmt.Sprintf("    %s %s — %s", d.Emoji, memberTargetName(d), d.Label)
		if d.Error != "" {
			line += ": " + d.Error
		}
		fmt.Fprintf(b, "%s%s%s\n", ANSIRed, line, ANSIReset)
		if externalID := v.Operations[i].ExternalID; externalID != "" {
			fmt.Fprintf(b, "      %sExternal apply ID: %s%s\n", ANSIDim, externalID, ANSIReset)
		}
	}
	if more := len(attention) - rolloutAttentionLimit; more > 0 {
		fmt.Fprintf(b, "    %s…and %d more%s\n", ANSIDim, more, ANSIReset)
	}
}

// isTargetFailureState reports whether a target's operation failed, or failed
// and is being retried.
func isTargetFailureState(opState string) bool {
	return state.IsState(opState, state.ApplyOperation.Failed, state.ApplyOperation.FailedRetryable)
}

// rolloutMemberNames is each member's target, in the order given.
func rolloutMemberNames(model presentation.Apply, members []int) []string {
	names := make([]string, len(members))
	for j, i := range members {
		names[j] = memberTargetName(model.Deployments[i])
	}
	return names
}

// memberTargetName is a member's target, or its name when it carries none.
func memberTargetName(d presentation.Deployment) string {
	if d.Target == "" {
		return d.Name
	}
	return d.Target
}

// FormatRolloutFooter writes the one command the rollout is waiting on, at
// the bottom where an operator looks for it: the pending action (cut over,
// resume, retry), or release while a failure holds the rollout. Every
// command addresses the whole apply. Whenever a member is still writing to
// its target, stop follows, so a pending action never takes stop away from
// live work; a terminal apply refuses stop, so it is never offered under one.
func FormatRolloutFooter(v RolloutView) string {
	var b strings.Builder
	applyCommand := func(verb string) string {
		return fmt.Sprintf("%s %s %s -e %s", cliname.Name(), verb, v.ApplyID, v.Environment)
	}
	next := v.Model.NextAction
	switch next.Kind {
	case presentation.NextActionCutover:
		writeRolloutCommand(&b, fmt.Sprintf("To cut over %s", next.Name), applyCommand("cutover"))
	case presentation.NextActionResume:
		writeRolloutCommand(&b, "To resume from where it stopped", applyCommand("start"))
	case presentation.NextActionReviewFailure:
		// A failed apply is recovered by a fresh apply, which resumes from
		// where this one stopped.
		writeRolloutCommand(&b, "To retry once the failure above is resolved", fmt.Sprintf("%s apply -e %s", cliname.Name(), v.Environment))
	case presentation.NextActionNone:
	}
	paused := state.IsState(v.Model.State, state.Apply.Paused)
	if paused {
		writeRolloutCommand(&b, "Paused after a failure — to let the held deployments proceed", applyCommand("release"))
	}
	if state.IsTerminalApplyState(v.Model.State) {
		return b.String()
	}
	if !paused && !presentation.OffersStop(v.Model.State) && !v.Model.HasStoppableLiveWork() {
		return b.String()
	}
	verb, label := "stop", "To stop this schema change"
	if strings.EqualFold(v.Engine, storage.EnginePlanetScale) {
		verb, label = "cancel", "To cancel this schema change"
	}
	writeRolloutCommand(&b, label, applyCommand(verb))
	return b.String()
}

// writeRolloutCommand writes one labelled command of the rollout footer.
func writeRolloutCommand(b *strings.Builder, label, command string) {
	fmt.Fprintf(b, "%s:\n  %s%s%s\n", label, ANSICyan, command, ANSIReset)
}
