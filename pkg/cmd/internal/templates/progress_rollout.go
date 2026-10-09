package templates

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/glyph"
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
	// Model is the apply's members as presentation.Derive projects them.
	// Each member carries its own state, error and data-plane identifiers,
	// so the rollup reads every target from one row.
	Model  presentation.Apply
	Tables []TableProgress
	// SetupPhase hides table progress while the apply is still in an engine
	// setup phase, where every table reads as queued.
	SetupPhase bool
	// DeferCutover is whether the apply waits for an operator at each
	// cutover. Without it SchemaBot cuts a ready member over itself, so the
	// footer offers no cutover command.
	DeferCutover bool
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

// RolloutCounts is the rollout's status counts as a label and its text, for
// the progress header and the watch view. A rollout of one deployment run
// table by table counts its table steps, "Tables: 1 of 3 done on 4 targets",
// since between tables most targets are done with one and waiting on the
// next, which a count of target states reads as queued. It counts the targets
// that ran a table, not the ones that already had the change, and adds the
// targets the PR comment's headline adds (TableStepOutcomes): "1 of 3 done on
// 4 targets · 3 failed". Any other rollout counts its members' states. The
// text is empty when there is nothing to count.
func RolloutCounts(model presentation.Apply, groups []presentation.Group) (label, text string) {
	if len(groups) == 1 && len(groups[0].Members) > 1 {
		steps, ok := model.TableSteps(groups[0])
		progress := model.TargetProgress(groups[0])
		if ok && progress.Total > progress.AlreadyHad {
			text := fmt.Sprintf("%d of %d done on %s", steps.Done, steps.Steps, targetCount(progress.Total-progress.AlreadyHad))
			return "Tables", withTableStepOutcomes(text, model, steps, progress)
		}
	}
	return RolloutCountsUnit(groups), FormatStateCounts(model.Counts)
}

// withTableStepOutcomes appends to a table-step count the target counts that
// TableStepOutcomes says it carries.
func withTableStepOutcomes(text string, model presentation.Apply, steps presentation.TableSteps, progress presentation.TargetProgress) string {
	settled := state.IsState(model.State, state.SettledApplyStates...)
	if outcomes := progress.TableStepOutcomes(steps, settled); len(outcomes) > 0 {
		text += " · " + FormatStateCounts(outcomes)
	}
	return text
}

// targetCount is "1 target" or "3 targets".
func targetCount(n int) string {
	if n == 1 {
		return "1 target"
	}
	return fmt.Sprintf("%d targets", n)
}

// FormatStateCounts joins a status histogram into "40 completed · 3 running".
func FormatStateCounts(counts []presentation.StateCount) string {
	parts := make([]string, 0, len(counts))
	for _, count := range counts {
		parts = append(parts, fmt.Sprintf("%d %s", count.Count, count.Label))
	}
	return strings.Join(parts, " · ")
}

// FormatTargetRollup renders a deployment that addresses several targets as
// one section, the way a sharded table rolls up its shards: one block per
// table and DDL across the targets, each DDL once and naming its targets when
// it runs on only some of them, and the targets that need an operator. Its size grows with distinct changes and failures, not with
// the number of targets.
func FormatTargetRollup(v RolloutView, g presentation.Group) string {
	var b strings.Builder
	status := FormatStateCounts(g.Counts)
	if steps, ok := v.Model.TableSteps(g); ok {
		status = withTableStepOutcomes(fmt.Sprintf("%d of %d tables done", steps.Done, steps.Steps), v.Model, steps, v.Model.TargetProgress(g))
	}
	fmt.Fprintf(&b, "%s %s — %s (%d targets)\n", g.Lead.Emoji, g.Deployment, status, len(g.Members))
	if !v.SetupPhase {
		writeTargetTables(&b, v, g)
	}
	writeTargetAttention(&b, v, g)
	// Table blocks carry their own trailing blank line, so the section is
	// trimmed to end on exactly one, whichever block closes it.
	return strings.TrimRight(b.String(), "\n") + "\n\n"
}

// writeTargetTables writes each table's progress across the targets that run
// it, one block per table and DDL. A block that runs on only some of the
// targets names them above its DDL. A target that has reported no table
// progress runs no block: missing detail is not a different change, so such
// targets are counted once below instead. Only a target still to run is
// waiting to report; one that already held the change was settled without
// running anything and is counted as such, and any other finished target is
// accounted for by the deployment's counts and the attention list.
func writeTargetTables(b *strings.Builder, v RolloutView, g presentation.Group) {
	var reporting []int
	silent, converged := 0, 0
	byMember := tablesByMember(v.Tables)
	signatures := make(map[string]bool)
	// settledUnreported counts the settled targets that reported no tables.
	// They are not waiting to report, so the footer leaves them out, but no
	// table detail is no evidence of a different change either.
	settledUnreported := 0
	for _, i := range g.Members {
		d := v.Model.Deployments[i]
		tables := byMember[rolloutMemberKey{d.Deployment, d.Target}]
		if len(tables) == 0 {
			switch {
			case d.AlreadyApplied():
				converged++
			// A settled target has its final outcome whether or not it
			// reported tables. A stopped one reports once the apply resumes.
			case !state.IsState(d.State, state.SettledApplyStates...):
				silent++
			default:
				settledUnreported++
			}
			continue
		}
		reporting = append(reporting, i)
		signatures[tableChangeSignature(tables)] = true
	}
	// A target that has not reported has started no table, so it ranks every
	// table as queued and a block that runs on every reporting target speaks
	// for it. When the reporting targets diverge it is not known to run any
	// one change, and no block speaks for it. A settled target with no tables
	// is not queued, but it is no evidence of a different change either, so a
	// block that runs on every reporting target speaks for it too, the way the
	// PR comment's does.
	lineSilent, lineSettled := silent, settledUnreported
	if len(signatures) > 1 {
		lineSilent, lineSettled = 0, 0
	}
	rolled := rolledTargetTables(v, byMember, reporting)
	settled := v.Model.RolloutSettled(g)
	for i := range rolled {
		rolled[i].UnreportedTargets = lineSilent
		rolled[i].RolloutSettled = settled
		if covered := len(rolled[i].Shards) + lineSilent + lineSettled; covered < len(g.Members)-converged {
			rolled[i].OnTargets = targetSubsetLabel(rolled[i].Shards, len(g.Members)-converged)
		}
	}
	sortRolledTables(rolled)
	// Tables are grouped under their namespace whenever they carry one, so
	// the same table changed in two schemas reads as two changes.
	if hasTableNamespaces(rolled) {
		b.WriteString(FormatNamespacedTables(rolled))
	} else {
		b.WriteString("\n")
		for _, t := range rolled {
			b.WriteString(FormatTableProgress(t))
			b.WriteString("\n")
		}
	}
	if converged > 0 {
		fmt.Fprintf(b, "  %s%d of %d targets already had this schema; nothing ran there.%s\n", ANSIDim, converged, len(g.Members), ANSIReset)
	}
	if silent > 0 {
		fmt.Fprintf(b, "  %s%d of %d targets have not reported progress yet.%s\n", ANSIDim, silent, len(g.Members), ANSIReset)
	}
}

// rolledTargetTables rolls each table and DDL the reporting members run up
// across the members that run it, in plan order: the first member's tables,
// then any table only a later member runs.
func rolledTargetTables(v RolloutView, byMember map[rolloutMemberKey][]TableProgress, members []int) []TableProgress {
	var rolled []TableProgress
	seen := make(map[string]bool)
	for _, i := range members {
		d := v.Model.Deployments[i]
		for _, t := range byMember[rolloutMemberKey{d.Deployment, d.Target}] {
			key := tableChangeKey(t)
			if seen[key] {
				continue
			}
			seen[key] = true
			rolled = append(rolled, tableAcrossTargets(v, byMember, members, t))
		}
	}
	return rolled
}

// targetSubsetLabel names the targets a table's DDL runs on when they are only
// some of the deployment's. Few read by name; more lead with how many of the
// deployment's targets they are, then name every one, since the list under
// the table is bounded and this label is the only place that says which
// targets run this DDL.
func targetSubsetLabel(targets []ShardProgress, total int) string {
	names := make([]string, len(targets))
	for i, t := range targets {
		names[i] = t.Shard
	}
	if len(targets) > memberNamesInlineLimit {
		return presentation.CoveragePhrase(presentation.TargetNoun, len(targets), total) + ": " + strings.Join(names, ", ")
	}
	if len(names) == 1 {
		return presentation.TargetNoun.Singular + " " + names[0]
	}
	return presentation.TargetNoun.Plural + " " + strings.Join(names, ", ")
}

// sortRolledTables orders tables rolled up across targets by
// presentation.TableRolloutRank, the order the PR comment lists them in, so a
// table finished on some targets stays above one no target has started. Tables
// of equal rank keep plan order. Each table's rank is computed once, so a
// watch frame does not rebuild it on every comparison.
func sortRolledTables(tables []TableProgress) {
	type ranked struct {
		table TableProgress
		rank  int
	}
	byRank := make([]ranked, len(tables))
	for i, t := range tables {
		byRank[i] = ranked{t, rolledTableRank(t)}
	}
	slices.SortStableFunc(byRank, func(a, b ranked) int { return a.rank - b.rank })
	for i, r := range byRank {
		tables[i] = r.table
	}
}

// rolledTableRank is a rolled-up table's presentation.TableRolloutRank, from
// its status on each target and its unreported targets queued.
func rolledTableRank(t TableProgress) int {
	statuses := make([]string, len(t.Shards), len(t.Shards)+t.UnreportedTargets)
	for i, target := range t.Shards {
		statuses[i] = target.Status
	}
	for range t.UnreportedTargets {
		statuses = append(statuses, state.Task.Pending)
	}
	return presentation.TableRolloutRank(statuses)
}

// rolloutMemberKey is the routing pair that names one rollout member.
type rolloutMemberKey struct {
	deployment, target string
}

// tablesByMember indexes the tables each rollout member copies, by both
// halves of the routing pair, as activeTablesForMember selects them. The
// rollup reads one member's tables once per table it rolls up, and the watch
// view renders on every frame, so the tables are walked once per render
// rather than once per lookup.
func tablesByMember(tables []TableProgress) map[rolloutMemberKey][]TableProgress {
	byMember := make(map[rolloutMemberKey][]TableProgress)
	for _, table := range tables {
		if table.TableName == "" {
			continue
		}
		key := rolloutMemberKey{table.Deployment, table.Target}
		byMember[key] = append(byMember[key], table)
	}
	return byMember
}

// tableChangeKey keys one table change by its namespace, table and DDL.
func tableChangeKey(t TableProgress) string {
	return t.Namespace + "\x00" + t.TableName + "\x00" + t.DDL
}

// tableChangeSignature keys the change a target runs by its tables and their
// DDL, independent of the order its tasks were listed in.
func tableChangeSignature(tables []TableProgress) string {
	parts := make([]string, len(tables))
	for i, t := range tables {
		parts[i] = tableChangeKey(t)
	}
	slices.Sort(parts)
	return strings.Join(parts, "\x01")
}

// tableAcrossTargets rolls table up across members: one entry per target,
// in the rollout's order, and the rows and ETA the engines reported for the
// targets that have started the change and not halted. A target's entry
// matches on DDL as well as table, so a table changed by two statements rolls
// up once per statement. The change reads as instant only when every target
// that ran it reports it instant: an engine decides that per target, so one
// target's instant ALTER says nothing about how another applied it. Once no
// target is working on the change and one has halted, those rows no longer
// speak for the table, which renders as its halt (formatHaltedAcrossTargets).
func tableAcrossTargets(v RolloutView, byMember map[rolloutMemberKey][]TableProgress, members []int, table TableProgress) TableProgress {
	rolled := TableProgress{
		TableName:     table.TableName,
		Namespace:     table.Namespace,
		Dialect:       table.Dialect,
		ChangeType:    table.ChangeType,
		DDL:           table.DDL,
		AcrossTargets: true,
	}
	statuses := make([]string, 0, len(members))
	allInstant := true
	for _, i := range members {
		d := v.Model.Deployments[i]
		for _, t := range byMember[rolloutMemberKey{d.Deployment, d.Target}] {
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
			allInstant = allInstant && t.IsInstant
			if countsTowardRolledRows(status) {
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
	rolled.IsInstant = len(statuses) > 0 && allInstant
	if rolled.RowsTotal > 0 {
		rolled.PercentComplete = int(min(rolled.RowsCopied, rolled.RowsTotal) * 100 / rolled.RowsTotal)
	}
	return rolled
}

// isHaltedAcrossTargets reports whether a table rolled up across targets has
// halted: no target is still working on the change, and one failed, is
// retrying, was stopped or was cancelled. Its rolled-up rows cover only the
// targets that finished, so no one bar or percentage speaks for the table.
func isHaltedAcrossTargets(t TableProgress) bool {
	return t.AcrossTargets && state.IsState(t.Status,
		state.Task.Failed, state.Task.FailedRetryable, state.Task.Stopped, state.Task.Cancelled)
}

// isPartlyCompletedAcrossTargets reports whether a table rolled up across
// targets has completed on some of them and is queued on the rest, with
// nothing in between. A target in its revert window has completed the change,
// and a target that has reported no progress has not started it. Its
// rolled-up status is queued, the revert window or complete, but the change is
// live only where it completed, so its line leads with that, as the PR
// comment's does.
func isPartlyCompletedAcrossTargets(t TableProgress) bool {
	if !t.AcrossTargets || !state.IsState(t.Status, state.Task.Pending, state.Task.RevertWindow, state.Task.Completed) {
		return false
	}
	completed, queued := partlyCompletedCounts(t.Shards)
	return completed > 0 && queued+t.UnreportedTargets > 0 && completed+queued == len(t.Shards)
}

// partlyCompletedCounts counts the targets that have completed a change and
// the targets still queued for it.
func partlyCompletedCounts(targets []ShardProgress) (completed, queued int) {
	for _, target := range targets {
		switch {
		case state.IsState(target.Status, state.Task.Completed, state.Task.RevertWindow):
			completed++
		case state.IsState(target.Status, state.Task.Pending):
			queued++
		}
	}
	return completed, queued
}

// formatPartlyCompletedAcrossTargets renders a table completed on some targets
// and queued on the rest: how many completed, then how many are queued, or not
// started once the rollout has settled, with each target's line beneath.
func formatPartlyCompletedAcrossTargets(t TableProgress) string {
	var b strings.Builder
	completed, queued := partlyCompletedCounts(t.Shards)
	fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: ✓ Complete on %d of %d targets · %d %s\n", t.TableName, completed, len(t.Shards)+t.UnreportedTargets, queued+t.UnreportedTargets, pendingTargetsWord(t))
	if t.DDL != "" {
		b.WriteString(formatTableDDL(t))
	}
	b.WriteString("\n")
	b.WriteString(formatTableParts(t))
	return b.String()
}

// isNotStartedAcrossTargets reports whether a table rolled up across targets
// is pending on every target of a rollout that has settled: no target ran it,
// and none will.
func isNotStartedAcrossTargets(t TableProgress) bool {
	return t.AcrossTargets && t.RolloutSettled && state.IsState(t.Status, state.Task.Pending)
}

// formatNotStartedAcrossTargets renders a table no target of a settled rollout
// ran the way the PR comment's table line does, with each target's line
// beneath.
func formatNotStartedAcrossTargets(t TableProgress) string {
	var b strings.Builder
	writeTableLine(&b, t, "⊘ Not started")
	if t.DDL != "" {
		b.WriteString(formatTableDDL(t))
	}
	b.WriteString("\n")
	b.WriteString(formatTableParts(t))
	return b.String()
}

// pendingTargetsWord is how a rolled-up table names the targets still pending
// on it: queued while the rollout runs, not started once it has settled.
func pendingTargetsWord(t TableProgress) string {
	return presentation.PendingWord(t.RolloutSettled)
}

// formatHaltedAcrossTargets renders a halted table across targets the way the
// PR comment's table line does: the table reads as its halt, with no bar, and
// each target's line beneath says where that target finished or halted.
func formatHaltedAcrossTargets(t TableProgress) string {
	var b strings.Builder
	writeTableLine(&b, t, "%s", haltedAcrossTargetsPhrase(t))
	if t.DDL != "" {
		b.WriteString(formatTableDDL(t))
	}
	b.WriteString("\n")
	b.WriteString(formatTableParts(t))
	return b.String()
}

// haltedAcrossTargetsPhrase names a halted table's state across its targets.
// A stop or cancel reads "not started" only when no target got as far as
// copying a row or completing the change: once one has, the change is
// partly or wholly live, and "not started" would be false.
func haltedAcrossTargetsPhrase(t TableProgress) string {
	notStarted := ""
	if !anyTargetStarted(t.Shards) {
		notStarted = " (not started)"
	}
	switch t.Status {
	case state.Task.Failed:
		return glyph.Failed + " Failed"
	case state.Task.FailedRetryable:
		return "Retrying"
	case state.Task.Stopped:
		return "⏹️ Stopped" + notStarted
	case state.Task.Cancelled:
		return "🚫 Cancelled" + notStarted
	default:
		return t.Status
	}
}

// anyTargetStarted reports whether any target completed the change or copied
// at least one row of it.
func anyTargetStarted(targets []ShardProgress) bool {
	for _, target := range targets {
		if target.Status == state.Task.Completed || target.RowsCopied > 0 {
			return true
		}
	}
	return false
}

// countsTowardRolledRows reports whether a target's rows join the table's
// rolled-up totals: a target still working on the change, including one past
// its row copy (catching up, checksumming, waiting for or cutting over), or
// one that has finished it. Leaving a finished-copy target out would sum only
// the targets still copying and read the rollout as further behind than the
// engines report. A queued target has reported nothing yet, and a halted one
// is named in its own line instead.
func countsTowardRolledRows(status string) bool {
	return state.IsInFlightTaskState(status) ||
		state.IsState(status, state.Task.Completed, state.Task.RevertWindow)
}

// inFlightRollupOrder is every in-flight task state, copying first and the
// rest in the order a target passes through them.
var inFlightRollupOrder = []string{
	state.Task.Running,
	state.Task.WaitingForDeploy,
	state.Task.Recovering,
	state.Task.CatchingUp,
	state.Task.Checksumming,
	state.Task.PostChecksum,
	state.Task.WaitingForCutover,
	state.Task.CuttingOver,
	state.Task.Reverting,
}

// rollupTaskStatus is a table's status across targets. While any target is
// still working on the change the table reads as in flight, never as failed:
// copying first, so the bar keeps showing the rows still moving, otherwise
// the earliest phase any target is in, so the table never reads further along
// than its slowest target. The targets that failed are named in the
// per-target lines and the attention list beneath it. With nothing in
// flight, a failure or halt comes first, then queued, then complete.
func rollupTaskStatus(statuses []string) string {
	for _, phase := range inFlightRollupOrder {
		if slices.Contains(statuses, phase) {
			return phase
		}
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
// and the data-plane operation and apply to look at, the way a sharded apply
// names a failed shard.
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
		if externalOperationID := d.ExternalOperationID; externalOperationID != "" {
			fmt.Fprintf(b, "      %sExternal operation ID: %s%s\n", ANSIDim, externalOperationID, ANSIReset)
		}
		if externalID := d.ExternalID; externalID != "" {
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

// memberTargetName is a member's target, or its name when it carries none.
func memberTargetName(d presentation.Deployment) string {
	if d.Target == "" {
		return d.Name
	}
	return d.Target
}

// FormatRolloutFooter writes the one command the rollout is waiting on, at
// the bottom where an operator looks for it: the pending action (cut over
// when the apply defers cutover, resume, retry), or release while a failure
// holds the rollout. Every command addresses the whole apply. Whether stop
// follows is presentation.Apply.OffersRolloutStop, which the PR comment's
// footer decides with too: a pending action never takes stop away from live
// work, a terminal apply refuses stop, and a failure on an apply that is
// still active is offered stop first, since a new apply is refused until this
// one settles.
func FormatRolloutFooter(v RolloutView) string {
	var b strings.Builder
	applyCommand := func(verb string) string {
		return fmt.Sprintf("%s %s %s -e %s", cliname.Name(), verb, v.ApplyID, v.Environment)
	}
	next := v.Model.NextAction
	switch next.Kind {
	case presentation.NextActionCutover:
		// Only an apply started with --defer-cutover waits for an operator at
		// each cutover; otherwise SchemaBot cuts the ready member over itself,
		// and offering the command would contradict that.
		if !v.DeferCutover {
			fmt.Fprintf(&b, "SchemaBot will cut over %s next — no action needed.\n", next.Name)
			break
		}
		writeRolloutCommand(&b, fmt.Sprintf("To cut over %s", next.Name), applyCommand("cutover"))
	case presentation.NextActionResume:
		writeRolloutCommand(&b, "To resume from where it stopped", applyCommand("start"))
	case presentation.NextActionReviewFailure:
		// A failed apply is recovered by a fresh apply, which resumes from
		// where this one stopped. Progress is read by apply ID and does not
		// know which schema directory the apply was planned from, so the
		// operator fills it in. Until the apply is terminal the fresh apply
		// is refused, so stop is offered below instead.
		if v.Model.OffersRetry() {
			writeRolloutCommand(&b, presentation.RetryLabel, fmt.Sprintf("%s apply -s <schema_dir> -e %s", cliname.Name(), v.Environment))
		}
	case presentation.NextActionNone:
	}
	if state.IsState(v.Model.State, state.Apply.Paused) {
		writeRolloutCommand(&b, "Paused after a failure — to let the held deployments proceed", applyCommand("release"))
	}
	if !v.Model.OffersRolloutStop() {
		return b.String()
	}
	verb, label := "stop", "To stop this schema change"
	if strings.EqualFold(v.Engine, storage.EnginePlanetScale) {
		verb, label = "cancel", "To cancel this schema change"
	}
	writeRolloutCommand(&b, label, applyCommand(verb))
	if v.Model.RetryWaitsOnActiveApply() {
		fmt.Fprintf(&b, "%s\n", presentation.RetryOnceSettledNote)
	}
	return b.String()
}

// writeRolloutCommand writes one labelled command of the rollout footer.
func writeRolloutCommand(b *strings.Builder, label, command string) {
	fmt.Fprintf(b, "%s:\n  %s%s%s\n", label, ANSICyan, command, ANSIReset)
}
