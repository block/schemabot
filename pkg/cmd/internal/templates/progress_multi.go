package templates

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/ui"
)

func writeMultiDeploymentProgress(data ProgressData) {
	model := presentation.Derive(progressOperationsForPresentation(data.Operations, data.Released))

	writeMultiDeploymentHeader(data, model)
	writeMultiDeploymentFirstFailure(model.FirstFailure)
	writeMultiDeploymentNextAction(model.NextAction)
	fmt.Println()

	for _, group := range GroupOperationIndicesByDeployment(data.Operations) {
		if len(group) == 1 {
			writeDeploymentProgressSection(model.Deployments[group[0]], data.Operations[group[0]], data)
			continue
		}
		writeGroupedDeploymentProgressSection(group, model, data)
	}
}

// GroupOperationIndicesByDeployment groups operation indices by deployment
// name, preserving first-appearance order. Multi-deployment rollouts have one
// operation per deployment, so every group is a singleton; engines that fan
// one deployment out into several operations (per-shard work plus finalizers)
// produce larger groups that render as one deployment section.
func GroupOperationIndicesByDeployment(ops []ProgressOperation) [][]int {
	byName := make(map[string][]int, len(ops))
	var order []string
	for i, op := range ops {
		if _, seen := byName[op.Deployment]; !seen {
			order = append(order, op.Deployment)
		}
		byName[op.Deployment] = append(byName[op.Deployment], i)
	}
	groups := make([][]int, 0, len(order))
	for _, name := range order {
		groups = append(groups, byName[name])
	}
	return groups
}

// progressOperationsForPresentation maps the parsed progress operations to the
// surface-neutral presentation inputs. released is the apply-level release latch
// (from ProgressData.Released): a released pause behaves like continue, so the
// held siblings proceed and the aggregate runs degraded instead of paused.
func progressOperationsForPresentation(ops []ProgressOperation, released bool) []presentation.Operation {
	presentationOps := make([]presentation.Operation, 0, len(ops))
	for _, op := range ops {
		presentationOps = append(presentationOps, presentation.Operation{
			Deployment:        op.Deployment,
			State:             op.State,
			Barrier:           op.CutoverPolicy == storage.CutoverPolicyBarrier,
			Parallel:          op.CutoverPolicy == storage.CutoverPolicyParallel,
			ContinueOnFailure: op.OnFailure == storage.OnFailureContinue,
			PauseOnFailure:    op.OnFailure == storage.OnFailurePause,
			Released:          released,
			Error:             op.ErrorMessage,
		})
	}
	return presentationOps
}

func writeMultiDeploymentHeader(data ProgressData, model presentation.Apply) {
	rows := []BoxRow{}
	if data.ApplyID != "" {
		rows = append(rows, BoxRow{"Apply ID", data.ApplyID})
	}
	if data.Environment != "" {
		rows = append(rows, BoxRow{"Environment", data.Environment})
	}
	rows = append(rows, BoxRow{"State", model.Label})
	if row, ok := volumeBoxRow(data.Volume, model.State); ok {
		rows = append(rows, row)
	}
	if data.Caller != "" {
		rows = append(rows, BoxRow{"Caller", data.Caller})
	}
	if data.PullRequestURL != "" {
		rows = append(rows, BoxRow{"PR", data.PullRequestURL})
	}
	if data.StartedAt != "" {
		if started, err := time.Parse(time.RFC3339, data.StartedAt); err == nil {
			rows = append(rows, BoxRow{"Started", started.Format("Jan 2 15:04:05 MST")})
		}
	}
	if dur := formatApplyDuration(data.StartedAt, data.CompletedAt); dur != "-" {
		rows = append(rows, BoxRow{"Duration", dur})
	}
	if counts := formatDeploymentCounts(model.Counts); counts != "" {
		rows = append(rows, BoxRow{operationCountsLabel(data.Operations), counts})
	}
	WriteBox(rows, "State", stateColorFunc(model.State))
}

// operationCountsLabel names the header histogram row. The per-status counts
// tally operations; for a classic rollout that equals deployments, so the
// deployment-facing label stays. When a deployment fans out into several
// operations the counts exceed the deployment count, and labeling them
// "Deployments" would misstate the topology.
func operationCountsLabel(ops []ProgressOperation) string {
	seen := make(map[string]struct{}, len(ops))
	for _, op := range ops {
		if _, dup := seen[op.Deployment]; dup {
			return "Operations"
		}
		seen[op.Deployment] = struct{}{}
	}
	return "Deployments"
}

func formatDeploymentCounts(counts []presentation.StateCount) string {
	parts := make([]string, 0, len(counts))
	for _, count := range counts {
		parts = append(parts, fmt.Sprintf("%d %s", count.Count, count.Label))
	}
	return strings.Join(parts, " · ")
}

func writeMultiDeploymentFirstFailure(failure *presentation.Deployment) {
	if failure == nil {
		return
	}
	if failure.Error == "" {
		fmt.Printf("\n  %s⚠ First failure: %s%s\n", ANSIRed, failure.Deployment, ANSIReset)
		return
	}
	fmt.Printf("\n  %s⚠ First failure: %s — %s%s\n", ANSIRed, failure.Deployment, failure.Error, ANSIReset)
}

func writeMultiDeploymentNextAction(next presentation.NextAction) {
	switch next.Kind {
	case presentation.NextActionCutover:
		fmt.Printf("\n  Next: cut over %s\n", next.Deployment)
	case presentation.NextActionResume:
		fmt.Println("\n  Next: resume apply")
	case presentation.NextActionReviewFailure:
		if next.Deployment == "" {
			fmt.Println("\n  Next: review failure")
			return
		}
		fmt.Printf("\n  Next: review failure in %s\n", next.Deployment)
	case presentation.NextActionNone:
	}
}

func writeDeploymentProgressSection(deployment presentation.Deployment, op ProgressOperation, data ProgressData) {
	fmt.Printf("%s %s — %s", deployment.Emoji, deployment.Deployment, deployment.Label)
	if op.Target != "" {
		fmt.Printf(" (%s)", op.Target)
	}
	fmt.Println()
	if op.ExternalOperationID != "" {
		fmt.Printf("  %sExternal operation ID: %s%s\n", ANSIDim, op.ExternalOperationID, ANSIReset)
	}
	if op.ExternalID != "" {
		fmt.Printf("  %sExternal apply ID: %s%s\n", ANSIDim, op.ExternalID, ANSIReset)
	}

	if deployment.Error != "" {
		fmt.Printf("  %s%s%s\n", ANSIRed, deployment.Error, ANSIReset)
	}

	writeDeploymentTables(deployment.Deployment, data)
	fmt.Println()
}

// writeGroupedDeploymentProgressSection renders one section for a deployment
// that owns several operations. The header aggregates the group with the same
// policy-aware projection the apply header uses; each operation then gets one
// line carrying its own kind, status, and external identifiers, so an operator
// can drill into the exact remote apply. The deployment's tables render once —
// task rows are deployment-scoped, not operation-scoped.
func writeGroupedDeploymentProgressSection(group []int, model presentation.Apply, data ProgressData) {
	ops := make([]ProgressOperation, 0, len(group))
	for _, i := range group {
		ops = append(ops, data.Operations[i])
	}
	sub := presentation.Derive(progressOperationsForPresentation(ops, data.Released))

	fmt.Printf("%s %s — %s", AggregateStateEmoji(sub.State), ops[0].Deployment, formatDeploymentCounts(sub.Counts))
	if ops[0].Target != "" {
		fmt.Printf(" (%s)", ops[0].Target)
	}
	fmt.Println()

	for n, i := range group {
		deployment := model.Deployments[i]
		op := data.Operations[i]
		fmt.Printf("  %s %s — %s%s%s%s\n",
			deployment.Emoji, OperationKindLabel(op, n), deployment.Label, ANSIDim, OperationIdentifierSuffix(op), ANSIReset)
		if deployment.Error != "" {
			fmt.Printf("      %s%s%s\n", ANSIRed, deployment.Error, ANSIReset)
		}
	}

	writeDeploymentTables(ops[0].Deployment, data)
	fmt.Println()
}

// OperationKindLabel names one operation of a grouped deployment section. The
// stored kind (work, group_finalizer, ...) is the operator-facing name; when a
// server predates the kind field the 1-based position keeps the line unique.
func OperationKindLabel(op ProgressOperation, n int) string {
	if op.OperationKind != "" {
		return op.OperationKind
	}
	return fmt.Sprintf("operation %d", n+1)
}

// OperationIdentifierSuffix renders the external identifiers an operator needs
// to find this operation in the remote data plane, as a dimmed suffix.
func OperationIdentifierSuffix(op ProgressOperation) string {
	var sb strings.Builder
	if op.ExternalOperationID != "" {
		fmt.Fprintf(&sb, " · external operation ID %s", op.ExternalOperationID)
	}
	if op.ExternalID != "" {
		fmt.Fprintf(&sb, " · external apply ID %s", op.ExternalID)
	}
	return sb.String()
}

// AggregateStateEmoji is the status glyph for a grouped deployment's derived
// aggregate state, mirroring the per-operation glyph vocabulary.
func AggregateStateEmoji(s string) string {
	switch {
	case state.IsState(s, state.Apply.Completed):
		return "✅"
	case state.IsState(s, state.Apply.Failed):
		return "❌"
	case state.IsState(s, state.Apply.FailedRetryable), state.IsState(s, state.Apply.CuttingOver):
		return "🔁"
	case state.IsState(s, state.Apply.WaitingForCutover):
		return "🟡"
	case state.IsState(s, state.Apply.Stopped), state.IsState(s, state.Apply.Paused):
		return "⏸"
	case state.IsState(s, state.Apply.Cancelled):
		return "⛔"
	case state.IsState(s, state.Apply.Reverted):
		return "↩️"
	case state.IsState(s, state.Apply.Pending), state.IsState(s, state.Apply.RevertWindow):
		return "⏳"
	default:
		return "🔄"
	}
}

// writeDeploymentTables renders the deployment's active table progress once
// per deployment section, outside the setup phase.
func writeDeploymentTables(deployment string, data ProgressData) {
	tables := activeTablesForDeployment(data.Tables, deployment)
	if len(tables) == 0 || state.IsSetupPhase(data.State) {
		return
	}
	sortActiveTables(tables)
	if hasTableNamespaces(tables) {
		fmt.Print(FormatNamespacedTables(tables))
		return
	}
	fmt.Println()
	for _, table := range tables {
		fmt.Print(FormatTableProgress(table))
	}
}

func activeTablesForDeployment(tables []TableProgress, deployment string) []TableProgress {
	activeTables := make([]TableProgress, 0, len(tables))
	for _, table := range tables {
		if table.Deployment == deployment && table.TableName != "" {
			activeTables = append(activeTables, table)
		}
	}
	return activeTables
}

func sortActiveTables(tables []TableProgress) {
	sort.SliceStable(tables, func(i, j int) bool {
		pi := ui.TableStatePriority(state.NormalizeTaskStatus(tables[i].Status))
		pj := ui.TableStatePriority(state.NormalizeTaskStatus(tables[j].Status))
		if pi != pj {
			return pi < pj
		}
		si := len(tables[i].Shards) > 0
		sj := len(tables[j].Shards) > 0
		if si != sj {
			return si
		}
		return false
	})
}

func hasTableNamespaces(tables []TableProgress) bool {
	for _, table := range tables {
		if table.Namespace != "" {
			return true
		}
	}
	return false
}
