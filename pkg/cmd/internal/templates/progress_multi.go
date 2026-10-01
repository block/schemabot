package templates

import (
	"fmt"
	"sort"
	"time"

	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/ui"
)

func writeMultiDeploymentProgress(data ProgressData) {
	model := presentation.Derive(ProgressOperationsForPresentation(data.Operations, data.Released))
	groups := model.Groups()
	view := RolloutView{
		ApplyID:      data.ApplyID,
		Environment:  data.Environment,
		Engine:       data.Engine,
		Model:        model,
		Tables:       data.Tables,
		SetupPhase:   state.IsSetupPhase(data.State),
		DeferCutover: data.Options["defer_cutover"] == "true",
	}

	writeMultiDeploymentHeader(data, model, groups)
	writeMultiDeploymentFirstFailure(model.FirstFailure)
	fmt.Println()

	// A deployment that addresses several targets renders as one rollup
	// section; any other member keeps a section of its own. Group members
	// index model.Deployments, which Derive returns index-parallel to
	// data.Operations, so each section renders its own operation's
	// identifiers — a keyed apply has many operations on the same deployment
	// name, so a name-based lookup cannot tell the sections apart.
	for _, g := range groups {
		if len(g.Members) > 1 {
			fmt.Print(FormatTargetRollup(view, g))
			continue
		}
		i := g.Members[0]
		writeDeploymentProgressSection(model.Deployments[i], data.Operations[i], data)
	}
	fmt.Print(FormatThrottleReference(data.Tables))
	fmt.Print(FormatRolloutFooter(view))
}

// ProgressOperationsForPresentation maps the parsed progress operations to the
// surface-neutral presentation inputs, for both the progress output and the
// watch view. released is the apply-level release latch (from
// ProgressData.Released): a released pause behaves like continue, so the held
// siblings proceed and the aggregate runs degraded instead of paused. The
// operation's key, kind and start time carry through, so the header settles
// exactly as the stored apply state does.
func ProgressOperationsForPresentation(ops []ProgressOperation, released bool) []presentation.Operation {
	presentationOps := make([]presentation.Operation, 0, len(ops))
	for _, op := range ops {
		presentationOps = append(presentationOps, presentation.Operation{
			Deployment:          op.Deployment,
			Target:              op.Target,
			State:               op.State,
			OperationKey:        op.OperationKey,
			Work:                op.OperationKind == storage.ApplyOperationKindWork,
			Finalizer:           op.OperationKind == storage.ApplyOperationKindGroupFinalizer,
			NeverStarted:        op.StartedAt == "",
			Barrier:             op.CutoverPolicy == storage.CutoverPolicyBarrier,
			Parallel:            op.CutoverPolicy == storage.CutoverPolicyParallel,
			ContinueOnFailure:   op.OnFailure == storage.OnFailureContinue,
			PauseOnFailure:      op.OnFailure == storage.OnFailurePause,
			Released:            released,
			Error:               op.ErrorMessage,
			ExternalID:          op.ExternalID,
			ExternalOperationID: op.ExternalOperationID,
		})
	}
	return presentationOps
}

func writeMultiDeploymentHeader(data ProgressData, model presentation.Apply, groups []presentation.Group) {
	rows := []BoxRow{}
	if data.ApplyID != "" {
		rows = append(rows, BoxRow{"Apply ID", data.ApplyID})
	}
	if data.Environment != "" {
		rows = append(rows, BoxRow{"Environment", data.Environment})
	}
	rows = append(rows, BoxRow{"State", model.Label})
	rows = append(rows, callerAndSourceBoxRows(data.Caller, data.PullRequestURL)...)
	if data.StartedAt != "" {
		if started, err := time.Parse(time.RFC3339, data.StartedAt); err == nil {
			rows = append(rows, BoxRow{"Started", started.Format("Jan 2 15:04:05 MST")})
		}
	}
	if dur := formatApplyDuration(data.StartedAt, data.CompletedAt); dur != "-" {
		rows = append(rows, BoxRow{"Duration", dur})
	}
	if counts := FormatStateCounts(model.Counts); counts != "" {
		rows = append(rows, BoxRow{RolloutCountsUnit(groups), counts})
	}
	WriteBox(rows, "State", stateColorFunc(model.State))
}

func writeMultiDeploymentFirstFailure(failure *presentation.Deployment) {
	if failure == nil {
		return
	}
	if failure.Error == "" {
		fmt.Printf("\n  %s"+glyph.Failed+" First failure: %s%s\n", ANSIRed, failure.Name, ANSIReset)
		return
	}
	fmt.Printf("\n  %s"+glyph.Failed+" First failure: %s — %s%s\n", ANSIRed, failure.Name, failure.Error, ANSIReset)
}

func writeDeploymentProgressSection(deployment presentation.Deployment, op ProgressOperation, data ProgressData) {
	fmt.Printf("%s %s", deployment.Emoji, deployment.Name)
	if op.OperationKey != "" {
		fmt.Printf(" · %s", op.OperationKey)
	}
	fmt.Printf(" — %s", deployment.Label)
	// A member whose name already carries its target does not repeat it in the
	// trailing parenthetical.
	if target := sectionTarget(op, data.Operations); target != "" && deployment.Name == deployment.Deployment {
		fmt.Printf(" (%s)", target)
	}
	fmt.Println()
	// The external operation ID identifies this operation's own data-plane row,
	// so it never falls back to a sibling's value.
	if op.ExternalOperationID != "" {
		fmt.Printf("  %sExternal operation ID: %s%s\n", ANSIDim, op.ExternalOperationID, ANSIReset)
	}
	if externalID := SectionExternalID(op, data.Operations); externalID != "" {
		fmt.Printf("  %sExternal apply ID: %s%s\n", ANSIDim, externalID, ANSIReset)
	}

	if deployment.Error != "" {
		fmt.Printf("  %s%s%s\n", ANSIRed, deployment.Error, ANSIReset)
	}

	// The selector takes the member's recorded target, not the resolved one the
	// header shows: a table row carries whatever its own operation carried, so
	// matching an inherited value would look for a target the rows do not have.
	tables := activeTablesForMember(data.Tables, deployment.Deployment, deployment.Target)
	if len(tables) > 0 && !state.IsSetupPhase(data.State) {
		sortActiveTables(tables)
		if hasTableNamespaces(tables) {
			fmt.Print(FormatNamespacedTables(tables))
		} else {
			fmt.Println()
			for _, table := range tables {
				fmt.Print(FormatTableProgress(table))
			}
		}
	}
	fmt.Println()
}

// sectionTarget resolves the target shown in a section header: the section's
// own operation when set, falling back to the one its deployment shares.
func sectionTarget(op ProgressOperation, ops []ProgressOperation) string {
	if op.Target != "" {
		return op.Target
	}
	target, _ := sharedAcrossDeployment(op, ops, func(o ProgressOperation) string { return o.Target })
	return target
}

// SectionExternalID resolves the external apply ID shown for a rollout member:
// the member's own operation when set, falling back to the one its deployment
// shares. It is exported because the progress renderer and the watch TUI both
// label a member with it, and a member must not be told two different apply IDs
// depending on which surface an operator is reading.
//
// The fallback is gated on the deployment addressing a single target, not on
// the IDs themselves agreeing. A deployment addressing several targets runs a
// separate data-plane apply per member, so the first member to dispatch is the
// only one carrying an ID at all: agreement among the IDs present would be
// vacuously true and would hand that ID to every member still waiting.
func SectionExternalID(op ProgressOperation, ops []ProgressOperation) string {
	if op.ExternalID != "" {
		return op.ExternalID
	}
	if _, single := sharedAcrossDeployment(op, ops, func(o ProgressOperation) string { return o.Target }); !single {
		return ""
	}
	externalID, _ := sharedAcrossDeployment(op, ops, func(o ProgressOperation) string { return o.ExternalID })
	return externalID
}

// sharedAcrossDeployment returns the value every operation of op's deployment
// agrees on, and reports whether they agree. Operations carrying no value at
// all agree with anything: a value none of them has recorded yet is not a
// disagreement.
//
// A keyed apply runs several operations of one deployment against one target
// and through one data-plane apply, so an operation that has not dispatched yet
// can take its value from a sibling. A deployment that addresses several targets
// has no such shared value: taking one there would label a member with another
// target's apply, telling an operator to go look at a member they are not
// watching. Showing nothing is the honest answer, and it resolves on the next
// poll once the operation dispatches and carries its own.
func sharedAcrossDeployment(op ProgressOperation, ops []ProgressOperation, valueOf func(ProgressOperation) string) (shared string, agreed bool) {
	for _, sibling := range ops {
		if sibling.Deployment != op.Deployment {
			continue
		}
		value := valueOf(sibling)
		if value == "" {
			continue
		}
		if shared != "" && shared != value {
			return "", false
		}
		shared = value
	}
	return shared, true
}

// activeTablesForMember selects the tables copied by one rollout member. Both
// halves of the routing pair are matched: two targets of one deployment each
// copy the same tables, and matching the deployment alone would list both
// members' copies under each of them.
func activeTablesForMember(tables []TableProgress, deployment, target string) []TableProgress {
	activeTables := make([]TableProgress, 0, len(tables))
	for _, table := range tables {
		if table.Deployment == deployment && table.Target == target && table.TableName != "" {
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
