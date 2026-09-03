package templates

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/glyph"
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

	// Derive returns one Deployment per input operation, in input order, so
	// model.Deployments[i] is the projection of data.Operations[i]. Pairing them
	// by index lets each section render its own operation's identifiers — a
	// keyed apply has many operations on the same deployment name, so a
	// name-based lookup cannot tell the sections apart.
	for i, deployment := range model.Deployments {
		writeDeploymentProgressSection(deployment, data.Operations[i], data)
	}
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
			Target:            op.Target,
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
	rows = append(rows, callerAndSourceBoxRows(data.Caller, data.PullRequestURL)...)
	if data.StartedAt != "" {
		if started, err := time.Parse(time.RFC3339, data.StartedAt); err == nil {
			rows = append(rows, BoxRow{"Started", started.Format("Jan 2 15:04:05 MST")})
		}
	}
	if dur := formatApplyDuration(data.StartedAt, data.CompletedAt); dur != "-" {
		rows = append(rows, BoxRow{"Duration", dur})
	}
	if counts := formatDeploymentCounts(model.Counts); counts != "" {
		rows = append(rows, BoxRow{"Deployments", counts})
	}
	WriteBox(rows, "State", stateColorFunc(model.State))
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
		fmt.Printf("\n  %s"+glyph.Failed+" First failure: %s%s\n", ANSIRed, failure.Name, ANSIReset)
		return
	}
	fmt.Printf("\n  %s"+glyph.Failed+" First failure: %s — %s%s\n", ANSIRed, failure.Name, failure.Error, ANSIReset)
}

func writeMultiDeploymentNextAction(next presentation.NextAction) {
	switch next.Kind {
	case presentation.NextActionCutover:
		fmt.Printf("\n  Next: cut over %s\n", next.Name)
	case presentation.NextActionResume:
		fmt.Println("\n  Next: resume apply")
	case presentation.NextActionReviewFailure:
		if next.Name == "" {
			fmt.Println("\n  Next: review failure")
			return
		}
		fmt.Printf("\n  Next: review failure in %s\n", next.Name)
	case presentation.NextActionNone:
	}
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
	if externalID := sectionExternalID(op, data.Operations); externalID != "" {
		fmt.Printf("  %sExternal apply ID: %s%s\n", ANSIDim, externalID, ANSIReset)
	}

	if deployment.Error != "" {
		fmt.Printf("  %s%s%s\n", ANSIRed, deployment.Error, ANSIReset)
	}

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

// deploymentTarget reports the one target every operation of op's deployment
// addresses, and whether there is exactly one. It is what decides when an
// operation that has not recorded its own routing may inherit a sibling's: a
// deployment addressing several targets runs a separate data-plane apply per
// target, so nothing about one member can be read off another.
func deploymentTarget(op ProgressOperation, ops []ProgressOperation) (target string, single bool) {
	for _, sibling := range ops {
		if sibling.Deployment != op.Deployment || sibling.Target == "" {
			continue
		}
		if target != "" && target != sibling.Target {
			return "", false
		}
		target = sibling.Target
	}
	return target, true
}

// sectionTarget resolves the target shown in a section header: the section's
// own operation when set, otherwise its deployment's when the deployment
// addresses a single one. A deployment addressing several shows no target
// rather than another member's.
func sectionTarget(op ProgressOperation, ops []ProgressOperation) string {
	if op.Target != "" {
		return op.Target
	}
	target, _ := deploymentTarget(op, ops)
	return target
}

// sectionExternalID resolves the external apply ID shown in a section: the
// section's own operation when set, falling back to a sibling's — a keyed
// apply's operations share one data-plane apply, so an operation that has not
// dispatched yet still shows it. The fallback applies only while the deployment
// addresses a single target, so it never shows one member the ID of another.
func sectionExternalID(op ProgressOperation, ops []ProgressOperation) string {
	if op.ExternalID != "" {
		return op.ExternalID
	}
	if _, single := deploymentTarget(op, ops); !single {
		return ""
	}
	for _, sibling := range ops {
		if sibling.Deployment == op.Deployment && sibling.ExternalID != "" {
			return sibling.ExternalID
		}
	}
	return ""
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
