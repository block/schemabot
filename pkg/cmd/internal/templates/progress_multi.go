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

	for _, deployment := range model.Deployments {
		writeDeploymentProgressSection(deployment, data)
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

func writeDeploymentProgressSection(deployment presentation.Deployment, data ProgressData) {
	fmt.Printf("%s %s — %s", deployment.Emoji, deployment.Deployment, deployment.Label)
	if target := targetForDeployment(data.Operations, deployment.Deployment); target != "" {
		fmt.Printf(" (%s)", target)
	}
	fmt.Println()
	if externalOperationID := externalOperationIDForDeployment(data.Operations, deployment.Deployment); externalOperationID != "" {
		fmt.Printf("  %sExternal operation ID: %s%s\n", ANSIDim, externalOperationID, ANSIReset)
	}
	if externalID := externalIDForDeployment(data.Operations, deployment.Deployment); externalID != "" {
		fmt.Printf("  %sExternal apply ID: %s%s\n", ANSIDim, externalID, ANSIReset)
	}

	if deployment.Error != "" {
		fmt.Printf("  %s%s%s\n", ANSIRed, deployment.Error, ANSIReset)
	}

	tables := activeTablesForDeployment(data.Tables, deployment.Deployment)
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

func targetForDeployment(ops []ProgressOperation, deployment string) string {
	for _, op := range ops {
		if op.Deployment == deployment {
			return op.Target
		}
	}
	return ""
}

func externalOperationIDForDeployment(ops []ProgressOperation, deployment string) string {
	for _, op := range ops {
		if op.Deployment == deployment && op.ExternalOperationID != "" {
			return op.ExternalOperationID
		}
	}
	return ""
}

func externalIDForDeployment(ops []ProgressOperation, deployment string) string {
	for _, op := range ops {
		if op.Deployment == deployment && op.ExternalID != "" {
			return op.ExternalID
		}
	}
	return ""
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
