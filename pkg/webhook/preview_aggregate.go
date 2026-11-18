package webhook

import (
	"strings"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// PreviewAggregateSummary renders a representative aggregate-check Details
// summary for TEMPLATES.md: the leader's own per-database checks with their
// change summaries and statuses, plus a folded Tenant deployments section. The
// mix of running, applied, and pending rows produces a blocking conclusion so
// both tables and a non-passing title are exercised.
func PreviewAggregateSummary() string {
	checks := []*storage.Check{
		{
			DatabaseType:  "vitess",
			DatabaseName:  "commerce",
			HasChanges:    true,
			Status:        checkStatusInProgress,
			ChangeSummary: "2 creates, 1 alter · 2 vschema updates",
		},
		{
			DatabaseType:  "mysql",
			DatabaseName:  "orders",
			HasChanges:    true,
			Status:        checkStatusCompleted,
			Conclusion:    checkConclusionSuccess,
			ChangeSummary: "1 alter",
		},
		{
			DatabaseType: aggregateSentinel,
			DatabaseName: "tenant-b",
			HasChanges:   true,
			Status:       checkStatusCompleted,
			Conclusion:   checkConclusionSuccess,
		},
		{
			DatabaseType: aggregateSentinel,
			DatabaseName: "tenant-c",
			HasChanges:   true,
			Status:       checkStatusInProgress,
		},
	}

	conclusion, _ := computeAggregate(checks, nil)
	title, summary := aggregateSummary(checks, conclusion, nil)
	return title + "\n\n" + summary
}

// PreviewAggregateCheckStopped renders the aggregate check published while a
// stopped apply holds the PR: the check concludes action_required so merge
// stays blocked without a run left spinning, and the title names the pause so
// a reader looks for the operator decision the apply is waiting on.
func PreviewAggregateCheckStopped() string {
	const stoppedApplyID = 42
	checks := []*storage.Check{
		{
			DatabaseType:  "mysql",
			DatabaseName:  "orders",
			ApplyID:       stoppedApplyID,
			HasChanges:    true,
			Status:        checkStatusInProgress,
			ChangeSummary: "1 alter",
		},
	}

	stopped := stoppedApplyIDs{stoppedApplyID: true}
	conclusion, _ := computeAggregate(checks, stopped)
	title, summary := aggregateSummary(checks, conclusion, stopped)
	return title + "\n\n" + summary
}

// PreviewAggregateCheckRollout renders the aggregate check of a production
// environment rolled out to four targets, at three points of one rollout: an
// apply narrowed to shop-002 has completed, a plan of the whole environment
// then finds the change pending on the other three, and the apply of the rest
// is running. The title and the row both say which targets the work is on, so
// the merge box and the check answer how far the change has rolled out.
func PreviewAggregateCheckRollout() string {
	row := func(status, conclusion string, outcome reviewDriftOutcome) *storage.Check {
		targets, withWork := outcome.work.rolloutCounts()
		return &storage.Check{
			DatabaseType: "mysql", DatabaseName: "orders", HasChanges: true,
			Status: status, Conclusion: conclusion,
			ChangeSummary: outcome.changeSummary("1 alter"), RolloutTargets: targets, RolloutTargetsWithWork: withWork,
		}
	}
	rest := memberWork{pending: 3, members: 4, names: []string{"shop-001", "shop-003", "shop-004"}}
	narrowed := &storage.Check{
		DatabaseType: "mysql", DatabaseName: "orders", HasChanges: true,
		Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired,
		BlockingReason: narrowedApplyBlock.blockingReason, ChangeSummary: narrowedApplyCompletedSummary("shop-002"),
	}
	scenarios := []struct {
		heading string
		check   *storage.Check
	}{
		{"After `schemabot apply -e production --target shop-002`", narrowed},
		{"After `schemabot plan -e production`", row(checkStatusCompleted, checkConclusionActionRequired, reviewDriftOutcome{state: driftClean, work: rest})},
		{"While `schemabot apply -e production` runs", row(checkStatusInProgress, "", reviewDriftOutcome{state: driftClean, work: rest, dispatchesApply: true})},
	}
	var sections []string
	for _, sc := range scenarios {
		checks := []*storage.Check{sc.check}
		conclusion, _ := computeAggregate(checks, nil)
		title, summary := aggregateSummary(checks, conclusion, nil)
		sections = append(sections, "**"+sc.heading+"**\n\n"+title+"\n\n"+summary)
	}
	return strings.Join(sections, "\n\n")
}

// PreviewAggregateCheckFileCapBlocked renders the failing aggregate-check
// output published when a PR changes more files than GitHub will report for a
// single pull request: the fixed failure title plus the cap message telling
// the author to split the PR so SchemaBot can see the full changed-file list.
func PreviewAggregateCheckFileCapBlocked() string {
	return "Plan failed\n\n" + prFileCapExceededBlock.message
}

// PreviewRollbackConfirmPlanWithoutRouting renders the refusal of a
// rollback-confirm whose pinned rollback plan was stored without the
// deployment it routes to, through the renderer rollback-confirm uses.
func PreviewRollbackConfirmPlanWithoutRouting() string {
	return templates.RenderGenericError(templates.SchemaErrorData{
		RequestedBy: "jackjackbits", Timestamp: "2026-01-15 14:30:00",
		Environment: "staging", CommandName: action.RollbackConfirm,
		ErrorDetail: rollbackExecutionErrorMessage("staging", "", &api.PlanRoutingMetadataError{PlanID: "plan_8f2c41", Field: "deployment"}),
	})
}

// PreviewConfirmationPrimaryTargetChanged renders the refusal when the primary
// member changes after review, using the same reason as both comparison paths.
func PreviewConfirmationPrimaryTargetChanged() string {
	return templates.RenderGenericError(templates.SchemaErrorData{
		RequestedBy: "jackjackbits", Timestamp: templates.NowFunc().UTC().Format("2006-01-02 15:04:05"),
		Environment: "production", CommandName: action.ApplyConfirm,
		ErrorDetail: unconfirmedWorkMessage(memberWork{}, primaryTargetDifferenceReason("us", workTarget)),
	})
}
