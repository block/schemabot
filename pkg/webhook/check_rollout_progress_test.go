package webhook

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func targetOp(target, opState string) *storage.ApplyOperation {
	return &storage.ApplyOperation{Target: target, Deployment: "default", OperationKey: target, State: opState}
}

// A rollout counts each target once, whatever number of operations it runs.
// A target with one table step done and one still copying is in progress. A
// group finalizer is not a target, and a target that already had the change
// ran nothing, so neither is counted.
func TestTallyTargetsCountsEachTargetOnce(t *testing.T) {
	converged := targetOp("shop-005", state.ApplyOperation.Completed)
	converged.AlreadyConverged = true
	finalizer := &storage.ApplyOperation{OperationKind: storage.ApplyOperationKindGroupFinalizer, State: state.ApplyOperation.Pending}
	ops := []*storage.ApplyOperation{
		targetOp("shop-001", state.ApplyOperation.Completed),
		targetOp("shop-002", state.ApplyOperation.Completed),
		targetOp("shop-002", state.ApplyOperation.Running),
		targetOp("shop-003", state.ApplyOperation.Pending),
		targetOp("shop-004", state.ApplyOperation.Failed),
		converged,
		finalizer,
	}

	assert.Equal(t, targetTally{Total: 4, Complete: 1, InProgress: 1, Queued: 1, Failed: 1}, tallyTargets(ops))
}

// While a rollout across several targets runs, the aggregate Check Run says
// how many targets have finished: in its title, in the database row's Status
// cell, and in a line under the table that counts the targets by state and
// links the live progress comment. Before any target finishes the title and
// the cell read as they always have, since a count of zero says nothing.
func TestAggregateCheckCountsFinishedRolloutTargets(t *testing.T) {
	check := &storage.Check{
		Repository: "octocat/shop", PullRequest: 12, ApplyID: 9,
		DatabaseType: "mysql", DatabaseName: "orders", HasChanges: true, Status: checkStatusInProgress,
		RolloutTargets: 32, RolloutTargetsWithWork: 32,
	}
	checks := []*storage.Check{check}

	t.Run("some targets finished", func(t *testing.T) {
		progress := rolloutProgress{9: {Total: 32, Complete: 5, InProgress: 4, Queued: 23, ProgressCommentID: 777}}
		title, summary := aggregateSummary(checks, "", nil, progress, "")
		assert.Equal(t, "Apply in progress on all 32 targets: 5 done", title)
		assert.Contains(t, summary, "In progress · 5 of 32 done")
		assert.Contains(t, summary, "- `orders` targets: 32 (4 in progress, 23 queued, 5 complete) · [progress](https://github.com/octocat/shop/pull/12#issuecomment-777)")
	})

	t.Run("no target finished yet", func(t *testing.T) {
		progress := rolloutProgress{9: {Total: 32, InProgress: 4, Queued: 28}}
		title, summary := aggregateSummary(checks, "", nil, progress, "")
		assert.Equal(t, "Apply in progress on all 32 targets", title)
		assert.NotContains(t, summary, " done")
		assert.Contains(t, summary, "- `orders` targets: 32 (4 in progress, 28 queued)\n", "no progress comment is tracked, so the line has no link")
	})

	t.Run("progress unavailable", func(t *testing.T) {
		title, summary := aggregateSummary(checks, "", nil, nil, "")
		assert.Equal(t, "Apply in progress on all 32 targets", title)
		assert.NotContains(t, summary, "targets: 32 (")
	})

	t.Run("stopped apply shows no counts", func(t *testing.T) {
		progress := rolloutProgress{9: {Total: 32, Complete: 5, Stopped: 27}}
		_, summary := aggregateSummary(checks, "", stoppedApplyIDs{9: true}, progress, "")
		assert.NotContains(t, summary, "targets: 32 (")
	})
}

// The observer offers the Check Run refresh when one more target of a
// rollout has finished, and only then: a tick with the same count offers
// nothing, a declined refresh is offered again on the next tick, and a single
// target is left to the terminal refresh.
func TestObserverOffersCheckRunRefreshWhenATargetFinishes(t *testing.T) {
	var offered int
	accept := true
	lease := storage.ApplyLease{ApplyID: 7, Owner: "host-a/1/driver-0", Token: "token"}
	observer := NewCommentObserver(CommentObserverConfig{
		Repo: "org/repo", PR: 42, ApplyID: 7, ApplyLease: lease,
		Logger:  &capturingLogger{},
		Storage: &leaseCheckStorage{applies: &leaseCheckApplyStore{}},
		OnTargetsFinishedHook: func(*storage.Apply) bool {
			offered++
			return accept
		},
	})
	apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-abc123", State: state.Apply.Running, LeaseOwner: lease.Owner, LeaseToken: lease.Token}
	tick := func(ops ...*storage.ApplyOperation) {
		observer.reportFinishedTargets(apply, ops)
	}
	running, done := state.ApplyOperation.Running, state.ApplyOperation.Completed

	tick(targetOp("eu", running), targetOp("us", running))
	assert.Equal(t, 0, offered, "no target has finished")

	tick(targetOp("eu", done), targetOp("us", running))
	assert.Equal(t, 1, offered, "the first target finished")

	tick(targetOp("eu", done), targetOp("us", running))
	assert.Equal(t, 1, offered, "the count did not move")

	accept = false
	tick(targetOp("eu", done), targetOp("us", done))
	require.Equal(t, 2, offered, "the second target finished")
	accept = true
	tick(targetOp("eu", done), targetOp("us", done))
	assert.Equal(t, 3, offered, "a declined refresh is offered again")
	tick(targetOp("eu", done), targetOp("us", done))
	assert.Equal(t, 3, offered, "a taken refresh is not offered again")

	single := NewCommentObserver(CommentObserverConfig{
		Repo: "org/repo", PR: 42, ApplyID: 7, ApplyLease: lease,
		Logger:  &capturingLogger{},
		Storage: &leaseCheckStorage{applies: &leaseCheckApplyStore{}},
		OnTargetsFinishedHook: func(*storage.Apply) bool {
			offered++
			return true
		},
	})
	single.reportFinishedTargets(apply, []*storage.ApplyOperation{targetOp("eu", done)})
	assert.Equal(t, 3, offered, "a single target is left to the terminal refresh")
}

// A fold stores its render as JSON and a refresh renders from what it reads
// back, so the round trip must give the same text: the counts' map keys, the
// stopped set, the prerequisite notice, and the stale-commit title included.
func TestAggregateRenderTextSurvivesStorage(t *testing.T) {
	render := aggregateRender{
		CheckRunID: 7, HeadSHA: "headsha1", Status: checkStatusInProgress,
		Contributions: []*storage.Check{
			{
				Repository: "octocat/shop", PullRequest: 12, ApplyID: 3,
				DatabaseType: "mysql", DatabaseName: "orders", HasChanges: true, Status: checkStatusInProgress,
				ChangeSummary: "1 alter · all 3 targets", RolloutTargets: 3, RolloutTargetsWithWork: 3,
			},
			{DatabaseType: "mysql", DatabaseName: "payments", ApplyID: 4, HasChanges: true, Status: checkStatusInProgress, ChangeSummary: "1 alter"},
		},
		Stopped:            stoppedApplyIDs{4: true},
		Progress:           rolloutProgress{3: {Total: 3, Complete: 1, InProgress: 2, ProgressCommentID: 4242}},
		PrerequisiteNotice: "Waiting on staging.",
	}
	for _, awaiting := range []bool{false, true} {
		render.AwaitingCurrentCommit = awaiting
		raw, err := json.Marshal(render)
		require.NoError(t, err)
		var stored aggregateRender
		require.NoError(t, json.Unmarshal(raw, &stored))

		wantTitle, wantSummary := render.text()
		gotTitle, gotSummary := stored.text()
		assert.Equal(t, wantTitle, gotTitle)
		assert.Equal(t, wantSummary, gotSummary)
		assert.Contains(t, gotSummary, "1 of 3 done")
	}
}

// Two deployments can each address a target of the same name. They are
// separate rollout members, so a finished us-east/orders is counted as done
// while us-west/orders still runs.
func TestTallyTargetsKeepsSameNamedTargetsOfDeploymentsApart(t *testing.T) {
	east := targetOp("orders", state.ApplyOperation.Completed)
	east.Deployment = "us-east"
	west := targetOp("orders", state.ApplyOperation.Running)
	west.Deployment = "us-west"

	assert.Equal(t, targetTally{Total: 2, Complete: 1, InProgress: 1}, tallyTargets([]*storage.ApplyOperation{east, west}))
}

// A PR running many rollouts at once keeps the progress lines within their
// share of the Check Run text, and counts the rollouts left out, so a publish
// never exceeds GitHub's text limit.
func TestRolloutProgressLinesStayWithinTheirBudget(t *testing.T) {
	var checks []*storage.Check
	progress := rolloutProgress{}
	for i := range 2000 {
		id := int64(i + 1)
		checks = append(checks, &storage.Check{
			Repository: "octocat/shop", PullRequest: 12, ApplyID: id,
			DatabaseType: "mysql", DatabaseName: fmt.Sprintf("orders_%04d", i), HasChanges: true, Status: checkStatusInProgress,
			RolloutTargets: 32, RolloutTargetsWithWork: 32,
		})
		progress[id] = targetTally{Total: 32, Complete: 5, InProgress: 4, Queued: 23, ProgressCommentID: 777}
	}

	section := renderRolloutProgress(checks, progress, nil, maxCheckRunTextLength/4)
	assert.LessOrEqual(t, len(section), maxCheckRunTextLength/4)
	assert.Contains(t, section, "- `orders_0000` targets: 32 (4 in progress, 23 queued, 5 complete)")
	assert.Regexp(t, `- … \d+ more rollouts in progress\n$`, section)

	_, summary := aggregateSummary(checks, "", nil, progress, "")
	assert.LessOrEqual(t, len(summary), maxCheckRunTextLength)
}

// A refresh folds in full when any stored per-database row the render was
// folded from was rewritten or removed, or a row for its environment
// appeared. Rows of other environments it never read, and a prerequisite's
// relabeled reason, do not count as a move.
func TestRenderInputsMoved(t *testing.T) {
	orders := &storage.Check{ID: 1, Environment: "production", DatabaseType: "mysql", DatabaseName: "orders", HeadSHA: "h", ApplyID: 3, Status: checkStatusInProgress}
	staging := &storage.Check{ID: 2, Environment: "staging", DatabaseType: "mysql", DatabaseName: "orders", HeadSHA: "h", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess}
	prerequisite := *staging
	prerequisite.BlockingReason = prerequisiteContribution
	render := aggregateRender{Inputs: renderInputsOf([]*storage.Check{orders, &prerequisite})}
	clone := func(c *storage.Check) *storage.Check { copied := *c; return &copied }

	assert.False(t, inputsMoved(render, "production", []*storage.Check{orders, staging}), "nothing moved")

	settled := clone(orders)
	settled.Status, settled.Conclusion = checkStatusCompleted, checkConclusionSuccess
	assert.True(t, inputsMoved(render, "production", []*storage.Check{settled, staging}), "a row it read settled")

	assert.True(t, inputsMoved(render, "production", []*storage.Check{staging}), "a row it read is gone")

	payments := &storage.Check{ID: 9, Environment: "production", DatabaseType: "mysql", DatabaseName: "payments"}
	assert.True(t, inputsMoved(render, "production", []*storage.Check{orders, staging, payments}), "a row for its environment appeared")

	sandbox := &storage.Check{ID: 10, Environment: "sandbox", DatabaseType: "mysql", DatabaseName: "payments"}
	assert.False(t, inputsMoved(render, "production", []*storage.Check{orders, staging, sandbox}), "a row of an environment it did not read")

	rerun := clone(staging)
	rerun.Status, rerun.Conclusion = checkStatusInProgress, ""
	assert.True(t, inputsMoved(render, "production", []*storage.Check{orders, rerun}), "a prerequisite it read moved")
}
