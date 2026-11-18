//go:build integration

package webhook

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// startRollout makes the fixture's apply a running rollout across three
// targets, one of them finished, that holds its stored check in progress and
// tracks its progress on a PR comment. The target counts are stored with the
// change summary, so the row carries one.
func (f *stoppedApplyCheckFixture) startRollout(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	f.setStoredState(t, state.Apply.Running)
	f.apply.State = state.Apply.Running
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), webhookIntegrationPollDeadline)
		defer cancel()
		_, err := f.db.ExecContext(cleanupCtx, "DELETE FROM apply_operations WHERE apply_id = ?", f.apply.ID)
		require.NoError(t, err)
		_, err = f.db.ExecContext(cleanupCtx, "DELETE FROM apply_comments WHERE apply_id = ?", f.apply.ID)
		require.NoError(t, err)
	})
	for target, opState := range map[string]string{
		"shop-001": state.ApplyOperation.Completed,
		"shop-002": state.ApplyOperation.Running,
		"shop-003": state.ApplyOperation.Pending,
	} {
		_, err := f.store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: f.apply.ID, Deployment: "default", Target: target, OperationKey: target, State: opState,
			CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt,
		})
		require.NoError(t, err)
	}
	require.NoError(t, f.store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: f.apply.ID, CommentState: state.Comment.Progress, GitHubCommentID: 5150,
	}))
	stored := f.requireStoredCheckHeld(t)
	stored.ChangeSummary = "1 alter · all 3 targets"
	stored.RolloutTargets = 3
	stored.RolloutTargetsWithWork = 3
	require.NoError(t, f.store.Checks().Upsert(ctx, stored))
}

// fold publishes the aggregate in full, the way any event that folds it does,
// and returns what it published.
func (f *stoppedApplyCheckFixture) fold(t *testing.T, step string) checkRunCapture {
	t.Helper()
	f.handler.publishApplyAggregate(t.Context(), f.apply, applyCheckLogFields(f.apply, step), false)
	return f.requirePublished(t, step)
}

// finishTarget completes one of the rollout's targets.
func (f *stoppedApplyCheckFixture) finishTarget(t *testing.T, target string) {
	t.Helper()
	_, err := f.db.ExecContext(t.Context(), "UPDATE apply_operations SET state = ? WHERE apply_id = ? AND target = ?",
		state.ApplyOperation.Completed, f.apply.ID, target)
	require.NoError(t, err)
}

// One of a rollout's three targets finishes after the aggregate was folded.
// The refresh rewrites the Check Run's text to count it, in the title and in a
// line that counts the targets and links the progress comment. It spends two
// GitHub requests, sends no status, writes no stored check state, and its text
// is the text a full fold publishes for the same counts.
func TestRolloutCheckRunCountsFinishedTargets(t *testing.T) {
	const repo = "octocat/rollout-progress"
	f := newStoppedApplyCheckFixture(t, repo, 41, "headsha1", "headsha1")
	f.startRollout(t)
	before := f.fold(t, "the fold before the target finishes")
	assert.Equal(t, "Apply in progress on all 3 targets: 1 done", before.Output.Title)

	f.finishTarget(t, "shop-002")
	f.takeGitHubRequests()
	f.handler.refreshRolloutCheckRun(t.Context(), f.apply)

	got := f.requirePublished(t, "a target finishing")
	assert.Empty(t, got.Status, "the refresh sends no status, so it cannot change what the run gates")
	assert.Equal(t, "Apply in progress on all 3 targets: 2 done", got.Output.Title)
	assert.Contains(t, got.Output.Summary, "In progress · 2 of 3 done")
	assert.Contains(t, got.Output.Summary,
		"- `stopped_apply_db` targets: 3 (1 queued, 2 complete) · [progress](https://github.com/octocat/rollout-progress/pull/41#issuecomment-5150)")
	assert.Equal(t, []string{
		"GET /repos/" + repo + "/commits/headsha1/check-runs",
		"PATCH /repos/" + repo + "/check-runs/1",
	}, f.takeGitHubRequests(), "the refresh finds the run and rewrites its text, nothing more")
	assert.Empty(t, f.published, "the refresh publishes once")
	f.requireStoredCheckHeld(t)

	full := f.fold(t, "a full fold with the same counts")
	assert.Equal(t, full.Output.Title, got.Output.Title)
	assert.Equal(t, full.Output.Summary, got.Output.Summary)
}

// A refresh offered when no target has moved since the fold makes no GitHub
// request at all.
func TestRolloutCheckRunRefreshWithNothingMovedMakesNoRequest(t *testing.T) {
	f := newStoppedApplyCheckFixture(t, "octocat/rollout-progress-still", 43, "headsha1", "headsha1")
	f.startRollout(t)
	f.fold(t, "the fold")
	f.takeGitHubRequests()

	f.handler.refreshRolloutCheckRun(t.Context(), f.apply)

	assert.Empty(t, f.takeGitHubRequests())
	assert.Empty(t, f.published)
}

// The repository turns its Check Runs off while a rollout runs. A target
// finishing makes no GitHub request, so a repository that asked for no Check
// Runs never has one rewritten.
func TestRolloutCheckRunRefreshSkipsARepositoryWithChecksOff(t *testing.T) {
	const repo = "octocat/rollout-progress-off"
	f := newStoppedApplyCheckFixture(t, repo, 45, "headsha1", "headsha1")
	f.startRollout(t)
	f.fold(t, "the fold")
	f.service.Config().Repos = map[string]api.RepoConfig{storage.CanonicalKey(repo): {EnableChecks: new(false)}}
	f.finishTarget(t, "shop-002")
	f.takeGitHubRequests()

	f.handler.refreshRolloutCheckRun(t.Context(), f.apply)

	assert.Empty(t, f.takeGitHubRequests())
	assert.Empty(t, f.published)
}

// The aggregate row carries no render, as one stored before renders were
// stored does. The refresh cannot render the text on its own, so it folds the
// aggregate in full, and the Check Run still shows the finished target.
func TestRolloutCheckRunRefreshFoldsWhenNoRenderIsStored(t *testing.T) {
	const repo, pr = "octocat/rollout-progress-unrendered", 44
	f := newStoppedApplyCheckFixture(t, repo, pr, "headsha1", "headsha1")
	f.startRollout(t)
	f.fold(t, "the fold")
	_, err := f.db.ExecContext(t.Context(), "UPDATE checks SET aggregate_render = NULL WHERE repository = ? AND pull_request = ?", repo, pr)
	require.NoError(t, err)
	f.finishTarget(t, "shop-002")
	f.takeGitHubRequests()

	f.handler.refreshRolloutCheckRun(t.Context(), f.apply)

	got := f.requirePublished(t, "the full fold")
	assert.Equal(t, checkStatusInProgress, got.Status)
	assert.Equal(t, "Apply in progress on all 3 targets: 2 done", got.Output.Title)
	assert.Contains(t, f.takeGitHubRequests(), "GET /repos/"+repo+"/pulls/44", "the full fold reads the PR")
	assert.Empty(t, f.published, "the refresh folds once")
}

// The apply finishes while a refresh is publishing: the terminal path writes
// the stored check state and publishes the passing run before the refresh's
// text lands. The refresh reads the stored check state again once it has
// published and folds once more, so the Check Run ends on the apply's outcome
// rather than with text that says the rollout is running.
func TestRolloutCheckRunRefreshEndsOnTheOutcomeWhenTheApplyFinishesMeanwhile(t *testing.T) {
	f := newStoppedApplyCheckFixture(t, "octocat/rollout-progress-finish", 42, "headsha1", "headsha1")
	f.startRollout(t)
	f.fold(t, "the fold before the target finishes")
	f.finishTarget(t, "shop-002")
	f.onPublish = func(n int) {
		if n != 2 {
			return
		}
		f.setStoredState(t, state.Apply.Completed)
		stored, err := f.store.Checks().Get(context.WithoutCancel(t.Context()), f.apply.Repository, f.apply.PullRequest, f.apply.Environment, f.apply.DatabaseType, f.apply.Database)
		require.NoError(t, err)
		stored.Status = checkStatusCompleted
		stored.Conclusion = checkConclusionSuccess
		require.NoError(t, f.store.Checks().Upsert(context.WithoutCancel(t.Context()), stored))
	}

	f.handler.refreshRolloutCheckRun(t.Context(), f.apply)

	first := f.requirePublished(t, "the refresh that read the rollout in progress")
	assert.Equal(t, "Apply in progress on all 3 targets: 2 done", first.Output.Title)
	got := f.requirePublished(t, "the fold after the stored check state settled")
	assert.Equal(t, checkStatusCompleted, got.Status)
	assert.Equal(t, checkConclusionSuccess, got.Conclusion)
	assert.Empty(t, f.published, "the refresh folds once more and stops")
}

// A target finishes while a fold publishes, and the apply settles while the
// fold's follow-up refresh rewrites the counts. The refresh finds the stored
// check state moved and the aggregate is folded once more from the stored
// check state read again, so the Check Run ends on the apply's outcome rather
// than on the in-progress rows the first fold read.
func TestRolloutCheckRunFoldEndsOnTheOutcomeWhenTheApplySettlesDuringItsRefresh(t *testing.T) {
	f := newStoppedApplyCheckFixture(t, "octocat/rollout-progress-fold-settle", 46, "headsha1", "headsha1")
	f.startRollout(t)
	f.onPublish = func(n int) {
		switch n {
		case 1:
			f.finishTarget(t, "shop-002")
		case 2:
			f.setStoredState(t, state.Apply.Completed)
			stored, err := f.store.Checks().Get(t.Context(), f.apply.Repository, f.apply.PullRequest, f.apply.Environment, f.apply.DatabaseType, f.apply.Database)
			require.NoError(t, err)
			stored.Status = checkStatusCompleted
			stored.Conclusion = checkConclusionSuccess
			require.NoError(t, f.store.Checks().Upsert(t.Context(), stored))
		}
	}

	first := f.fold(t, "the fold that read the rollout in progress")
	assert.Equal(t, "Apply in progress on all 3 targets: 1 done", first.Output.Title)
	refresh := f.requirePublished(t, "the refresh with the finished target")
	assert.Equal(t, "Apply in progress on all 3 targets: 2 done", refresh.Output.Title)
	got := f.requirePublished(t, "the fold from the settled stored check state")
	assert.Equal(t, checkStatusCompleted, got.Status)
	assert.Equal(t, checkConclusionSuccess, got.Conclusion)
	assert.Empty(t, f.published, "the aggregate is folded once more and stops")
}
