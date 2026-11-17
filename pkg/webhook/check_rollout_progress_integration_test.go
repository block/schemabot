//go:build integration

package webhook

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
		_, err := f.db.ExecContext(t.Context(), "DELETE FROM apply_operations WHERE apply_id = ?", f.apply.ID)
		require.NoError(t, err)
		_, err = f.db.ExecContext(t.Context(), "DELETE FROM apply_comments WHERE apply_id = ?", f.apply.ID)
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

// One of a rollout's three targets finishes. The aggregate Check Run is
// republished to say so, in its title and in a line that counts the targets
// and links the progress comment. The stored check state is not written.
func TestRolloutCheckRunCountsFinishedTargets(t *testing.T) {
	f := newStoppedApplyCheckFixture(t, "octocat/rollout-progress", 41, "headsha1", "headsha1")
	f.startRollout(t)

	f.handler.refreshRolloutCheckRun(t.Context(), f.apply)

	got := f.requirePublished(t, "a target finishing")
	assert.Equal(t, checkStatusInProgress, got.Status)
	assert.Equal(t, "Apply in progress on all 3 targets: 1 done", got.Output.Title)
	assert.Contains(t, got.Output.Summary, "In progress · 1 of 3 done")
	assert.Contains(t, got.Output.Summary,
		"- `stopped_apply_db` targets: 3 (1 in progress, 1 queued, 1 complete) · [progress](https://github.com/octocat/rollout-progress/pull/41#issuecomment-5150)")
	assert.Empty(t, f.published, "a refresh with nothing moving publishes once")
	f.requireStoredCheckHeld(t)
}

// The apply finishes while a progress refresh is publishing: the terminal path
// writes the stored check state and publishes the passing run before the
// refresh's in-progress run lands. The refresh reads the stored check state
// again once it has published and publishes once more, so the Check Run ends
// on the apply's outcome rather than stuck in progress.
func TestRolloutCheckRunRefreshEndsOnTheOutcomeWhenTheApplyFinishesMeanwhile(t *testing.T) {
	f := newStoppedApplyCheckFixture(t, "octocat/rollout-progress-finish", 42, "headsha1", "headsha1")
	f.startRollout(t)
	f.onPublish = func(n int) {
		if n != 1 {
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
	assert.Equal(t, checkStatusInProgress, first.Status)
	got := f.requirePublished(t, "the publish after the stored check state settled")
	assert.Equal(t, checkStatusCompleted, got.Status)
	assert.Equal(t, checkConclusionSuccess, got.Conclusion)
	assert.Empty(t, f.published, "the refresh publishes once more and stops")
}
