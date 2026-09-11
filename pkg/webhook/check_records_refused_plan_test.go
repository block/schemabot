//go:build integration

package webhook

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

// An operator stops a long-running apply, pushes a commit that shrinks the
// schema change, and cancels the apply. The plan that ran on the new commit
// while the apply still held the check is refused by the ownership guard, so
// the stored row stays on the apply's commit — and the aggregate on the new
// commit holds that row as blocking rather than passing on results computed
// for a different tree. The refusal must be reported to the caller, because a
// plan whose result never landed cannot converge the PR on its own.
func TestPlanCheckWriteRefusedByInFlightApply(t *testing.T) {
	ctx := t.Context()

	db, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { _ = db.Close() })

	const (
		repo     = "octocat/refused-plan"
		pr       = 11
		dbn      = "refused_plan_db"
		env      = "production"
		applySHA = "aaaaaaa"
		newSHA   = "bbbbbbb"
	)

	clear := func(c context.Context) {
		_, err := db.ExecContext(c, "DELETE FROM checks WHERE repository = ? AND pull_request = ?", repo, pr)
		require.NoError(t, err)
		_, err = db.ExecContext(c, "DELETE FROM applies WHERE repository = ? AND pull_request = ?", repo, pr)
		require.NoError(t, err)
	}
	clear(ctx)
	t.Cleanup(func() { clear(context.WithoutCancel(ctx)) })

	st := mysqlstore.New(db)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := api.New(st, &api.ServerConfig{}, nil, logger)
	h := NewHandler(svc, &fakeClientFactory{}, nil, logger)

	apply := &storage.Apply{
		ApplyIdentifier: "apply-refused-plan",
		Database:        dbn,
		DatabaseType:    "mysql",
		Repository:      repo,
		PullRequest:     pr,
		Environment:     env,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Running,
	}
	applyID, err := st.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	require.NoError(t, st.Checks().Upsert(ctx, &storage.Check{
		Repository: repo, PullRequest: pr, HeadSHA: applySHA,
		Environment: env, DatabaseType: "mysql", DatabaseName: dbn,
		CheckRunID: 1, ApplyID: applyID, HasChanges: true, Status: checkStatusInProgress,
	}))

	// A plan runs on the new commit while the apply still holds the check.
	planned := &storage.Check{
		Repository: repo, PullRequest: pr, HeadSHA: newSHA,
		Environment: env, DatabaseType: "mysql", DatabaseName: dbn,
		HasChanges: true, Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired,
	}
	stored, err := st.Checks().UpsertPlanResult(ctx, planned, storage.PlanDriftNotEvaluated)
	require.NoError(t, err)
	assert.False(t, stored, "the guard must report that it refused the plan write")

	afterPlan, err := st.Checks().Get(ctx, repo, pr, env, "mysql", dbn)
	require.NoError(t, err)
	require.NotNil(t, afterPlan)
	assert.Equal(t, applySHA, afterPlan.HeadSHA, "a refused write must not re-pin the row to the new commit")
	assert.Equal(t, applyID, afterPlan.ApplyID)

	// Cancelling releases ownership, but the row still names the apply's commit.
	apply.State = state.Apply.Cancelled
	updated, err := h.updateCheckRecordForApplyResult(ctx, repo, pr, apply)
	require.NoError(t, err)
	require.True(t, updated)

	afterCancel, err := st.Checks().Get(ctx, repo, pr, env, "mysql", dbn)
	require.NoError(t, err)
	require.NotNil(t, afterCancel)
	assert.Zero(t, afterCancel.ApplyID, "cancelling a forward apply releases check ownership")
	assert.Equal(t, applySHA, afterCancel.HeadSHA, "the terminal write keeps the row on the apply's commit")

	// The released row is the one the re-plan is owed for: it is unowned, and
	// the commit the PR is gated on has no result of its own.
	assert.True(t, replanOwedAfterTerminalApply(afterCancel, openPRAt(newSHA)))

	// Until that plan lands, the aggregate on the new commit holds the row as
	// blocking rather than folding a result computed for another commit.
	contributions, staleCount := normalizeStaleContributions([]*storage.Check{afterCancel}, newSHA)
	assert.Equal(t, 1, staleCount)
	_, status := computeAggregate(contributions)
	assert.Equal(t, checkStatusInProgress, status)
	assert.False(t, anyInProgressOnCommit([]*storage.Check{afterCancel}, newSHA),
		"nothing is running for the current commit, so the title reports an awaited result")
}

// A long apply succeeds after the PR head has moved. The plan that ran on the
// new commit was refused while the apply held the check, and the terminal write
// keeps the row on the apply's own commit — so the aggregate on the new commit
// holds a success it already has as blocking. The apply settling has to leave
// the row open to a plan for the current commit, or the PR is gated on a check
// no path will ever refresh.
func TestCheckConvergesAfterSuccessfulApplyOnMovedHead(t *testing.T) {
	ctx := t.Context()

	db, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { _ = db.Close() })

	const (
		repo     = "octocat/converge-after-apply"
		pr       = 12
		dbn      = "converge_after_apply_db"
		env      = "production"
		applySHA = "ccccccc"
		newSHA   = "ddddddd"
	)

	clear := func(c context.Context) {
		_, err := db.ExecContext(c, "DELETE FROM checks WHERE repository = ? AND pull_request = ?", repo, pr)
		require.NoError(t, err)
		_, err = db.ExecContext(c, "DELETE FROM applies WHERE repository = ? AND pull_request = ?", repo, pr)
		require.NoError(t, err)
	}
	clear(ctx)
	t.Cleanup(func() { clear(context.WithoutCancel(ctx)) })

	st := mysqlstore.New(db)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := api.New(st, &api.ServerConfig{}, nil, logger)
	h := NewHandler(svc, &fakeClientFactory{}, nil, logger)

	apply := &storage.Apply{
		ApplyIdentifier: "apply-converge-after-apply",
		Database:        dbn,
		DatabaseType:    "mysql",
		Repository:      repo,
		PullRequest:     pr,
		Environment:     env,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Running,
	}
	applyID, err := st.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	require.NoError(t, st.Checks().Upsert(ctx, &storage.Check{
		Repository: repo, PullRequest: pr, HeadSHA: applySHA,
		Environment: env, DatabaseType: "mysql", DatabaseName: dbn,
		CheckRunID: 1, ApplyID: applyID, HasChanges: true, Status: checkStatusInProgress,
	}))

	// The head moves and the plan for it is refused while the apply runs.
	refused := &storage.Check{
		Repository: repo, PullRequest: pr, HeadSHA: newSHA,
		Environment: env, DatabaseType: "mysql", DatabaseName: dbn,
		Status: checkStatusCompleted, Conclusion: checkConclusionSuccess,
	}
	stored, err := st.Checks().UpsertPlanResult(ctx, refused, storage.PlanDriftNotEvaluated)
	require.NoError(t, err)
	require.False(t, stored, "the guard must refuse a plan write while the apply holds the check")

	apply.State = state.Apply.Completed
	updated, err := h.updateCheckRecordForApplyResult(ctx, repo, pr, apply)
	require.NoError(t, err)
	require.True(t, updated)

	afterApply, err := st.Checks().Get(ctx, repo, pr, env, "mysql", dbn)
	require.NoError(t, err)
	require.NotNil(t, afterApply)
	assert.Equal(t, checkConclusionSuccess, afterApply.Conclusion)
	assert.Equal(t, applyID, afterApply.ApplyID, "a successful apply keeps its check claimed")
	assert.Equal(t, applySHA, afterApply.HeadSHA, "the terminal write keeps the row on the apply's commit")

	// The aggregate on the current commit cannot use that success: it was
	// computed for a commit the PR is no longer gated on.
	contributions, staleCount := normalizeStaleContributions([]*storage.Check{afterApply}, newSHA)
	require.Equal(t, 1, staleCount)
	_, status := computeAggregate(contributions)
	require.Equal(t, checkStatusInProgress, status)

	// So a plan for the current commit is owed, and the settled row admits it.
	assert.True(t, replanOwedAfterTerminalApply(afterApply, openPRAt(newSHA)))

	replanned := &storage.Check{
		Repository: repo, PullRequest: pr, HeadSHA: newSHA,
		Environment: env, DatabaseType: "mysql", DatabaseName: dbn,
		Status: checkStatusCompleted, Conclusion: checkConclusionSuccess,
	}
	stored, err = st.Checks().UpsertPlanResult(ctx, replanned, storage.PlanDriftNotEvaluated)
	require.NoError(t, err)
	require.True(t, stored, "a settled apply must leave the row open to a plan for the current commit")

	converged, err := st.Checks().Get(ctx, repo, pr, env, "mysql", dbn)
	require.NoError(t, err)
	require.NotNil(t, converged)
	assert.Equal(t, newSHA, converged.HeadSHA)
	_, staleCount = normalizeStaleContributions([]*storage.Check{converged}, newSHA)
	assert.Zero(t, staleCount, "the aggregate now folds a result computed for the gating commit")
}
