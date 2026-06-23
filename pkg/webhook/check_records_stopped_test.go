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

// A schema change that is stopped and then resumed to completion must end with
// its required status check reflecting the successful apply. Stopping is a
// resumable pause, so it must not terminalize the stored check: if a stop drove
// the check to a terminal conclusion, the in_progress-gated completion update
// would be rejected and the check would stay stuck at its pre-apply state until
// an operator manually reconciled it.
func TestUpdateCheckRecordForApplyResult_StoppedThenCompleted(t *testing.T) {
	ctx := t.Context()

	db, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { _ = db.Close() })

	const (
		repo = "octocat/stopped-check"
		pr   = 7
		dbn  = "stopped_check_db"
		env  = "staging"
	)

	clear := func(c context.Context) {
		_, err := db.ExecContext(c, "DELETE FROM checks WHERE repository = ? AND pull_request = ?", repo, pr)
		require.NoError(t, err)
		_, err = db.ExecContext(c, "DELETE FROM applies WHERE repository = ? AND pull_request = ?", repo, pr)
		require.NoError(t, err)
	}
	clear(ctx)
	// Cleanup runs after t.Context() is cancelled, so derive a non-cancelled
	// context from it for the cleanup deletes.
	t.Cleanup(func() { clear(context.WithoutCancel(ctx)) })

	st := mysqlstore.New(db)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := api.New(st, &api.ServerConfig{}, nil, logger)
	h := NewHandler(svc, &fakeClientFactory{}, nil, logger)

	apply := &storage.Apply{
		ApplyIdentifier: "apply-stopped-resume",
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

	// The apply started, so the check is in_progress and owned by this apply.
	require.NoError(t, st.Checks().Upsert(ctx, &storage.Check{
		Repository:   repo,
		PullRequest:  pr,
		HeadSHA:      "abc123",
		Environment:  env,
		DatabaseType: "mysql",
		DatabaseName: dbn,
		CheckRunID:   1,
		ApplyID:      applyID,
		HasChanges:   true,
		Status:       checkStatusInProgress,
	}))

	// Stopping the apply must leave the check in_progress (owned by this apply),
	// not terminalize it.
	apply.State = state.Apply.Stopped
	updated, err := h.updateCheckRecordForApplyResult(ctx, repo, pr, apply)
	require.NoError(t, err)
	assert.False(t, updated, "a stopped apply must not update the terminal check state")

	afterStop, err := st.Checks().Get(ctx, repo, pr, env, "mysql", dbn)
	require.NoError(t, err)
	require.NotNil(t, afterStop)
	assert.Equal(t, checkStatusInProgress, afterStop.Status, "stop must leave the check in_progress so a resume can complete it")
	assert.Empty(t, afterStop.Conclusion, "stop must not write a terminal conclusion")
	assert.Equal(t, applyID, afterStop.ApplyID, "stop must not release check ownership")

	// Resuming and completing the same apply must now drive the check to success.
	apply.State = state.Apply.Completed
	updated, err = h.updateCheckRecordForApplyResult(ctx, repo, pr, apply)
	require.NoError(t, err)
	assert.True(t, updated, "the resumed apply's completion must update the check")

	afterComplete, err := st.Checks().Get(ctx, repo, pr, env, "mysql", dbn)
	require.NoError(t, err)
	require.NotNil(t, afterComplete)
	assert.Equal(t, checkStatusCompleted, afterComplete.Status)
	assert.Equal(t, checkConclusionSuccess, afterComplete.Conclusion)
}
