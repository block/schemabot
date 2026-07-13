//go:build integration

package webhook

import (
	"context"
	"database/sql"
	"log/slog"
	"net/url"
	"os"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

// A rollback reverts a previously applied schema change, so once it completes
// the required status check must read action_required — the PR's change is gone
// from the environment and the PR must not merge as-is. Operation-level operator
// claiming drives the rollback to terminal through the durable summary path
// (refreshChecksForTerminalApply), which suppresses the rollback command's own
// observer, so the durable path must itself recognize a rollback and refuse to
// mark the check successful.
func TestRefreshChecksForTerminalApply_CompletedRollbackIsActionRequired(t *testing.T) {
	ctx := t.Context()

	db, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { _ = db.Close() })

	const (
		repo = "octocat/rollback-check"
		pr   = 8
		dbn  = "rollback_check_db"
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

	// The terminal refresh resolves a GitHub client to update the aggregate
	// Check Run after writing stored state. Point it at an unrouted test server:
	// the stored update (the behavior under test) lands first, and the GitHub
	// refresh fails gracefully without a real installation.
	ghClient := gh.NewClient(nil)
	ghClient.BaseURL, err = url.Parse("http://127.0.0.1:0/")
	require.NoError(t, err)
	factory := &fakeClientFactory{client: ghclient.NewInstallationClient(ghClient, logger)}
	h := NewHandler(svc, factory, nil, logger)

	apply := &storage.Apply{
		ApplyIdentifier: "apply-rollback-terminal",
		Database:        dbn,
		DatabaseType:    "mysql",
		Repository:      repo,
		PullRequest:     pr,
		Environment:     env,
		Engine:          storage.EngineSpirit,
		InstallationID:  4242,
		State:           state.Apply.Completed,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{AllowUnsafe: true, Rollback: true}),
	}
	applyID, err := st.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	// rollback-confirm marked the check in_progress and owned by the rollback
	// apply before the operator drove it.
	require.NoError(t, st.Checks().Upsert(ctx, &storage.Check{
		Repository:   repo,
		PullRequest:  pr,
		HeadSHA:      "abc123",
		Environment:  env,
		DatabaseType: "mysql",
		DatabaseName: dbn,
		CheckRunID:   1,
		ApplyID:      applyID,
		HasChanges:   false,
		Status:       checkStatusInProgress,
	}))

	h.refreshChecksForTerminalApply(ctx, apply, "test rollback terminal")

	check, err := st.Checks().Get(ctx, repo, pr, env, "mysql", dbn)
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionActionRequired, check.Conclusion, "a completed rollback must block the PR, not pass it")
	assert.Equal(t, rollbackCompletedBlock.blockingReason, check.BlockingReason)
	assert.True(t, check.HasChanges, "the PR's reverted change is still outstanding")
	assert.Zero(t, check.ApplyID, "rollback finalization releases check ownership so a re-apply can take over")
}
