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

// A fast apply can reach a terminal state before the webhook goroutine claims
// the stored check state. The driver's terminal update then skips fail-closed
// (the row is not yet apply-owned), so the claim is the last writer: it must
// notice the apply already finished and immediately converge the stored check
// state to the terminal outcome. Otherwise the check stays in_progress forever
// and the PR's required check never reflects the successful apply.
func TestUpdateCheckRecordForApplyStart_ConvergesWhenApplyAlreadyTerminal(t *testing.T) {
	ctx := t.Context()

	db, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	const (
		repo = "octocat/claim-race-check"
		pr   = 9
		dbn  = "claim_race_check_db"
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

	// The claim and the terminal refresh both refresh the aggregate Check Run
	// after writing stored state. Point GitHub at an unrouted test server: the
	// stored updates (the behavior under test) land first, and the GitHub
	// refresh fails gracefully without a real installation.
	ghClient := gh.NewClient(nil)
	ghClient.BaseURL, err = url.Parse("http://127.0.0.1:0/")
	require.NoError(t, err)
	installClient := ghclient.NewInstallationClient(ghClient, logger)
	factory := &fakeClientFactory{client: installClient}
	h := NewHandler(svc, factory, nil, logger)

	// The driver already drove the apply to completion.
	apply := &storage.Apply{
		ApplyIdentifier: "apply-claim-race-terminal",
		Database:        dbn,
		DatabaseType:    "mysql",
		Repository:      repo,
		PullRequest:     pr,
		Environment:     env,
		Engine:          storage.EngineSpirit,
		InstallationID:  4242,
		State:           state.Apply.Completed,
	}
	applyID, err := st.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	// The stored check state is still what the plan wrote: pending changes,
	// not owned by any apply.
	require.NoError(t, st.Checks().Upsert(ctx, &storage.Check{
		Repository:   repo,
		PullRequest:  pr,
		HeadSHA:      "abc123",
		Environment:  env,
		DatabaseType: "mysql",
		DatabaseName: dbn,
		CheckRunID:   1,
		HasChanges:   true,
		Status:       checkStatusCompleted,
		Conclusion:   checkConclusionActionRequired,
	}))

	// The driver's terminal update runs before the claim lands: the row is not
	// apply-owned yet, so the conditional update skips fail-closed.
	updated, err := h.updateCheckRecordForApplyResult(ctx, repo, pr, apply)
	require.NoError(t, err)
	require.False(t, updated, "terminal update must not complete a check the apply does not own")

	check, err := st.Checks().Get(ctx, repo, pr, env, "mysql", dbn)
	require.NoError(t, err)
	require.NotNil(t, check)
	require.Equal(t, checkConclusionActionRequired, check.Conclusion, "skipped terminal update must leave the plan result in place")
	require.Zero(t, check.ApplyID)

	// The webhook claim lands after the apply finished. It must converge the
	// stored check state to the apply's terminal outcome instead of leaving the
	// row in_progress with no writer left to complete it.
	schema := &ghclient.SchemaRequestResult{Type: "mysql", Database: dbn}
	require.NoError(t, h.updateCheckRecordForApplyStart(ctx, installClient, repo, pr, schema, env, applyID))

	check, err = st.Checks().Get(ctx, repo, pr, env, "mysql", dbn)
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionSuccess, check.Conclusion, "the completed apply's success must land even when the claim raced behind it")
	assert.Equal(t, applyID, check.ApplyID)
	assert.False(t, check.HasChanges)
}

// While the apply is still in flight when the claim lands, the claim keeps the
// stored check state in_progress and owned by the apply so the driver's
// eventual terminal update can complete it.
func TestUpdateCheckRecordForApplyStart_KeepsInProgressForRunningApply(t *testing.T) {
	ctx := t.Context()

	db, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	const (
		repo = "octocat/claim-running-check"
		pr   = 10
		dbn  = "claim_running_check_db"
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

	ghClient := gh.NewClient(nil)
	ghClient.BaseURL, err = url.Parse("http://127.0.0.1:0/")
	require.NoError(t, err)
	installClient := ghclient.NewInstallationClient(ghClient, logger)
	factory := &fakeClientFactory{client: installClient}
	h := NewHandler(svc, factory, nil, logger)

	apply := &storage.Apply{
		ApplyIdentifier: "apply-claim-running",
		Database:        dbn,
		DatabaseType:    "mysql",
		Repository:      repo,
		PullRequest:     pr,
		Environment:     env,
		Engine:          storage.EngineSpirit,
		InstallationID:  4242,
		State:           state.Apply.Running,
	}
	applyID, err := st.Applies().Create(ctx, apply)
	require.NoError(t, err)

	require.NoError(t, st.Checks().Upsert(ctx, &storage.Check{
		Repository:   repo,
		PullRequest:  pr,
		HeadSHA:      "abc123",
		Environment:  env,
		DatabaseType: "mysql",
		DatabaseName: dbn,
		CheckRunID:   1,
		HasChanges:   true,
		Status:       checkStatusCompleted,
		Conclusion:   checkConclusionActionRequired,
	}))

	schema := &ghclient.SchemaRequestResult{Type: "mysql", Database: dbn}
	require.NoError(t, h.updateCheckRecordForApplyStart(ctx, installClient, repo, pr, schema, env, applyID))

	check, err := st.Checks().Get(ctx, repo, pr, env, "mysql", dbn)
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, checkStatusInProgress, check.Status)
	assert.Empty(t, check.Conclusion)
	assert.Equal(t, applyID, check.ApplyID)
	assert.True(t, check.HasChanges)
}
