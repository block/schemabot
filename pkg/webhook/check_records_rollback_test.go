//go:build integration

package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
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

// runRollbackTerminalRefresh drives refreshChecksForTerminalApply for a
// completed rollback against a fake GitHub serving an open PR with the given
// changed files, and returns the resulting stored check plus any PR comments
// posted during the refresh.
func runRollbackTerminalRefresh(t *testing.T, repo string, pr int, dbn string, files []*gh.CommitFile) (*storage.Check, chan string) {
	t.Helper()
	ctx := t.Context()
	const env = "staging"

	db, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { _ = db.Close() })

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

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	ghClient := gh.NewClient(nil)
	ghClient.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)

	mux.HandleFunc(fmt.Sprintf("GET /repos/%s/pulls/%d", repo, pr), func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			State: new("open"),
			Head:  &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: new("abc123")},
			Base:  &gh.PullRequestBranch{Ref: new("main"), SHA: new("def456")},
			User:  &gh.User{Login: new("testuser")},
		})
	})
	mux.HandleFunc(fmt.Sprintf("GET /repos/%s/pulls/%d/files", repo, pr), func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(files)
	})
	mux.HandleFunc(fmt.Sprintf("POST /repos/%s/check-runs", repo), func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 300})
	})
	mux.HandleFunc(fmt.Sprintf("PATCH /repos/%s/check-runs/", repo), func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 300})
	})
	comments := make(chan string, 10)
	mux.HandleFunc(fmt.Sprintf("POST /repos/%s/issues/%d/comments", repo, pr), commentRecorder(t, comments))

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
	return check, comments
}

// A completed rollback restores the target schema, so a PR whose head no
// longer contains any SchemaBot inputs matches the live database again. The
// terminal refresh must converge the PR's checks to passing and say so on the
// PR timeline — without it the operator has to know to comment
// `schemabot plan` to clear the rollback block by hand.
func TestRefreshChecksForTerminalApply_CompletedRollbackConvergesChecksOnEmptyPR(t *testing.T) {
	check, comments := runRollbackTerminalRefresh(t, "octocat/rollback-converge", 9, "rollback_converge_db",
		[]*gh.CommitFile{{Filename: new("README.md"), Status: new("modified")}})

	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionSuccess, check.Conclusion, "rollback restored the target schema and the PR has no schema inputs left")
	assert.Empty(t, check.BlockingReason)
	assert.False(t, check.HasChanges)
	assert.Zero(t, check.ApplyID)

	comment := requireComment(t, comments, "checks-refreshed comment after completed rollback")
	assert.Contains(t, comment, "No Managed Schema Changes")
	assert.Contains(t, comment, "Triggered automatically after the rollback completed")
	assert.NotContains(t, comment, "Requested by")
}

// A PR that still carries SchemaBot schema files after its rollback completes
// must keep the rollback block: the rolled-back changes have to be re-applied
// or re-planned deliberately, so the terminal refresh converges nothing and
// posts nothing.
func TestRefreshChecksForTerminalApply_CompletedRollbackKeepsBlockWhenPRHasSchemaFiles(t *testing.T) {
	check, comments := runRollbackTerminalRefresh(t, "octocat/rollback-block", 11, "rollback_block_db",
		[]*gh.CommitFile{{Filename: new("db/users.sql"), Status: new("modified")}})

	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionActionRequired, check.Conclusion, "PR still declares schema changes; the rollback block must stand")
	assert.Equal(t, rollbackCompletedBlock.blockingReason, check.BlockingReason)
	assert.True(t, check.HasChanges)

	select {
	case body := <-comments:
		t.Fatalf("no comment expected while the rollback block stands, got: %s", body)
	default:
	}
}
