//go:build integration

package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
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

	db, err := sql.Open("block-mysql", e2eSchemabotDSN)
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
	require.NoError(t, h.updateCheckRecordForApplyStart(ctx, installClient, repo, pr, schema, env, apply))

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

	db, err := sql.Open("block-mysql", e2eSchemabotDSN)
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
	apply.ID = applyID

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
	require.NoError(t, h.updateCheckRecordForApplyStart(ctx, installClient, repo, pr, schema, env, apply))

	check, err := st.Checks().Get(ctx, repo, pr, env, "mysql", dbn)
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, checkStatusInProgress, check.Status)
	assert.Empty(t, check.Conclusion)
	assert.Equal(t, applyID, check.ApplyID)
	assert.True(t, check.HasChanges)
}

// The full merge-gate chain for an apply that starts on a commit whose
// aggregate Check Run already concluded green — the rollback-confirm flow: the
// original apply completed and concluded the aggregate as success, then a
// rollback begins on the same head SHA. The check claim must flip the stored
// per-database state to in_progress, and the aggregate recompute must publish
// the rewind as a fresh in-progress Check Run rather than updating the
// concluded one (GitHub keeps a concluded run's conclusion, which would leave
// a passing merge gate over the active rollback drive).
func TestUpdateCheckRecordForApplyStart_RollbackOnConcludedAggregatePublishesFreshCheckRun(t *testing.T) {
	ctx := t.Context()

	db, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	const (
		repo    = "octocat/rollback-green-gate"
		pr      = 12
		dbn     = "rollback_green_gate_db"
		env     = "staging"
		headSHA = "abc123"
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
	svc := api.New(st, &api.ServerConfig{Repos: map[string]api.RepoConfig{repo: {}}}, nil, logger)

	ghClient, mux := setupGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/rollback-green-gate/pulls/12", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"head": map[string]any{"sha": headSHA, "ref": "feature-branch"},
			"base": map[string]any{"sha": "def456", "ref": "main"},
			"user": map[string]any{"login": "testuser"},
		}))
	})
	var created []checkRunCapture
	mux.HandleFunc("POST /repos/octocat/rollback-green-gate/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		created = append(created, body)
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 999}))
	})
	var patched bool
	mux.HandleFunc("PATCH /repos/octocat/rollback-green-gate/check-runs/", func(w http.ResponseWriter, _ *http.Request) {
		patched = true
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 555}))
	})
	serveTrustedCheckRuns(t, mux, repo, headSHA, ghclient.CheckRunResult{
		ID: 555, Name: aggregateCheckName,
		Status: checkStatusCompleted, Conclusion: checkConclusionSuccess,
	})
	installClient := ghclient.NewInstallationClientWithSlug(ghClient, logger, "schemabot")
	factory := &fakeClientFactory{client: installClient}
	h := NewHandler(svc, factory, nil, logger)

	// The original apply completed; its per-database check and the aggregate
	// Check Run both concluded success.
	originalApply := &storage.Apply{
		ApplyIdentifier: "apply-original-completed",
		Database:        dbn,
		DatabaseType:    "mysql",
		Repository:      repo,
		PullRequest:     pr,
		Environment:     env,
		Engine:          storage.EngineSpirit,
		InstallationID:  4242,
		State:           state.Apply.Completed,
	}
	originalApplyID, err := st.Applies().Create(ctx, originalApply)
	require.NoError(t, err)

	require.NoError(t, st.Checks().Upsert(ctx, &storage.Check{
		Repository:   repo,
		PullRequest:  pr,
		HeadSHA:      headSHA,
		Environment:  env,
		DatabaseType: "mysql",
		DatabaseName: dbn,
		ApplyID:      originalApplyID,
		HasChanges:   true,
		Status:       checkStatusCompleted,
		Conclusion:   checkConclusionSuccess,
	}))
	require.NoError(t, st.Checks().Upsert(ctx, &storage.Check{
		Repository:   repo,
		PullRequest:  pr,
		HeadSHA:      headSHA,
		Environment:  aggregateSentinel,
		DatabaseType: aggregateSentinel,
		DatabaseName: aggregateSentinel,
		CheckRunID:   555,
		Status:       checkStatusCompleted,
		Conclusion:   checkConclusionSuccess,
	}))

	// The rollback apply is running on the same head SHA.
	rollbackApply := &storage.Apply{
		ApplyIdentifier: "apply-rollback-drive",
		Database:        dbn,
		DatabaseType:    "mysql",
		Repository:      repo,
		PullRequest:     pr,
		Environment:     env,
		Engine:          storage.EngineSpirit,
		InstallationID:  4242,
		State:           state.Apply.Running,
	}
	rollbackApplyID, err := st.Applies().Create(ctx, rollbackApply)
	require.NoError(t, err)
	rollbackApply.ID = rollbackApplyID

	schema := &ghclient.SchemaRequestResult{Type: "mysql", Database: dbn}
	require.NoError(t, h.updateCheckRecordForApplyStart(ctx, installClient, repo, pr, schema, env, rollbackApply))

	// The rollback now owns the per-database check, back in progress.
	check, err := st.Checks().Get(ctx, repo, pr, env, "mysql", dbn)
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, checkStatusInProgress, check.Status)
	assert.Empty(t, check.Conclusion)
	assert.Equal(t, rollbackApplyID, check.ApplyID)

	// The concluded Check Run was not reused; a fresh in-progress run gates the
	// PR and the stored aggregate state tracks it.
	assert.False(t, patched, "the concluded Check Run must not be reused: GitHub keeps its conclusion, leaving a passing check over the rollback drive")
	require.Len(t, created, 1, "the rewind must publish exactly one fresh Check Run")
	assert.Equal(t, aggregateCheckName, created[0].Name)
	assert.Equal(t, checkStatusInProgress, created[0].Status)
	assert.Empty(t, created[0].Conclusion)

	aggregate, err := st.Checks().Get(ctx, repo, pr, aggregateSentinel, aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	require.NotNil(t, aggregate)
	assert.Equal(t, int64(999), aggregate.CheckRunID)
	assert.Equal(t, checkStatusInProgress, aggregate.Status)
	assert.Empty(t, aggregate.Conclusion)
}
