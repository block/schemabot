//go:build integration

package webhook

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// An apply failed and its terminal publish never landed: the PR still shows the
// last progress comment, and stored check state still names the apply as in
// progress, so the Check Run blocks the PR as "Apply in progress". Terminal
// outcome reconciliation repairs both halves independently. The first pass
// meets a GitHub error posting the summary but still completes the check; the
// next pass posts the summary; a pass after that finds nothing left to do.
func TestReconcileTerminalApplyOutcomesRecoversAFailedPublish(t *testing.T) {
	ctx := t.Context()

	const (
		repo    = "org/reconcile-outcome"
		pr      = 44
		dbName  = "e2e_reconcile_outcome_db"
		env     = "staging"
		headSHA = "outcomesha1"
	)

	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))
	// Redundant early-exit closer: svc owns the storage (and this handle) and
	// closes it below, so discard the guaranteed already-closed error.
	t.Cleanup(func() { _ = schemabotDB.Close() })
	st := mysqlstore.New(schemabotDB)

	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM checks WHERE repository = ?", repo)
	require.NoError(t, err)
	apply := seedReconcileScenario(t, st, schemabotDB, repo, dbName, state.Apply.Failed, state.Task.Failed)
	require.NoError(t, st.Checks().Upsert(ctx, &storage.Check{
		Repository:   repo,
		PullRequest:  pr,
		HeadSHA:      headSHA,
		Environment:  env,
		DatabaseType: storage.DatabaseTypeMySQL,
		DatabaseName: dbName,
		ApplyID:      apply.ID,
		HasChanges:   true,
		Status:       checkStatusInProgress,
	}))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	// The PR is closed so no re-plan follows the check refresh.
	mux.HandleFunc("GET /repos/"+repo+"/pulls/44", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head:  &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: new(headSHA)},
			Base:  &gh.PullRequestBranch{Ref: new("main"), SHA: new("base456")},
			User:  &gh.User{Login: new("testuser")},
			State: new("closed"),
		})
	})

	var githubDown atomic.Bool
	githubDown.Store(true)
	summaries := make(chan string, 4)
	mux.HandleFunc("POST /repos/"+repo+"/issues/44/comments", func(w http.ResponseWriter, r *http.Request) {
		if githubDown.Load() {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		summaries <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 7001})
	})

	checkRuns := make(chan checkRunCapture, 4)
	mux.HandleFunc("POST /repos/"+repo+"/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var got checkRunCapture
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		checkRuns <- got
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 8001})
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	ghc := gh.NewClient(nil)
	ghc.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)
	factory := &fakeClientFactory{client: ghclient.NewInstallationClient(ghc, logger)}
	svc := api.New(st, &api.ServerConfig{}, map[string]tern.Client{}, logger)
	t.Cleanup(func() { utils.CloseAndLog(svc) })
	h := NewHandler(svc, factory, nil, logger)

	// Pass 1: the summary post fails, and the check is repaired regardless.
	h.ReconcileTerminalApplyOutcomes(ctx)

	stored, err := st.Checks().Get(ctx, repo, pr, env, storage.DatabaseTypeMySQL, dbName)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, checkStatusCompleted, stored.Status)
	assert.Equal(t, checkConclusionFailure, stored.Conclusion)
	select {
	case got := <-checkRuns:
		assert.Equal(t, headSHA, got.HeadSHA)
		assert.Equal(t, checkStatusCompleted, got.Status)
		assert.Equal(t, checkConclusionFailure, got.Conclusion, "the Check Run reports the apply's failure")
	case <-time.After(webhookIntegrationPollDeadline):
		require.FailNow(t, "timed out waiting for the repaired Check Run")
	}
	summary, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	if summary != nil {
		assert.Zero(t, summary.GitHubCommentID, "a summary GitHub rejected is not recorded as posted")
	}

	// Pass 2: GitHub is back and the summary lands.
	githubDown.Store(false)
	h.ReconcileTerminalApplyOutcomes(ctx)

	select {
	case body := <-summaries:
		assert.Contains(t, body, "Schema Change Failed")
		assert.Contains(t, body, apply.ApplyIdentifier)
	case <-time.After(webhookIntegrationPollDeadline):
		require.FailNow(t, "timed out waiting for the reconciled summary")
	}
	summary, err = st.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, summary)
	assert.Equal(t, int64(7001), summary.GitHubCommentID)

	// Pass 3: nothing is left to repair, so nothing is published again.
	h.ReconcileTerminalApplyOutcomes(ctx)
	assert.Empty(t, summaries, "the summary is posted once")
	assert.Empty(t, checkRuns, "a completed check is not republished")
}
