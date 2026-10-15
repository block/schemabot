//go:build integration

package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
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

// An operator stops a running apply. The stop must not complete the PR's
// check — merge stays blocked until the apply is started or cancelled — but the
// Check Run must stop saying the apply is in progress. The terminal refresh
// republishes the aggregate so the PR shows the apply as stopped, leaves the
// stored check in_progress and owned by the apply, and does not re-plan while
// the apply still holds the check.
func TestRefreshChecksForTerminalApplyPublishesStoppedApply(t *testing.T) {
	ctx := t.Context()

	db, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { _ = db.Close() })

	const (
		repo    = "octocat/stopped-apply-publish"
		pr      = 22
		dbn     = "stopped_apply_publish_db"
		env     = "staging"
		headSHA = "headsha1"
	)

	clear := func(c context.Context) {
		_, err := db.ExecContext(c, "DELETE FROM checks WHERE repository = ? AND pull_request = ?", repo, pr)
		require.NoError(t, err)
		_, err = db.ExecContext(c, "DELETE FROM applies WHERE repository = ? AND pull_request = ?", repo, pr)
		require.NoError(t, err)
	}
	clear(ctx)
	t.Cleanup(func() { clear(context.WithoutCancel(ctx)) })

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	mux.HandleFunc("GET /repos/"+repo+"/pulls/22", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head:  &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: new(headSHA)},
			Base:  &gh.PullRequestBranch{Ref: new("main"), SHA: new("base456")},
			User:  &gh.User{Login: new("testuser")},
			State: new("open"),
		})
	})
	published := make(chan checkRunCapture, 4)
	mux.HandleFunc("POST /repos/"+repo+"/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var got checkRunCapture
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		published <- got
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})

	st := mysqlstore.New(db)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := api.New(st, &api.ServerConfig{}, nil, logger)

	ghc := gh.NewClient(nil)
	ghc.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)
	factory := &fakeClientFactory{client: ghclient.NewInstallationClient(ghc, logger)}
	h := NewHandler(svc, factory, nil, logger)

	apply := &storage.Apply{
		ApplyIdentifier: "apply-stopped-publish",
		Database:        dbn,
		DatabaseType:    "mysql",
		Repository:      repo,
		PullRequest:     pr,
		Environment:     env,
		Engine:          storage.EngineSpirit,
		InstallationID:  4242,
		State:           state.Apply.Stopped,
	}
	applyID, err := st.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	require.NoError(t, st.Checks().Upsert(ctx, &storage.Check{
		Repository:   repo,
		PullRequest:  pr,
		HeadSHA:      headSHA,
		Environment:  env,
		DatabaseType: "mysql",
		DatabaseName: dbn,
		ApplyID:      applyID,
		HasChanges:   true,
		Status:       checkStatusInProgress,
	}))

	h.refreshChecksForTerminalApply(ctx, apply, "test stopped apply publish")

	stored, err := st.Checks().Get(ctx, repo, pr, env, "mysql", dbn)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, checkStatusInProgress, stored.Status, "a stop must leave the stored check in_progress")
	assert.Empty(t, stored.Conclusion, "a stop must not write a terminal conclusion")
	assert.Equal(t, applyID, stored.ApplyID, "a stop must not release check ownership")

	select {
	case got := <-published:
		assert.Equal(t, headSHA, got.HeadSHA)
		assert.Equal(t, checkStatusInProgress, got.Status, "merge stays blocked while the apply is stopped")
		assert.Empty(t, got.Conclusion)
		require.NotNil(t, got.Output)
		assert.Equal(t, "1 apply stopped", got.Output.Title)
	default:
		t.Fatal("stopping the apply published no aggregate Check Run")
	}
	assert.Empty(t, published, "the stop publishes the aggregate once and does not re-plan")
}
