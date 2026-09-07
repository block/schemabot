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
	"sync/atomic"
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

// An apply runs on one commit while the PR moves to another, and the terminal
// outcome must reach the commit the PR is gated on. Publishing on the apply's
// commit does not merely put the outcome on a run GitHub no longer displays:
// the fold refuses a write aimed at a commit that is not the head and arms a
// re-fold instead, and that re-fold runs only on an aggregate leader. This
// deployment is not one, so an outcome aimed at the wrong commit is published
// nowhere at all.
//
// The PR head is read once and answers both the commit to publish on and the
// fold's own currency check, so the PR is fetched exactly once. The PR here is
// closed so no re-plan follows the publish, isolating the commit the terminal
// refresh chooses on its own.
func TestRefreshChecksForTerminalApplyPublishesOnPRHead(t *testing.T) {
	ctx := t.Context()

	db, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { _ = db.Close() })

	const (
		repo     = "octocat/terminal-head-publish"
		pr       = 21
		dbn      = "terminal_head_publish_db"
		env      = "production"
		applySHA = "applysha1"
		headSHA  = "headsha2"
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

	var prFetches atomic.Int64
	mux.HandleFunc("GET /repos/"+repo+"/pulls/21", func(w http.ResponseWriter, _ *http.Request) {
		prFetches.Add(1)
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head:  &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: new(headSHA)},
			Base:  &gh.PullRequestBranch{Ref: new("main"), SHA: new("base456")},
			User:  &gh.User{Login: new("testuser")},
			State: new("closed"),
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
	cfg := &api.ServerConfig{}
	require.False(t, cfg.IsAggregateLeaderForRepo(repo),
		"the publish must reach the head without a re-fold, which only a leader arms")
	svc := api.New(st, cfg, nil, logger)

	ghc := gh.NewClient(nil)
	ghc.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)
	factory := &fakeClientFactory{client: ghclient.NewInstallationClient(ghc, logger)}
	h := NewHandler(svc, factory, nil, logger)

	apply := &storage.Apply{
		ApplyIdentifier: "apply-terminal-head-publish",
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

	require.NoError(t, st.Checks().Upsert(ctx, &storage.Check{
		Repository:   repo,
		PullRequest:  pr,
		HeadSHA:      applySHA,
		Environment:  env,
		DatabaseType: "mysql",
		DatabaseName: dbn,
		ApplyID:      applyID,
		HasChanges:   true,
		Status:       checkStatusInProgress,
	}))

	h.refreshChecksForTerminalApply(ctx, apply, "test terminal head publish")

	stored, err := st.Checks().Get(ctx, repo, pr, env, "mysql", dbn)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, applySHA, stored.HeadSHA, "the terminal write keeps the stored row on the apply's commit")

	select {
	case got := <-published:
		assert.Equal(t, headSHA, got.HeadSHA,
			"the aggregate must be published on the commit the PR is gated on, not the apply's")
	default:
		t.Fatal("terminal refresh published no aggregate Check Run")
	}

	assert.Equal(t, int64(1), prFetches.Load(),
		"one read of the head answers both the publish target and the fold's currency check")
}
