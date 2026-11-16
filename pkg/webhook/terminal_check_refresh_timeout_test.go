//go:build integration

package webhook

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

// An apply finishes while GitHub accepts the request for the PR head and never
// answers. The terminal refresh runs on the apply's drive path with no deadline
// from its caller, so it must give up on its own rather than hold that path.
// Nothing is published on a head it never read, so the PR keeps showing the
// apply as running and merge stays blocked.
func TestRefreshChecksForTerminalApplyGivesUpWhenGitHubNeverAnswers(t *testing.T) {
	ctx := t.Context()

	db, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { _ = db.Close() })

	const (
		repo     = "octocat/terminal-refresh-timeout"
		pr       = 24
		dbn      = "terminal_refresh_timeout_db"
		env      = "production"
		applySHA = "applysha1"
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

	mux.HandleFunc("GET /repos/"+repo+"/pulls/24", func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	var publishes atomic.Int64
	mux.HandleFunc("POST /repos/"+repo+"/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		publishes.Add(1)
		w.WriteHeader(http.StatusCreated)
	})

	st := mysqlstore.New(db)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := api.New(st, &api.ServerConfig{}, nil, logger)

	ghc := gh.NewClient(nil)
	ghc.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)
	factory := &fakeClientFactory{client: ghclient.NewInstallationClient(ghc, logger)}
	h := NewHandler(svc, factory, nil, logger)
	h.terminalCheckRefreshTimeoutOverride = 200 * time.Millisecond

	apply := &storage.Apply{
		ApplyIdentifier: "apply-terminal-refresh-timeout",
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

	done := make(chan struct{})
	go func() {
		defer close(done)
		// The drive-path hooks hand the refresh a context with no deadline.
		h.refreshChecksForTerminalApply(context.WithoutCancel(ctx), apply, "test terminal refresh timeout")
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("terminal refresh did not give up on a GitHub request that never answers")
	}

	assert.Zero(t, publishes.Load(), "no Check Run is published on a PR head that was never read")
}
