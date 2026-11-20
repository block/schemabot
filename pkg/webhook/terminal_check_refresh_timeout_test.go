//go:build integration

package webhook

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
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
// The stored check write comes first and lands, so the stored check is complete
// and terminal check reconciliation has nothing left to retry. Nothing is
// published on a head the refresh never read, so the PR keeps showing the apply
// as running and merge stays blocked, and the log says so.
func TestRefreshChecksForTerminalApplyGivesUpWhenGitHubNeverAnswers(t *testing.T) {
	f := newTerminalRefreshTimeoutFixture(t, "octocat/terminal-refresh-timeout", "terminal_refresh_timeout_db", nil)

	f.refresh(t)

	assert.Positive(t, f.headReads.Load(), "the refresh got as far as the request that never answers and gave up there")
	assert.Zero(t, f.publishes.Load(), "no Check Run is published on a PR head that was never read")

	stored := f.storedCheck(t)
	assert.Equal(t, checkStatusCompleted, stored.Status, "the stored check write lands before GitHub is asked for the head")
	assert.Equal(t, checkConclusionSuccess, stored.Conclusion)

	warnings := logRecordsContaining(t, f.logs, terminalCheckRefreshCutAfterStoredCheck)
	require.Len(t, warnings, 1, "one line says the refresh reached its deadline and what the PR is left showing")
	assert.Equal(t, f.apply.ApplyIdentifier, warnings[0]["apply_id"])
	assert.Equal(t, true, warnings[0]["stored_check_completed"])
	assert.Equal(t, float64(terminalRefreshTestDeadline), warnings[0]["deadline"],
		"the refresh ran under the deadline the handler was given")
}

// An apply finishes while the storage call that completes its stored check
// never returns. The refresh gives up on its own here too, before it reaches
// GitHub. The stored check stays in progress, which keeps merge blocked, and the
// apply is still one terminal check reconciliation finds and retries.
func TestRefreshChecksForTerminalApplyGivesUpWhenStorageNeverAnswers(t *testing.T) {
	f := newTerminalRefreshTimeoutFixture(t, "octocat/terminal-refresh-storage-timeout", "terminal_refresh_storage_timeout_db",
		func(st storage.Storage) storage.Storage { return &stalledCheckCompletionStorage{Storage: st} })

	f.refresh(t)

	assert.Zero(t, f.headReads.Load(), "the refresh gives up before it asks GitHub for the PR head")
	assert.Zero(t, f.publishes.Load(), "no Check Run is published for a stored check that was never completed")

	stored := f.storedCheck(t)
	assert.Equal(t, checkStatusInProgress, stored.Status, "the stored check stays in progress, so merge stays blocked")

	stranded, err := f.store.Applies().FindTerminalWithInProgressCheck(t.Context(), terminalCheckReconcileBatch)
	require.NoError(t, err)
	var strandedIDs []string
	for _, a := range stranded {
		strandedIDs = append(strandedIDs, a.ApplyIdentifier)
	}
	assert.Contains(t, strandedIDs, f.apply.ApplyIdentifier, "terminal check reconciliation still finds the apply and retries it")

	warnings := logRecordsContaining(t, f.logs, terminalCheckRefreshCutBeforeStoredCheck)
	require.Len(t, warnings, 1, "one line says the refresh reached its deadline and what retries it")
	assert.Equal(t, f.apply.ApplyIdentifier, warnings[0]["apply_id"])
	assert.Equal(t, false, warnings[0]["stored_check_completed"])
}

// stalledCheckCompletionStorage holds the write that completes a stored check
// until its context ends, the way a storage connection that stops answering
// does.
type stalledCheckCompletionStorage struct {
	storage.Storage
}

func (s *stalledCheckCompletionStorage) Checks() storage.CheckStore {
	return &stalledCheckCompletionStore{CheckStore: s.Storage.Checks()}
}

type stalledCheckCompletionStore struct {
	storage.CheckStore
}

func (s *stalledCheckCompletionStore) CompleteForApply(ctx context.Context, _ *storage.Check, _ *storage.Apply) (bool, error) {
	<-ctx.Done()
	return false, ctx.Err()
}

// terminalRefreshTestDeadline is the refresh deadline the fixture's handler
// runs under. It is far below the guard that fails a refresh that never gives
// up, and long enough for the stored check write that comes before the GitHub
// request to finish on a busy database, so the refresh is cut at the call each
// test stalls and not before it.
const terminalRefreshTestDeadline = time.Second

// terminalRefreshTimeoutFixture is a completed apply that still owns an
// in-progress stored check, on a PR whose head GitHub never answers for.
type terminalRefreshTimeoutFixture struct {
	handler *Handler
	// store is the real storage, without the wrapper the handler runs on.
	store     storage.Storage
	apply     *storage.Apply
	headReads atomic.Int64
	publishes atomic.Int64
	logs      *syncBuffer
}

const (
	terminalRefreshTimeoutPR  = 24
	terminalRefreshTimeoutEnv = "production"
)

// newTerminalRefreshTimeoutFixture builds the fixture for repo and database.
// wrap, when set, replaces the storage the handler runs on.
func newTerminalRefreshTimeoutFixture(t *testing.T, repo, database string, wrap func(storage.Storage) storage.Storage) *terminalRefreshTimeoutFixture {
	t.Helper()
	ctx := t.Context()

	db, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { _ = db.Close() })

	clear := func(c context.Context) {
		_, err := db.ExecContext(c, "DELETE FROM checks WHERE repository = ? AND pull_request = ?", repo, terminalRefreshTimeoutPR)
		require.NoError(t, err)
		_, err = db.ExecContext(c, "DELETE FROM applies WHERE repository = ? AND pull_request = ?", repo, terminalRefreshTimeoutPR)
		require.NoError(t, err)
	}
	clear(ctx)
	t.Cleanup(func() { clear(context.WithoutCancel(ctx)) })

	f := &terminalRefreshTimeoutFixture{logs: &syncBuffer{}}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("handler logs:\n%s", f.logs.String())
		}
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	mux.HandleFunc(fmt.Sprintf("GET /repos/%s/pulls/%d", repo, terminalRefreshTimeoutPR), func(_ http.ResponseWriter, r *http.Request) {
		f.headReads.Add(1)
		<-r.Context().Done()
	})
	mux.HandleFunc("POST /repos/"+repo+"/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		f.publishes.Add(1)
		w.WriteHeader(http.StatusCreated)
	})

	f.store = mysqlstore.New(db)
	handlerStore := f.store
	if wrap != nil {
		handlerStore = wrap(f.store)
	}
	logger := slog.New(slog.NewJSONHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	svc := api.New(handlerStore, &api.ServerConfig{}, nil, logger)

	ghc := gh.NewClient(nil)
	ghc.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)
	factory := &fakeClientFactory{client: ghclient.NewInstallationClient(ghc, logger)}
	f.handler = NewHandler(svc, factory, nil, logger)
	f.handler.terminalCheckRefreshTimeoutOverride = terminalRefreshTestDeadline

	f.apply = &storage.Apply{
		ApplyIdentifier: "apply-" + database,
		Database:        database,
		DatabaseType:    "mysql",
		Repository:      repo,
		PullRequest:     terminalRefreshTimeoutPR,
		Environment:     terminalRefreshTimeoutEnv,
		Engine:          storage.EngineSpirit,
		InstallationID:  4242,
		State:           state.Apply.Completed,
	}
	applyID, err := f.store.Applies().Create(ctx, f.apply)
	require.NoError(t, err)
	f.apply.ID = applyID

	require.NoError(t, f.store.Checks().Upsert(ctx, &storage.Check{
		Repository:   repo,
		PullRequest:  terminalRefreshTimeoutPR,
		HeadSHA:      "applysha1",
		Environment:  terminalRefreshTimeoutEnv,
		DatabaseType: "mysql",
		DatabaseName: database,
		ApplyID:      applyID,
		HasChanges:   true,
		Status:       checkStatusInProgress,
	}))
	return f
}

// refresh runs the terminal refresh for the fixture's apply and fails the test
// if the refresh has not given up well past its deadline.
func (f *terminalRefreshTimeoutFixture) refresh(t *testing.T) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		// The drive-path hooks hand the refresh a context with no deadline. The
		// test's context has none either, and it ends when the test does, so a
		// refresh that never gives up is released before the server closes.
		f.handler.refreshChecksForTerminalApply(t.Context(), f.apply, "test terminal refresh timeout")
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "terminal refresh did not give up on a call that never answers")
	}
}

// storedCheck reads the apply's stored check from the real storage.
func (f *terminalRefreshTimeoutFixture) storedCheck(t *testing.T) *storage.Check {
	t.Helper()
	stored, err := f.store.Checks().Get(t.Context(), f.apply.Repository, f.apply.PullRequest, f.apply.Environment, f.apply.DatabaseType, f.apply.Database)
	require.NoError(t, err)
	require.NotNil(t, stored, "the fixture's stored check")
	return stored
}
