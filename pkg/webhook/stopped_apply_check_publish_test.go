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
	"strconv"
	"sync"
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

// An operator stops a running apply, then starts it again. While stopped, the
// Check Run concludes action_required titled "1 apply stopped": merge stays
// blocked, and the PR shows it is waiting on an operator rather than on work.
// The stored check stays in_progress and owned by the apply, and no re-plan
// runs while the apply holds the check. When the apply resumes, the Check Run
// goes back to in progress as soon as a driver claims it, published as a fresh
// run because GitHub cannot move a concluded run back out of completed.
func TestStoppedApplyCheckRunFollowsStopAndResume(t *testing.T) {
	ctx := t.Context()
	f := newStoppedApplyCheckFixture(t, "octocat/stopped-apply-publish", 22, "headsha1", "headsha1")

	f.handler.refreshChecksForTerminalApply(ctx, f.apply, "test stopped apply publish")

	f.requireStoredCheckHeld(t)
	got := f.requirePublished(t, "stopping the apply")
	assert.Equal(t, f.headSHA, got.HeadSHA)
	assert.Equal(t, checkStatusCompleted, got.Status)
	assert.Equal(t, checkConclusionActionRequired, got.Conclusion, "merge stays blocked while the apply is stopped")
	assert.Equal(t, "1 apply stopped", got.Output.Title)
	assert.Empty(t, f.published, "the stop publishes the aggregate once and does not re-plan")

	// The operator starts the apply. The driver's start claim commits it from
	// stopped to resuming and reports the claim with the apply as it read it,
	// still stopped. The handler republishes the aggregate from storage.
	f.setStoredState(t, state.Apply.Resuming)
	f.service.OnStoppedApplyClaimed(f.apply)

	got = f.requirePublished(t, "resuming the apply")
	assert.Equal(t, f.headSHA, got.HeadSHA)
	assert.Equal(t, checkStatusInProgress, got.Status, "a resumed apply's check is in progress again")
	assert.Empty(t, got.Conclusion)
	assert.Equal(t, "Apply in progress", got.Output.Title)
	f.requireStoredCheckHeld(t)
}

// Folds for one PR are not serialized, and each reads which applies are
// stopped before the GitHub round trips that publish it. When the apply stops
// or starts again while a fold is publishing, the run that fold leaves no
// longer matches the apply, so the fold reads the applies again and folds once
// more. The Check Run ends on the apply's current state whichever way it moved.
func TestAggregateRefoldsWhenAnApplyStopsOrStartsWhilePublishing(t *testing.T) {
	cases := []struct {
		name           string
		foldedFrom     string
		movedTo        string
		wantFirst      string
		wantStatus     string
		wantConclusion string
		wantTitle      string
	}{
		{
			name:       "an apply started again while its stop publishes ends in progress",
			foldedFrom: state.Apply.Stopped, movedTo: state.Apply.Resuming,
			wantFirst:  checkStatusCompleted,
			wantStatus: checkStatusInProgress, wantTitle: "Apply in progress",
		},
		{
			name:       "an apply stopped while its progress publishes ends stopped",
			foldedFrom: state.Apply.Running, movedTo: state.Apply.Stopped,
			wantFirst:  checkStatusInProgress,
			wantStatus: checkStatusCompleted, wantConclusion: checkConclusionActionRequired, wantTitle: "1 apply stopped",
		},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newStoppedApplyCheckFixture(t, fmt.Sprintf("octocat/stopped-apply-refold-%d", i), 30+i, "headsha1", "headsha1")
			f.setStoredState(t, tc.foldedFrom)
			f.apply.State = tc.foldedFrom
			f.onPublish = func(n int) {
				if n == 1 {
					f.setStoredState(t, tc.movedTo)
				}
			}

			f.handler.republishAggregateForHeldCheck(t.Context(), f.apply, "test refold")

			first := f.requirePublished(t, "the fold that read the earlier state")
			assert.Equal(t, tc.wantFirst, first.Status)
			got := f.requirePublished(t, "the re-fold over the current state")
			assert.Equal(t, tc.wantStatus, got.Status)
			assert.Equal(t, tc.wantConclusion, got.Conclusion)
			assert.Equal(t, tc.wantTitle, got.Output.Title)
			assert.Empty(t, f.published, "the fold re-folds once")
			f.requireStoredCheckHeld(t)
		})
	}
}

// An apply started on one commit is stopped after the PR moves to another. The
// Check Run on the commit the PR is gated on still says the apply is stopped
// and concludes action_required, rather than waiting on results for the new
// commit: no plan for the new commit replaces a stopped apply's result. The
// stored check keeps the apply's commit and its owner.
func TestStoppedApplyOnOlderCommitShowsStoppedOnPRHead(t *testing.T) {
	f := newStoppedApplyCheckFixture(t, "octocat/stopped-apply-older-commit", 23, "applysha1", "headsha2")

	f.handler.refreshChecksForTerminalApply(t.Context(), f.apply, "test stopped apply on older commit")

	stored := f.requireStoredCheckHeld(t)
	assert.Equal(t, "applysha1", stored.HeadSHA, "the stop keeps the stored check on the apply's commit")
	got := f.requirePublished(t, "stopping the apply")
	assert.Equal(t, "headsha2", got.HeadSHA, "the outcome is published on the commit the PR is gated on")
	assert.Equal(t, checkStatusCompleted, got.Status)
	assert.Equal(t, checkConclusionActionRequired, got.Conclusion, "merge stays blocked while the apply is stopped")
	assert.Equal(t, "1 apply stopped", got.Output.Title)
}

// stoppedApplyCheckFixture is a stopped apply that owns its database's stored
// check, recorded on applySHA, on a PR whose head is headSHA. GitHub is faked:
// every aggregate publish creates a Check Run, and the head commit's
// check-runs lookup reports the newest one, the way GitHub resolves the run
// that gates the PR.
type stoppedApplyCheckFixture struct {
	db        *sql.DB
	store     storage.Storage
	service   *api.Service
	handler   *Handler
	apply     *storage.Apply
	headSHA   string
	published chan checkRunCapture
	// onPublish, when set, runs as GitHub receives the nth aggregate publish,
	// before it answers, standing in for whatever else happens meanwhile.
	onPublish func(n int)
}

func newStoppedApplyCheckFixture(t *testing.T, repo string, pr int, applySHA, headSHA string) *stoppedApplyCheckFixture {
	t.Helper()
	ctx := t.Context()

	db, err := sql.Open("block-mysql", e2eSchemabotDSN)
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
	t.Cleanup(func() { clear(context.WithoutCancel(ctx)) })

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	mux.HandleFunc(fmt.Sprintf("GET /repos/%s/pulls/%d", repo, pr), func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head:  &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: new(headSHA)},
			Base:  &gh.PullRequestBranch{Ref: new("main"), SHA: new("base456")},
			User:  &gh.User{Login: new("testuser")},
			State: new("open"),
		})
	})
	var (
		mu        sync.Mutex
		newest    checkRunCapture
		newestID  int64
		publishes int
	)
	f := &stoppedApplyCheckFixture{headSHA: headSHA, published: make(chan checkRunCapture, 4)}
	record := func(got checkRunCapture) {
		mu.Lock()
		newest = got
		publishes++
		n := publishes
		mu.Unlock()
		if f.onPublish != nil {
			f.onPublish(n)
		}
		f.published <- got
	}
	mux.HandleFunc("POST /repos/"+repo+"/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var got checkRunCapture
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		mu.Lock()
		newestID++
		id := newestID
		mu.Unlock()
		record(got)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id})
	})
	mux.HandleFunc("PATCH /repos/"+repo+"/check-runs/{id}", func(w http.ResponseWriter, r *http.Request) {
		var got checkRunCapture
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		record(got)
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		require.NoError(t, err)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id})
	})
	mux.HandleFunc("GET /repos/"+repo+"/commits/"+headSHA+"/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		run := map[string]any{
			"id": newestID, "name": newest.Name, "status": newest.Status,
			"conclusion": newest.Conclusion, "app": map[string]any{"slug": "schemabot"},
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"total_count": 1, "check_runs": []any{run}})
	})

	st := mysqlstore.New(db)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := api.New(st, &api.ServerConfig{}, nil, logger)

	ghc := gh.NewClient(nil)
	ghc.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)
	factory := &fakeClientFactory{client: ghclient.NewInstallationClientWithSlug(ghc, logger, "schemabot")}

	apply := &storage.Apply{
		ApplyIdentifier: "apply-" + applySHA,
		Database:        "stopped_apply_db",
		DatabaseType:    "mysql",
		Repository:      repo,
		PullRequest:     pr,
		Environment:     "staging",
		Engine:          storage.EngineSpirit,
		InstallationID:  4242,
		State:           state.Apply.Stopped,
	}
	apply.ID, err = st.Applies().Create(ctx, apply)
	require.NoError(t, err)

	require.NoError(t, st.Checks().Upsert(ctx, &storage.Check{
		Repository:   repo,
		PullRequest:  pr,
		HeadSHA:      applySHA,
		Environment:  apply.Environment,
		DatabaseType: apply.DatabaseType,
		DatabaseName: apply.Database,
		ApplyID:      apply.ID,
		HasChanges:   true,
		Status:       checkStatusInProgress,
	}))

	f.db = db
	f.store = st
	f.service = svc
	f.handler = NewHandler(svc, factory, nil, logger)
	f.apply = apply
	return f
}

// setStoredState writes the apply's state the way a driver's claim or a stop
// does. A general Update refuses those transitions, so write the row directly.
func (f *stoppedApplyCheckFixture) setStoredState(t *testing.T, applyState string) {
	t.Helper()
	_, err := f.db.ExecContext(context.WithoutCancel(t.Context()), "UPDATE applies SET state = ? WHERE id = ?", applyState, f.apply.ID)
	require.NoError(t, err)
}

// requireStoredCheckHeld asserts the apply still holds its database's stored
// check: in_progress with no conclusion, owned by the apply.
func (f *stoppedApplyCheckFixture) requireStoredCheckHeld(t *testing.T) *storage.Check {
	t.Helper()
	stored, err := f.store.Checks().Get(t.Context(), f.apply.Repository, f.apply.PullRequest, f.apply.Environment, f.apply.DatabaseType, f.apply.Database)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, checkStatusInProgress, stored.Status, "the stored check stays in_progress while the apply holds it")
	assert.Empty(t, stored.Conclusion, "the stored check carries no terminal conclusion")
	assert.Equal(t, f.apply.ID, stored.ApplyID, "the apply keeps ownership of the stored check")
	return stored
}

// requirePublished returns the next aggregate Check Run the step published.
func (f *stoppedApplyCheckFixture) requirePublished(t *testing.T, step string) checkRunCapture {
	t.Helper()
	select {
	case got := <-f.published:
		require.NotNil(t, got.Output)
		return got
	case <-time.After(webhookIntegrationPollDeadline):
		require.FailNowf(t, "no aggregate Check Run published", "%s published no aggregate Check Run", step)
		return checkRunCapture{}
	}
}
