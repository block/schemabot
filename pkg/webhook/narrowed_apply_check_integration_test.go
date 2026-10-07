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
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

// An operator lands a change on payments-002 alone with
// `schemabot apply -e production --target payments-002` while the PR's check
// for production reads success from an earlier plan. The apply changes one of
// the environment's targets, so the check must block merge from the moment it
// dispatches, keep blocking once it completes, and lift only when a plan of the
// whole environment records its own result (MG-12).
func TestNarrowedApplyBlocksCheckUntilWholeEnvironmentPlan(t *testing.T) {
	ctx := t.Context()

	db, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	const (
		repo    = "octocat/narrowed-apply-check"
		pr      = 9
		dbn     = "narrowed_apply_check_db"
		env     = "production"
		headSHA = "nsha123"
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

	// The record path re-fetches the PR to confirm the plan's head SHA is still
	// current; serve a PR pinned to it. Check Run calls are unrouted: the
	// stored state under test lands before any of them.
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	mux.HandleFunc("GET /repos/"+repo+"/pulls/9", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: new(headSHA)},
			Base: &gh.PullRequestBranch{Ref: new("main"), SHA: new("base456")},
			User: &gh.User{Login: new("testuser")},
		})
	})
	ghc := gh.NewClient(nil)
	ghc.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)
	installClient := ghclient.NewInstallationClient(ghc, logger)
	h := NewHandler(svc, &fakeClientFactory{client: installClient}, nil, logger)

	schema := &ghclient.SchemaRequestResult{Repository: repo, PullRequest: pr, Database: dbn, Type: "mysql", HeadSHA: headSHA}
	getCheck := func(t *testing.T) *storage.Check {
		t.Helper()
		check, err := st.Checks().Get(ctx, repo, pr, env, "mysql", dbn)
		require.NoError(t, err)
		require.NotNil(t, check)
		return check
	}

	require.NoError(t, st.Checks().Upsert(ctx, &storage.Check{
		Repository: repo, PullRequest: pr, HeadSHA: headSHA, Environment: env,
		DatabaseType: "mysql", DatabaseName: dbn, CheckRunID: 1,
		Status: checkStatusCompleted, Conclusion: checkConclusionSuccess,
	}))

	narrowedPlan := &apitypes.PlanResponse{
		PlanID:     "plan-narrowed",
		NarrowedTo: "prod/payments-002",
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace:    dbn,
			TableChanges: []*apitypes.TableChangeResponse{{TableName: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: "alter"}},
		}},
	}
	gotSHA, err := h.storeApplyCheckRecord(ctx, installClient, repo, pr, schema, narrowedPlan, env, reviewDriftOutcome{state: driftNotEvaluated}, false)
	require.NoError(t, err)
	assert.Equal(t, headSHA, gotSHA)

	dispatched := getCheck(t)
	assert.Equal(t, checkConclusionFailure, dispatched.Conclusion, "a narrowed apply blocks the check before it dispatches")
	assert.Equal(t, narrowedApplyBlock.blockingReason, dispatched.BlockingReason)
	assert.Equal(t, narrowedApplyCheckSummary, dispatched.ChangeSummary)

	apply := &storage.Apply{
		ApplyIdentifier: "apply-narrowed-terminal",
		Database:        dbn,
		DatabaseType:    "mysql",
		Repository:      repo,
		PullRequest:     pr,
		Environment:     env,
		Engine:          storage.EngineSpirit,
		InstallationID:  4242,
		State:           state.Apply.Completed,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{NarrowedTo: "prod/payments-002"}),
	}
	applyID, err := st.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	// The apply start claimed the check for this apply.
	dispatched.ApplyID = applyID
	dispatched.Status = checkStatusInProgress
	dispatched.Conclusion = ""
	require.NoError(t, st.Checks().Upsert(ctx, dispatched))

	h.refreshChecksForTerminalApply(ctx, apply, "test narrowed apply terminal")

	completed := getCheck(t)
	assert.Equal(t, checkStatusCompleted, completed.Status)
	assert.Equal(t, checkConclusionActionRequired, completed.Conclusion, "a completed narrowed apply must not pass the check")
	assert.Equal(t, narrowedApplyBlock.blockingReason, completed.BlockingReason)
	assert.True(t, completed.HasChanges, "nothing shows the other targets have the change")
	assert.Zero(t, completed.ApplyID, "ownership is released so a plan of the whole environment can lift the block")

	// A plan of the whole environment that finds every target at the desired
	// schema replaces the block with its own result.
	_, replanned, err := h.upsertPlanCheckRecord(ctx, installClient, repo, pr, schema,
		&apitypes.PlanResponse{PlanID: "plan-whole-rollout"}, env, reviewDriftOutcome{state: driftClean})
	require.NoError(t, err)
	require.NotNil(t, replanned)
	lifted := getCheck(t)
	assert.Equal(t, checkConclusionSuccess, lifted.Conclusion)
	assert.Empty(t, lifted.BlockingReason)
}
