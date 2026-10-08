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
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

// narrowedApplyCheckFixture is a handler over real check storage whose GitHub
// client serves one PR pinned to headSHA, so the stored check records under
// test land without any Check Run calls.
type narrowedApplyCheckFixture struct {
	h       *Handler
	st      storage.Storage
	client  *ghclient.InstallationClient
	schema  *ghclient.SchemaRequestResult
	repo    string
	pr      int
	env     string
	headSHA string
}

func newNarrowedApplyCheckFixture(t *testing.T, repo string, pr int, database string) *narrowedApplyCheckFixture {
	t.Helper()
	ctx := t.Context()

	db, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	const headSHA = "nsha123"
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
	mux.HandleFunc(fmt.Sprintf("GET /repos/%s/pulls/%d", repo, pr), func(w http.ResponseWriter, _ *http.Request) {
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

	return &narrowedApplyCheckFixture{
		h:       NewHandler(svc, &fakeClientFactory{client: installClient}, nil, logger),
		st:      st,
		client:  installClient,
		schema:  &ghclient.SchemaRequestResult{Repository: repo, PullRequest: pr, Database: database, Type: "mysql", HeadSHA: headSHA},
		repo:    repo,
		pr:      pr,
		env:     "production",
		headSHA: headSHA,
	}
}

func (f *narrowedApplyCheckFixture) check(t *testing.T) *storage.Check {
	t.Helper()
	check, err := f.st.Checks().Get(t.Context(), f.repo, f.pr, f.env, "mysql", f.schema.Database)
	require.NoError(t, err)
	require.NotNil(t, check)
	return check
}

// storeNarrowedApply records the check state of an apply narrowed to
// payments-002 as it dispatches.
func (f *narrowedApplyCheckFixture) storeNarrowedApply(t *testing.T) {
	t.Helper()
	narrowedPlan := &apitypes.PlanResponse{
		PlanID:     "plan-narrowed",
		NarrowedTo: "prod/payments-002",
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace:    f.schema.Database,
			TableChanges: []*apitypes.TableChangeResponse{{TableName: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: "alter"}},
		}},
	}
	gotSHA, err := f.h.storeApplyCheckRecord(t.Context(), f.client, f.repo, f.pr, f.schema, narrowedPlan, f.env, reviewDriftOutcome{state: driftNotEvaluated}, false)
	require.NoError(t, err)
	assert.Equal(t, f.headSHA, gotSHA)
}

// An operator lands a change on payments-002 alone with
// `schemabot apply -e production --target payments-002` while the PR's check
// for production reads success from an earlier plan. The apply changes one of
// the environment's targets, so the check must block merge from the moment it
// dispatches, keep blocking once it completes, and lift only when a plan of the
// whole environment records its own result (MG-12).
func TestNarrowedApplyBlocksCheckUntilWholeEnvironmentPlan(t *testing.T) {
	ctx := t.Context()
	f := newNarrowedApplyCheckFixture(t, "octocat/narrowed-apply-check", 9, "narrowed_apply_check_db")
	h, st, installClient, schema := f.h, f.st, f.client, f.schema
	repo, pr, dbn, env, headSHA := f.repo, f.pr, f.schema.Database, f.env, f.headSHA
	getCheck := f.check

	require.NoError(t, st.Checks().Upsert(ctx, &storage.Check{
		Repository: repo, PullRequest: pr, HeadSHA: headSHA, Environment: env,
		DatabaseType: "mysql", DatabaseName: dbn, CheckRunID: 1,
		Status: checkStatusCompleted, Conclusion: checkConclusionSuccess,
	}))

	f.storeNarrowedApply(t)

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

// An operator rolls a change out to payments-002 first, then applies the
// rest of production. The apply of the whole environment records its own
// check state in place of narrowed_apply, so finishing the rollout is the
// only step the operator takes.
func TestWholeEnvironmentApplyReplacesNarrowedApplyBlock(t *testing.T) {
	f := newNarrowedApplyCheckFixture(t, "octocat/narrowed-apply-then-whole", 11, "narrowed_apply_then_whole_db")
	f.storeNarrowedApply(t)
	require.Equal(t, narrowedApplyBlock.blockingReason, f.check(t).BlockingReason)

	wholePlan := &apitypes.PlanResponse{
		PlanID: "plan-rest-of-rollout",
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace:    f.schema.Database,
			TableChanges: []*apitypes.TableChangeResponse{{TableName: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: "alter"}},
		}},
	}
	_, err := f.h.storeApplyCheckRecord(t.Context(), f.client, f.repo, f.pr, f.schema, wholePlan, f.env, reviewDriftOutcome{state: driftClean}, false)
	require.NoError(t, err)

	replaced := f.check(t)
	assert.Empty(t, replaced.BlockingReason, "the whole-environment apply speaks for every target")
	assert.NotEqual(t, narrowedApplyCheckSummary, replaced.ChangeSummary)
}

// A commit removed the schema change after an earlier apply on production
// had started, so the check is blocked for reconciliation: the target may
// carry work the PR no longer describes (MG-6). An operator then lands a
// narrowed apply. The check keeps the reconciliation block rather than taking
// narrowed_apply, which an ordinary plan of the whole environment would lift.
func TestNarrowedApplyKeepsReconciliationBlock(t *testing.T) {
	ctx := t.Context()
	f := newNarrowedApplyCheckFixture(t, "octocat/narrowed-apply-reconcile", 10, "narrowed_apply_reconcile_db")

	earlier := &storage.Apply{
		ApplyIdentifier: "apply-before-schema-removed",
		Database:        f.schema.Database,
		DatabaseType:    "mysql",
		Repository:      f.repo,
		PullRequest:     f.pr,
		Environment:     f.env,
		Engine:          storage.EngineSpirit,
		InstallationID:  4242,
		State:           state.Apply.Completed,
	}
	earlierID, err := f.st.Applies().Create(ctx, earlier)
	require.NoError(t, err)

	require.NoError(t, f.st.Checks().Upsert(ctx, &storage.Check{
		Repository: f.repo, PullRequest: f.pr, HeadSHA: "older-sha", Environment: f.env,
		DatabaseType: "mysql", DatabaseName: f.schema.Database, CheckRunID: 1,
		ApplyID: earlierID, HasChanges: true, Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired,
		BlockingReason: schemaRemovedAfterApplyBlock.blockingReason,
		ErrorMessage:   schemaRemovedAfterApplyBlock.message,
		ChangeSummary:  "schema change removed after its apply started",
	}))

	f.storeNarrowedApply(t)

	kept := f.check(t)
	assert.Equal(t, schemaRemovedAfterApplyBlock.blockingReason, kept.BlockingReason, "a reconciliation block is never traded for narrowed_apply")
	assert.Equal(t, checkConclusionActionRequired, kept.Conclusion)
	assert.Equal(t, schemaRemovedAfterApplyBlock.message, kept.ErrorMessage)
	assert.Equal(t, earlierID, kept.ApplyID, "the started apply still owns the row")
	assert.Equal(t, f.headSHA, kept.HeadSHA, "the row moves to the current head so the aggregate reads it")
}

// A plan of production found its targets diverged (or a namespace placed on
// the wrong target) and blocked the check on live deployment state. An
// operator then lands a narrowed apply, which runs no rollup. The check keeps
// the stored block, which only a fresh rollup of the whole environment may
// clear, rather than taking narrowed_apply and reading as the next step of an
// ordinary rollout.
func TestNarrowedApplyKeepsRollupBlock(t *testing.T) {
	for i, block := range []checkBlockReason{reviewTimeDeploymentDriftBlock, namespacePlacementRefusedBlock} {
		t.Run(block.blockingReason, func(t *testing.T) {
			f := newNarrowedApplyCheckFixture(t, "octocat/narrowed-apply-rollup", 20+i, "narrowed_apply_rollup_db")
			require.NoError(t, f.st.Checks().Upsert(t.Context(), &storage.Check{
				Repository: f.repo, PullRequest: f.pr, HeadSHA: "older-sha", Environment: f.env,
				DatabaseType: "mysql", DatabaseName: f.schema.Database, CheckRunID: 1,
				HasChanges: true, Status: checkStatusCompleted, Conclusion: checkConclusionFailure,
				BlockingReason: block.blockingReason,
				ErrorMessage:   block.message,
				ChangeSummary:  "targets diverged at review time",
			}))

			f.storeNarrowedApply(t)

			kept := f.check(t)
			assert.Equal(t, block.blockingReason, kept.BlockingReason, "a block only a rollup may clear is never traded for narrowed_apply")
			assert.Equal(t, checkConclusionFailure, kept.Conclusion)
			assert.Equal(t, block.message, kept.ErrorMessage)
			assert.Equal(t, "targets diverged at review time", kept.ChangeSummary)
			assert.Equal(t, f.headSHA, kept.HeadSHA, "the row moves to the current head so the aggregate reads it")
		})
	}
}
