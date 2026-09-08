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
	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

// A Vitess plan whose only work is a VSchema update carries zero table changes,
// but it still requires an apply: the stored check state must record pending
// changes and block the PR (action_required), exactly as a DDL-bearing plan
// would. A plan with no work at all must still record a passing check.
func TestUpsertPlanCheckRecord_VSchemaOnlyPlanRequiresApply(t *testing.T) {
	ctx := t.Context()

	db, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	const (
		repo    = "octocat/vschema-only-check"
		pr      = 7
		dbn     = "vschema_only_check_db"
		env     = "staging"
		headSHA = "vsha123"
	)

	clear := func(c context.Context) {
		_, err := db.ExecContext(c, "DELETE FROM checks WHERE repository = ? AND pull_request = ?", repo, pr)
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
	// current; serve a PR pinned to the plan's SHA.
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	mux.HandleFunc("GET /repos/"+repo+"/pulls/7", func(w http.ResponseWriter, _ *http.Request) {
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
	factory := &fakeClientFactory{client: installClient}
	h := NewHandler(svc, factory, nil, logger)

	schema := &ghclient.SchemaRequestResult{
		Repository:  repo,
		PullRequest: pr,
		Database:    dbn,
		Type:        "vitess",
		HeadSHA:     headSHA,
	}

	t.Run("vschema-only plan records pending changes", func(t *testing.T) {
		planResp := &apitypes.PlanResponse{
			Changes: []*apitypes.SchemaChangeResponse{{
				Namespace: "boardgames_sharded",
				Metadata: map[string]string{
					apitypes.VSchemaDiffMetadataKey: "--- current\n+++ new\n+    \"xxhash\": {\"type\": \"xxhash\"}",
				},
			}},
		}

		gotSHA, check, err := h.upsertPlanCheckRecord(ctx, installClient, repo, pr, schema, planResp, env, reviewDriftOutcome{})
		require.NoError(t, err)
		assert.Equal(t, headSHA, gotSHA)
		require.NotNil(t, check)
		assert.True(t, check.HasChanges, "a VSchema update is pending work")
		assert.Equal(t, checkConclusionActionRequired, check.Conclusion, "an unapplied VSchema update must block the PR")
		assert.Contains(t, check.ChangeSummary, "1 vschema update")

		stored, err := st.Checks().Get(ctx, repo, pr, env, "vitess", dbn)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.True(t, stored.HasChanges)
		assert.Equal(t, checkConclusionActionRequired, stored.Conclusion)
	})

	t.Run("plan with no work records success", func(t *testing.T) {
		planResp := &apitypes.PlanResponse{
			Changes: []*apitypes.SchemaChangeResponse{{Namespace: "boardgames_sharded"}},
		}

		_, check, err := h.upsertPlanCheckRecord(ctx, installClient, repo, pr, schema, planResp, env, reviewDriftOutcome{})
		require.NoError(t, err)
		require.NotNil(t, check)
		assert.False(t, check.HasChanges)
		assert.Equal(t, checkConclusionSuccess, check.Conclusion)
	})
}
