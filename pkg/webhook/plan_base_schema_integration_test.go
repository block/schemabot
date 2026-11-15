//go:build integration

package webhook

import (
	"encoding/json"
	"fmt"
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
	"github.com/block/schemabot/pkg/storage"
)

const baseSchemaUsersTable = "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

// newBaseSchemaPlanFlow serves a PR proposing a users table to a handler
// planning dbName against a real target, in staging and any further
// environments named.
func newBaseSchemaPlanFlow(t *testing.T, dbName string, environments ...string) (*Handler, *http.ServeMux, *planFlowResult) {
	t.Helper()
	svc := setupE2EService(t, dbName)
	configureE2EServiceEnvironments(t, svc, dbName, environments...)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": baseSchemaUsersTable}, schemabotConfig, dbName)
	return newE2EHandler(t, svc, client), mux, result
}

// requestPlan comments `comment` on the PR and returns the plan comment
// SchemaBot posts in reply.
func requestPlan(t *testing.T, h *Handler, result *planFlowResult, comment string) string {
	t.Helper()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: comment, isPR: true}, nil))
	require.Equal(t, http.StatusOK, rr.Code)
	select {
	case body := <-result.comments:
		require.Contains(t, body, "Schema Change Plan")
		return body
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the plan comment")
		return ""
	}
}

// A PR branched off main before main changed the schema directory the
// environment plans from. The plan still posts, since a reviewer needs to see
// it, but it leads with a notice that the branch is missing main's newer
// change and that apply is blocked until the branch has it, so nobody reads
// the plan's DDL as what merging would apply.
func TestE2EPlanCommandWarnsBranchMissingBaseSchemaChanges(t *testing.T) {
	h, mux, result := newBaseSchemaPlanFlow(t, "webhook_plan_stale_base")
	registerStaleBaseSchemaComparison(t, mux, result)

	body := requestPlan(t, h, result, "schemabot plan -e staging")

	assert.Contains(t, body, "**This branch is missing newer changes from `main` to `schema`.**")
	assert.Contains(t, body, "apply is blocked until the branch has them")
	assert.Contains(t, body, "CREATE TABLE `users`", "the plan itself still posts")
}

// The same notice leads each environment of a plan covering every
// environment, which is the comment a PR gets from a bare `schemabot plan`.
func TestE2EMultiEnvPlanWarnsBranchMissingBaseSchemaChanges(t *testing.T) {
	h, mux, result := newBaseSchemaPlanFlow(t, "webhook_multienv_plan_stale_base")
	registerStaleBaseSchemaComparison(t, mux, result)

	body := requestPlan(t, h, result, "schemabot plan")

	assert.Contains(t, body, "**This branch is missing newer changes from `main` to `schema`.**")
	assert.Contains(t, body, "CREATE TABLE `users`")
}

// A PR whose branch has every change main made to its schema gets a plan
// with no freshness notice.
func TestE2EPlanCommandHasNoNoticeWhenBaseSchemaIsCurrent(t *testing.T) {
	h, _, result := newBaseSchemaPlanFlow(t, "webhook_plan_current_base")

	body := requestPlan(t, h, result, "schemabot plan -e staging")

	assert.NotContains(t, body, "is missing newer changes")
	assert.NotContains(t, body, "Could not check whether")
}

// GitHub fails the merge-base comparison. The plan still posts, and says the
// check could not run rather than implying the branch is current, since the
// apply checks again and fails closed.
func TestE2EPlanCommandSaysBaseSchemaCouldNotBeChecked(t *testing.T) {
	h, mux, result := newBaseSchemaPlanFlow(t, "webhook_plan_unchecked_base")
	registerFailingBaseSchemaComparison(mux, result)

	body := requestPlan(t, h, result, "schemabot plan -e staging")

	assert.Contains(t, body, "**Could not check whether `main` has newer changes to this schema.** Apply checks again before it runs.")
	assert.NotContains(t, body, "is missing newer changes")
	assert.Contains(t, body, "CREATE TABLE `users`")
}

// registerStaleBaseSchemaComparisonForHead serves the stale merge-base
// comparison for a PR head other than the default, on top of the routes
// registerStaleBaseSchemaComparison serves.
func registerStaleBaseSchemaComparisonForHead(t *testing.T, mux *http.ServeMux, headSHA string) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/base-tip-sha..."+headSHA, func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{
			MergeBaseCommit: &gh.RepositoryCommit{SHA: new("def456")},
		}))
	})
}

// A push whose auto-plan finds no DDL would normally post no comment on a PR
// that has never shown one, since the passing check run says it all. A branch
// missing main's newer schema change is not that case: apply will refuse it,
// so the auto-plan posts the notice. The check run still passes, since the
// notice is comment text and changes no check state.
func TestE2EAutoPlanNoChangesWarnsBranchMissingBaseSchemaChanges(t *testing.T) {
	dbName := "webhook_autoplan_nochange_stale_base"
	f := setupNoChangesAutoPlan(t, dbName)
	registerStaleBaseSchemaComparison(t, f.mux, f.result)

	f.synchronize(t, "", "abc123")

	select {
	case body := <-f.result.comments:
		assert.Contains(t, body, "**This branch is missing newer changes from `main` to `schema`.**")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the plan comment carrying the base-schema notice")
	}
	requireOnlyVisiblePlanComment(t, f, dbName, "abc123", false)
}

// A PR shows a plan with DDL, then main changes the schema and the author
// pushes a commit that touches no schema input. Such a push normally keeps the
// visible plan, since its schema inputs did not change. The base branch moved
// without a push to the PR, though, so the visible plan cannot carry the
// notice: the auto-plan posts one that does and the prior head's plan goes.
func TestE2EAutoPlanSchemaNeutralSyncReplacesPlanWithoutBaseSchemaNotice(t *testing.T) {
	dbName := "webhook_autoplan_neutral_stale_base"
	f := setupAutoPlanCommentFixture(t, dbName, false)
	schemaNeutralSync(t, f)
	registerStaleBaseSchemaComparison(t, f.mux, f.result)
	registerStaleBaseSchemaComparisonForHead(t, f.mux, "newsha")
	insertPlanCommentRowWithOutcome(t, f.svc.Storage(), "octocat/hello-world", 1, dbName, "staging", "oldsha", 9001, "IC_shows_ddl", false)

	f.synchronize(t, "oldsha", "newsha")

	select {
	case body := <-f.result.comments:
		assert.Contains(t, body, "**This branch is missing newer changes from `main` to `schema`.**")
		assert.Contains(t, body, "CREATE TABLE")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the plan comment carrying the base-schema notice")
	}
	select {
	case id := <-f.deleted:
		assert.Equal(t, "9001", id)
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the prior head's plan comment to be deleted")
	}
	requireOnlyVisiblePlanComment(t, f, dbName, "newsha", false)
}

// A plan covering staging and production checks both environments' schema
// against main in one comparison: one read of main's tip and one merge-base
// lookup, however many environments the comment covers.
func TestE2EMultiEnvPlanComparesBaseSchemaOnce(t *testing.T) {
	h, mux, result := newBaseSchemaPlanFlow(t, "webhook_multienv_plan_one_compare", "production")
	var compares atomic.Int32
	registerStaleBaseSchemaComparisonWithHook(t, mux, result, func() { compares.Add(1) })

	body := requestPlan(t, h, result, "schemabot plan")

	assert.Contains(t, body, "Staging")
	assert.Contains(t, body, "Production")
	assert.Contains(t, body, "is missing newer changes from `main` to `schema`")
	assert.Equal(t, int32(1), compares.Load(), "one merge-base comparison covers every environment")
}

// registerFailingBaseSchemaComparison moves the base branch and fails the
// merge-base comparison the base-schema check reads.
func registerFailingBaseSchemaComparison(mux *http.ServeMux, result *planFlowResult) {
	tip := func() string { return "base-tip-sha" }
	result.BaseTip.Store(&tip)
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/base-tip-sha...abc123", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
}

// GitHub fails the comparison for a plan covering staging and production.
// Each environment says the check could not run, rather than the comment
// implying either is current.
func TestE2EMultiEnvPlanSaysBaseSchemaCouldNotBeChecked(t *testing.T) {
	h, mux, result := newBaseSchemaPlanFlow(t, "webhook_multienv_plan_unchecked_base", "production")
	registerFailingBaseSchemaComparison(mux, result)

	body := requestPlan(t, h, result, "schemabot plan")

	assert.Contains(t, body, "Production")
	assert.Contains(t, body, "**Could not check whether `main` has newer changes to this schema.**")
	assert.NotContains(t, body, "is missing newer changes")
}

// A plan narrowed to one rollout target with --target diffs the same branch,
// so it carries the same notice when the branch is missing a newer base
// change.
func TestE2ENarrowedPlanWarnsBranchMissingBaseSchemaChanges(t *testing.T) {
	p := newNamespacePlacementHarness(t, []api.TargetEntry{
		{Target: "orders-001", Namespaces: []string{"ns_0"}},
		{Target: "orders-002", Namespaces: []string{"ns_1"}},
	})
	registerStaleBaseSchemaComparison(t, p.mux, p.github)

	body := p.comment(t, "schemabot plan -e production --target orders-001")

	assert.Contains(t, body, "**This branch is missing newer changes from `main` to `schema`.**")
	assert.Contains(t, body, "orders-001")
}

// A PR shows a plan with DDL that warns the branch is missing a newer base
// change. The author merges main, a push that touches no schema input. The
// schema inputs did not change, but the warning no longer holds, so the
// auto-plan posts a plan without it and the warning's comment goes, rather
// than leaving reviewers told that apply is blocked.
func TestE2EAutoPlanSchemaNeutralSyncReplacesPlanWhoseBaseSchemaNoticeNoLongerHolds(t *testing.T) {
	dbName := "webhook_autoplan_neutral_notice_cleared"
	f := setupAutoPlanCommentFixture(t, dbName, false)
	schemaNeutralSync(t, f)
	require.NoError(t, f.svc.Storage().PlanComments().Insert(t.Context(), &storage.PlanComment{
		Repository: "octocat/hello-world", PullRequest: 1, DatabaseName: dbName, DatabaseType: "mysql",
		EnvironmentScope: "staging", HeadSHA: "oldsha", GitHubCommentID: 9001, GitHubNodeID: "IC_shows_notice",
		BaseSchemaNotice: true,
	}))

	f.synchronize(t, "oldsha", "newsha")

	select {
	case body := <-f.result.comments:
		assert.NotContains(t, body, "is missing newer changes")
		assert.Contains(t, body, "CREATE TABLE")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the plan comment without the base-schema notice")
	}
	select {
	case id := <-f.deleted:
		assert.Equal(t, "9001", id)
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the comment carrying the stale notice to be deleted")
	}
	requireOnlyVisiblePlanComment(t, f, dbName, "newsha", false)
}
