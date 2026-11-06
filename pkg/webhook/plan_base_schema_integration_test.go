//go:build integration

package webhook

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const baseSchemaUsersTable = "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

// newBaseSchemaPlanFlow serves a PR proposing a users table to a handler
// planning dbName against a real target.
func newBaseSchemaPlanFlow(t *testing.T, dbName string) (*Handler, *http.ServeMux, *planFlowResult) {
	t.Helper()
	svc := setupE2EService(t, dbName)

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
	tip := func() string { return "base-tip-sha" }
	result.BaseTip.Store(&tip)
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/base-tip-sha...abc123", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	body := requestPlan(t, h, result, "schemabot plan -e staging")

	assert.Contains(t, body, "**Could not check whether `main` has newer changes to this schema.** Apply checks again before it runs.")
	assert.NotContains(t, body, "is missing newer changes")
	assert.Contains(t, body, "CREATE TABLE `users`")
}
