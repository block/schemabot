package webhook

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/webhook/templates"
)

func TestAssertSchemaStillCurrent_MatchProceeds(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	schema := &ghclient.SchemaRequestResult{
		Repository: "octocat/hello-world",
		Database:   "testdb",
		Type:       "mysql",
		HeadSHA:    "sha-abc",
	}

	rejected := h.assertSchemaStillCurrent(
		t.Context(),
		"octocat/hello-world", 1, int64(12345),
		schema,
		"sha-abc",
		"staging",
		"alice",
		"apply-confirm",
	)

	assert.False(t, rejected, "matching SHAs must not reject")

	select {
	case body := <-comments:
		t.Fatalf("no comment should be posted on a match; got: %s", body)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestAssertSchemaStillCurrent_MismatchRejectsAndPostsComment(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	schema := &ghclient.SchemaRequestResult{
		Repository: "octocat/hello-world",
		Database:   "testdb",
		Type:       "mysql",
		HeadSHA:    "discovery-sha",
	}

	rejected := h.assertSchemaStillCurrent(
		t.Context(),
		"octocat/hello-world", 1, int64(12345),
		schema,
		"current-sha",
		"staging",
		"alice",
		"apply-confirm",
	)

	require.True(t, rejected, "differing SHAs must reject")

	select {
	case body := <-comments:
		assert.Contains(t, body, "Rejected", "title should call out rejection")
		assert.Contains(t, body, "new commits since discovery", "should explain why")
		assert.Contains(t, body, "discovery-sha", "must include discovery SHA")
		assert.Contains(t, body, "current-sha", "must include current SHA")
		assert.Contains(t, body, "testdb", "must include database")
		assert.Contains(t, body, "staging", "must include environment")
		assert.Contains(t, body, "schemabot apply-confirm -e staging", "retry hint must use the action and env")
		assert.Contains(t, body, "alice", "must attribute the requester")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for rejection comment")
	}
}

func TestMetricActionKey(t *testing.T) {
	cases := map[string]string{
		"plan":          "plan",
		"apply":         "apply",
		"apply-confirm": "apply_confirm",
	}
	for in, want := range cases {
		assert.Equal(t, want, metricActionKey(in), "input=%q", in)
	}
}

// The base-schema check cannot read the PR, so it knows neither the base
// branch nor the merge base. Every environment of the plan says the check
// could not run, with no base branch to name, rather than passing as current.
func TestPlanBaseSchemasUnverifiedWhenPullRequestReadFails(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")
	h := &Handler{logger: testLogger()}

	schema := &ghclient.SchemaRequestResult{Database: "orders", Type: "mysql", SchemaPath: "schema", HeadSHA: "abc123"}
	results := h.planBaseSchemas(t.Context(), ghclient.NewInstallationClient(client, testLogger()), "octocat/hello-world", 1,
		map[string]*ghclient.SchemaRequestResult{"staging": schema, "production": schema})

	require.Len(t, results, 2)
	for _, env := range []string{"staging", "production"} {
		assert.Equal(t, &templates.BaseSchemaData{Unverified: true}, results[env], env)
	}
}
