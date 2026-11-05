package livee2e

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Another App can post a Check Run with SchemaBot's name. Only SchemaBot's
// App counts, and among its runs the newest one.
func TestCheckRunReadsOnlyTheAppsNewestRun(t *testing.T) {
	runs := []*github.CheckRun{
		{ID: new(int64(3)), Status: new("completed"), Conclusion: new("success"), App: &github.App{Slug: new("other-app")}},
		{ID: new(int64(1)), Status: new("completed"), Conclusion: new("success"), App: &github.App{Slug: new("schemabot")}},
		{ID: new(int64(2)), Status: new("in_progress"), App: &github.App{Slug: new("schemabot")}},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/repos/octo/schema/commits/sha-1/check-runs", r.URL.Path)
		assert.Equal(t, "SchemaBot (development)", r.URL.Query().Get("check_name"))
		assert.NoError(t, json.NewEncoder(w).Encode(github.ListCheckRunsResults{Total: new(len(runs)), CheckRuns: runs}))
	}))
	defer server.Close()
	base, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client := github.NewClient(server.Client())
	client.BaseURL = base
	gh := &apiGitHub{client: client, owner: "octo", repo: "schema"}

	got, err := gh.CheckRun(t.Context(), "sha-1", "SchemaBot (development)", "schemabot")
	require.NoError(t, err)
	assert.Equal(t, &CheckRun{Status: "in_progress"}, got)

	got, err = gh.CheckRun(t.Context(), "sha-1", "SchemaBot (development)", "missing-app")
	require.NoError(t, err)
	assert.Nil(t, got)
}
