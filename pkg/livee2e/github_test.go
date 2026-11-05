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

// newTestGitHub returns a client for owner/repo whose API calls go to handler.
func newTestGitHub(t *testing.T, owner, repo string, handler http.Handler) *apiGitHub {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	base, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client := github.NewClient(server.Client())
	client.BaseURL = base
	return &apiGitHub{client: client, owner: owner, repo: repo}
}

// Another App can post a Check Run with SchemaBot's name. Only SchemaBot's
// App counts, and among its runs the newest one.
func TestCheckRunReadsOnlyTheAppsNewestRun(t *testing.T) {
	runs := []*github.CheckRun{
		{ID: new(int64(3)), Status: new("completed"), Conclusion: new("success"), App: &github.App{Slug: new("other-app")}},
		{ID: new(int64(1)), Status: new("completed"), Conclusion: new("success"), App: &github.App{Slug: new("schemabot")}},
		{ID: new(int64(2)), Status: new("in_progress"), App: &github.App{Slug: new("schemabot")}},
	}
	gh := newTestGitHub(t, "octo", "schema", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/repos/octo/schema/commits/sha-1/check-runs", r.URL.Path)
		assert.Equal(t, "SchemaBot (development)", r.URL.Query().Get("check_name"))
		assert.NoError(t, json.NewEncoder(w).Encode(github.ListCheckRunsResults{Total: new(len(runs)), CheckRuns: runs}))
	}))

	got, err := gh.CheckRun(t.Context(), "sha-1", "SchemaBot (development)", "schemabot")
	require.NoError(t, err)
	assert.Equal(t, &CheckRun{Status: "in_progress"}, got)

	got, err = gh.CheckRun(t.Context(), "sha-1", "SchemaBot (development)", "missing-app")
	require.NoError(t, err)
	assert.Nil(t, got)
}

// GitHub answers 422 both for a branch that is already gone and for a
// deletion it refuses. Only the first counts as deleted.
func TestDeleteBranchTreatsOnlyAMissingBranchAsDeleted(t *testing.T) {
	for _, tc := range []struct {
		name      string
		refStatus int
		wantErr   bool
	}{
		{name: "branch already gone", refStatus: http.StatusNotFound},
		{name: "deletion refused", refStatus: http.StatusOK, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gh := newTestGitHub(t, "octo", "schema", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodDelete:
					assert.Equal(t, "/repos/octo/schema/git/refs/heads/schemabot-e2e/run", r.URL.Path)
					w.WriteHeader(http.StatusUnprocessableEntity)
					_, err := w.Write([]byte(`{"message":"Reference cannot be deleted"}`))
					assert.NoError(t, err)
				case http.MethodGet:
					assert.Equal(t, "/repos/octo/schema/git/ref/heads/schemabot-e2e/run", r.URL.Path)
					w.WriteHeader(tc.refStatus)
					_, err := w.Write([]byte(`{"ref":"refs/heads/schemabot-e2e/run","object":{"sha":"sha-1"}}`))
					assert.NoError(t, err)
				}
			}))

			err := gh.DeleteBranch(t.Context(), "schemabot-e2e/run")
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

// A path that names a directory has no blob SHA to replace.
func TestPutFileRejectsADirectory(t *testing.T) {
	gh := newTestGitHub(t, "octo", "schema", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		_, err := w.Write([]byte(`[{"type":"file","name":"orders.sql","path":"schema/orders.sql"}]`))
		assert.NoError(t, err)
	}))

	_, err := gh.PutFile(t.Context(), "schemabot-e2e/run", "schema", "add table", []byte("CREATE TABLE t (id int);"))
	require.ErrorContains(t, err, "is a directory")
}

// GitHub can report the repository in a different case than the client was
// configured with. A fork's pull request still stays out.
func TestOpenPullRequestsMatchesTheRepositoryInAnyCase(t *testing.T) {
	prs := []*github.PullRequest{
		{Number: new(1), Head: &github.PullRequestBranch{Ref: new("schemabot-e2e/a"), Repo: &github.Repository{FullName: new("octo/schema")}}},
		{Number: new(2), Head: &github.PullRequestBranch{Ref: new("schemabot-e2e/b"), Repo: &github.Repository{FullName: new("fork/schema")}}},
		{Number: new(3), Head: &github.PullRequestBranch{Ref: new("feature"), Repo: &github.Repository{FullName: new("octo/schema")}}},
	}
	gh := newTestGitHub(t, "Octo", "Schema", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.NoError(t, json.NewEncoder(w).Encode(prs))
	}))

	got, err := gh.OpenPullRequests(t.Context(), "schemabot-e2e/")
	require.NoError(t, err)
	assert.Equal(t, []PullRequest{{Number: 1, Branch: "schemabot-e2e/a"}}, got)
}
