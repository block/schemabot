package livee2e

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
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

// pagedServer serves pages, numbered from 1, and links each to the next the
// way GitHub does. A client that asks for more pages than there are gets an
// empty, unlinked one, so a loop that stops advancing fails instead of
// spinning.
func pagedServer[T any](t *testing.T, pages []T, check func(*http.Request)) http.Handler {
	t.Helper()
	served := 0
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if check != nil {
			check(r)
		}
		served++
		if served > len(pages) {
			t.Errorf("the client asked for %d pages, but there are %d", served, len(pages))
			var empty T
			assert.NoError(t, json.NewEncoder(w).Encode(empty))
			return
		}
		page := 1
		if p := r.URL.Query().Get("page"); p != "" {
			var err error
			page, err = strconv.Atoi(p)
			require.NoError(t, err)
		}
		require.LessOrEqual(t, page, len(pages), "the client asked for a page past the last")
		if page < len(pages) {
			next := *r.URL
			q := next.Query()
			q.Set("page", strconv.Itoa(page+1))
			next.RawQuery = q.Encode()
			w.Header().Set("Link", fmt.Sprintf(`<http://%s%s>; rel="next"`, r.Host, next.RequestURI()))
		}
		assert.NoError(t, json.NewEncoder(w).Encode(pages[page-1]))
	})
}

// A pull request with more than one page of comments still yields the newest
// one, which is the one SchemaBot posted last.
func TestCommentsFollowsEveryPage(t *testing.T) {
	comment := func(id int64, login, kind string) *github.IssueComment {
		return &github.IssueComment{ID: new(id), Body: new(fmt.Sprintf("comment %d", id)), User: &github.User{Login: new(login), Type: new(kind)}}
	}
	pages := [][]*github.IssueComment{
		{comment(1, "octocat", "User"), comment(2, "schemabot[bot]", "Bot")},
		{comment(3, "octocat", "User")},
		{comment(4, "schemabot[bot]", "Bot")},
	}
	gh := newTestGitHub(t, "octo", "schema", pagedServer(t, pages, func(r *http.Request) {
		assert.Equal(t, "/repos/octo/schema/issues/7/comments", r.URL.Path)
	}))

	got, err := gh.Comments(t.Context(), 7)
	require.NoError(t, err)
	require.Len(t, got, 4)
	for i, c := range got {
		assert.Equal(t, int64(i+1), c.ID)
	}
	assert.Equal(t, Comment{ID: 4, Author: "schemabot[bot]", IsBot: true, Body: "comment 4"}, got[3])
}

func TestCheckRunFollowsEveryPage(t *testing.T) {
	run := func(id int64, status string) *github.CheckRun {
		return &github.CheckRun{ID: new(id), Status: new(status), App: &github.App{Slug: new("schemabot")}}
	}
	pages := []github.ListCheckRunsResults{
		{Total: new(2), CheckRuns: []*github.CheckRun{run(1, "in_progress")}},
		{Total: new(2), CheckRuns: []*github.CheckRun{run(2, "completed")}},
	}
	gh := newTestGitHub(t, "octo", "schema", pagedServer(t, pages, nil))

	got, err := gh.CheckRun(t.Context(), "sha-1", "SchemaBot (development)", "schemabot")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "completed", got.Status, "the newest run is on the last page")
}

func TestOpenPullRequestsFollowsEveryPage(t *testing.T) {
	pr := func(number int, branch string) *github.PullRequest {
		return &github.PullRequest{Number: new(number), Head: &github.PullRequestBranch{Ref: new(branch), Repo: &github.Repository{FullName: new("octo/schema")}}}
	}
	pages := [][]*github.PullRequest{
		{pr(1, "schemabot-e2e/a"), pr(2, "feature")},
		{pr(3, "schemabot-e2e/b")},
	}
	gh := newTestGitHub(t, "octo", "schema", pagedServer(t, pages, nil))

	got, err := gh.OpenPullRequests(t.Context(), "schemabot-e2e/")
	require.NoError(t, err)
	assert.Equal(t, []PullRequest{{Number: 1, Branch: "schemabot-e2e/a"}, {Number: 3, Branch: "schemabot-e2e/b"}}, got)
}

// putFileServer serves a contents lookup for schema/orders.sql that finds
// existing, or nothing when existing is nil, and records the write.
func putFileServer(t *testing.T, existing *github.RepositoryContent, written *github.RepositoryContentFileOptions) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/repos/octo/schema/contents/schema/orders.sql", r.URL.Path)
		switch r.Method {
		case http.MethodGet:
			assert.Equal(t, "schemabot-e2e/run", r.URL.Query().Get("ref"))
			if existing == nil {
				w.WriteHeader(http.StatusNotFound)
				_, err := w.Write([]byte(`{"message":"Not Found"}`))
				assert.NoError(t, err)
				return
			}
			assert.NoError(t, json.NewEncoder(w).Encode(existing))
		case http.MethodPut:
			assert.NoError(t, json.NewDecoder(r.Body).Decode(written))
			assert.NoError(t, json.NewEncoder(w).Encode(github.RepositoryContentResponse{Commit: github.Commit{SHA: new("commit-2")}}))
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	})
}

func TestPutFileCreatesAMissingFile(t *testing.T) {
	var written github.RepositoryContentFileOptions
	gh := newTestGitHub(t, "octo", "schema", putFileServer(t, nil, &written))

	sha, err := gh.PutFile(t.Context(), "schemabot-e2e/run", "schema/orders.sql", "add table", []byte("CREATE TABLE t (id int);"))
	require.NoError(t, err)
	assert.Equal(t, "commit-2", sha)
	assert.Nil(t, written.SHA, "creating a file takes no blob SHA")
	assert.Equal(t, "schemabot-e2e/run", written.GetBranch())
	assert.Equal(t, "add table", written.GetMessage())
	assert.Equal(t, "CREATE TABLE t (id int);", string(written.Content))
}

func TestPutFileReplacesAnExistingFile(t *testing.T) {
	existing := &github.RepositoryContent{
		Type:     new("file"),
		Path:     new("schema/orders.sql"),
		SHA:      new("blob-1"),
		Encoding: new("base64"),
		Content:  new(base64.StdEncoding.EncodeToString([]byte("CREATE TABLE t (id int);"))),
	}
	var written github.RepositoryContentFileOptions
	gh := newTestGitHub(t, "octo", "schema", putFileServer(t, existing, &written))

	_, err := gh.PutFile(t.Context(), "schemabot-e2e/run", "schema/orders.sql", "change column", []byte("CREATE TABLE t (id bigint);"))
	require.NoError(t, err)
	assert.Equal(t, "blob-1", written.GetSHA(), "replacing a file names the blob it replaces")
	assert.Equal(t, "CREATE TABLE t (id bigint);", string(written.Content))
}

func TestGitHubUnavailableReadsRateLimitsAndServerErrors(t *testing.T) {
	cases := map[string]struct {
		status int
		header map[string]string
		body   string
		want   bool
	}{
		"primary rate limit": {
			status: http.StatusForbidden,
			header: map[string]string{"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1793900000"},
			body:   `{"message":"API rate limit exceeded"}`,
			want:   true,
		},
		"secondary rate limit": {
			status: http.StatusForbidden,
			header: map[string]string{"Retry-After": "30"},
			body:   `{"message":"You have exceeded a secondary rate limit","documentation_url":"https://docs.github.com/rest/overview/rate-limits-for-the-rest-api#about-secondary-rate-limits"}`,
			want:   true,
		},
		"too many requests":    {status: http.StatusTooManyRequests, body: `{"message":"slow down"}`, want: true},
		"server error":         {status: http.StatusBadGateway, body: `{"message":"Server Error"}`, want: true},
		"missing branch":       {status: http.StatusNotFound, body: `{"message":"Not Found"}`, want: false},
		"forbidden":            {status: http.StatusForbidden, body: `{"message":"Resource not accessible by integration"}`, want: false},
		"unprocessable change": {status: http.StatusUnprocessableEntity, body: `{"message":"Validation Failed"}`, want: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			gh := newTestGitHub(t, "octo", "schema", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for k, v := range tc.header {
					w.Header().Set(k, v)
				}
				w.WriteHeader(tc.status)
				_, err := w.Write([]byte(tc.body))
				assert.NoError(t, err)
			}))
			_, err := gh.BranchHead(t.Context(), "main")
			require.Error(t, err)
			assert.Equal(t, tc.want, githubUnavailable(err), "%v", err)
		})
	}
	assert.False(t, githubUnavailable(errors.New("plain")))
	assert.False(t, githubUnavailable(nil))
}
