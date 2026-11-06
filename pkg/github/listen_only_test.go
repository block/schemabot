package github

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/metrics"
)

const (
	listenOnlyTestRepo = "acme/monorepo"
	onboardedTestRepo  = "acme/onboarded"
)

type listenOnlyRepoSet map[string]bool

func (s listenOnlyRepoSet) HasListenOnlyRepos() bool {
	return len(s) > 0
}

func (s listenOnlyRepoSet) IsRepoListenOnly(repo string) bool {
	return s[strings.ToLower(repo)]
}

// recordingGitHubServer answers every request with a minimal success body and
// records the method and path of each one it receives.
type recordingGitHubServer struct {
	mu       sync.Mutex
	received []string
}

func (s *recordingGitHubServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.received = append(s.received, r.Method+" "+r.URL.Path)
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodDelete {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err := json.NewEncoder(w).Encode(map[string]any{"id": 7, "node_id": "IC_7", "body": "hello"}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *recordingGitHubServer) requests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.received...)
}

func newListenOnlyTestClient(t *testing.T, recorder *recordingGitHubServer) *InstallationClient {
	t.Helper()
	server := httptest.NewServer(recorder)
	t.Cleanup(server.Close)

	transport := newListenOnlyTransport(newGitHubMetricsTransport(http.DefaultTransport, 42, nil), listenOnlyRepoSet{listenOnlyTestRepo: true}, 42, nil)
	client := gh.NewClient(&http.Client{Transport: transport})
	client.DisableRateLimitCheck = true
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL
	return NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// A listen-only repository is read as usual, but every client method that
// writes to it fails with ErrListenOnlyRepository before anything reaches
// GitHub, so a pull request in that repository never shows a comment,
// reaction, review request, or Check Run from SchemaBot.
func TestListenOnlyRepositoryRefusesEveryWrite(t *testing.T) {
	recorder := &recordingGitHubServer{}
	ic := newListenOnlyTestClient(t, recorder)
	ctx := t.Context()

	body, err := ic.GetIssueComment(ctx, listenOnlyTestRepo, 7)
	require.NoError(t, err)
	assert.Equal(t, "hello", body)

	checkRun := CheckRunOptions{Name: "SchemaBot", Status: "completed", Conclusion: "success", Output: &CheckRunOutput{Title: "t", Summary: "s"}}
	writes := map[string]func() error{
		"create issue comment": func() error {
			_, _, err := ic.CreateIssueComment(ctx, listenOnlyTestRepo, 1, "plan")
			return err
		},
		"edit issue comment":   func() error { return ic.EditIssueComment(ctx, listenOnlyTestRepo, 7, "plan") },
		"delete issue comment": func() error { return ic.DeleteIssueComment(ctx, listenOnlyTestRepo, 7) },
		"minimize comment":     func() error { return ic.MinimizeComment(ctx, listenOnlyTestRepo, "IC_7") },
		"add reaction":         func() error { return ic.AddReactionToComment(ctx, listenOnlyTestRepo, 7, "eyes") },
		"create check run": func() error {
			_, err := ic.CreateCheckRun(ctx, listenOnlyTestRepo, "abc123", checkRun)
			return err
		},
		"update check run": func() error { return ic.UpdateCheckRun(ctx, listenOnlyTestRepo, 9, checkRun) },
		"request reviewers": func() error {
			return ic.RequestReviewers(ctx, listenOnlyTestRepo, 1, []string{"octocat"}, nil)
		},
	}
	for name, write := range writes {
		err := write()
		assert.ErrorIs(t, err, ErrListenOnlyRepository, "%s should be refused", name)
	}

	assert.Equal(t, []string{"GET /repos/acme/monorepo/issues/comments/7"}, recorder.requests(),
		"only the read should reach GitHub")
}

// Repositories that are not listen-only are written to as usual by the same
// client.
func TestListenOnlyTransportWritesToOtherRepositories(t *testing.T) {
	recorder := &recordingGitHubServer{}
	ic := newListenOnlyTestClient(t, recorder)

	id, _, err := ic.CreateIssueComment(t.Context(), onboardedTestRepo, 1, "plan")
	require.NoError(t, err)
	assert.Equal(t, int64(7), id)
	assert.Equal(t, []string{"POST /repos/acme/onboarded/issues/1/comments"}, recorder.requests())
}

// GraphQL sends reads as POST, so a GraphQL request is sent only when its
// operation is a known read. A GraphQL request names its repository only in
// the request context, so a GraphQL write without one is refused rather than
// assumed to target another repository. An installation token exchange names
// no repository and is always sent.
func TestListenOnlyTransportClassifiesRequestsWithoutRepositoryPath(t *testing.T) {
	tests := []struct {
		name      string
		method    string
		path      string
		operation string
		wantSent  bool
	}{
		{name: "graphql read", method: http.MethodPost, path: "/graphql", operation: metrics.GitHubOperationGraphQLTreeEntries, wantSent: true},
		{name: "graphql mutation", method: http.MethodPost, path: "/graphql", operation: metrics.GitHubOperationGraphQLMinimizeComment, wantSent: false},
		{name: "graphql unknown operation", method: http.MethodPost, path: "/graphql", operation: metrics.GitHubOperationUnknown, wantSent: false},
		{name: "graphql mutation without repository", method: http.MethodPost, path: "/graphql", wantSent: false},
		{name: "installation token", method: http.MethodPost, path: "/app/installations/42/access_tokens", wantSent: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sent := false
			base := roundTripFunc(func(*http.Request) (*http.Response, error) {
				sent = true
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
			})
			transport := newListenOnlyTransport(base, listenOnlyRepoSet{listenOnlyTestRepo: true}, 42, nil)

			ctx := t.Context()
			if tt.operation != "" {
				ctx = withGitHubRateLimitContext(ctx, tt.operation, listenOnlyTestRepo)
			}
			req, err := http.NewRequestWithContext(ctx, tt.method, "https://api.github.com"+tt.path, strings.NewReader("{}"))
			require.NoError(t, err)

			resp, err := transport.RoundTrip(req)
			if resp != nil {
				require.NoError(t, resp.Body.Close())
			}
			assert.Equal(t, tt.wantSent, sent)
			if tt.wantSent {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrListenOnlyRepository)
			}
		})
	}
}

// A refused write is counted on the GitHub request metric as suppressed, so a
// listen-only deployment shows how many writes it would have made without
// counting them as traffic or errors.
func TestListenOnlyTransportRecordsSuppressedWrites(t *testing.T) {
	reader := newGitHubMetricsTestReader(t)
	base := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("a suppressed write must not reach the base transport")
	})
	transport := newListenOnlyTransport(base, listenOnlyRepoSet{listenOnlyTestRepo: true}, 42, func() string { return "schemabot-dev" })

	body := &closeTrackingBody{Reader: strings.NewReader(`{"body":"plan"}`)}
	req, err := http.NewRequestWithContext(context.WithoutCancel(t.Context()), http.MethodPost, "https://api.github.com/repos/ACME/monorepo/issues/1/comments", body)
	require.NoError(t, err)

	resp, err := transport.RoundTrip(req)
	if resp != nil {
		require.NoError(t, resp.Body.Close())
	}
	require.Nil(t, resp)
	require.ErrorIs(t, err, ErrListenOnlyRepository)
	assert.True(t, body.closed, "the request body should be closed even though nothing was sent")

	total, attrs := collectGitHubRequestCounter(t, reader)
	assert.Equal(t, int64(1), total)
	assert.Equal(t, metrics.GitHubRequestStatusSuppressed, attrs["status"])
	assert.Equal(t, metrics.GitHubRequestCategoryWrite, attrs["category"])
	assert.Equal(t, metrics.GitHubOperationCreateIssueComment, attrs["operation"])
	assert.Equal(t, "schemabot-dev", attrs["github_app"])
}

type closeTrackingBody struct {
	io.Reader
	closed bool
}

func (b *closeTrackingBody) Close() error {
	b.closed = true
	return nil
}

// A deployment without listen-only repositories sends every request without
// inspecting it, and the rate-limit transport wraps the metrics transport
// directly either way, so cooldown and quota telemetry keeps the App and
// installation that made the request.
func TestInstallationBaseTransportOrder(t *testing.T) {
	appSlug := func() string { return "schemabot-dev" }

	withoutListenOnly := newInstallationBaseTransport(42, appSlug, listenOnlyRepoSet{})
	quota, ok := withoutListenOnly.(*githubQuotaWaitTransport)
	require.True(t, ok, "without listen-only repositories the rate-limit transport is outermost, got %T", withoutListenOnly)
	assert.Equal(t, int64(42), quota.installationID)
	assert.Equal(t, "schemabot-dev", quota.appSlug())

	withListenOnly := newInstallationBaseTransport(42, appSlug, listenOnlyRepoSet{listenOnlyTestRepo: true})
	guard, ok := withListenOnly.(*listenOnlyTransport)
	require.True(t, ok, "the listen-only guard wraps the rate-limit transport, got %T", withListenOnly)
	quota, ok = guard.base.(*githubQuotaWaitTransport)
	require.True(t, ok, "the listen-only guard sends through the rate-limit transport, got %T", guard.base)
	assert.Equal(t, int64(42), quota.installationID)
	assert.Equal(t, "schemabot-dev", quota.appSlug())
}
