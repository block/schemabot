package github

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bradleyfalzon/ghinstallation/v2"
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
	transport := newListenOnlyTransport(base, listenOnlyRepoSet{listenOnlyTestRepo: true}, 42, func() string { return "acme-schemabot" })

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
	assert.Equal(t, "acme-schemabot", attrs["github_app"])
}

type closeTrackingBody struct {
	io.Reader
	closed bool
}

func (b *closeTrackingBody) Close() error {
	b.closed = true
	return nil
}

// The rate-limit transport wraps the metrics transport directly, so cooldown
// and quota telemetry keeps the App and installation that made the request.
func TestInstallationBaseTransportKeepsRateLimitIdentity(t *testing.T) {
	quota, ok := newInstallationBaseTransport(42, func() string { return "acme-schemabot" }).(*githubQuotaWaitTransport)
	require.True(t, ok, "the rate-limit transport is the outermost base transport")
	assert.Equal(t, int64(42), quota.installationID)
	assert.Equal(t, "acme-schemabot", quota.appSlug())
}

// A deployment without listen-only repositories sends every request through
// the installation token transport unchanged. With one, the listen-only guard
// wraps the token transport, so token exchanges never reach the guard.
func TestInstallationTransportOrder(t *testing.T) {
	key := testInstallationPrivateKey(t)

	withoutListenOnly, err := newInstallationTransport(1, 42, key, nil, listenOnlyRepoSet{})
	require.NoError(t, err)
	_, ok := withoutListenOnly.(*ghinstallation.Transport)
	assert.True(t, ok, "without listen-only repositories the token transport is outermost, got %T", withoutListenOnly)

	withListenOnly, err := newInstallationTransport(1, 42, key, nil, listenOnlyRepoSet{listenOnlyTestRepo: true})
	require.NoError(t, err)
	guard, ok := withListenOnly.(*listenOnlyTransport)
	require.True(t, ok, "the listen-only guard is outermost, got %T", withListenOnly)
	_, ok = guard.base.(*ghinstallation.Transport)
	assert.True(t, ok, "the listen-only guard sends through the token transport, got %T", guard.base)
}

// The first request an installation client sends is often a read of a
// listen-only repository, and the token exchange it triggers carries that
// read's context. The exchange still reaches GitHub, so the read succeeds,
// and a write to the same repository is still refused.
func TestListenOnlyInstallationClientExchangesTokenForRead(t *testing.T) {
	var (
		mu       sync.Mutex
		received []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		received = append(received, r.Method+" "+r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/app/installations/42/access_tokens" {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "ghs_test", "expires_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)})
			return
		}
		assert.Equal(t, "token ghs_test", r.Header.Get("Authorization"), "API requests use the exchanged installation token")
		_ = json.NewEncoder(w).Encode(map[string]any{"total_count": 0, "check_runs": []any{}})
	}))
	t.Cleanup(server.Close)

	transport, err := newInstallationTransport(1, 42, testInstallationPrivateKey(t), nil, listenOnlyRepoSet{listenOnlyTestRepo: true})
	require.NoError(t, err)
	guard, ok := transport.(*listenOnlyTransport)
	require.True(t, ok)
	tokenTransport, ok := guard.base.(*ghinstallation.Transport)
	require.True(t, ok)
	tokenTransport.BaseURL = server.URL

	client := gh.NewClient(&http.Client{Transport: transport})
	client.DisableRateLimitCheck = true
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL
	ic := NewInstallationClientWithSlug(client, slog.New(slog.NewTextHandler(io.Discard, nil)), "acme-schemabot")

	// FindCheckRunByName names its repository in the request context, which
	// the token exchange it triggers inherits.
	run, _, err := ic.FindCheckRunByName(t.Context(), listenOnlyTestRepo, "abc123", "SchemaBot")
	require.NoError(t, err, "a read of a listen-only repository must not be blocked by its token exchange")
	assert.Nil(t, run)

	_, _, err = ic.CreateIssueComment(t.Context(), listenOnlyTestRepo, 1, "plan")
	require.ErrorIs(t, err, ErrListenOnlyRepository)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{
		"POST /app/installations/42/access_tokens",
		"GET /repos/acme/monorepo/commits/abc123/check-runs",
	}, received)
}

func testInstallationPrivateKey(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}
