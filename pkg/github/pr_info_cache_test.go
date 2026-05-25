package github

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFetchPullRequest_CacheCollapsesDuplicateCallsWithinScope locks in the
// request-scoped dedup invariant: every FetchPullRequest call inside a
// single ctx scope (one WithPRInfoCache wrap) for the same (repo, pr)
// must hit GitHub exactly once, even when issued through different
// InstallationClients sharing that ctx.
func TestFetchPullRequest_CacheCollapsesDuplicateCallsWithinScope(t *testing.T) {
	server, calls := newPRFakeGitHubServer(t)
	defer server.Close()

	ic1 := newPRTestInstallationClient(t, server)
	ic2 := newPRTestInstallationClient(t, server)

	ctx := WithPRInfoCache(t.Context())

	want := &PullRequestInfo{HeadRef: "feature", HeadSHA: "abc123", BaseRef: "main", BaseSHA: "def456", User: "octocat"}

	got, err := ic1.FetchPullRequest(ctx, "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	got, err = ic1.FetchPullRequest(ctx, "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	got, err = ic2.FetchPullRequest(ctx, "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, want, got, "second InstallationClient sharing the same ctx-scoped cache must also see the cached result")

	assert.Equal(t, int32(1), calls.Load(),
		"three FetchPullRequest calls inside one ctx scope must collapse to one upstream GitHub call")
}

// TestFetchPullRequest_FreshCachePerScope locks in the structural safety
// invariant: a new WithPRInfoCache wrap (simulating a new webhook
// delivery) gets a fresh cache. Even if an earlier scope cached a result
// for (repo, pr), the next scope must refetch from GitHub. This is the
// property that prevents stale HeadSHA across deliveries.
func TestFetchPullRequest_FreshCachePerScope(t *testing.T) {
	server, calls := newPRFakeGitHubServer(t)
	defer server.Close()

	ic := newPRTestInstallationClient(t, server)

	// First scope (simulating delivery #1).
	ctx1 := WithPRInfoCache(t.Context())
	_, err := ic.FetchPullRequest(ctx1, "octo/repo", 42)
	require.NoError(t, err)
	_, err = ic.FetchPullRequest(ctx1, "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, int32(1), calls.Load(), "two calls in one scope should dedupe to one fetch")

	// Second scope (simulating delivery #2 — e.g., a synchronize event
	// after a new commit was pushed). Must NOT reuse the first scope's
	// cache.
	ctx2 := WithPRInfoCache(t.Context())
	_, err = ic.FetchPullRequest(ctx2, "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load(), "second scope must refetch — caches are not shared across scopes")
}

// TestFetchPullRequest_NoCacheFallsThrough verifies that a ctx without a
// request-scoped cache (tests or ad-hoc callers) falls through to a raw
// fetch on every call. No memoisation, no panic.
func TestFetchPullRequest_NoCacheFallsThrough(t *testing.T) {
	server, calls := newPRFakeGitHubServer(t)
	defer server.Close()

	ic := newPRTestInstallationClient(t, server)

	for range 3 {
		_, err := ic.FetchPullRequest(t.Context(), "octo/repo", 42)
		require.NoError(t, err)
	}
	assert.Equal(t, int32(3), calls.Load(), "no cache on ctx → every call must hit GitHub")
}

// TestFetchPullRequest_ErrorsAreNotCached verifies that a failed fetch
// does not poison the request-scoped cache: a subsequent call within the
// same scope must retry rather than return the stale error.
func TestFetchPullRequest_ErrorsAreNotCached(t *testing.T) {
	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/octo/repo/pulls/42", func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n < 3 {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"head": map[string]string{"sha": "abc123"},
			"base": map[string]string{},
			"user": map[string]string{},
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	ic := newPRTestInstallationClient(t, server)
	ctx := WithPRInfoCache(t.Context())

	_, err := ic.FetchPullRequest(ctx, "octo/repo", 42)
	require.Error(t, err)
	_, err = ic.FetchPullRequest(ctx, "octo/repo", 42)
	require.Error(t, err, "second call should re-attempt — errors must not be cached")

	got, err := ic.FetchPullRequest(ctx, "octo/repo", 42)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "abc123", got.HeadSHA)
	assert.Equal(t, int32(3), calls.Load())
}

// TestFetchPullRequestNoCache_AlwaysHitsGitHubAndDoesNotPoisonCache locks
// in the dedupe-friendly vs revalidation-friendly contract: the cached
// variant (FetchPullRequest) returns the request-scoped cached value;
// the bypassing variant (FetchPullRequestNoCache) always issues a fresh
// GitHub request even when a cached value exists for the same scope and
// (repo, pr). The bypassing variant must NOT write its fresh result
// back into the cache — otherwise discovery sites within the same scope
// would observe a phantom "second SHA" mid-delivery and lose internal
// consistency.
//
// This is the invariant that defends the auto-confirm / apply-confirm
// SHA re-checks against the staleness Codex flagged on PR #114: if a
// new commit lands after CreateSchemaRequestFromPR populates the cache,
// the revalidation fetch must see the current GitHub HEAD instead of
// the cached snapshot.
func TestFetchPullRequestNoCache_AlwaysHitsGitHubAndDoesNotPoisonCache(t *testing.T) {
	var calls atomic.Int32
	var headSHA atomic.Value
	headSHA.Store("abc")
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/octo/repo/pulls/42", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"head": map[string]string{"ref": "feature", "sha": headSHA.Load().(string)},
			"base": map[string]string{"ref": "main", "sha": "base"},
			"user": map[string]string{"login": "octocat"},
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	ic := newPRTestInstallationClient(t, server)
	ctx := WithPRInfoCache(t.Context())

	// Discovery phase: populate the cache with HeadSHA=abc.
	got, err := ic.FetchPullRequest(ctx, "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, "abc", got.HeadSHA)
	assert.Equal(t, int32(1), calls.Load(), "first FetchPullRequest must hit GitHub")

	// Dedupe-friendly second call: cache HIT, no GitHub round trip.
	got, err = ic.FetchPullRequest(ctx, "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, "abc", got.HeadSHA)
	assert.Equal(t, int32(1), calls.Load(), "second FetchPullRequest must dedupe via the cache")

	// Simulate a new commit landing on the PR branch between discovery
	// and the auto-confirm / apply-confirm revalidation.
	headSHA.Store("def")

	// Revalidation-friendly call: must bypass the cache and return def.
	fresh, err := ic.FetchPullRequestNoCache(ctx, "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, "def", fresh.HeadSHA,
		"FetchPullRequestNoCache must bypass the cache and return the current GitHub HEAD")
	assert.Equal(t, int32(2), calls.Load(), "FetchPullRequestNoCache must always hit GitHub")

	// Subsequent dedupe-friendly call must still observe the original
	// cached abc — FetchPullRequestNoCache must NOT have written def
	// back into the cache and corrupted other discovery callers' view.
	got, err = ic.FetchPullRequest(ctx, "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, "abc", got.HeadSHA,
		"FetchPullRequestNoCache must not poison the cache with its fresh result")
	assert.Equal(t, int32(2), calls.Load(), "post-bypass FetchPullRequest must still HIT the original cache entry")
}

// TestFetchPullRequestNoCache_NoCacheOnContextStillFetches verifies that
// the bypassing variant works the same way whether or not a request-scoped
// cache is attached — it always issues a fresh GitHub request.
func TestFetchPullRequestNoCache_NoCacheOnContextStillFetches(t *testing.T) {
	server, calls := newPRFakeGitHubServer(t)
	defer server.Close()

	ic := newPRTestInstallationClient(t, server)

	for range 3 {
		_, err := ic.FetchPullRequestNoCache(t.Context(), "octo/repo", 42)
		require.NoError(t, err)
	}
	assert.Equal(t, int32(3), calls.Load(), "FetchPullRequestNoCache must hit GitHub on every call regardless of ctx cache state")
}

// TestFetchPullRequest_ReturnsIndependentCopies verifies that callers
// within a single scope receive independent *PullRequestInfo values —
// mutating one must not affect another caller's view.
func TestFetchPullRequest_ReturnsIndependentCopies(t *testing.T) {
	server, _ := newPRFakeGitHubServer(t)
	defer server.Close()

	ic := newPRTestInstallationClient(t, server)
	ctx := WithPRInfoCache(t.Context())

	first, err := ic.FetchPullRequest(ctx, "octo/repo", 42)
	require.NoError(t, err)
	second, err := ic.FetchPullRequest(ctx, "octo/repo", 42)
	require.NoError(t, err)

	first.HeadSHA = "mutated"
	assert.Equal(t, "abc123", second.HeadSHA, "second caller must not observe the first caller's mutation")
}

// TestForInstallation_CachesByInstallationID locks in the invariant that
// repeat ForInstallation calls for the same installationID return the
// same InstallationClient instance, so the underlying http.Client,
// ghinstallation transport (and its installation-token cache), and any
// shared per-installation state survive across webhook deliveries.
// Different installationIDs must still receive distinct clients.
func TestForInstallation_CachesByInstallationID(t *testing.T) {
	c := &Client{
		appID:         12345,
		logger:        slog.New(slog.NewTextHandler(prDiscardWriter{}, &slog.HandlerOptions{Level: slog.LevelError})),
		installations: make(map[int64]*InstallationClient),
		privateKey:    testRSAKeyPEM(t),
	}
	c.storeAppSlug("schemabot") // bypass the slug-fetch retry path

	a1, err := c.ForInstallation(100)
	require.NoError(t, err)
	a2, err := c.ForInstallation(100)
	require.NoError(t, err)
	b, err := c.ForInstallation(200)
	require.NoError(t, err)

	assert.Same(t, a1, a2, "same installationID must return the cached InstallationClient")
	assert.NotSame(t, a1, b, "distinct installationIDs must return distinct InstallationClients")
}

// TestForInstallation_RefreshesSlugOnCachedClient covers the slug recovery
// path: an InstallationClient constructed before slug recovery (with an
// empty appSlug) must observe the recovered slug on the next
// ForInstallation call, so it does not stay stranded with an empty slug
// for the lifetime of the process.
func TestForInstallation_RefreshesSlugOnCachedClient(t *testing.T) {
	c := &Client{
		appID:         12345,
		logger:        slog.New(slog.NewTextHandler(prDiscardWriter{}, &slog.HandlerOptions{Level: slog.LevelError})),
		installations: make(map[int64]*InstallationClient),
		privateKey:    testRSAKeyPEM(t),
	}
	c.storeAppSlug("") // slug was unavailable at construction time
	// Bypass the slug-fetch retry by claiming we just tried.
	c.lastSlugAttempt = time.Now()

	ic1, err := c.ForInstallation(100)
	require.NoError(t, err)
	assert.Equal(t, "", ic1.loadAppSlug(), "client constructed before recovery must start with empty slug")

	// Simulate the slug becoming available later.
	c.storeAppSlug("schemabot")

	ic2, err := c.ForInstallation(100)
	require.NoError(t, err)
	assert.Same(t, ic1, ic2, "same InstallationClient should be returned (no rebuild)")
	assert.Equal(t, "schemabot", ic2.loadAppSlug(), "cached InstallationClient must adopt the recovered slug")
}

// TestForInstallation_AppSlugIsRaceFreeUnderConcurrency locks in the
// invariant that concurrent ForInstallation calls refreshing the cached
// InstallationClient's appSlug do not race with concurrent isOwnAppSlug
// reads on that same client. Without atomic.Pointer access, the race
// detector would flag the unsynchronised string mutation here.
func TestForInstallation_AppSlugIsRaceFreeUnderConcurrency(t *testing.T) {
	c := &Client{
		appID:         12345,
		logger:        slog.New(slog.NewTextHandler(prDiscardWriter{}, &slog.HandlerOptions{Level: slog.LevelError})),
		installations: make(map[int64]*InstallationClient),
		privateKey:    testRSAKeyPEM(t),
	}
	c.storeAppSlug("schemabot")

	// Prime the cache so subsequent ForInstallation calls hit the
	// refresh path that writes existing.appSlug.
	ic, err := c.ForInstallation(100)
	require.NoError(t, err)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Writers: drive ForInstallation, which refreshes existing.appSlug.
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					// Alternate slug values to maximise the chance of a
					// torn read if access were unsynchronised.
					c.storeAppSlug("schemabot")
					_, _ = c.ForInstallation(100)
					c.storeAppSlug("schemabot-alt")
					_, _ = c.ForInstallation(100)
				}
			}
		})
	}

	// Readers: pound isOwnAppSlug on the cached client.
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					ic.isOwnAppSlug("schemabot")
					ic.isOwnAppSlug("other")
				}
			}
		})
	}

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// newPRFakeGitHubServer returns an httptest server that responds to
// GET /repos/octo/repo/pulls/42 with a canonical PR payload, and the
// counter of how many times the endpoint has been hit.
func newPRFakeGitHubServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/octo/repo/pulls/42", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"head": map[string]string{"ref": "feature", "sha": "abc123"},
			"base": map[string]string{"ref": "main", "sha": "def456"},
			"user": map[string]string{"login": "octocat"},
		})
	})
	return httptest.NewServer(mux), &calls
}

// newPRTestInstallationClient constructs an InstallationClient pointed at
// the given httptest server. No cache is attached at the client level —
// the request-scoped cache (if any) lives on ctx.
func newPRTestInstallationClient(t *testing.T, server *httptest.Server) *InstallationClient {
	t.Helper()
	ghc := gh.NewClient(nil)
	ghc.BaseURL, _ = url.Parse(server.URL + "/")
	return &InstallationClient{
		client: ghc,
		logger: slog.New(slog.NewTextHandler(prDiscardWriter{}, &slog.HandlerOptions{Level: slog.LevelError})),
	}
}

// prDiscardWriter swallows slog output so test logs stay quiet.
type prDiscardWriter struct{}

func (prDiscardWriter) Write(p []byte) (int, error) { return len(p), nil }

// testRSAKeyPEM generates a fresh 2048-bit RSA private key and returns it
// PEM-encoded. ghinstallation.New requires a parseable RSA private key
// even though no JWT is actually exercised here.
func testRSAKeyPEM(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
}
