package github

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/metrics"
)

// etagPRServer is a fake GitHub pull request endpoint that versions the pull
// request by head SHA and honors If-None-Match the way GitHub does: a request
// carrying the current ETag gets 304 with no body.
type etagPRServer struct {
	mu          sync.Mutex
	headSHA     string
	sendETag    bool
	ifNoneMatch []string
	notModified int
	fullReads   int
}

func (s *etagPRServer) etag() string {
	return `W/"` + s.headSHA + `"`
}

func (s *etagPRServer) setHead(sha string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.headSHA = sha
}

func (s *etagPRServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sent := r.Header.Get("If-None-Match")
	s.ifNoneMatch = append(s.ifNoneMatch, sent)
	if s.sendETag && sent != "" && sent == s.etag() {
		s.notModified++
		w.Header().Set("ETag", s.etag())
		w.WriteHeader(http.StatusNotModified)
		return
	}
	s.fullReads++
	if s.sendETag {
		w.Header().Set("ETag", s.etag())
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"state": "open",
		"head":  map[string]string{"ref": "feature", "sha": s.headSHA},
		"base": map[string]any{
			"ref": "main", "sha": "base111",
			"repo": map[string]string{"default_branch": "main"},
		},
		"user": map[string]string{"login": "octocat"},
	})
}

func newETagPRTestClient(t *testing.T, server *etagPRServer) *InstallationClient {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("GET /repos/octo/repo/pulls/42", server)
	httpServer := httptest.NewServer(mux)
	t.Cleanup(httpServer.Close)

	ghc := gh.NewClient(nil)
	baseURL, err := url.Parse(httpServer.URL + "/")
	require.NoError(t, err)
	ghc.BaseURL = baseURL
	return NewInstallationClient(ghc, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// A pull request read twice without changing is read in full once; the second
// read is a conditional request that GitHub answers with 304, and the caller
// still gets the complete pull request info.
func TestFetchPullRequest_UnchangedPRIsAnsweredNotModified(t *testing.T) {
	server := &etagPRServer{headSHA: "head111", sendETag: true}
	ic := newETagPRTestClient(t, server)

	want := &PullRequestInfo{
		HeadRef: "feature", HeadSHA: "head111", BaseRef: "main", BaseSHA: "base111",
		DefaultBranch: "main", User: "octocat", State: "open",
	}

	first, err := ic.FetchPullRequestNoCache(t.Context(), "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, want, first)

	second, err := ic.FetchPullRequestNoCache(t.Context(), "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, want, second)

	assert.Equal(t, []string{"", `W/"head111"`}, server.ifNoneMatch,
		"the second read must send the ETag from the first")
	assert.Equal(t, 1, server.fullReads)
	assert.Equal(t, 1, server.notModified)

	second.HeadSHA = "mutated"
	third, err := ic.FetchPullRequestNoCache(t.Context(), "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, "head111", third.HeadSHA, "a caller mutating its result must not change the stored copy")
}

// A safety re-check after a push must see the new head. The stored ETag no
// longer matches, so GitHub answers in full and the new head replaces the
// stored entry; the next read is conditional on the new ETag.
func TestFetchPullRequest_ChangedPRIsReadInFull(t *testing.T) {
	server := &etagPRServer{headSHA: "head111", sendETag: true}
	ic := newETagPRTestClient(t, server)

	before, err := ic.FetchPullRequestNoCache(t.Context(), "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, "head111", before.HeadSHA)

	server.setHead("head222")

	after, err := ic.FetchPullRequestNoCache(t.Context(), "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, "head222", after.HeadSHA, "a pushed commit must be visible on the next read")

	again, err := ic.FetchPullRequestNoCache(t.Context(), "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, "head222", again.HeadSHA)

	assert.Equal(t, []string{"", `W/"head111"`, `W/"head222"`}, server.ifNoneMatch)
	assert.Equal(t, 2, server.fullReads)
	assert.Equal(t, 1, server.notModified)
}

// A response without an ETag is not stored, so every later read is a full,
// unconditional read.
func TestFetchPullRequest_NoETagMeansUnconditionalReads(t *testing.T) {
	server := &etagPRServer{headSHA: "head111"}
	ic := newETagPRTestClient(t, server)

	for range 3 {
		info, err := ic.FetchPullRequestNoCache(t.Context(), "octo/repo", 42)
		require.NoError(t, err)
		assert.Equal(t, "head111", info.HeadSHA)
	}
	assert.Equal(t, []string{"", "", ""}, server.ifNoneMatch)
	assert.Equal(t, 3, server.fullReads)
}

// The request-scoped cache still collapses repeat reads within a delivery,
// and a new delivery's first read is a conditional request rather than a
// full one.
func TestFetchPullRequest_NewDeliveryReadIsConditional(t *testing.T) {
	server := &etagPRServer{headSHA: "head111", sendETag: true}
	ic := newETagPRTestClient(t, server)

	delivery1 := WithPRInfoCache(t.Context())
	for range 2 {
		_, err := ic.FetchPullRequest(delivery1, "octo/repo", 42)
		require.NoError(t, err)
	}
	delivery2 := WithPRInfoCache(t.Context())
	info, err := ic.FetchPullRequest(delivery2, "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, "head111", info.HeadSHA)

	assert.Equal(t, []string{"", `W/"head111"`}, server.ifNoneMatch)
	assert.Equal(t, 1, server.fullReads)
	assert.Equal(t, 1, server.notModified)
}

func TestPRETagCache_StartsOverWhenFull(t *testing.T) {
	cache := newPRETagCache()
	for pr := range maxCachedPRETags {
		cache.put("octo/repo", pr, strconv.Itoa(pr), PullRequestInfo{HeadSHA: strconv.Itoa(pr)})
	}
	entry, ok := cache.get("octo/repo", 0)
	require.True(t, ok)
	assert.Equal(t, "0", entry.etag)

	cache.put("octo/repo", 0, "updated", PullRequestInfo{HeadSHA: "updated"})
	assert.Len(t, cache.entries, maxCachedPRETags, "replacing a stored pull request must not start the cache over")

	cache.put("octo/repo", maxCachedPRETags, "new", PullRequestInfo{HeadSHA: "new"})
	assert.Len(t, cache.entries, 1)
	_, ok = cache.get("octo/repo", 0)
	assert.False(t, ok)
	entry, ok = cache.get("octo/repo", maxCachedPRETags)
	require.True(t, ok)
	assert.Equal(t, "new", entry.etag)
}

func TestPRETagCache_NilAndEmptyETag(t *testing.T) {
	var nilCache *prETagCache
	nilCache.put("octo/repo", 1, "etag", PullRequestInfo{})
	_, ok := nilCache.get("octo/repo", 1)
	assert.False(t, ok)

	cache := newPRETagCache()
	cache.put("octo/repo", 1, "", PullRequestInfo{HeadSHA: "abc"})
	_, ok = cache.get("octo/repo", 1)
	assert.False(t, ok, "a response without an ETag must not be stored")
}

func TestGitHubResponseStatus_NotModified(t *testing.T) {
	resp := &http.Response{StatusCode: http.StatusNotModified}
	assert.Equal(t, metrics.GitHubRequestStatusNotModified,
		gitHubResponseStatus(resp, metrics.GitHubOperationFetchPullRequest))
}
