package github

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/metrics"
)

// etagPRServer is a fake GitHub pull request endpoint that versions the pull
// request by head SHA and honors If-None-Match the way GitHub does: a request
// carrying the current ETag gets 304 with no body. While unavailable is set it
// answers every request 503, as GitHub does during an outage.
type etagPRServer struct {
	mu          sync.Mutex
	headSHA     string
	sendETag    bool
	unavailable bool
	ifNoneMatch []string
	notModified int
	fullReads   int
	refused     int
}

func (s *etagPRServer) etag() string {
	return `W/"` + s.headSHA + `"`
}

func (s *etagPRServer) setHead(sha string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.headSHA = sha
}

func (s *etagPRServer) setSendETag(send bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendETag = send
}

func (s *etagPRServer) setUnavailable(unavailable bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unavailable = unavailable
}

func (s *etagPRServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sent := r.Header.Get("If-None-Match")
	s.ifNoneMatch = append(s.ifNoneMatch, sent)
	if s.unavailable {
		s.refused++
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
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
		"state":  "closed",
		"merged": true,
		"head":   map[string]string{"ref": "feature", "sha": s.headSHA},
		"base": map[string]any{
			"ref": "main", "sha": "base111",
			"repo": map[string]string{"default_branch": "main"},
		},
		"user":  map[string]string{"login": "octocat"},
		"stack": map[string]any{"base": map[string]string{"ref": "release"}},
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
		DefaultBranch: "main", User: "octocat", State: "closed", Merged: true,
		NativeStack: true, StackBaseRef: "release",
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

// Every PullRequestInfo field must survive a 304. The fake server populates
// every field the pull request response carries, so a field added to
// PullRequestInfo without being read from the response, and stored with the
// ETag, fails here instead of reading as its zero value on a 304.
func TestFetchPullRequest_NotModifiedReturnsEveryField(t *testing.T) {
	server := &etagPRServer{headSHA: "head111", sendETag: true}
	ic := newETagPRTestClient(t, server)

	full, err := ic.FetchPullRequestNoCache(t.Context(), "octo/repo", 42)
	require.NoError(t, err)
	fromETag, err := ic.FetchPullRequestNoCache(t.Context(), "octo/repo", 42)
	require.NoError(t, err)
	require.Equal(t, 1, server.notModified, "the second read must be answered 304")
	assert.Equal(t, full, fromETag)

	v := reflect.ValueOf(*fromETag)
	for i := range v.NumField() {
		assert.False(t, v.Field(i).IsZero(),
			"PullRequestInfo.%s is empty after a 304: read it from the pull request response and populate it in the fake server", v.Type().Field(i).Name)
	}
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

// A full response without an ETag is returned as read and is not stored. The
// pair stored earlier stays in place, so later reads keep sending its ETag.
// GitHub answers each of them in full, and answers 304 again only once the
// pull request matches that ETag, when the stored info is once more correct.
func TestFetchPullRequest_ResponseWithoutETagKeepsTheEarlierPair(t *testing.T) {
	server := &etagPRServer{headSHA: "head111", sendETag: true}
	ic := newETagPRTestClient(t, server)

	_, err := ic.FetchPullRequestNoCache(t.Context(), "octo/repo", 42)
	require.NoError(t, err)

	server.setHead("head222")
	server.setSendETag(false)
	for range 2 {
		info, err := ic.FetchPullRequestNoCache(t.Context(), "octo/repo", 42)
		require.NoError(t, err)
		assert.Equal(t, "head222", info.HeadSHA, "the response without an ETag is the one returned")
	}
	assert.Equal(t, []string{"", `W/"head111"`, `W/"head111"`}, server.ifNoneMatch)
	assert.Equal(t, 3, server.fullReads)
	assert.Equal(t, 0, server.notModified)

	server.setHead("head111")
	server.setSendETag(true)
	info, err := ic.FetchPullRequestNoCache(t.Context(), "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, "head111", info.HeadSHA)
	assert.Equal(t, 1, server.notModified)
}

// When GitHub is unavailable, a read fails even though an earlier read stored
// the pull request. The stored copy is returned only after GitHub confirms it
// with a 304, so a safety re-check never acts on a head it could not verify.
// Once GitHub answers again, the stored copy is confirmed and used as before.
func TestFetchPullRequest_UnavailableGitHubIsAnErrorDespiteAStoredCopy(t *testing.T) {
	setGitHubUnavailableReadRetryDelay(t, time.Millisecond)
	server := &etagPRServer{headSHA: "head111", sendETag: true}
	ic := newETagPRTestClient(t, server)

	_, err := ic.FetchPullRequestNoCache(t.Context(), "octo/repo", 42)
	require.NoError(t, err)

	server.setUnavailable(true)
	info, err := ic.FetchPullRequestNoCache(t.Context(), "octo/repo", 42)
	require.ErrorIs(t, err, ErrGitHubUnavailable)
	assert.Nil(t, info, "an unavailable GitHub must not be answered from the stored copy")
	assert.Equal(t, githubUnavailableReadRetryMaxAttempts, server.refused, "every attempt asked GitHub")

	server.setUnavailable(false)
	info, err = ic.FetchPullRequestNoCache(t.Context(), "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, "head111", info.HeadSHA)
	assert.Equal(t, 1, server.notModified)
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
