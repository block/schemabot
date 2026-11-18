package github

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// blobName is the git object name of a blob holding content.
func blobName(content string) string {
	sum := sha1.Sum(fmt.Appendf(nil, "blob %d\x00%s", len(content), content))
	return hex.EncodeToString(sum[:])
}

// countingFetch returns content for every read and counts the reads.
func countingFetch(content string, reads *atomic.Int64) func(context.Context) (string, error) {
	return func(context.Context) (string, error) {
		reads.Add(1)
		return content, nil
	}
}

func TestBlobCache_ReadsEachBlobOnce(t *testing.T) {
	cache := newBlobCache(maxCachedBlobBytes)
	content := "CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`));"
	var reads atomic.Int64

	for range 3 {
		got, err := cache.read(t.Context(), "octo/repo", blobName(content), countingFetch(content, &reads))
		require.NoError(t, err)
		assert.Equal(t, content, got)
	}
	assert.Equal(t, int64(1), reads.Load(), "a held blob is not read again")

	_, err := cache.read(t.Context(), "octo/other", blobName(content), countingFetch(content, &reads))
	require.NoError(t, err)
	assert.Equal(t, int64(2), reads.Load(), "a blob is held per repository")
}

func TestBlobCache_ConcurrentReadsShareOneFetch(t *testing.T) {
	cache := newBlobCache(maxCachedBlobBytes)
	content := "CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`));"
	sha := blobName(content)
	var reads atomic.Int64
	fetching := make(chan struct{})
	release := make(chan struct{})
	fetch := func(context.Context) (string, error) {
		if reads.Add(1) == 1 {
			close(fetching)
		}
		<-release
		return content, nil
	}

	const readers = 20
	var started, wg sync.WaitGroup
	started.Add(readers)
	results := make([]string, readers)
	for i := range readers {
		wg.Go(func() {
			started.Done()
			got, err := cache.read(t.Context(), "octo/repo", sha, fetch)
			assert.NoError(t, err)
			results[i] = got
		})
	}
	started.Wait()
	<-fetching
	close(release)
	wg.Wait()

	assert.Equal(t, int64(1), reads.Load(), "concurrent reads of one blob make one request")
	for _, got := range results {
		assert.Equal(t, content, got)
	}
}

func TestBlobCache_FailedReadIsNotHeld(t *testing.T) {
	cache := newBlobCache(maxCachedBlobBytes)
	content := "CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`));"
	sha := blobName(content)
	unavailable := errors.New("github unavailable")

	_, err := cache.read(t.Context(), "octo/repo", sha, func(context.Context) (string, error) { return "", unavailable })
	require.ErrorIs(t, err, unavailable)

	var reads atomic.Int64
	got, err := cache.read(t.Context(), "octo/repo", sha, countingFetch(content, &reads))
	require.NoError(t, err)
	assert.Equal(t, content, got)
	assert.Equal(t, int64(1), reads.Load(), "the read after a failure goes to GitHub")
}

// Content that does not hash to the blob's name is an error, never returned
// and never held, so a later read of the blob goes back to GitHub.
func TestBlobCache_ContentNotMatchingItsNameIsAnError(t *testing.T) {
	cache := newBlobCache(maxCachedBlobBytes)
	content := "CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`));"
	sha := blobName(content)
	var reads atomic.Int64

	got, err := cache.read(t.Context(), "octo/repo", sha, countingFetch("CREATE TABLE `other` (`id` bigint);", &reads))
	require.Error(t, err)
	assert.Empty(t, got)
	assert.Contains(t, err.Error(), sha)

	got, err = cache.read(t.Context(), "octo/repo", sha, countingFetch(content, &reads))
	require.NoError(t, err)
	assert.Equal(t, content, got)
	assert.Equal(t, int64(2), reads.Load(), "the mismatched content was not held")

	var uncached *blobCache
	_, err = uncached.read(t.Context(), "octo/repo", sha, countingFetch("tampered", &reads))
	require.Error(t, err, "content is verified even without a cache")
}

func TestBlobCache_VerifiesSHA256Names(t *testing.T) {
	content := "CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`));"
	sum := sha256.Sum256(fmt.Appendf(nil, "blob %d\x00%s", len(content), content))
	sha := hex.EncodeToString(sum[:])

	var reads atomic.Int64
	got, err := newBlobCache(maxCachedBlobBytes).read(t.Context(), "octo/repo", sha, countingFetch(content, &reads))
	require.NoError(t, err)
	assert.Equal(t, content, got)

	_, err = newBlobCache(maxCachedBlobBytes).read(t.Context(), "octo/repo", sha, countingFetch(content+"\n", &reads))
	require.Error(t, err)
}

// A blob named by something other than a full object name cannot be verified,
// so it is read every time and never held.
func TestBlobCache_PartialNameIsNotHeld(t *testing.T) {
	cache := newBlobCache(maxCachedBlobBytes)
	var reads atomic.Int64
	for range 2 {
		got, err := cache.read(t.Context(), "octo/repo", "abc123", countingFetch("content", &reads))
		require.NoError(t, err)
		assert.Equal(t, "content", got)
	}
	assert.Equal(t, int64(2), reads.Load())
	assert.Zero(t, cache.bytes)
}

// The held content stays within the byte bound by dropping the least recently
// read blob, and a blob larger than the bound is returned but not held.
func TestBlobCache_EvictsLeastRecentlyReadByBytes(t *testing.T) {
	a, b, c := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)
	cache := newBlobCache(100)
	var reads atomic.Int64
	read := func(content string) {
		t.Helper()
		got, err := cache.read(t.Context(), "octo/repo", blobName(content), countingFetch(content, &reads))
		require.NoError(t, err)
		require.Equal(t, content, got)
	}

	read(a)
	read(b)
	read(a) // a is now the most recently read
	require.Equal(t, int64(2), reads.Load())

	read(c) // 120 bytes would exceed the bound: b, the least recently read, goes
	assert.Equal(t, 80, cache.bytes)
	assert.Len(t, cache.entries, 2)

	read(a)
	read(c)
	assert.Equal(t, int64(3), reads.Load(), "a and c are still held")
	read(b)
	assert.Equal(t, int64(4), reads.Load(), "b was dropped and is read again")

	large := strings.Repeat("x", 101)
	read(large)
	read(large)
	assert.Equal(t, int64(6), reads.Load(), "a blob larger than the bound is never held")
	assert.LessOrEqual(t, cache.bytes, 100)
}

// A reader that gives up stops waiting without failing the other readers of
// the same blob, and the blob is still held for later reads.
func TestBlobCache_CancelledReaderDoesNotFailOthers(t *testing.T) {
	cache := newBlobCache(maxCachedBlobBytes)
	content := "CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`));"
	sha := blobName(content)
	var reads atomic.Int64
	fetching := make(chan struct{})
	release := make(chan struct{})
	fetch := func(ctx context.Context) (string, error) {
		reads.Add(1)
		close(fetching)
		<-release
		if err := ctx.Err(); err != nil {
			return "", err
		}
		return content, nil
	}

	firstCtx, cancelFirst := context.WithCancel(t.Context())
	firstErr := make(chan error, 1)
	go func() {
		_, err := cache.read(firstCtx, "octo/repo", sha, fetch)
		firstErr <- err
	}()
	<-fetching

	second := make(chan string, 1)
	go func() {
		got, err := cache.read(t.Context(), "octo/repo", sha, fetch)
		assert.NoError(t, err)
		second <- got
	}()

	cancelFirst()
	require.ErrorIs(t, <-firstErr, context.Canceled)
	close(release)
	assert.Equal(t, content, <-second)

	got, err := cache.read(t.Context(), "octo/repo", sha, fetch)
	require.NoError(t, err)
	assert.Equal(t, content, got)
	assert.Equal(t, int64(1), reads.Load())
}

// blobServer serves git blobs and counts the reads of each.
type blobServer struct {
	mu      sync.Mutex
	content map[string]string
	reads   map[string]int
}

func (s *blobServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sha := r.PathValue("sha")
	s.mu.Lock()
	content, ok := s.content[sha]
	s.reads[sha]++
	s.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"sha":%q,"encoding":"base64","content":%q}`, sha, base64.StdEncoding.EncodeToString([]byte(content)))
}

// Installation clients made by one Client share its blobs, so a blob one
// installation read is not read again by another, and a client built on its
// own still reads each blob once.
func TestFetchBlobContent_ReadsEachBlobOnceAcrossClients(t *testing.T) {
	content := "CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`));"
	sha := blobName(content)
	server := &blobServer{content: map[string]string{sha: content}, reads: map[string]int{}}
	mux := http.NewServeMux()
	mux.Handle("GET /repos/octo/repo/git/blobs/{sha}", server)
	httpServer := httptest.NewServer(mux)
	t.Cleanup(httpServer.Close)
	newClient := func(blobs *blobCache) *InstallationClient {
		ghc := gh.NewClient(nil)
		baseURL, err := url.Parse(httpServer.URL + "/")
		require.NoError(t, err)
		ghc.BaseURL = baseURL
		ic := NewInstallationClient(ghc, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if blobs != nil {
			ic.blobs = blobs
		}
		return ic
	}

	shared := newBlobCache(maxCachedBlobBytes)
	for _, ic := range []*InstallationClient{newClient(shared), newClient(shared)} {
		got, err := ic.FetchBlobContent(t.Context(), "octo/repo", sha)
		require.NoError(t, err)
		assert.Equal(t, content, got)
	}
	assert.Equal(t, 1, server.reads[sha], "clients sharing a cache read the blob once")

	own := newClient(nil)
	for range 2 {
		_, err := own.FetchBlobContent(t.Context(), "octo/repo", sha)
		require.NoError(t, err)
	}
	assert.Equal(t, 2, server.reads[sha], "a client with its own cache reads the blob once")
}

// Content GitHub serves for a blob that does not hash to the blob's name fails
// the read rather than reaching a plan.
func TestFetchBlobContent_ContentNotMatchingItsNameFails(t *testing.T) {
	content := "CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`));"
	sha := blobName(content)
	server := &blobServer{content: map[string]string{sha: "CREATE TABLE `t` (`id` int);"}, reads: map[string]int{}}
	mux := http.NewServeMux()
	mux.Handle("GET /repos/octo/repo/git/blobs/{sha}", server)
	httpServer := httptest.NewServer(mux)
	t.Cleanup(httpServer.Close)
	ghc := gh.NewClient(nil)
	baseURL, err := url.Parse(httpServer.URL + "/")
	require.NoError(t, err)
	ghc.BaseURL = baseURL
	ic := NewInstallationClient(ghc, slog.New(slog.NewTextHandler(io.Discard, nil)))

	got, err := ic.FetchBlobContent(t.Context(), "octo/repo", sha)
	require.Error(t, err)
	assert.Empty(t, got)
	assert.Contains(t, err.Error(), "not to the blob's name "+sha)
}
