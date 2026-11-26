package github

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gitBlobSHA returns the name git gives a blob holding content.
func gitBlobSHA(content string) string {
	sum := sha1.Sum([]byte("blob " + strconv.Itoa(len(content)) + "\x00" + content))
	return hex.EncodeToString(sum[:])
}

func serveContentsFile(t *testing.T, mux *http.ServeMux, filePath string, reads *atomic.Int64, content func(ref string) string) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/"+filePath, func(w http.ResponseWriter, r *http.Request) {
		reads.Add(1)
		require.NoError(t, json.NewEncoder(w).Encode(gh.RepositoryContent{
			Type: new("file"), Name: new(filePath[strings.LastIndex(filePath, "/")+1:]), Path: new(filePath),
			Encoding: new("base64"),
			Content:  new(base64.StdEncoding.EncodeToString([]byte(content(r.URL.Query().Get("ref"))))),
		}))
	})
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

const usersTable = "CREATE TABLE users (id INT);"

func ordersTableAt(ref string) string {
	return "CREATE TABLE orders (id INT); -- " + ref
}

// schemaDirServer serves a two-file schema directory whose listing reports
// each file's blob SHA per ref, and counts every content read by path.
// users.sql has the same blob at every ref; orders.sql changes per ref.
func schemaDirServer(t *testing.T) (*InstallationClient, map[string]*atomic.Int64) {
	t.Helper()
	client, mux := setupRateLimitedTestGitHubServer(t)
	reads := map[string]*atomic.Int64{"schema/users.sql": {}, "schema/orders.sql": {}}
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema", func(w http.ResponseWriter, r *http.Request) {
		ref := r.URL.Query().Get("ref")
		require.NoError(t, json.NewEncoder(w).Encode([]gh.RepositoryContent{
			{Type: new("file"), Name: new("users.sql"), Path: new("schema/users.sql"), SHA: new(gitBlobSHA(usersTable))},
			{Type: new("file"), Name: new("orders.sql"), Path: new("schema/orders.sql"), SHA: new(gitBlobSHA(ordersTableAt(ref)))},
		}))
	})
	serveContentsFile(t, mux, "schema/users.sql", reads["schema/users.sql"], func(string) string { return usersTable })
	serveContentsFile(t, mux, "schema/orders.sql", reads["schema/orders.sql"], ordersTableAt)
	return NewInstallationClient(client, discardLogger()), reads
}

func filesByPath(files []GitHubFile) map[string]string {
	out := make(map[string]string, len(files))
	for _, f := range files {
		out[f.Path] = f.Content
	}
	return out
}

// Plans of two pushes to a PR, and of each environment of one push, list the
// schema directory at a commit SHA. A file whose blob did not change between
// them is read from GitHub once; a file whose blob changed is read at each
// commit, and each plan sees its own commit's content.
func TestFetchSchemaFilesOptimizedReadsUnchangedBlobsOnce(t *testing.T) {
	firstPush, secondPush := strings.Repeat("a", 40), strings.Repeat("b", 40)
	ic, reads := schemaDirServer(t)

	for range 2 { // two environments planned from the first push
		files, err := ic.FetchSchemaFilesOptimized(t.Context(), "octocat/hello-world", firstPush, "schema", "mysql")
		require.NoError(t, err)
		assert.Equal(t, map[string]string{
			"schema/users.sql":  usersTable,
			"schema/orders.sql": ordersTableAt(firstPush),
		}, filesByPath(files))
	}
	files, err := ic.FetchSchemaFilesOptimized(t.Context(), "octocat/hello-world", secondPush, "schema", "mysql")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"schema/users.sql":  usersTable,
		"schema/orders.sql": ordersTableAt(secondPush),
	}, filesByPath(files))

	assert.Equal(t, int64(1), reads["schema/users.sql"].Load(), "the unchanged blob is read once across environments and pushes")
	assert.Equal(t, int64(2), reads["schema/orders.sql"].Load(), "the changed blob is read once per distinct blob")
}

// A listing taken at a branch name can name other content by the time each
// file is read, so files listed at a branch are read every time, never served
// from or added to the cache.
func TestFetchSchemaFilesOptimizedDoesNotCacheBranchReads(t *testing.T) {
	ic, reads := schemaDirServer(t)

	for range 2 {
		_, err := ic.FetchSchemaFilesOptimized(t.Context(), "octocat/hello-world", "main", "schema", "mysql")
		require.NoError(t, err)
	}

	assert.Equal(t, int64(2), reads["schema/users.sql"].Load())
	assert.Equal(t, int64(2), reads["schema/orders.sql"].Load())
}

// A schema file can be a symbolic link to a file elsewhere in the repository.
// GitHub lists it as a file under the link's own blob SHA, which stays the
// same when the file it points at changes, while a read of its path returns
// the target's content. Each push is planned from the content at its own
// commit, so such a file is read every time.
func TestFetchSchemaFilesOptimizedReadsASymlinkedFileAtEachCommit(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	firstPush, secondPush := strings.Repeat("a", 40), strings.Repeat("b", 40)
	linkBlob := gitBlobSHA("../shared/users.sql")
	targetAt := map[string]string{
		firstPush:  usersTable,
		secondPush: "CREATE TABLE users (id INT, email VARCHAR(255));",
	}
	var reads atomic.Int64
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]gh.RepositoryContent{
			{Type: new("file"), Name: new("users.sql"), Path: new("schema/users.sql"), SHA: new(linkBlob)},
		}))
	})
	serveContentsFile(t, mux, "schema/users.sql", &reads, func(ref string) string { return targetAt[ref] })
	ic := NewInstallationClient(client, discardLogger())

	for _, push := range []string{firstPush, secondPush, secondPush} {
		files, err := ic.FetchSchemaFilesOptimized(t.Context(), "octocat/hello-world", push, "schema", "mysql")
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"schema/users.sql": targetAt[push]}, filesByPath(files), "push %s", push)
	}

	assert.Equal(t, int64(3), reads.Load(), "a symlinked file is read at every plan")
}

// Files in a namespace directory and files reached through a symlinked
// namespace are read once per blob, the same as files at the schema root.
func TestFetchSchemaFilesOptimizedReadsNamespaceBlobsOnce(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	const itemsTable, ordersTable = "CREATE TABLE items (id INT);", "CREATE TABLE orders (id INT);"
	reads := map[string]*atomic.Int64{"schema/app/items.sql": {}, "canonical/shard/orders.sql": {}}
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]gh.RepositoryContent{
			{Type: new("dir"), Name: new("app"), Path: new("schema/app")},
			{Type: new("symlink"), Name: new("shard_001"), Path: new("schema/shard_001"), Target: new("../canonical/shard")},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema/app", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]gh.RepositoryContent{
			{Type: new("file"), Name: new("items.sql"), Path: new("schema/app/items.sql"), SHA: new(gitBlobSHA(itemsTable))},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/canonical/shard", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]gh.RepositoryContent{
			{Type: new("file"), Name: new("orders.sql"), Path: new("canonical/shard/orders.sql"), SHA: new(gitBlobSHA(ordersTable))},
		}))
	})
	serveContentsFile(t, mux, "schema/app/items.sql", reads["schema/app/items.sql"], func(string) string { return itemsTable })
	serveContentsFile(t, mux, "canonical/shard/orders.sql", reads["canonical/shard/orders.sql"], func(string) string { return ordersTable })
	ic := NewInstallationClient(client, discardLogger())

	for _, push := range []string{strings.Repeat("a", 40), strings.Repeat("b", 40)} {
		files, err := ic.FetchSchemaFilesOptimized(t.Context(), "octocat/hello-world", push, "schema", "vitess")
		require.NoError(t, err)
		assert.Equal(t, map[string]string{
			"schema/app/items.sql":        itemsTable,
			"schema/shard_001/orders.sql": ordersTable,
		}, filesByPath(files))
	}

	assert.Equal(t, int64(1), reads["schema/app/items.sql"].Load(), "a file in a namespace directory is read once")
	assert.Equal(t, int64(1), reads["canonical/shard/orders.sql"].Load(), "a file reached through a symlinked namespace is read once")
}

// When the schema directory cannot be listed, its files are read as blobs from
// the git tree. A blob that two pushes share is read once.
func TestFetchSchemaFilesFromTreeReadsUnchangedBlobsOnce(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	firstPush, secondPush := strings.Repeat("a", 40), strings.Repeat("b", 40)
	var reads atomic.Int64
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"message":"Not Found"}`, http.StatusNotFound)
	})
	for _, push := range []string{firstPush, secondPush} {
		mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/"+push, func(w http.ResponseWriter, _ *http.Request) {
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": []any{
				map[string]any{"path": "schema/users.sql", "type": "blob", "mode": "100644", "sha": gitBlobSHA(usersTable)},
			}}))
		})
	}
	mux.HandleFunc("GET /repos/octocat/hello-world/git/blobs/"+gitBlobSHA(usersTable), func(w http.ResponseWriter, _ *http.Request) {
		reads.Add(1)
		require.NoError(t, json.NewEncoder(w).Encode(gh.Blob{
			Content: new(base64.StdEncoding.EncodeToString([]byte(usersTable))), Encoding: new("base64"),
		}))
	})
	ic := NewInstallationClient(client, discardLogger())

	for _, push := range []string{firstPush, secondPush} {
		files, err := ic.FetchSchemaFilesOptimized(t.Context(), "octocat/hello-world", push, "schema", "mysql")
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"schema/users.sql": usersTable}, filesByPath(files))
	}

	assert.Equal(t, int64(1), reads.Load(), "the blob both pushes share is read once")
}

// A read that fails is not remembered, so the next read asks GitHub again. A
// blob is held per repository, so the same blob in another repository is read
// from that repository.
func TestCachedBlobContentKeepsOnlySuccessfulReadsPerRepository(t *testing.T) {
	ic := NewInstallationClient(nil, discardLogger())
	sha := gitBlobSHA(usersTable)
	var fetches int
	fetch := func(context.Context) (string, error) {
		fetches++
		return usersTable, nil
	}

	_, err := ic.cachedBlobContent(t.Context(), "octocat/hello-world", sha, func(context.Context) (string, error) {
		return "", errors.New("GitHub is unavailable")
	})
	require.Error(t, err)

	for range 2 {
		content, err := ic.cachedBlobContent(t.Context(), "octocat/hello-world", sha, fetch)
		require.NoError(t, err)
		assert.Equal(t, usersTable, content)
	}
	assert.Equal(t, 1, fetches, "the read after a failed one reaches GitHub, and the one after that is served from the cache")

	content, err := ic.cachedBlobContent(t.Context(), "octocat/other", sha, fetch)
	require.NoError(t, err)
	assert.Equal(t, usersTable, content)
	assert.Equal(t, 2, fetches, "the same blob in another repository is read from that repository")
}

// Content is held only under the name git gives it, for both of git's object
// name lengths. The expected names are git's own, from `git hash-object`.
func TestBlobContentCacheHoldsContentOnlyUnderItsOwnName(t *testing.T) {
	const (
		repo         = "octocat/hello-world"
		usersSHA1    = "cdb3ca0c65678979763bacb9ef6c8ab69c05162c"
		usersSHA256  = "e8fa91f60a799478cfe9ea36dbf31d60dadbea6ec3eb049d7149978f45005de1"
		emptySHA1    = "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
		anotherBlob  = "1111111111111111111111111111111111111111"
		abbreviation = "cdb3ca0"
	)
	tests := []struct {
		name, sha, content string
		held               bool
	}{
		{name: "a SHA-1 name of the content", sha: usersSHA1, content: usersTable, held: true},
		{name: "a SHA-256 name of the content", sha: usersSHA256, content: usersTable, held: true},
		{name: "the empty blob", sha: emptySHA1, content: "", held: true},
		{name: "the name of another blob", sha: anotherBlob, content: usersTable, held: false},
		{name: "the name of the empty blob with content", sha: emptySHA1, content: usersTable, held: false},
		{name: "an abbreviated name", sha: abbreviation, content: usersTable, held: false},
		{name: "a branch name", sha: "main", content: usersTable, held: false},
		{name: "no name", sha: "", content: usersTable, held: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cache := newBlobContentCache()
			cache.put(repo, tc.sha, tc.content)
			content, ok := cache.get(repo, tc.sha)
			assert.Equal(t, tc.held, ok)
			if tc.held {
				assert.Equal(t, tc.content, content)
			}
		})
	}
}

// The cache is bounded by the memory its entries take, their keys and
// bookkeeping included. An entry that would push it past its limit starts it
// over empty rather than growing without bound, an entry that alone exceeds
// the limit is not held, and holding a blob again counts it once.
func TestBlobContentCacheStartsOverAtItsLimit(t *testing.T) {
	const repo = "octocat/hello-world"
	held := func(cache *blobContentCache, content string) bool {
		_, ok := cache.get(repo, gitBlobSHA(content))
		return ok
	}
	// Two of these fit together with room to spare; three do not.
	third := strings.Repeat("x", maxCachedBlobBytes/3)
	first, second := third+"1", third+"2"

	cache := newBlobContentCache()
	cache.put(repo, gitBlobSHA(first), first)
	cache.put(repo, gitBlobSHA(second), second)
	for range 3 {
		cache.put(repo, gitBlobSHA(first), first)
	}
	require.True(t, held(cache, first))
	require.True(t, held(cache, second), "holding a blob again does not count it twice")

	last := third + "3"
	cache.put(repo, gitBlobSHA(last), last)
	assert.False(t, held(cache, first), "the cache starts over instead of exceeding its limit")
	assert.False(t, held(cache, second))
	assert.True(t, held(cache, last))

	// Content that fits the limit alone but not with its key and bookkeeping.
	tooLarge := strings.Repeat("y", maxCachedBlobBytes-blobEntryOverheadBytes)
	cache.put(repo, gitBlobSHA(tooLarge), tooLarge)
	assert.False(t, held(cache, tooLarge), "an entry larger than the limit is not held")
	assert.True(t, held(cache, last), "and it does not empty the cache")
}

// blobReadTestDeadline bounds every wait in the shared blob read tests.
const blobReadTestDeadline = 5 * time.Second

// gatedSchemaFile serves schema/users.sql through the Contents API and holds
// every read until release is called, so a test can line up the callers that
// miss on the file while the first read is in progress. The first read reports
// on started. answer writes the response to each read, numbered from one.
type gatedSchemaFile struct {
	ic      *InstallationClient
	reads   atomic.Int64
	started chan struct{}
	release func()
}

func newGatedSchemaFile(t *testing.T, answer func(read int64, ref string, w http.ResponseWriter)) *gatedSchemaFile {
	t.Helper()
	client, mux := setupRateLimitedTestGitHubServer(t)
	released := make(chan struct{})
	g := &gatedSchemaFile{
		ic:      NewInstallationClient(client, discardLogger()),
		started: make(chan struct{}, 1),
		release: sync.OnceFunc(func() { close(released) }),
	}
	t.Cleanup(g.release)
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema/users.sql", func(w http.ResponseWriter, r *http.Request) {
		read := g.reads.Add(1)
		if read == 1 {
			g.started <- struct{}{}
		}
		select {
		case <-released:
			answer(read, r.URL.Query().Get("ref"), w)
		case <-r.Context().Done():
		}
	})
	return g
}

func writeUsersFile(t *testing.T, w http.ResponseWriter, content string) {
	t.Helper()
	require.NoError(t, json.NewEncoder(w).Encode(gh.RepositoryContent{
		Type: new("file"), Name: new("users.sql"), Path: new("schema/users.sql"), Encoding: new("base64"),
		Content: new(base64.StdEncoding.EncodeToString([]byte(content))),
	}))
}

type blobReadResult struct {
	content string
	err     error
}

// readInBackground reads schema/users.sql at ref through the blob cache under
// the key sha, as a listing at ref that reported that SHA would.
func (g *gatedSchemaFile) readInBackground(ctx context.Context, sha, ref string) <-chan blobReadResult {
	done := make(chan blobReadResult, 1)
	go func() {
		content, err := g.ic.cachedBlobContent(ctx, "octocat/hello-world", sha, func(ctx context.Context) (string, error) {
			return g.ic.FetchFileContent(ctx, "octocat/hello-world", "schema/users.sql", ref)
		})
		done <- blobReadResult{content: content, err: err}
	}()
	return done
}

// joinRead starts a read that reports once it waits on the read already in
// progress, and returns when it does.
func (g *gatedSchemaFile) joinRead(t *testing.T, ctx context.Context, sha, ref string) <-chan blobReadResult {
	t.Helper()
	waiter := policyWaitContext{Context: ctx, waiting: make(chan struct{}, 1)}
	done := g.readInBackground(waiter, sha, ref)
	receivePolicyTestResult(t, ctx, waiter.waiting)
	return done
}

// Two environments of one push, or two pushes that share a schema file, read
// the same blob at the same time. GitHub is asked for it once, and both
// callers get its content.
func TestCachedBlobContentSharesOneReadBetweenConcurrentMisses(t *testing.T) {
	deadline, cancel := context.WithTimeout(t.Context(), blobReadTestDeadline)
	defer cancel()
	firstPush, secondPush := strings.Repeat("a", 40), strings.Repeat("b", 40)
	g := newGatedSchemaFile(t, func(_ int64, _ string, w http.ResponseWriter) { writeUsersFile(t, w, usersTable) })
	sha := gitBlobSHA(usersTable)

	first := g.readInBackground(deadline, sha, firstPush)
	receivePolicyTestResult(t, deadline, g.started)
	second := g.joinRead(t, deadline, sha, secondPush)
	g.release()

	for _, done := range []<-chan blobReadResult{first, second} {
		result := receivePolicyTestResult(t, deadline, done)
		require.NoError(t, result.err)
		assert.Equal(t, usersTable, result.content)
	}
	assert.Equal(t, int64(1), g.reads.Load(), "concurrent misses on one blob make one GitHub read")
}

// The caller that started the shared read gives up while it is in progress.
// It returns its own context's error at once, and the read continues for the
// caller still waiting on it, so GitHub is still asked only once.
func TestCachedBlobContentReadSurvivesTheCallerThatStartedIt(t *testing.T) {
	deadline, cancel := context.WithTimeout(t.Context(), blobReadTestDeadline)
	defer cancel()
	push := strings.Repeat("a", 40)
	g := newGatedSchemaFile(t, func(_ int64, _ string, w http.ResponseWriter) { writeUsersFile(t, w, usersTable) })
	sha := gitBlobSHA(usersTable)

	starterCtx, cancelStarter := context.WithCancel(deadline)
	defer cancelStarter()
	starter := g.readInBackground(starterCtx, sha, push)
	receivePolicyTestResult(t, deadline, g.started)
	waiter := g.joinRead(t, deadline, sha, push)

	cancelStarter()
	assert.ErrorIs(t, receivePolicyTestResult(t, deadline, starter).err, context.Canceled,
		"the caller that gave up returns while the read is still held")
	g.release()

	result := receivePolicyTestResult(t, deadline, waiter)
	require.NoError(t, result.err)
	assert.Equal(t, usersTable, result.content)
	assert.Equal(t, int64(1), g.reads.Load())
}

// GitHub is unavailable for every attempt of a shared read. The read fails
// every caller waiting on it, so the outage costs one read with its retries,
// not one per caller, and it is not kept: the next caller reads the blob from
// GitHub again.
func TestCachedBlobContentDoesNotKeepAFailedSharedRead(t *testing.T) {
	t.Cleanup(SetUnavailableReadRetryDelayForTest(time.Millisecond))
	deadline, cancel := context.WithTimeout(t.Context(), blobReadTestDeadline)
	defer cancel()
	push := strings.Repeat("a", 40)
	g := newGatedSchemaFile(t, func(read int64, _ string, w http.ResponseWriter) {
		if read <= githubUnavailableReadRetryMaxAttempts {
			http.Error(w, `{"message":"Service Unavailable"}`, http.StatusServiceUnavailable)
			return
		}
		writeUsersFile(t, w, usersTable)
	})
	sha := gitBlobSHA(usersTable)

	first := g.readInBackground(deadline, sha, push)
	receivePolicyTestResult(t, deadline, g.started)
	second := g.joinRead(t, deadline, sha, push)
	g.release()
	for _, done := range []<-chan blobReadResult{first, second} {
		result := receivePolicyTestResult(t, deadline, done)
		assert.True(t, IsUnavailableError(result.err), "every waiter gets the shared failure: %v", result.err)
	}
	assert.Equal(t, int64(githubUnavailableReadRetryMaxAttempts), g.reads.Load(), "one shared read with its retries")

	result := receivePolicyTestResult(t, deadline, g.readInBackground(deadline, sha, push))
	require.NoError(t, result.err)
	assert.Equal(t, usersTable, result.content)
	assert.Equal(t, int64(githubUnavailableReadRetryMaxAttempts+1), g.reads.Load(), "the caller after a failed read reads again")
}

// The shared read keeps the client's retries: GitHub answers the first attempt
// with a server error, the read retries inside the flight, and both waiting
// callers get the content from the one read.
func TestCachedBlobContentSharedReadRetriesUnavailableGitHub(t *testing.T) {
	t.Cleanup(SetUnavailableReadRetryDelayForTest(time.Millisecond))
	deadline, cancel := context.WithTimeout(t.Context(), blobReadTestDeadline)
	defer cancel()
	push := strings.Repeat("a", 40)
	g := newGatedSchemaFile(t, func(read int64, _ string, w http.ResponseWriter) {
		if read == 1 {
			http.Error(w, `{"message":"Service Unavailable"}`, http.StatusServiceUnavailable)
			return
		}
		writeUsersFile(t, w, usersTable)
	})
	sha := gitBlobSHA(usersTable)

	first := g.readInBackground(deadline, sha, push)
	receivePolicyTestResult(t, deadline, g.started)
	second := g.joinRead(t, deadline, sha, push)
	g.release()

	for _, done := range []<-chan blobReadResult{first, second} {
		result := receivePolicyTestResult(t, deadline, done)
		require.NoError(t, result.err)
		assert.Equal(t, usersTable, result.content)
	}
	assert.Equal(t, int64(2), g.reads.Load(), "one shared read: the failed attempt and its retry")
}

// A symbolic link to a file is listed under the link's own blob SHA, while a
// read returns the target's content at that commit. Two pushes that changed
// the target read the link at the same time under one key. Each gets its own
// commit's content: the caller whose read was shared reads the file again.
func TestCachedBlobContentReadsASymlinkedFileAtEachCommitConcurrently(t *testing.T) {
	deadline, cancel := context.WithTimeout(t.Context(), blobReadTestDeadline)
	defer cancel()
	firstPush, secondPush := strings.Repeat("a", 40), strings.Repeat("b", 40)
	targetAt := map[string]string{
		firstPush:  usersTable,
		secondPush: "CREATE TABLE users (id INT, email VARCHAR(255));",
	}
	g := newGatedSchemaFile(t, func(_ int64, ref string, w http.ResponseWriter) { writeUsersFile(t, w, targetAt[ref]) })
	linkBlob := gitBlobSHA("../shared/users.sql")

	first := g.readInBackground(deadline, linkBlob, firstPush)
	receivePolicyTestResult(t, deadline, g.started)
	second := g.joinRead(t, deadline, linkBlob, secondPush)
	g.release()

	firstResult := receivePolicyTestResult(t, deadline, first)
	require.NoError(t, firstResult.err)
	assert.Equal(t, targetAt[firstPush], firstResult.content)
	secondResult := receivePolicyTestResult(t, deadline, second)
	require.NoError(t, secondResult.err)
	assert.Equal(t, targetAt[secondPush], secondResult.content, "the second push gets its own commit's content")
	assert.Equal(t, int64(2), g.reads.Load())
}

// A push whose unchanged file sits at a commit GitHub no longer serves is read
// at the same time as a push at a live commit. The read by path fails only for
// the first commit, so the caller at the live commit reads the file for itself
// and gets its content instead of the other commit's not-found error.
func TestCachedBlobContentDoesNotShareAnotherCommitsNotFound(t *testing.T) {
	deadline, cancel := context.WithTimeout(t.Context(), blobReadTestDeadline)
	defer cancel()
	goneRef, liveRef := strings.Repeat("a", 40), strings.Repeat("b", 40)
	g := newGatedSchemaFile(t, func(_ int64, ref string, w http.ResponseWriter) {
		if ref == goneRef {
			http.Error(w, `{"message":"No commit found for the ref"}`, http.StatusNotFound)
			return
		}
		writeUsersFile(t, w, usersTable)
	})
	sha := gitBlobSHA(usersTable)

	first := g.readInBackground(deadline, sha, goneRef)
	receivePolicyTestResult(t, deadline, g.started)
	second := g.joinRead(t, deadline, sha, liveRef)
	g.release()

	assert.True(t, IsNotFoundError(receivePolicyTestResult(t, deadline, first).err))
	result := receivePolicyTestResult(t, deadline, second)
	require.NoError(t, result.err, "the caller at the live commit reads the file itself")
	assert.Equal(t, usersTable, result.content)
	assert.Equal(t, int64(2), g.reads.Load())
}

// A caller whose context has already ended starts no read: a later caller
// with a live context finds neither a read in progress nor a cached result,
// and makes the read itself.
func TestCachedBlobContentStartsNoReadForAnEndedContext(t *testing.T) {
	deadline, cancel := context.WithTimeout(t.Context(), blobReadTestDeadline)
	defer cancel()
	ic := NewInstallationClient(nil, discardLogger())
	sha := gitBlobSHA(usersTable)
	var endedFetches, liveFetches atomic.Int64

	ended, cancelEnded := context.WithCancel(deadline)
	cancelEnded()
	_, err := ic.cachedBlobContent(ended, "octocat/hello-world", sha, func(context.Context) (string, error) {
		endedFetches.Add(1)
		return usersTable, nil
	})
	require.ErrorIs(t, err, context.Canceled)

	content, err := ic.cachedBlobContent(deadline, "octocat/hello-world", sha, func(context.Context) (string, error) {
		liveFetches.Add(1)
		return usersTable, nil
	})
	require.NoError(t, err)
	assert.Equal(t, usersTable, content)
	assert.Equal(t, int64(1), liveFetches.Load(), "the live caller found no read the ended caller started")
	assert.Equal(t, int64(0), endedFetches.Load())
}

// queuedSchemaFiles serves a schema directory of more files than a schema
// fetch reads at once. Every content read reports on arrivals and is held
// until release is called. With tree set, the directory cannot be listed and
// its files are read as blobs from the git tree.
type queuedSchemaFiles struct {
	ic       *InstallationClient
	files    map[string]string // blob SHA to content
	arrivals chan struct{}
	release  func()
}

// schemaFetchConcurrency is how many files a schema fetch reads at once.
const schemaFetchConcurrency = 10

func newQueuedSchemaFiles(t *testing.T, tree bool) *queuedSchemaFiles {
	t.Helper()
	client, mux := setupRateLimitedTestGitHubServer(t)
	released := make(chan struct{})
	q := &queuedSchemaFiles{
		ic:       NewInstallationClient(client, discardLogger()),
		files:    make(map[string]string),
		arrivals: make(chan struct{}, schemaFetchConcurrency+1),
		release:  sync.OnceFunc(func() { close(released) }),
	}
	t.Cleanup(q.release)
	var listing []gh.RepositoryContent
	var treeEntries []any
	contentByPath := make(map[string]string)
	for i := range schemaFetchConcurrency + 1 {
		name := "t" + strconv.Itoa(i) + ".sql"
		content := "CREATE TABLE t" + strconv.Itoa(i) + " (id INT);"
		sha := gitBlobSHA(content)
		q.files[sha] = content
		contentByPath[name] = content
		listing = append(listing, gh.RepositoryContent{Type: new("file"), Name: new(name), Path: new("schema/" + name), SHA: new(sha)})
		treeEntries = append(treeEntries, map[string]any{"path": "schema/" + name, "type": "blob", "mode": "100644", "sha": sha})
	}
	hold := func(w http.ResponseWriter, r *http.Request, body any) {
		q.arrivals <- struct{}{}
		select {
		case <-released:
			require.NoError(t, json.NewEncoder(w).Encode(body))
		case <-r.Context().Done():
		}
	}
	if tree {
		mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, `{"message":"Not Found"}`, http.StatusNotFound)
		})
		mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/{sha}", func(w http.ResponseWriter, _ *http.Request) {
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": treeEntries}))
		})
		mux.HandleFunc("GET /repos/octocat/hello-world/git/blobs/{sha}", func(w http.ResponseWriter, r *http.Request) {
			content := q.files[r.PathValue("sha")]
			hold(w, r, gh.Blob{Content: new(base64.StdEncoding.EncodeToString([]byte(content))), Encoding: new("base64")})
		})
		return q
	}
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(listing))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema/{file}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("file")
		hold(w, r, gh.RepositoryContent{
			Type: new("file"), Name: new(name), Path: new("schema/" + name), Encoding: new("base64"),
			Content: new(base64.StdEncoding.EncodeToString([]byte(contentByPath[name]))),
		})
	})
	return q
}

// A schema fetch is cancelled while it reads as many files as it reads at
// once, with one more file queued behind them. The reads already in progress
// continue and are kept for later callers, but the queued file starts no read
// once the fetch has ended. Both the directory listing and the git tree
// fallback behave this way.
func TestFetchSchemaFilesStartsNoReadAfterCancellation(t *testing.T) {
	for _, tc := range []struct {
		name string
		tree bool
	}{{name: "directory listing"}, {name: "git tree fallback", tree: true}} {
		t.Run(tc.name, func(t *testing.T) {
			deadline, cancel := context.WithTimeout(t.Context(), blobReadTestDeadline)
			defer cancel()
			q := newQueuedSchemaFiles(t, tc.tree)

			fetchCtx, cancelFetch := context.WithCancel(deadline)
			defer cancelFetch()
			done := make(chan error, 1)
			go func() {
				_, err := q.ic.FetchSchemaFilesOptimized(fetchCtx, "octocat/hello-world", strings.Repeat("a", 40), "schema", "mysql")
				done <- err
			}()
			for range schemaFetchConcurrency {
				receivePolicyTestResult(t, deadline, q.arrivals)
			}
			cancelFetch()
			assert.ErrorIs(t, receivePolicyTestResult(t, deadline, done), context.Canceled)
			q.release()

			// A file whose read started is served from that read; a file that
			// started none is read here.
			var unread []string
			for sha, want := range q.files {
				content, err := q.ic.cachedBlobContent(deadline, "octocat/hello-world", sha, func(context.Context) (string, error) {
					unread = append(unread, sha)
					return want, nil
				})
				require.NoError(t, err)
				assert.Equal(t, want, content)
			}
			assert.Len(t, unread, 1, "only the queued file is unread, and every read in progress was kept")
		})
	}
}

// The same blob in two repositories is two reads at the same time, never one
// shared across repositories.
func TestCachedBlobContentDoesNotShareAReadAcrossRepositories(t *testing.T) {
	deadline, cancel := context.WithTimeout(t.Context(), blobReadTestDeadline)
	defer cancel()
	ic := NewInstallationClient(nil, discardLogger())
	sha := gitBlobSHA(usersTable)
	var fetches atomic.Int64
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	fetch := func(context.Context) (string, error) {
		fetches.Add(1)
		started <- struct{}{}
		<-release
		return usersTable, nil
	}
	done := make(chan error, 2)
	for _, repo := range []string{"octocat/a", "octocat/b"} {
		go func() {
			_, err := ic.cachedBlobContent(deadline, repo, sha, fetch)
			done <- err
		}()
	}
	receivePolicyTestResult(t, deadline, started)
	receivePolicyTestResult(t, deadline, started)
	close(release)
	require.NoError(t, receivePolicyTestResult(t, deadline, done))
	require.NoError(t, receivePolicyTestResult(t, deadline, done))
	assert.Equal(t, int64(2), fetches.Load())
}

// pausedContext holds the first call to Err until resume is closed.
// cachedBlobContent checks Err after its cache lookup and before it starts or
// joins a shared read, so the pause holds a caller between the two.
type pausedContext struct {
	context.Context
	once   sync.Once
	paused chan struct{}
	resume chan struct{}
}

func (c *pausedContext) Err() error {
	c.once.Do(func() {
		c.paused <- struct{}{}
		<-c.resume
	})
	return c.Context.Err()
}

// A caller misses the cache, and before it starts its shared read another
// caller reads the blob and caches it. The shared read finds the blob cached
// and makes no GitHub read.
func TestCachedBlobContentSharedReadUsesABlobCachedSinceTheMiss(t *testing.T) {
	deadline, cancel := context.WithTimeout(t.Context(), blobReadTestDeadline)
	defer cancel()
	ic := NewInstallationClient(nil, discardLogger())
	sha := gitBlobSHA(usersTable)
	var lateFetches atomic.Int64

	late := &pausedContext{Context: deadline, paused: make(chan struct{}, 1), resume: make(chan struct{})}
	resume := sync.OnceFunc(func() { close(late.resume) })
	t.Cleanup(resume)
	lateDone := make(chan blobReadResult, 1)
	go func() {
		content, err := ic.cachedBlobContent(late, "octocat/hello-world", sha, func(context.Context) (string, error) {
			lateFetches.Add(1)
			return usersTable, nil
		})
		lateDone <- blobReadResult{content: content, err: err}
	}()
	receivePolicyTestResult(t, deadline, late.paused)

	content, err := ic.cachedBlobContent(deadline, "octocat/hello-world", sha, func(context.Context) (string, error) {
		return usersTable, nil
	})
	require.NoError(t, err)
	require.Equal(t, usersTable, content)
	resume()

	result := receivePolicyTestResult(t, deadline, lateDone)
	require.NoError(t, result.err)
	assert.Equal(t, usersTable, result.content)
	assert.Equal(t, int64(0), lateFetches.Load(), "the shared read found the blob cached since the miss")
}

// A shared read stopped by its own time limit while the client waits to retry
// an unavailable GitHub is reported as GitHub being unavailable, the same as
// one stopped mid-request. A caller that gave up, or another error, is
// reported as it is.
func TestSharedReadErrorReportsItsLimitAsUnavailable(t *testing.T) {
	t.Cleanup(SetUnavailableReadRetryDelayForTest(10 * time.Second))
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema/users.sql", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"message":"Service Unavailable"}`, http.StatusServiceUnavailable)
	})
	ic := NewInstallationClient(client, discardLogger())
	limited, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	_, err := ic.FetchFileContent(limited, "octocat/hello-world", "schema/users.sql", strings.Repeat("a", 40))
	require.ErrorIs(t, err, context.DeadlineExceeded)

	classified := sharedReadError(limited, err)
	assert.True(t, IsUnavailableError(classified), "a read stopped by its limit is unavailable: %v", classified)
	assert.Equal(t, 1, strings.Count(classified.Error(), ErrGitHubUnavailable.Error()), "classified once: %v", classified)
	assert.Equal(t, classified, sharedReadError(limited, classified), "an unavailable error is kept as it is")

	notFound := &gh.ErrorResponse{Response: &http.Response{StatusCode: http.StatusNotFound}}
	assert.Equal(t, error(notFound), sharedReadError(limited, notFound), "another error after the limit is kept as it is")

	gaveUp, cancelGaveUp := context.WithCancel(t.Context())
	cancelGaveUp()
	assert.False(t, IsUnavailableError(sharedReadError(gaveUp, gaveUp.Err())), "a caller that gave up is not GitHub being unavailable")
}
