package github

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bradleyfalzon/ghinstallation/v2"
	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/schemabot/pkg/metrics"
)

const (
	quotaReserveTestRepo    = "octocat/hello-world"
	quotaReserveTestHeadSHA = "head-sha"
	quotaReserveWarning     = "GitHub core quota is at its reserve; schema file and tree reads are refused until the quota resets, and check runs, pull request reads, and comments still go through"
)

// quotaReserveTestServer is a GitHub stand-in whose every response reports the
// core quota the test sets, so a test can walk an installation down to its
// reserve and past a reset.
type quotaReserveTestServer struct {
	ic        *InstallationClient
	remaining atomic.Int64
	resetUnix atomic.Int64
	clock     atomic.Int64 // unix nanoseconds the reserve transport reads as now

	mu   sync.Mutex
	hits map[string]int
}

func newQuotaReserveTestServer(t *testing.T, reserve int64) *quotaReserveTestServer {
	t.Helper()
	s := &quotaReserveTestServer{hits: map[string]int{}}
	s.clock.Store(time.Now().UnixNano())

	mux := http.NewServeMux()
	fileContent := base64.StdEncoding.EncodeToString([]byte("CREATE TABLE t (id INT PRIMARY KEY);"))
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema/t.sql", func(w http.ResponseWriter, _ *http.Request) {
		writeQuotaReserveJSON(t, w, map[string]any{"type": "file", "name": "t.sql", "path": "schema/t.sql", "encoding": "base64", "content": fileContent})
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/{sha}", func(w http.ResponseWriter, _ *http.Request) {
		writeQuotaReserveJSON(t, w, map[string]any{"sha": "tree-sha", "tree": []any{}, "truncated": false})
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/blobs/{sha}", func(w http.ResponseWriter, _ *http.Request) {
		writeQuotaReserveJSON(t, w, map[string]any{"sha": "blob-sha", "encoding": "base64", "content": fileContent})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		writeQuotaReserveJSON(t, w, map[string]any{"id": 7})
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		writeQuotaReserveJSON(t, w, map[string]any{"number": 1, "state": "open", "head": map[string]any{"ref": "feature", "sha": quotaReserveTestHeadSHA}, "base": map[string]any{"ref": "main", "sha": "base-sha"}})
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.hits[r.Method+" "+r.URL.Path]++
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(headerRateLimitResource, "core")
		w.Header().Set(headerRateLimitLimit, "15000")
		w.Header().Set(headerRateLimitRemaining, strconv.FormatInt(s.remaining.Load(), 10))
		w.Header().Set(headerRateLimitReset, strconv.FormatInt(s.resetUnix.Load(), 10))
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)

	rt := newQuotaReserveTransport(metricTestTransport(http.DefaultTransport), reserve, 42, func() string { return "test-app" })
	if reserved, ok := rt.(*quotaReserveTransport); ok {
		reserved.now = func() time.Time { return time.Unix(0, s.clock.Load()) }
	}
	client := gh.NewClient(&http.Client{Transport: rt})
	client.DisableRateLimitCheck = true
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL
	s.ic = NewInstallationClientWithSlug(client, slog.New(slog.NewTextHandler(io.Discard, nil)), "test-app")
	return s
}

func writeQuotaReserveJSON(t *testing.T, w http.ResponseWriter, body any) {
	t.Helper()
	assert.NoError(t, json.NewEncoder(w).Encode(body))
}

func (s *quotaReserveTestServer) hitCount(route string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[route]
}

func (s *quotaReserveTestServer) createCheckRun(t *testing.T) error {
	t.Helper()
	_, err := s.ic.CreateCheckRun(t.Context(), quotaReserveTestRepo, quotaReserveTestHeadSHA, CheckRunOptions{Name: "SchemaBot", Status: "in_progress"})
	return err
}

func (s *quotaReserveTestServer) fetchFileContent(t *testing.T) error {
	t.Helper()
	_, err := s.ic.FetchFileContent(t.Context(), quotaReserveTestRepo, "schema/t.sql", quotaReserveTestHeadSHA)
	return err
}

func captureQuotaReserveWarnings(t *testing.T) func() []map[string]any {
	t.Helper()
	var mu sync.Mutex
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&lockedWriter{mu: &mu, w: &logs}, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return func() []map[string]any {
		mu.Lock()
		defer mu.Unlock()
		var warnings []map[string]any
		decoder := json.NewDecoder(bytes.NewReader(logs.Bytes()))
		for decoder.More() {
			var entry map[string]any
			require.NoError(t, decoder.Decode(&entry))
			if entry["msg"] == quotaReserveWarning {
				warnings = append(warnings, entry)
			}
		}
		return warnings
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func reserveRejectionsByOperation(t *testing.T, data map[string]metricdata.Metrics) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	quota, ok := data[quotaMetric]
	if !ok {
		return out
	}
	for _, p := range quota.Data.(metricdata.Sum[int64]).DataPoints {
		origin, _ := p.Attributes.Value("origin")
		if origin.AsString() != metrics.GitHubRejectionOriginReserve {
			continue
		}
		operation, _ := p.Attributes.Value("operation")
		assertThrottlingIdentity(t, p.Attributes, operation.AsString())
		out[operation.AsString()] += p.Value
	}
	return out
}

// An installation burns its core quota down to the reserve. From then until
// the reset, schema file, blob, and tree reads are refused before they reach
// GitHub, while Check Run writes and pull request reads still go out, so a
// burst of plan work delays plans instead of leaving required checks unable to
// post. Once the reset passes, schema reads go out again.
func TestQuotaReserveDefersSchemaInputReadsUntilReset(t *testing.T) {
	reader := newGitHubMetricsTestReader(t)
	warnings := captureQuotaReserveWarnings(t)
	s := newQuotaReserveTestServer(t, 100)
	reset := time.Unix(s.clock.Load()/int64(time.Second), 0).Add(10 * time.Minute)
	s.resetUnix.Store(reset.Unix())

	// Above the reserve, schema reads go out.
	s.remaining.Store(500)
	require.NoError(t, s.fetchFileContent(t))
	assert.Equal(t, 1, s.hitCount("GET /repos/octocat/hello-world/contents/schema/t.sql"))

	// A Check Run write reports the quota down to the reserve.
	s.remaining.Store(100)
	require.NoError(t, s.createCheckRun(t))

	err := s.fetchFileContent(t)
	require.ErrorIs(t, err, ErrGitHubQuotaReserved)
	var reserved *QuotaReservedError
	require.ErrorAs(t, err, &reserved)
	assert.Equal(t, metrics.GitHubOperationFetchFileContent, reserved.Operation)
	assert.Equal(t, int64(100), reserved.Remaining)
	assert.Equal(t, int64(100), reserved.Reserve)
	assert.True(t, reset.Equal(reserved.ResetAt), "reset %s, got %s", reset, reserved.ResetAt)
	assert.False(t, IsUnavailableError(err), "a reserve refusal is not an outage, so read retries do not wait on it")

	_, _, err = s.ic.FetchGitTree(t.Context(), quotaReserveTestRepo, "tree-sha")
	require.ErrorIs(t, err, ErrGitHubQuotaReserved)
	_, err = s.ic.FetchBlobContent(t.Context(), quotaReserveTestRepo, "blob-sha")
	require.ErrorIs(t, err, ErrGitHubQuotaReserved)

	assert.Equal(t, 1, s.hitCount("GET /repos/octocat/hello-world/contents/schema/t.sql"), "a refused read never reaches GitHub")
	assert.Equal(t, 0, s.hitCount("GET /repos/octocat/hello-world/git/trees/tree-sha"))
	assert.Equal(t, 0, s.hitCount("GET /repos/octocat/hello-world/git/blobs/blob-sha"))

	// Check Run writes and pull request reads keep spending the reserve.
	s.remaining.Store(40)
	require.NoError(t, s.createCheckRun(t))
	_, err = s.ic.FetchPullRequest(t.Context(), quotaReserveTestRepo, 1)
	require.NoError(t, err)
	assert.Equal(t, 2, s.hitCount("POST /repos/octocat/hello-world/check-runs"))
	assert.Equal(t, 1, s.hitCount("GET /repos/octocat/hello-world/pulls/1"))

	data := throttlingData(t, reader)
	assert.Equal(t, map[string]int64{
		metrics.GitHubOperationFetchFileContent: 1,
		metrics.GitHubOperationFetchGitTree:     1,
		metrics.GitHubOperationFetchBlob:        1,
	}, reserveRejectionsByOperation(t, data))

	got := warnings()
	require.Len(t, got, 1, "one warning per installation per reset window")
	assert.Equal(t, float64(42), got[0]["installation_id"])
	assert.Equal(t, "test-app", got[0]["github_app"])
	assert.Equal(t, float64(100), got[0]["remaining"])
	assert.Equal(t, float64(100), got[0]["reserve"])
	assert.Equal(t, reset.UTC().Format(time.RFC3339), got[0]["reset_time"])
	assert.Equal(t, quotaReserveTestRepo, got[0]["repo"])

	// After the reset, schema reads go out again, and the new window's report
	// lifts the reserve.
	s.clock.Store(reset.Add(time.Second).UnixNano())
	nextReset := reset.Add(time.Hour)
	s.resetUnix.Store(nextReset.Unix())
	s.remaining.Store(14999)
	require.NoError(t, s.fetchFileContent(t))
	require.NoError(t, s.fetchFileContent(t))
	assert.Equal(t, 3, s.hitCount("GET /repos/octocat/hello-world/contents/schema/t.sql"))

	// Reaching the reserve in the next window warns again.
	s.remaining.Store(90)
	require.NoError(t, s.createCheckRun(t))
	require.ErrorIs(t, s.fetchFileContent(t), ErrGitHubQuotaReserved)
	assert.Len(t, warnings(), 2)
}

// With no reserve configured, nothing is refused: a schema read goes out even
// when GitHub reports no quota left on a successful response.
func TestQuotaReserveZeroNeverRefuses(t *testing.T) {
	reader := newGitHubMetricsTestReader(t)
	s := newQuotaReserveTestServer(t, 0)
	s.resetUnix.Store(time.Now().Add(10 * time.Minute).Unix())
	s.remaining.Store(0)

	require.NoError(t, s.createCheckRun(t))
	require.NoError(t, s.fetchFileContent(t))
	_, _, err := s.ic.FetchGitTree(t.Context(), quotaReserveTestRepo, "tree-sha")
	require.NoError(t, err)

	assert.Equal(t, 1, s.hitCount("GET /repos/octocat/hello-world/contents/schema/t.sql"))
	assert.Equal(t, 1, s.hitCount("GET /repos/octocat/hello-world/git/trees/tree-sha"))
	assert.Empty(t, reserveRejectionsByOperation(t, throttlingData(t, reader)))
}

// Responses arrive out of order. A late response from an earlier reset window,
// or one about another rate-limit resource, must not lift a reserve the
// current window has reached.
func TestQuotaReserveIgnoresEarlierWindowsAndOtherResources(t *testing.T) {
	now := time.Now()
	rt, ok := newQuotaReserveTransport(http.DefaultTransport, 100, 42, nil).(*quotaReserveTransport)
	require.True(t, ok)
	rt.now = func() time.Time { return now }
	reset := now.Add(10 * time.Minute).Truncate(time.Second)

	observe := func(resource string, remaining int64, resetAt time.Time) {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://api.github.com/repos/octocat/hello-world/pulls/1", nil)
		resp := &http.Response{Header: http.Header{}}
		resp.Header.Set(headerRateLimitResource, resource)
		resp.Header.Set(headerRateLimitRemaining, strconv.FormatInt(remaining, 10))
		resp.Header.Set(headerRateLimitReset, strconv.FormatInt(resetAt.Unix(), 10))
		rt.observe(req, resp)
	}

	observe("core", 80, reset)
	observe("core", 4000, reset.Add(-time.Hour))
	observe("core", 120, reset)
	observe("graphql", 4000, reset.Add(time.Hour))

	refusal, _ := rt.refusal(metrics.GitHubOperationFetchFileContent)
	require.NotNil(t, refusal)
	assert.Equal(t, int64(80), refusal.Remaining, "the lowest report in the current window holds")
	assert.True(t, reset.Equal(refusal.ResetAt))
}

// The reserve sits above the installation token transport. A token exchange
// reports the App's own quota, which says nothing about the installation's,
// so an exhausted App quota on the exchange must not hold back schema reads.
func TestQuotaReserveWrapsInstallationTokenTransport(t *testing.T) {
	var contentReads atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(headerRateLimitResource, "core")
		w.Header().Set(headerRateLimitReset, strconv.FormatInt(time.Now().Add(10*time.Minute).Unix(), 10))
		if r.URL.Path == "/app/installations/42/access_tokens" {
			w.Header().Set(headerRateLimitRemaining, "0")
			w.WriteHeader(http.StatusCreated)
			writeQuotaReserveJSON(t, w, map[string]any{"token": "ghs_test", "expires_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)})
			return
		}
		contentReads.Add(1)
		w.Header().Set(headerRateLimitRemaining, "4000")
		writeQuotaReserveJSON(t, w, map[string]any{"type": "file", "name": "t.sql", "path": "schema/t.sql", "encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte("x"))})
	}))
	t.Cleanup(server.Close)

	transport, err := newInstallationTransport(1, 42, testInstallationPrivateKey(t), nil, listenOnlyRepoSet{}, 100)
	require.NoError(t, err)
	reserve, ok := transport.(*quotaReserveTransport)
	require.True(t, ok, "the quota reserve is outermost without listen-only repositories, got %T", transport)
	tokenTransport, ok := reserve.base.(*ghinstallation.Transport)
	require.True(t, ok, "the quota reserve sends through the token transport, got %T", reserve.base)
	tokenTransport.BaseURL = server.URL

	client := gh.NewClient(&http.Client{Transport: transport})
	client.DisableRateLimitCheck = true
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL
	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))

	for range 2 {
		_, err := ic.FetchFileContent(t.Context(), quotaReserveTestRepo, "schema/t.sql", quotaReserveTestHeadSHA)
		require.NoError(t, err)
	}
	assert.Equal(t, int64(2), contentReads.Load())

	withListenOnly, err := newInstallationTransport(1, 42, testInstallationPrivateKey(t), nil, listenOnlyRepoSet{listenOnlyTestRepo: true}, 100)
	require.NoError(t, err)
	guard, ok := withListenOnly.(*listenOnlyTransport)
	require.True(t, ok, "the listen-only guard stays outermost, got %T", withListenOnly)
	_, ok = guard.base.(*quotaReserveTransport)
	assert.True(t, ok, "the listen-only guard sends through the quota reserve, got %T", guard.base)
}

// A refused schema read surfaces from plan request construction as the typed
// error, so the caller can tell a quota deferral from a real failure.
func TestQuotaReserveRefusalPropagatesFromSchemaRequest(t *testing.T) {
	s := newQuotaReserveTestServer(t, 100)
	s.resetUnix.Store(time.Unix(0, s.clock.Load()).Add(10 * time.Minute).Unix())
	s.remaining.Store(50)
	require.NoError(t, s.createCheckRun(t))

	_, err := s.ic.FetchSchemaFilesOptimized(t.Context(), quotaReserveTestRepo, quotaReserveTestHeadSHA, "schema", "mysql")
	require.ErrorIs(t, err, ErrGitHubQuotaReserved)

	config := &SchemabotConfig{Database: "app", Type: "mysql"}
	_, err = s.ic.CreateSchemaRequestForConfig(t.Context(), quotaReserveTestRepo, 1, "staging", config, "schema", nil)
	require.ErrorIs(t, err, ErrGitHubQuotaReserved)
	var reserved *QuotaReservedError
	require.ErrorAs(t, err, &reserved)
	assert.Equal(t, metrics.GitHubOperationFetchFileContent, reserved.Operation)
	assert.Equal(t, 1, s.hitCount("GET /repos/octocat/hello-world/pulls/1"), "the pull request read still goes out")
}
