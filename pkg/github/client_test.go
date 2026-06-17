package github

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateCheckRunRetriesSecondaryRateLimitedWrite(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	var attempts atomic.Int64
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Contains(t, string(body), "schemabot/staging")
		if attempts.Add(1) == 1 {
			writeGitHubSecondaryRateLimitError(t, w, http.StatusTooManyRequests)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(gh.CheckRun{ID: new(int64(1234))}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	checkRunID, err := ic.CreateCheckRun(t.Context(), "octocat/hello-world", "abc123", CheckRunOptions{
		Name:   "schemabot/staging",
		Status: "in_progress",
	})

	require.NoError(t, err)
	assert.Equal(t, int64(1234), checkRunID)
	assert.Equal(t, int64(2), attempts.Load())
}

func TestUpdateCheckRunRetriesSecondaryRateLimitWrite(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	var attempts atomic.Int64
	mux.HandleFunc("PATCH /repos/octocat/hello-world/check-runs/1234", func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			writeGitHubSecondaryRateLimitError(t, w, http.StatusForbidden)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(gh.CheckRun{ID: new(int64(1234))}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := ic.UpdateCheckRun(t.Context(), "octocat/hello-world", 1234, CheckRunOptions{
		Name:   "schemabot/staging",
		Status: "completed",
	})

	require.NoError(t, err)
	assert.Equal(t, int64(2), attempts.Load())
}

func TestCreateIssueCommentRetriesSecondaryRateLimitedWrite(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	var attempts atomic.Int64
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			writeGitHubSecondaryRateLimitError(t, w, http.StatusTooManyRequests)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(gh.IssueComment{ID: new(int64(5678))}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	commentID, err := ic.CreateIssueComment(t.Context(), "octocat/hello-world", 1, "hello")

	require.NoError(t, err)
	assert.Equal(t, int64(5678), commentID)
	assert.Equal(t, int64(2), attempts.Load())
}

func TestAddReactionToCommentRetriesSecondaryRateLimitedWrite(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	var attempts atomic.Int64
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/5678/reactions", func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			writeGitHubSecondaryRateLimitError(t, w, http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(gh.Reaction{ID: new(int64(9012))}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := ic.AddReactionToComment(t.Context(), "octocat/hello-world", 5678, "eyes")

	require.NoError(t, err)
	assert.Equal(t, int64(2), attempts.Load())
}

func TestEditIssueCommentDoesNotRetryNonRateLimitWriteError(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	var attempts atomic.Int64
	mux.HandleFunc("PATCH /repos/octocat/hello-world/issues/comments/5678", func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
		require.NoError(t, json.NewEncoder(w).Encode(gh.ErrorResponse{Message: "bad credentials"}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := ic.EditIssueComment(t.Context(), "octocat/hello-world", 5678, "hello")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
	assert.Equal(t, int64(1), attempts.Load())
}

// TestFindCheckRunByNameIgnoresForeignAppCheckRuns covers a PR commit that
// carries two completed check runs with the same name: a passing run created
// by another GitHub App and SchemaBot's own run, which has not passed. The
// lookup must return SchemaBot's own run so a same-named foreign run can
// never satisfy a gate that relies on this lookup.
func TestFindCheckRunByNameIgnoresForeignAppCheckRuns(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "SchemaBot (staging)", r.URL.Query().Get("check_name"))
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"total_count": 2,
			"check_runs": []map[string]any{
				{"id": 7, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "github-actions"}},
				{"id": 3, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "action_required", "app": map[string]any{"slug": "schemabot"}},
			},
		}))
	})

	ic := NewInstallationClientWithSlug(client, slog.New(slog.NewTextHandler(io.Discard, nil)), "schemabot")
	result, untrustedApps, err := ic.FindCheckRunByName(t.Context(), "octocat/hello-world", "abc123", "SchemaBot (staging)")

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(3), result.ID)
	assert.Equal(t, "SchemaBot (staging)", result.Name)
	assert.Equal(t, "completed", result.Status)
	assert.Equal(t, "action_required", result.Conclusion)
	assert.Equal(t, []string{"github-actions"}, untrustedApps, "the ignored foreign app is reported for triage")
}

// TestFindCheckRunByNameReturnsNilWhenOnlyForeignAppRunsExist covers a PR
// commit where the only check run with the requested name was created by
// another GitHub App. The lookup must report the check run as missing so
// callers treat the gate as unsatisfied.
func TestFindCheckRunByNameReturnsNilWhenOnlyForeignAppRunsExist(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"total_count": 1,
			"check_runs": []map[string]any{
				{"id": 7, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "github-actions"}},
			},
		}))
	})

	ic := NewInstallationClientWithSlug(client, slog.New(slog.NewTextHandler(io.Discard, nil)), "schemabot")
	result, untrustedApps, err := ic.FindCheckRunByName(t.Context(), "octocat/hello-world", "abc123", "SchemaBot (staging)")

	require.NoError(t, err)
	assert.Nil(t, result)
	assert.Equal(t, []string{"github-actions"}, untrustedApps, "callers can distinguish an untrusted check from a missing one")
}

// TestFindCheckRunByNameReturnsMostRecentOwnAppRun covers a PR commit with
// several own-app check runs sharing the same name. The lookup must return
// the most recently created run (highest check run ID) regardless of the
// order GitHub lists them in, so a stale earlier run cannot mask the current
// gate state.
func TestFindCheckRunByNameReturnsMostRecentOwnAppRun(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"total_count": 2,
			"check_runs": []map[string]any{
				{"id": 3, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "failure", "app": map[string]any{"slug": "schemabot"}},
				{"id": 9, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "schemabot"}},
			},
		}))
	})

	ic := NewInstallationClientWithSlug(client, slog.New(slog.NewTextHandler(io.Discard, nil)), "schemabot")
	result, untrustedApps, err := ic.FindCheckRunByName(t.Context(), "octocat/hello-world", "abc123", "SchemaBot (staging)")

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(9), result.ID)
	assert.Equal(t, "success", result.Conclusion)
	assert.Empty(t, untrustedApps)
}

// TestFindCheckRunByNameErrorsWhenOwnAppSlugUnknown covers the case where the
// client does not know its own GitHub App slug, so check run ownership cannot
// be verified. The lookup must return an error without querying GitHub —
// ownership ambiguity must never be converted into a result a gate could
// trust.
func TestFindCheckRunByNameErrorsWhenOwnAppSlugUnknown(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	var requests atomic.Int64
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"total_count": 1,
			"check_runs": []map[string]any{
				{"id": 7, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "schemabot"}},
			},
		}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	result, untrustedApps, err := ic.FindCheckRunByName(t.Context(), "octocat/hello-world", "abc123", "SchemaBot (staging)")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "check run ownership cannot be verified")
	assert.Contains(t, err.Error(), "octocat/hello-world")
	assert.Contains(t, err.Error(), "abc123")
	assert.Nil(t, result)
	assert.Empty(t, untrustedApps)
	assert.Equal(t, int64(0), requests.Load())
}

// A schema directory listed via the GitHub Contents API at the documented
// 1000-entry cap is treated as truncated: schema discovery fails closed with an
// error naming the directory rather than proceeding with a possibly-incomplete
// list, which would otherwise surface as spurious DROP TABLE proposals or missed
// changes in the declarative differ.
func TestFetchSchemaFilesOptimizedFailsClosedWhenDirectoryAtCap(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)

	entries := make([]gh.RepositoryContent, 0, maxGitHubDirEntries)
	for i := range maxGitHubDirEntries {
		entries = append(entries, gh.RepositoryContent{
			Type: new("file"),
			Name: new("t" + strconv.Itoa(i) + ".sql"),
			Path: new("schema/t" + strconv.Itoa(i) + ".sql"),
		})
	}
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(entries))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	files, err := ic.FetchSchemaFilesOptimized(t.Context(), "octocat/hello-world", "abc123", "schema", "mysql")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDirListingCapped)
	assert.Contains(t, err.Error(), "schema")
	assert.Nil(t, files)
}

// A schema directory below the Contents API cap is listed and its managed
// schema files are fetched unchanged.
func TestFetchSchemaFilesOptimizedSmallDirectory(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)

	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema", func(w http.ResponseWriter, _ *http.Request) {
		entries := []gh.RepositoryContent{
			{Type: new("file"), Name: new("users.sql"), Path: new("schema/users.sql")},
			{Type: new("file"), Name: new("README.md"), Path: new("schema/README.md")},
		}
		require.NoError(t, json.NewEncoder(w).Encode(entries))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema/users.sql", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.RepositoryContent{
			Type:     new("file"),
			Name:     new("users.sql"),
			Path:     new("schema/users.sql"),
			Encoding: new("base64"),
			Content:  new(base64.StdEncoding.EncodeToString([]byte("CREATE TABLE users (id INT);"))),
		}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	files, err := ic.FetchSchemaFilesOptimized(t.Context(), "octocat/hello-world", "abc123", "schema", "mysql")

	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.Equal(t, "users.sql", files[0].Name)
	assert.Equal(t, "schema/users.sql", files[0].Path)
	assert.Equal(t, "CREATE TABLE users (id INT);", files[0].Content)
}

// TestFindCheckRunByNameAcceptsTrustedSiblingAppCheckRun covers the
// split-deployment topology where the looked-up check run was created by a
// sibling SchemaBot deployment's GitHub App. The lookup must return the
// sibling App's run when its slug is configured as trusted, while a
// same-named run from an unconfigured app is still ignored.
func TestFindCheckRunByNameAcceptsTrustedSiblingAppCheckRun(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"total_count": 2,
			"check_runs": []map[string]any{
				{"id": 9, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "failure", "app": map[string]any{"slug": "github-actions"}},
				{"id": 3, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "schemabot-staging"}},
			},
		}))
	})

	ic := NewInstallationClientWithSlug(client, slog.New(slog.NewTextHandler(io.Discard, nil)), "schemabot-production", "schemabot-staging")
	result, untrustedApps, err := ic.FindCheckRunByName(t.Context(), "octocat/hello-world", "abc123", "SchemaBot (staging)")

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(3), result.ID)
	assert.Equal(t, "completed", result.Status)
	assert.Equal(t, "success", result.Conclusion)
	assert.Equal(t, []string{"github-actions"}, untrustedApps)
}

// TestFindCheckRunByNameProceedsWithTrustedSlugsWhenOwnSlugUnknown covers a
// client that failed to fetch its own App slug but has trusted sibling slugs
// configured. The configured slugs are statically verifiable, so the lookup
// proceeds and matches the sibling App's run instead of failing closed on the
// missing own slug.
func TestFindCheckRunByNameProceedsWithTrustedSlugsWhenOwnSlugUnknown(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"total_count": 1,
			"check_runs": []map[string]any{
				{"id": 4, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "schemabot-staging"}},
			},
		}))
	})

	ic := NewInstallationClientWithSlug(client, slog.New(slog.NewTextHandler(io.Discard, nil)), "", "schemabot-staging")
	result, untrustedApps, err := ic.FindCheckRunByName(t.Context(), "octocat/hello-world", "abc123", "SchemaBot (staging)")

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(4), result.ID)
	assert.Equal(t, "success", result.Conclusion)
	assert.Empty(t, untrustedApps)
}

// TestGetPRCheckStatusesClassifiesTrustedSiblingAppAsSchemaBot verifies that
// aggregate checks created by a trusted sibling SchemaBot deployment's App are
// classified as SchemaBot checks. Sibling aggregate checks are governed by
// SchemaBot's own promotion and merge gates, so the passing-checks gate must
// not treat them as external CI that can block apply.
func TestGetPRCheckStatusesClassifiesTrustedSiblingAppAsSchemaBot(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/status", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"state": "pending", "statuses": []map[string]any{}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"total_count": 3,
			"check_runs": []map[string]any{
				{"id": 1, "name": "SchemaBot (production)", "status": "completed", "conclusion": "action_required", "app": map[string]any{"slug": "schemabot-production"}},
				{"id": 2, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "action_required", "app": map[string]any{"slug": "schemabot-staging"}},
				{"id": 3, "name": "ci/build", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "github-actions"}},
			},
		}))
	})

	ic := NewInstallationClientWithSlug(client, slog.New(slog.NewTextHandler(io.Discard, nil)), "schemabot-production", "schemabot-staging")
	statuses, err := ic.GetPRCheckStatuses(t.Context(), "octocat/hello-world", "abc123", nil)

	require.NoError(t, err)
	require.Len(t, statuses, 3)
	byName := make(map[string]PRCheckStatus, len(statuses))
	for _, s := range statuses {
		byName[s.Name] = s
	}
	assert.True(t, byName["SchemaBot (production)"].IsSchemaBot, "own App check must be classified as SchemaBot")
	assert.True(t, byName["SchemaBot (staging)"].IsSchemaBot, "trusted sibling App check must be classified as SchemaBot")
	assert.False(t, byName["ci/build"].IsSchemaBot, "unrelated app check must remain external CI")
}

func setupRateLimitedTestGitHubServer(t *testing.T) (*gh.Client, *http.ServeMux) {
	t.Helper()

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(&http.Client{Transport: newGitHubRateLimitTransport(http.DefaultTransport)})
	client.DisableRateLimitCheck = true
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL

	return client, mux
}

func writeGitHubSecondaryRateLimitError(t *testing.T, w http.ResponseWriter, statusCode int) {
	t.Helper()

	w.Header().Set("X-RateLimit-Remaining", "1")
	w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(-time.Second).Unix(), 10))
	w.WriteHeader(statusCode)
	require.NoError(t, json.NewEncoder(w).Encode(gh.ErrorResponse{
		Message:          "You have exceeded a secondary rate limit",
		DocumentationURL: "https://docs.github.com/rest/using-the-rest-api/rate-limits-for-the-rest-api#about-secondary-rate-limits",
	}))
}
