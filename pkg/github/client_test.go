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
		require.NoError(t, json.NewEncoder(w).Encode(gh.IssueComment{ID: new(int64(5678)), NodeID: new("IC_node5678")}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	commentID, nodeID, err := ic.CreateIssueComment(t.Context(), "octocat/hello-world", 1, "hello")

	require.NoError(t, err)
	assert.Equal(t, int64(5678), commentID)
	assert.Equal(t, "IC_node5678", nodeID)
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

func TestFetchChangedFilesBetweenRequiresLinearHistory(t *testing.T) {
	tests := []struct {
		status  string
		wantErr bool
	}{
		{status: "ahead"},
		{status: "identical"},
		{status: "behind", wantErr: true},
		{status: "diverged", wantErr: true},
		{wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			client, mux := setupRateLimitedTestGitHubServer(t)
			mux.HandleFunc("GET /repos/octocat/hello-world/compare/base...head", func(w http.ResponseWriter, _ *http.Request) {
				require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{
					Status: &tt.status,
					Files: []*gh.CommitFile{{
						Filename:  new("app/service.go"),
						Status:    new("modified"),
						Additions: new(4),
						Deletions: new(2),
					}},
				}))
			})

			ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
			files, err := ic.FetchChangedFilesBetween(t.Context(), "octocat/hello-world", "base", "head")
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "cannot prove linear history")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "app/service.go", files[0].Filename)
		})
	}
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
	_, err := ic.EditIssueComment(t.Context(), "octocat/hello-world", 5678, "hello")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
	assert.Equal(t, int64(1), attempts.Load())
}

// An edit returns when GitHub created the comment, so a caller can age the
// comment without spending a separate read on it.
func TestEditIssueCommentReturnsCommentCreatedAt(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("PATCH /repos/octocat/hello-world/issues/comments/5678", func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte(`{"id":5678,"body":"hello","created_at":"2026-06-16T19:42:00Z"}`))
		require.NoError(t, err)
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	createdAt, err := ic.EditIssueComment(t.Context(), "octocat/hello-world", 5678, "hello")

	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 6, 16, 19, 42, 0, 0, time.UTC), createdAt.UTC())
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

// A namespace directory under the schema root may be a symlink to a directory
// elsewhere in the repository (the layout some services use to share one
// canonical table set across many keyspaces). The server-side discovery follows
// the symlink, fetches the target's files, and presents them under the symlink's
// name so each symlinked keyspace becomes its own namespace — even when several
// symlinks point at the same target.
func TestFetchSchemaFilesOptimizedFollowsSymlinkedNamespaces(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)

	// The Contents API directory listing returns symlink entries WITHOUT a
	// target; the target is only populated when the symlink path is fetched
	// directly (mirrored by the per-symlink handlers below).
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema", func(w http.ResponseWriter, _ *http.Request) {
		entries := []gh.RepositoryContent{
			{Type: new("file"), Name: new("schemabot.yaml"), Path: new("schema/schemabot.yaml")},
			{Type: new("symlink"), Name: new("shard_001"), Path: new("schema/shard_001")},
			{Type: new("symlink"), Name: new("shard_002"), Path: new("schema/shard_002")},
		}
		require.NoError(t, json.NewEncoder(w).Encode(entries))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema/shard_001", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.RepositoryContent{
			Type: new("symlink"), Name: new("shard_001"), Path: new("schema/shard_001"), Target: new("../canonical/shard"),
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema/shard_002", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.RepositoryContent{
			Type: new("symlink"), Name: new("shard_002"), Path: new("schema/shard_002"), Target: new("../canonical/shard"),
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/canonical/shard", func(w http.ResponseWriter, _ *http.Request) {
		entries := []gh.RepositoryContent{
			{Type: new("file"), Name: new("orders.sql"), Path: new("canonical/shard/orders.sql")},
		}
		require.NoError(t, json.NewEncoder(w).Encode(entries))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/canonical/shard/orders.sql", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.RepositoryContent{
			Type:     new("file"),
			Name:     new("orders.sql"),
			Path:     new("canonical/shard/orders.sql"),
			Encoding: new("base64"),
			Content:  new(base64.StdEncoding.EncodeToString([]byte("CREATE TABLE orders (id INT);"))),
		}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	files, err := ic.FetchSchemaFilesOptimized(t.Context(), "octocat/hello-world", "abc123", "schema", "vitess")
	require.NoError(t, err)

	// Both symlinked keyspaces are discovered, each carrying the shared target's
	// content but labeled under its own namespace path.
	require.Len(t, files, 2)
	assert.Equal(t, "schema/shard_001/orders.sql", files[0].Path)
	assert.Equal(t, "schema/shard_002/orders.sql", files[1].Path)
	for _, f := range files {
		assert.Equal(t, "orders.sql", f.Name)
		assert.Equal(t, "CREATE TABLE orders (id INT);", f.Content)
	}

	// Grouping keys each file under its symlink (keyspace) name.
	grouped, _, err := groupFilesByNamespace(files, "schema", "", nil)
	require.NoError(t, err)
	require.Contains(t, grouped, "shard_001")
	require.Contains(t, grouped, "shard_002")
}

// A symlinked namespace whose target escapes the repository root is rejected
// rather than followed, so a crafted schema layout cannot read files outside the
// repository.
func TestFetchSchemaFilesOptimizedRejectsEscapingSymlink(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)

	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema", func(w http.ResponseWriter, _ *http.Request) {
		entries := []gh.RepositoryContent{
			{Type: new("symlink"), Name: new("evil"), Path: new("schema/evil"), Target: new("../../../../etc")},
		}
		require.NoError(t, json.NewEncoder(w).Encode(entries))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err := ic.FetchSchemaFilesOptimized(t.Context(), "octocat/hello-world", "abc123", "schema", "mysql")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "outside the repository")
}

// A namespace symlink that resolves back to its own path is rejected rather than
// re-fetched, which would otherwise loop or surface a confusing "not a directory"
// error from the directory listing.
func TestFetchSchemaFilesOptimizedRejectsSelfReferentialSymlink(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)

	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema", func(w http.ResponseWriter, _ *http.Request) {
		entries := []gh.RepositoryContent{
			{Type: new("symlink"), Name: new("shard_001"), Path: new("schema/shard_001"), Target: new("shard_001")},
		}
		require.NoError(t, json.NewEncoder(w).Encode(entries))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err := ic.FetchSchemaFilesOptimized(t.Context(), "octocat/hello-world", "abc123", "schema", "mysql")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "distinct directory")
}

// When the Contents API cannot list the schema root, discovery falls back to the
// git tree. A symlinked namespace there is a mode-120000 blob whose target's
// files live outside the schema root, so the fallback cannot resolve it and must
// fail closed rather than silently dropping the symlinked schema (which would
// otherwise surface as a missing keyspace / spurious DROP proposals).
func TestFetchSchemaFilesFromTreeFailsClosedOnSymlink(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)

	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"truncated": false,
			"tree": []map[string]any{
				{"path": "schema/shard_001", "mode": "120000", "type": "blob", "sha": "deadbeef"},
			},
		}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err := ic.FetchSchemaFilesOptimized(t.Context(), "octocat/hello-world", "abc123", "schema", "mysql")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "symlink")
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

// registerBaseTipRef serves the base-branch ref resolution the freshness
// comparison performs before comparing commits. The tip SHA is deliberately
// distinct from any PR-object base snapshot: the comparison must observe the
// branch's current tip, which GitHub only exposes via the ref.
func registerBaseTipRef(t *testing.T, mux *http.ServeMux, tipSHA string) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.Reference{
			Ref:    new("refs/heads/main"),
			Object: &gh.GitObject{Type: new("commit"), SHA: &tipSHA},
		}))
	})
}

func TestSchemaPathsChangedSinceMergeBase(t *testing.T) {
	tests := []struct {
		name         string
		mergeTreeSHA string
		tipTreeSHA   string
		mergeHasPath bool
		tipHasPath   bool
		wantChanged  []string
	}{
		{
			name:         "same schema tree ignores unrelated base commits",
			mergeTreeSHA: "schema-tree",
			tipTreeSHA:   "schema-tree",
			mergeHasPath: true,
			tipHasPath:   true,
		},
		{
			name:         "schema tree changed on base tip is stale",
			mergeTreeSHA: "schema-tree-old",
			tipTreeSHA:   "schema-tree-new",
			mergeHasPath: true,
			tipHasPath:   true,
			wantChanged:  []string{"schema"},
		},
		{
			name:        "schema path added on base is stale",
			tipTreeSHA:  "schema-tree-new",
			tipHasPath:  true,
			wantChanged: []string{"schema"},
		},
		{
			name:         "schema path removed on base is stale",
			mergeTreeSHA: "schema-tree-old",
			mergeHasPath: true,
			wantChanged:  []string{"schema"},
		},
		{
			name: "path absent at both commits is unchanged",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux := setupRateLimitedTestGitHubServer(t)
			registerBaseTipRef(t, mux, "base-tip-sha")
			mux.HandleFunc("GET /repos/octocat/hello-world/compare/base-tip-sha...head-sha", func(w http.ResponseWriter, _ *http.Request) {
				require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{
					MergeBaseCommit: &gh.RepositoryCommit{SHA: new("merge-base-sha")},
				}))
			})
			registerRootTree := func(pattern string, hasPath bool, schemaTreeSHA string) {
				mux.HandleFunc(pattern, func(w http.ResponseWriter, _ *http.Request) {
					entries := []*gh.TreeEntry{}
					if hasPath {
						entries = append(entries, &gh.TreeEntry{Path: new("schema"), Type: new("tree"), SHA: &schemaTreeSHA})
					}
					require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Entries: entries}))
				})
			}
			registerRootTree("GET /repos/octocat/hello-world/git/trees/merge-base-sha", tt.mergeHasPath, tt.mergeTreeSHA)
			registerRootTree("GET /repos/octocat/hello-world/git/trees/base-tip-sha", tt.tipHasPath, tt.tipTreeSHA)

			ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
			changed, baseTipSHA, err := ic.SchemaPathsChangedSinceMergeBase(t.Context(), "octocat/hello-world", "main", "head-sha", []string{"schema"})

			require.NoError(t, err)
			assert.Equal(t, tt.wantChanged, changed)
			assert.Equal(t, "base-tip-sha", baseTipSHA)
		})
	}
}

// A read that fails on a later path still returns the path already found
// changed, so the caller can answer "stale" rather than "could not check" for a
// branch already proven to be missing a newer base change.
func TestSchemaPathsChangedSinceMergeBaseKeepsChangedPathsBesideReadError(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	registerBaseTipRef(t, mux, "base-tip-sha")
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/base-tip-sha...head-sha", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{
			MergeBaseCommit: &gh.RepositoryCommit{SHA: new("merge-base-sha")},
		}))
	})
	for commit, schemaTree := range map[string]string{"merge-base-sha": "schema-tree-old", "base-tip-sha": "schema-tree-new"} {
		mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/"+commit, func(w http.ResponseWriter, _ *http.Request) {
			require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Entries: []*gh.TreeEntry{
				{Path: new("schema"), Type: new("tree"), SHA: new(schemaTree)},
				{Path: new("links"), Type: new("tree"), SHA: new("links-tree")},
			}}))
		})
	}
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/links-tree", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	changed, baseTipSHA, err := ic.SchemaPathsChangedSinceMergeBase(t.Context(), "octocat/hello-world", "main", "head-sha", []string{"schema", "links/staging"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolve schema path links/staging")
	assert.Equal(t, []string{"schema"}, changed)
	assert.Equal(t, "base-tip-sha", baseTipSHA)
}

func TestSchemaPathsChangedSinceMergeBaseFailsWithoutMergeBase(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	registerBaseTipRef(t, mux, "base-tip-sha")
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/base-tip-sha...head-sha", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, _, err := ic.SchemaPathsChangedSinceMergeBase(t.Context(), "octocat/hello-world", "main", "head-sha", []string{"schema"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "returned no merge base")
}

// The base branch is compared by resolving its ref to the current tip; if the
// ref cannot be resolved the comparison must fail (the caller rejects the
// apply) rather than report the schema paths as unchanged.
func TestSchemaPathsChangedSinceMergeBaseFailsWhenBaseRefUnresolvable(t *testing.T) {
	client, _ := setupRateLimitedTestGitHubServer(t)

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, _, err := ic.SchemaPathsChangedSinceMergeBase(t.Context(), "octocat/hello-world", "main", "head-sha", []string{"schema"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "get ref heads/main")
}

func TestSchemaPathsChangedSinceMergeBaseFailsWhenBaseRefHasNoCommit(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.Reference{Ref: new("refs/heads/main")}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, _, err := ic.SchemaPathsChangedSinceMergeBase(t.Context(), "octocat/hello-world", "main", "head-sha", []string{"schema"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolved to no commit")
}

func TestSchemaPathsChangedSinceMergeBaseDetectsSymlinkRetarget(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	var mergeRootRequests, baseRootRequests atomic.Int32
	var mergeSchemaRequests, baseSchemaRequests atomic.Int32
	registerBaseTipRef(t, mux, "base-tip-sha")
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/base-tip-sha...head-sha", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{
			MergeBaseCommit: &gh.RepositoryCommit{SHA: new("merge-base-sha")},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/merge-base-sha", func(w http.ResponseWriter, _ *http.Request) {
		mergeRootRequests.Add(1)
		require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Entries: []*gh.TreeEntry{
			{Path: new("schema"), Type: new("tree"), SHA: new("merge-schema-tree")},
		}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/base-tip-sha", func(w http.ResponseWriter, _ *http.Request) {
		baseRootRequests.Add(1)
		require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Entries: []*gh.TreeEntry{
			{Path: new("schema"), Type: new("tree"), SHA: new("base-schema-tree")},
		}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/merge-schema-tree", func(w http.ResponseWriter, _ *http.Request) {
		mergeSchemaRequests.Add(1)
		require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Entries: []*gh.TreeEntry{
			{Path: new("base"), Type: new("tree"), SHA: new("resolved-schema-tree")},
			{Path: new("production"), Type: new("blob"), Mode: new(gitSymlinkMode), SHA: new("old-link")},
		}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/base-schema-tree", func(w http.ResponseWriter, _ *http.Request) {
		baseSchemaRequests.Add(1)
		require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Entries: []*gh.TreeEntry{
			{Path: new("base"), Type: new("tree"), SHA: new("resolved-schema-tree")},
			{Path: new("production"), Type: new("blob"), Mode: new(gitSymlinkMode), SHA: new("new-link")},
		}}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	changed, baseTipSHA, err := ic.SchemaPathsChangedSinceMergeBase(t.Context(), "octocat/hello-world", "main", "head-sha", []string{"schema/base", "schema/production"})

	require.NoError(t, err)
	assert.Equal(t, []string{"schema/production"}, changed, "only the retargeted symlink changed")
	assert.Equal(t, "base-tip-sha", baseTipSHA)
	assert.Equal(t, int32(1), mergeRootRequests.Load())
	assert.Equal(t, int32(1), baseRootRequests.Load())
	assert.Equal(t, int32(1), mergeSchemaRequests.Load())
	assert.Equal(t, int32(1), baseSchemaRequests.Load())
}

func TestSchemaPathsChangedSinceMergeBaseUsesRootTreeSHA(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	registerBaseTipRef(t, mux, "base-tip-sha")
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/base-tip-sha...head-sha", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{
			MergeBaseCommit: &gh.RepositoryCommit{SHA: new("merge-base-sha")},
		}))
	})
	for _, ref := range []string{"merge-base-sha", "base-tip-sha"} {
		mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/"+ref, func(w http.ResponseWriter, _ *http.Request) {
			require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{SHA: new("same-root-tree")}))
		})
	}

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	changed, _, err := ic.SchemaPathsChangedSinceMergeBase(t.Context(), "octocat/hello-world", "main", "head-sha", []string{"."})

	require.NoError(t, err)
	assert.Empty(t, changed, "different commit SHAs with the same root tree are unchanged")
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

// serveMergeQueueEntries serves GitHub's merge queue GraphQL query: the
// entry head commits of each base branch's queue, one page per call, so a
// test can check that every page is read. A branch absent from the map has no
// merge queue. It returns the branches each request asked for, in order.
func serveMergeQueueEntries(t *testing.T, mux *http.ServeMux, pagesByBranch map[string][][]string) *[]string {
	t.Helper()
	var asked []string
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Contains(t, body.Query, "mergeQueue(branch: $branch)")
		assert.Equal(t, "octocat", body.Variables["owner"])
		assert.Equal(t, "hello-world", body.Variables["name"])
		branch, _ := body.Variables["branch"].(string)
		asked = append(asked, branch)
		pages, ok := pagesByBranch[branch]
		if !ok {
			_, err := w.Write([]byte(`{"data":{"repository":{"mergeQueue":null}}}`))
			require.NoError(t, err)
			return
		}
		page := 0
		if after, _ := body.Variables["after"].(string); after != "" {
			page, _ = strconv.Atoi(after)
		}
		nodes := make([]map[string]any, 0, len(pages[page]))
		for _, oid := range pages[page] {
			nodes = append(nodes, map[string]any{"headCommit": map[string]string{"oid": oid}})
		}
		hasNext := page+1 < len(pages)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"repository": map[string]any{"mergeQueue": map[string]any{
			"entries": map[string]any{"nodes": nodes, "pageInfo": map[string]any{"hasNextPage": hasNext, "endCursor": strconv.Itoa(page + 1)}},
		}}}}))
	})
	return &asked
}

// The queue branches are listed in one request for every base branch, and
// each group's base branch and PR come from its branch name, including a base
// branch whose own name contains slashes. Each base branch's merge queue is
// read once, every page of it, to confirm the groups.
func TestListMergeQueueHeadsReadsEveryMergeGroup(t *testing.T) {
	t.Parallel()

	client, mux := setupRateLimitedTestGitHubServer(t)
	requests := 0
	mux.HandleFunc("GET /repos/octocat/hello-world/git/matching-refs/heads/gh-readonly-queue/", func(w http.ResponseWriter, _ *http.Request) {
		requests++
		require.NoError(t, json.NewEncoder(w).Encode([]*gh.Reference{
			{Ref: new("refs/heads/gh-readonly-queue/main/pr-412-0a1b2c3d"), Object: &gh.GitObject{SHA: new("group-412")}},
			{Ref: new("refs/heads/gh-readonly-queue/main/pr-415-9f8e7d6c"), Object: &gh.GitObject{SHA: new("group-415")}},
			{Ref: new("refs/heads/gh-readonly-queue/release/2026-11/pr-418-4e5f6a7b"), Object: &gh.GitObject{SHA: new("group-418")}},
		}))
	})
	asked := serveMergeQueueEntries(t, mux, map[string][][]string{
		"main":            {{"group-412"}, {"group-415"}},
		"release/2026-11": {{"group-418"}},
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	heads, err := ic.ListMergeQueueHeads(t.Context(), "octocat/hello-world")

	require.NoError(t, err)
	assert.Equal(t, 1, requests)
	assert.Equal(t, []string{"main", "main", "release/2026-11"}, *asked, "each base branch's queue is read once, every page of it")
	assert.Equal(t, []MergeQueueHead{
		{Ref: "gh-readonly-queue/main/pr-412-0a1b2c3d", BaseRef: "main", PR: 412, HeadSHA: "group-412"},
		{Ref: "gh-readonly-queue/main/pr-415-9f8e7d6c", BaseRef: "main", PR: 415, HeadSHA: "group-415"},
		{Ref: "gh-readonly-queue/release/2026-11/pr-418-4e5f6a7b", BaseRef: "release/2026-11", PR: 418, HeadSHA: "group-418"},
	}, heads)
}

// Anyone who can push a branch can name it gh-readonly-queue/..., and a
// passing check on such a commit would pass whatever else points at it. So a
// branch counts as a merge group only when GitHub's merge queue for its base
// branch has an entry headed at the branch's commit. A branch the queue does
// not list, or whose name does not say its base branch, is left out.
func TestListMergeQueueHeadsLeavesOutBranchesTheQueueDoesNotList(t *testing.T) {
	t.Parallel()

	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/git/matching-refs/heads/gh-readonly-queue/", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]*gh.Reference{
			{Ref: new("refs/heads/gh-readonly-queue/main/pr-412-0a1b2c3d"), Object: &gh.GitObject{SHA: new("group-412")}},
			{Ref: new("refs/heads/gh-readonly-queue/main/pr-77-feedface"), Object: &gh.GitObject{SHA: new("pushed-by-hand")}},
			{Ref: new("refs/heads/gh-readonly-queue/no-pr-segment"), Object: &gh.GitObject{SHA: new("unparsed")}},
		}))
	})
	asked := serveMergeQueueEntries(t, mux, map[string][][]string{"main": {{"group-412"}}})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	heads, err := ic.ListMergeQueueHeads(t.Context(), "octocat/hello-world")

	require.NoError(t, err)
	assert.Equal(t, []MergeQueueHead{
		{Ref: "gh-readonly-queue/main/pr-412-0a1b2c3d", BaseRef: "main", PR: 412, HeadSHA: "group-412"},
	}, heads)
	assert.Equal(t, []string{"main"}, *asked, "a branch with no base branch in its name asks no queue")
}

// A base branch with queue branches but no merge queue GitHub will return, as
// when the App cannot read merge queues, is an error rather than an empty
// queue, so the caller reports the queue as unread instead of clean.
func TestListMergeQueueHeadsFailsWhenTheQueueCannotBeRead(t *testing.T) {
	t.Parallel()

	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/git/matching-refs/heads/gh-readonly-queue/", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]*gh.Reference{
			{Ref: new("refs/heads/gh-readonly-queue/main/pr-412-0a1b2c3d"), Object: &gh.GitObject{SHA: new("group-412")}},
		}))
	})
	serveMergeQueueEntries(t, mux, map[string][][]string{})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	heads, err := ic.ListMergeQueueHeads(t.Context(), "octocat/hello-world")

	require.Error(t, err)
	assert.Nil(t, heads)
	assert.Contains(t, err.Error(), "GitHub returned no merge queue for branch main")
}

// The lookup finds an open PR whose head is the commit, ignoring PRs that only
// contain it or are closed, and reads every page before saying there is none.
func TestOpenPullRequestHeadedAtMatchesOnlyAnOpenPRsHead(t *testing.T) {
	t.Parallel()

	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/group-412/pulls", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") == "" {
			w.Header().Set("Link", `<https://api.github.com/repos/octocat/hello-world/commits/group-412/pulls?page=2>; rel="next"`)
			require.NoError(t, json.NewEncoder(w).Encode([]*gh.PullRequest{
				{Number: new(7), State: new("open"), Head: &gh.PullRequestBranch{SHA: new("later-commit")}},
				{Number: new(8), State: new("closed"), Head: &gh.PullRequestBranch{SHA: new("group-412")}},
			}))
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode([]*gh.PullRequest{
			{Number: new(9), State: new("open"), Head: &gh.PullRequestBranch{SHA: new("group-412")}},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/group-418/pulls", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]*gh.PullRequest{}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	pr, err := ic.OpenPullRequestHeadedAt(t.Context(), "octocat/hello-world", "group-412")
	require.NoError(t, err)
	assert.Equal(t, 9, pr)

	pr, err = ic.OpenPullRequestHeadedAt(t.Context(), "octocat/hello-world", "group-418")
	require.NoError(t, err)
	assert.Zero(t, pr)
}

// A merge group branch whose name does not follow the pr-<n>-<sha> shape
// leaves the PR unknown, and the base branch too when no PR segment is found.
func TestMergeQueueHeadFromUnexpectedBranchName(t *testing.T) {
	t.Parallel()

	assert.Equal(t, MergeQueueHead{Ref: "gh-readonly-queue/main/group-7", HeadSHA: "sha7"},
		mergeQueueHeadFrom("heads/gh-readonly-queue/main/group-7", "sha7"))
	assert.Equal(t, MergeQueueHead{Ref: "gh-readonly-queue/main/pr-x-1a2b", BaseRef: "main", HeadSHA: "sha8"},
		mergeQueueHeadFrom("heads/gh-readonly-queue/main/pr-x-1a2b", "sha8"))
}

// A failed listing is an error, never an empty queue, so a caller cannot
// mistake a GitHub failure for a queue with nothing blocked in it.
func TestListMergeQueueHeadsReturnsListingFailure(t *testing.T) {
	t.Parallel()

	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/git/matching-refs/heads/gh-readonly-queue/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		require.NoError(t, json.NewEncoder(w).Encode(gh.ErrorResponse{Message: "Resource not accessible by integration"}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	heads, err := ic.ListMergeQueueHeads(t.Context(), "octocat/hello-world")

	require.Error(t, err)
	assert.Nil(t, heads)
	assert.Contains(t, err.Error(), "list merge queue refs for octocat/hello-world")
}
