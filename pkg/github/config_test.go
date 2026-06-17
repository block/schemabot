package github

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestSchemabotConfigRejectsEnvironments(t *testing.T) {
	yamlData := `
database: testdb
type: mysql
environments:
  - staging
  - production
`
	var config SchemabotConfig
	decoder := yaml.NewDecoder(strings.NewReader(yamlData))
	decoder.KnownFields(true)
	err := decoder.Decode(&config)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field environments not found")
}

func TestFindAllConfigsForPRClassifiesGitHubUnavailable(t *testing.T) {
	setGitHubUnavailableReadRetryDelay(t, time.Millisecond)

	client, mux := setupConfigTestGitHubServer(t)
	requests := 0
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		requests++
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err := ic.FindAllConfigsForPR(t.Context(), "octocat/hello-world", 1)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrGitHubUnavailable)
	assert.Equal(t, githubUnavailableReadRetryMaxAttempts, requests)
}

func TestFindAllConfigsForPRRetriesUnavailablePullRequestRead(t *testing.T) {
	setGitHubUnavailableReadRetryDelay(t, time.Millisecond)

	client, mux := setupConfigTestGitHubServer(t)
	requests := 0
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		requests++
		if requests == 1 {
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{SHA: new("abc123")},
		}))
	})
	registerPullRequestFiles(t, mux, []*gh.CommitFile{{
		Filename: new("apps/widgets/schema/schemabot.yaml"),
		Status:   new("added"),
	}})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/schemabot.yaml", "database: widgets\ntype: mysql\n")

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	configs, err := ic.FindAllConfigsForPR(t.Context(), "octocat/hello-world", 1)

	require.NoError(t, err)
	require.Len(t, configs, 1)
	assert.Equal(t, "widgets", configs[0].Config.Database)
	assert.Equal(t, 2, requests)
}

func TestFindAllConfigsForPRRetriesUnavailablePRFilesRead(t *testing.T) {
	setGitHubUnavailableReadRetryDelay(t, time.Millisecond)

	client, mux := setupConfigTestGitHubServer(t)
	registerPullRequest(t, mux, "abc123")
	requests := 0
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		requests++
		if requests == 1 {
			http.Error(w, "bad gateway", http.StatusBadGateway)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode([]*gh.CommitFile{{
			Filename: new("apps/widgets/schema/schemabot.yaml"),
			Status:   new("added"),
		}}))
	})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/schemabot.yaml", "database: widgets\ntype: mysql\n")

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	configs, err := ic.FindAllConfigsForPR(t.Context(), "octocat/hello-world", 1)

	require.NoError(t, err)
	require.Len(t, configs, 1)
	assert.Equal(t, "widgets", configs[0].Config.Database)
	assert.Equal(t, 2, requests)
}

func TestFindAllConfigsForPRRetriesUnavailableConfigContentRead(t *testing.T) {
	setGitHubUnavailableReadRetryDelay(t, time.Millisecond)

	client, mux := setupConfigTestGitHubServer(t)
	registerPullRequest(t, mux, "abc123")
	registerPullRequestFiles(t, mux, []*gh.CommitFile{{
		Filename: new("apps/widgets/schema/schemabot.yaml"),
		Status:   new("added"),
	}})
	requests := 0
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/apps/widgets/schema/schemabot.yaml", func(w http.ResponseWriter, _ *http.Request) {
		requests++
		if requests == 1 {
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(gh.RepositoryContent{
			Type:     new("file"),
			Encoding: new("base64"),
			Content:  new(base64.StdEncoding.EncodeToString([]byte("database: widgets\ntype: mysql\n"))),
		}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	configs, err := ic.FindAllConfigsForPR(t.Context(), "octocat/hello-world", 1)

	require.NoError(t, err)
	require.Len(t, configs, 1)
	assert.Equal(t, "widgets", configs[0].Config.Database)
	assert.Equal(t, 2, requests)
}

func TestFindAllConfigsForPRDoesNotClassifyRateLimitAsGitHubUnavailable(t *testing.T) {
	setGitHubUnavailableReadRetryDelay(t, time.Millisecond)

	client, mux := setupConfigTestGitHubServer(t)
	requests := 0
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.WriteHeader(http.StatusForbidden)
		_, err := w.Write([]byte(`{"message":"API rate limit exceeded"}`))
		require.NoError(t, err)
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err := ic.FindAllConfigsForPR(t.Context(), "octocat/hello-world", 1)
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrGitHubUnavailable))
	assert.Equal(t, 1, requests)
}

func TestFindAllConfigsForPRDoesNotClassifyMissingConfigAsGitHubUnavailable(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{SHA: new("abc123")},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]*gh.CommitFile{{
			Filename: new("schema/users.sql"),
			Status:   new("modified"),
		}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	configs, err := ic.FindAllConfigsForPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	assert.Empty(t, configs)
}

func TestFindAllConfigsForPRDiscoversChangedConfigFile(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerPullRequest(t, mux, "abc123")
	registerPullRequestFiles(t, mux, []*gh.CommitFile{{
		Filename: new("apps/widgets/schema/schemabot.yaml"),
		Status:   new("added"),
	}})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/schemabot.yaml", "database: widgets\ntype: mysql\n")

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	configs, err := ic.FindAllConfigsForPR(t.Context(), "octocat/hello-world", 1)

	require.NoError(t, err)
	require.Len(t, configs, 1)
	assert.Equal(t, "widgets", configs[0].Config.Database)
	assert.Equal(t, "apps/widgets/schema", configs[0].SchemaDir)
}

func TestFindConfigForPRDiscoversChangedConfigFile(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerPullRequest(t, mux, "abc123")
	registerPullRequestFiles(t, mux, []*gh.CommitFile{{
		Filename: new("apps/widgets/schema/schemabot.yaml"),
		Status:   new("added"),
	}})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/schemabot.yaml", "database: widgets\ntype: mysql\n")

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	config, schemaDir, err := ic.FindConfigForPR(t.Context(), "octocat/hello-world", 1)

	require.NoError(t, err)
	assert.Equal(t, "widgets", config.Database)
	assert.Equal(t, "apps/widgets/schema", schemaDir)
}

func TestFindConfigByDatabaseNameUsesChangedConfigFileBeforeRepoScan(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerPullRequest(t, mux, "abc123")
	registerPullRequestFiles(t, mux, []*gh.CommitFile{{
		Filename: new("apps/widgets/schema/schemabot.yaml"),
		Status:   new("added"),
	}})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/schemabot.yaml", "database: widgets\ntype: mysql\n")

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	config, schemaDir, err := ic.FindConfigByDatabaseName(t.Context(), "octocat/hello-world", 1, "widgets")

	require.NoError(t, err)
	assert.Equal(t, "widgets", config.Database)
	assert.Equal(t, "apps/widgets/schema", schemaDir)
}

func TestFindConfigByDatabaseNameUsesChangedSchemaFileBeforeRepoScan(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerPullRequest(t, mux, "abc123")
	registerPullRequestFiles(t, mux, []*gh.CommitFile{{
		Filename: new("apps/widgets/schema/main/widgets.sql"),
		Status:   new("modified"),
	}})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/schemabot.yaml", "database: widgets\ntype: mysql\n")
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"truncated": true,
			"tree":      []any{},
		}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	config, schemaDir, err := ic.FindConfigByDatabaseName(t.Context(), "octocat/hello-world", 1, "widgets")

	require.NoError(t, err)
	assert.Equal(t, "widgets", config.Database)
	assert.Equal(t, "apps/widgets/schema", schemaDir)
}

func TestFindAllConfigsForPRSkipsRemovedConfigFile(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerPullRequest(t, mux, "abc123")
	registerPullRequestFiles(t, mux, []*gh.CommitFile{{
		Filename: new("apps/widgets/schema/schemabot.yaml"),
		Status:   new("removed"),
	}})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	configs, err := ic.FindAllConfigsForPR(t.Context(), "octocat/hello-world", 1)

	require.NoError(t, err)
	assert.Empty(t, configs)
}

func TestFindConfigForPRSkipsRemovedConfigFile(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerPullRequest(t, mux, "abc123")
	registerPullRequestFiles(t, mux, []*gh.CommitFile{{
		Filename: new("apps/widgets/schema/schemabot.yaml"),
		Status:   new("removed"),
	}})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, _, err := ic.FindConfigForPR(t.Context(), "octocat/hello-world", 1)

	require.ErrorIs(t, err, ErrNoConfig)
}

func TestFindAllConfigsFailsClosedOnTruncatedGitTree(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"truncated": true,
			"tree":      []any{},
		}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err := ic.FindAllConfigs(t.Context(), "octocat/hello-world", "abc123")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrGitTreeTruncated)
}

func TestFindAllConfigsForPRFailsClosedOnIncompletePRFileList(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerPullRequest(t, mux, "abc123")

	files := make([]*gh.CommitFile, maxGitHubPRFiles)
	filename := "docs/readme.md"
	status := "modified"
	for i := range files {
		files[i] = &gh.CommitFile{Filename: &filename, Status: &status}
	}
	registerPullRequestFiles(t, mux, files)

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err := ic.FindAllConfigsForPR(t.Context(), "octocat/hello-world", 1)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrPRFilesIncomplete)
	assert.False(t, errors.Is(err, ErrNoConfig))
}

func TestFetchSchemaFilesOptimizedWalksSchemaDirectoryOnly(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerDirectoryContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema", []*gh.RepositoryContent{
		{Type: new("file"), Name: new("schemabot.yaml"), Path: new("apps/widgets/schema/schemabot.yaml")},
		{Type: new("dir"), Name: new("main"), Path: new("apps/widgets/schema/main")},
		{Type: new("dir"), Name: new("archive"), Path: new("apps/widgets/schema/archive")},
	})
	registerDirectoryContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/main", []*gh.RepositoryContent{
		{Type: new("file"), Name: new("widgets.sql"), Path: new("apps/widgets/schema/main/widgets.sql")},
		{Type: new("file"), Name: new("vschema.json"), Path: new("apps/widgets/schema/main/vschema.json")},
		{Type: new("dir"), Name: new("nested"), Path: new("apps/widgets/schema/main/nested")},
	})
	registerDirectoryContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/archive", []*gh.RepositoryContent{
		{Type: new("file"), Name: new("old.sql"), Path: new("apps/widgets/schema/archive/old.sql")},
	})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/main/widgets.sql", "CREATE TABLE widgets (id bigint primary key);\n")
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/main/vschema.json", "{}\n")
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/archive/old.sql", "CREATE TABLE old_widgets (id bigint primary key);\n")

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	files, err := ic.FetchSchemaFilesOptimized(t.Context(), "octocat/hello-world", "abc123", "apps/widgets/schema", "vitess")

	require.NoError(t, err)
	require.Len(t, files, 3)
	assert.Equal(t, "apps/widgets/schema/archive/old.sql", files[0].Path)
	assert.Equal(t, "apps/widgets/schema/main/vschema.json", files[1].Path)
	assert.Equal(t, "apps/widgets/schema/main/widgets.sql", files[2].Path)
}

// TestFindConfigForPR_AuthFailureDoesNotFallThroughToNoConfig verifies that
// auth errors propagate instead of being swallowed as ErrNoConfig.
func TestFindConfigForPR_AuthFailureDoesNotFallThroughToNoConfig(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)

	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{SHA: new("abc123")},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]*gh.CommitFile{{
			Filename: new("schema/mydb/orders.sql"),
			Status:   new("modified"),
		}}))
	})
	// Config fetch returns 401 (simulates stale installation token)
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"message":"Bad credentials"}`))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, _, err := ic.FindConfigForPR(t.Context(), "octocat/hello-world", 1)

	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrNoConfig))
	assert.Contains(t, err.Error(), "401")
}

func setupConfigTestGitHubServer(t *testing.T) (*gh.Client, *http.ServeMux) {
	t.Helper()

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL

	return client, mux
}

func setGitHubUnavailableReadRetryDelay(t *testing.T, delay time.Duration) {
	t.Helper()

	originalDelay := githubUnavailableReadRetryDelay
	githubUnavailableReadRetryDelay = delay
	t.Cleanup(func() {
		githubUnavailableReadRetryDelay = originalDelay
	})
}

func registerPullRequest(t *testing.T, mux *http.ServeMux, headSHA string) {
	t.Helper()

	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{SHA: new(headSHA)},
		}))
	})
}

func registerPullRequestFiles(t *testing.T, mux *http.ServeMux, files []*gh.CommitFile) {
	t.Helper()

	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(files))
	})
}

func registerFileContent(t *testing.T, mux *http.ServeMux, endpointPath, content string) {
	t.Helper()

	mux.HandleFunc("GET "+endpointPath, func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.RepositoryContent{
			Type:     new("file"),
			Encoding: new("base64"),
			Content:  new(base64.StdEncoding.EncodeToString([]byte(content))),
		}))
	})
}

func registerDirectoryContent(t *testing.T, mux *http.ServeMux, endpointPath string, contents []*gh.RepositoryContent) {
	t.Helper()

	mux.HandleFunc("GET "+endpointPath, func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(contents))
	})
}
