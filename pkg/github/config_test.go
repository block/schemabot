package github

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
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

func TestHasSchemaInputFiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		files []PRFile
		want  bool
	}{
		{
			name:  "schema SQL file",
			files: []PRFile{{Filename: "schema/users.sql", Status: "modified"}},
			want:  true,
		},
		{
			name:  "VSchema file",
			files: []PRFile{{Filename: "schema/main/vschema.json", Status: "modified"}},
			want:  true,
		},
		{
			name:  "SchemaBot config file",
			files: []PRFile{{Filename: "schema/schemabot.yaml", Status: "modified"}},
			want:  true,
		},
		{
			name:  "renamed schema file",
			files: []PRFile{{Filename: "docs/users.md", PreviousFilename: "schema/users.sql", Status: "renamed"}},
			want:  true,
		},
		{
			name:  "application file",
			files: []PRFile{{Filename: "app/service.go", Status: "modified"}},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, HasSchemaInputFiles(tt.files))
		})
	}
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

// TestFindConfigsForPRFilesProbesEachDirectoryOnce exercises discovery for a
// PR that touches many schema files under shared directory trees — the shape
// of an onboarding PR that adds a declarative schema root while deleting a
// legacy SQL tree. Discovery must fetch each candidate directory's
// schemabot.yaml at most once, so the GitHub call volume is bounded by the
// number of unique directories rather than changed files × directory depth,
// and large PRs complete within the webhook processing deadline.
func TestFindConfigsForPRFilesProbesEachDirectoryOnce(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerPullRequest(t, mux, "abc123")

	var files []*gh.CommitFile
	for i := range 40 {
		files = append(files, &gh.CommitFile{
			Filename: new(fmt.Sprintf("apps/widgets/service/resources/legacy-sql/v%04d__change.sql", i)),
			Status:   new("removed"),
		})
	}
	for i := range 10 {
		files = append(files, &gh.CommitFile{
			Filename: new(fmt.Sprintf("apps/widgets/service/resources/schema/widgets/table_%d.sql", i)),
			Status:   new("added"),
		})
	}
	registerPullRequestFiles(t, mux, files)

	var mu sync.Mutex
	probesByPath := make(map[string]int)
	countProbe := func(r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		probesByPath[r.URL.Path]++
	}

	mux.HandleFunc("GET /repos/octocat/hello-world/contents/apps/widgets/service/resources/schema/schemabot.yaml", func(w http.ResponseWriter, r *http.Request) {
		countProbe(r)
		require.NoError(t, json.NewEncoder(w).Encode(gh.RepositoryContent{
			Type:     new("file"),
			Encoding: new("base64"),
			Content:  new(base64.StdEncoding.EncodeToString([]byte("database: widgets\ntype: mysql\n"))),
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/", func(w http.ResponseWriter, r *http.Request) {
		countProbe(r)
		w.WriteHeader(http.StatusNotFound)
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	configs, err := ic.FindAllConfigsForPR(t.Context(), "octocat/hello-world", 1)

	require.NoError(t, err)
	require.Len(t, configs, 1)
	assert.Equal(t, "widgets", configs[0].Config.Database)
	assert.Equal(t, "apps/widgets/service/resources/schema", configs[0].SchemaDir)

	// Every candidate directory between the changed files and the repo root,
	// deduplicated: the two file trees share ancestors, so the probe count
	// must not scale with the number of changed files.
	mu.Lock()
	defer mu.Unlock()
	for probePath, count := range probesByPath {
		assert.Equalf(t, 1, count, "directory probed more than once: %s", probePath)
	}
	assert.Len(t, probesByPath, 8)
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

func TestCreateSchemaRequestFromPRUsesEnvironmentSymlinkSchemaRoot(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerPullRequest(t, mux, "abc123")
	registerPullRequestFiles(t, mux, []*gh.CommitFile{{
		Filename: new("services/orders/schema/schemabot.yaml"),
		Status:   new("modified"),
	}, {
		Filename: new("services/orders/schema/base/orders_001/orders.sql"),
		Status:   new("added"),
	}})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/services/orders/schema/schemabot.yaml", "database: orders\ntype: mysql\n")
	registerSymlinkContent(t, mux, "/repos/octocat/hello-world/contents/services/orders/schema/production", "services/orders/schema/production", "base")
	registerDirectoryContent(t, mux, "/repos/octocat/hello-world/contents/services/orders/schema/base", []*gh.RepositoryContent{
		{Type: new("dir"), Name: new("orders_001"), Path: new("services/orders/schema/base/orders_001")},
	})
	registerDirectoryContent(t, mux, "/repos/octocat/hello-world/contents/services/orders/schema/base/orders_001", []*gh.RepositoryContent{
		{Type: new("file"), Name: new("orders.sql"), Path: new("services/orders/schema/base/orders_001/orders.sql")},
	})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/services/orders/schema/base/orders_001/orders.sql", "CREATE TABLE orders (id bigint primary key);\n")

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	result, err := ic.CreateSchemaRequestFromPR(t.Context(), "octocat/hello-world", 1, "production", "", nil)

	require.NoError(t, err)
	assert.Equal(t, "orders", result.Database)
	assert.Equal(t, "mysql", result.Type)
	assert.Equal(t, "services/orders/schema/base", result.SchemaPath)
	require.Contains(t, result.SchemaFiles, "orders_001")
	assert.Equal(t, "CREATE TABLE orders (id bigint primary key);\n", result.SchemaFiles["orders_001"].Files["orders.sql"])
}

func TestCreateSchemaRequestFromPRUsesRepoRootEnvironmentSymlinkSchemaRoot(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerPullRequest(t, mux, "abc123")
	registerPullRequestFiles(t, mux, []*gh.CommitFile{{
		Filename: new("schemabot.yaml"),
		Status:   new("modified"),
	}, {
		Filename: new("base/orders_001/orders.sql"),
		Status:   new("added"),
	}})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/schemabot.yaml", "database: orders\ntype: mysql\n")
	registerSymlinkContent(t, mux, "/repos/octocat/hello-world/contents/production", "production", "base")
	registerDirectoryContent(t, mux, "/repos/octocat/hello-world/contents/base", []*gh.RepositoryContent{
		{Type: new("dir"), Name: new("orders_001"), Path: new("base/orders_001")},
	})
	registerDirectoryContent(t, mux, "/repos/octocat/hello-world/contents/base/orders_001", []*gh.RepositoryContent{
		{Type: new("file"), Name: new("orders.sql"), Path: new("base/orders_001/orders.sql")},
	})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/base/orders_001/orders.sql", "CREATE TABLE orders (id bigint primary key);\n")

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	result, err := ic.CreateSchemaRequestFromPR(t.Context(), "octocat/hello-world", 1, "production", "", nil)

	require.NoError(t, err)
	assert.Equal(t, "orders", result.Database)
	assert.Equal(t, "base", result.SchemaPath)
	require.Contains(t, result.SchemaFiles, "orders_001")
	assert.Equal(t, "CREATE TABLE orders (id bigint primary key);\n", result.SchemaFiles["orders_001"].Files["orders.sql"])
}

func TestResolveSchemaRootForEnvironmentRejectsPathTraversal(t *testing.T) {
	ic := &InstallationClient{}
	for _, environment := range []string{"../other", "prod/blue", ".", "..", `prod\blue`} {
		t.Run(environment, func(t *testing.T) {
			_, err := ic.resolveSchemaRootForEnvironment(t.Context(), "octocat/hello-world", "abc123", "services/orders/schema", environment)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "must be a single path segment")
		})
	}
}

func TestCreateSchemaRequestFromPRFallsBackWhenEnvironmentSchemaRootMissing(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerPullRequest(t, mux, "abc123")
	registerPullRequestFiles(t, mux, []*gh.CommitFile{{
		Filename: new("apps/widgets/schema/main/widgets.sql"),
		Status:   new("modified"),
	}})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/schemabot.yaml", "database: widgets\ntype: vitess\n")
	registerDirectoryContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema", []*gh.RepositoryContent{
		{Type: new("dir"), Name: new("main"), Path: new("apps/widgets/schema/main")},
	})
	registerDirectoryContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/main", []*gh.RepositoryContent{
		{Type: new("file"), Name: new("widgets.sql"), Path: new("apps/widgets/schema/main/widgets.sql")},
	})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/main/widgets.sql", "CREATE TABLE widgets (id bigint primary key);\n")

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	result, err := ic.CreateSchemaRequestFromPR(t.Context(), "octocat/hello-world", 1, "production", "", nil)

	require.NoError(t, err)
	assert.Equal(t, "apps/widgets/schema", result.SchemaPath)
	require.Contains(t, result.SchemaFiles, "main")
	assert.Equal(t, "CREATE TABLE widgets (id bigint primary key);\n", result.SchemaFiles["main"].Files["widgets.sql"])
}

func TestCreateSchemaRequestFromPRValidatesEnvironmentBeforeResolvingEnvironmentSchemaRoot(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerPullRequest(t, mux, "abc123")
	registerPullRequestFiles(t, mux, []*gh.CommitFile{{
		Filename: new("services/orders/schema/schemabot.yaml"),
		Status:   new("modified"),
	}})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/services/orders/schema/schemabot.yaml", "database: orders\ntype: mysql\n")
	environmentRootRequests := 0
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/services/orders/schema/production", func(w http.ResponseWriter, _ *http.Request) {
		environmentRootRequests++
		require.NoError(t, json.NewEncoder(w).Encode([]*gh.RepositoryContent{
			{Type: new("dir"), Name: new("orders_001"), Path: new("services/orders/schema/production/orders_001")},
		}))
	})

	validationErr := errors.New("environment rejected")
	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err := ic.CreateSchemaRequestFromPR(t.Context(), "octocat/hello-world", 1, "production", "", func(database, environment string) error {
		assert.Equal(t, "orders", database)
		assert.Equal(t, "production", environment)
		return validationErr
	})

	require.ErrorIs(t, err, validationErr)
	assert.Zero(t, environmentRootRequests)
}

func TestCreateSchemaRequestFromPRRejectsEnvironmentSymlinkOutsideSchemaDirectory(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerPullRequest(t, mux, "abc123")
	registerPullRequestFiles(t, mux, []*gh.CommitFile{{
		Filename: new("apps/widgets/schema/schemabot.yaml"),
		Status:   new("modified"),
	}})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/schemabot.yaml", "database: widgets\ntype: vitess\n")
	registerSymlinkContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/production", "apps/widgets/schema/production", "../other")

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err := ic.CreateSchemaRequestFromPR(t.Context(), "octocat/hello-world", 1, "production", "", nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "points outside schema directory")
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

func registerSymlinkContent(t *testing.T, mux *http.ServeMux, endpointPath, linkPath, target string) {
	t.Helper()

	mux.HandleFunc("GET "+endpointPath, func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.RepositoryContent{
			Type:   new("symlink"),
			Path:   new(linkPath),
			Target: new(target),
		}))
	})
}

func registerDirectoryContent(t *testing.T, mux *http.ServeMux, endpointPath string, contents []*gh.RepositoryContent) {
	t.Helper()

	mux.HandleFunc("GET "+endpointPath, func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(contents))
	})
}

// registerTruncatedRepoWithSchemaSubtree registers git-tree handlers for a
// repository whose whole-repo recursive listing is truncated but whose
// subtrees resolve: shallow fetches of the root and intermediate levels return
// directory entries, and the apps/widgets/schema subtree lists subtreeEntries
// (paths relative to that directory).
func registerTruncatedRepoWithSchemaSubtree(t *testing.T, mux *http.ServeMux, subtreeEntries []map[string]any) {
	t.Helper()

	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("recursive") == "1" {
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"truncated": true,
				"tree":      []any{},
			}))
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": []any{
			map[string]any{"path": "apps", "type": "tree", "sha": "tree-apps"},
			map[string]any{"path": "README.md", "type": "blob", "sha": "blob-readme"},
		}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/tree-apps", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": []any{
			map[string]any{"path": "widgets", "type": "tree", "sha": "tree-widgets"},
		}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/tree-widgets", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": []any{
			map[string]any{"path": "schema", "type": "tree", "sha": "tree-schema"},
		}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/tree-schema", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": subtreeEntries}))
	})
}

func registerGitBlob(t *testing.T, mux *http.ServeMux, sha, content string) {
	t.Helper()

	mux.HandleFunc("GET /repos/octocat/hello-world/git/blobs/"+sha, func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.Blob{
			Content:  new(base64.StdEncoding.EncodeToString([]byte(content))),
			Encoding: new("base64"),
		}))
	})
}

func widgetsConfigDirHints(t *testing.T, dirs ...string) func(repo string) ([]string, bool) {
	t.Helper()

	return func(repo string) ([]string, bool) {
		require.Equal(t, "octocat/hello-world", repo)
		return dirs, true
	}
}

// A repo-root schema path cannot be recovered through a subtree fetch when
// the repository tree is truncated — the root tree is the truncated listing
// itself — so schema-file loading keeps the truncation error instead of
// misreporting an empty schema directory.
func TestFetchSchemaFilesFromTreeTruncatedRepoRootFailsClosed(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerTruncatedRepoWithSchemaSubtree(t, mux, nil)

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))

	_, err := ic.fetchSchemaFilesFromTree(t.Context(), "octocat/hello-world", "abc123", ".")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrGitTreeTruncated)
}

func TestFindAllConfigsTruncatedTreeFallsBackToConfigDirHints(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerTruncatedRepoWithSchemaSubtree(t, mux, []map[string]any{
		{"path": "schemabot.yaml", "type": "blob", "sha": "blob-config", "mode": "100644"},
		{"path": "main/widgets.sql", "type": "blob", "sha": "blob-sql", "mode": "100644"},
	})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/schemabot.yaml", "database: widgets\ntype: mysql\n")

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ic.SetConfigDirHints(widgetsConfigDirHints(t, "apps/widgets/schema"))

	result, err := ic.FindAllConfigs(t.Context(), "octocat/hello-world", "abc123")

	require.NoError(t, err)
	require.Len(t, result.ValidConfigs, 1)
	assert.Equal(t, "widgets", result.ValidConfigs[0].Config.Database)
	assert.Equal(t, "apps/widgets/schema", result.ValidConfigs[0].SchemaDir)
	assert.Equal(t, "apps/widgets/schema/schemabot.yaml", result.ValidConfigs[0].Path)
	assert.Empty(t, result.InvalidConfigs)
}

func TestFindAllConfigsTruncatedTreeFindsConfigNestedBelowHintDir(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerTruncatedRepoWithSchemaSubtree(t, mux, []map[string]any{
		{"path": "payments/schemabot.yaml", "type": "blob", "sha": "blob-config", "mode": "100644"},
		{"path": "payments/transactions.sql", "type": "blob", "sha": "blob-sql", "mode": "100644"},
	})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/payments/schemabot.yaml", "database: payments\ntype: mysql\n")

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ic.SetConfigDirHints(widgetsConfigDirHints(t, "apps/widgets/schema"))

	result, err := ic.FindAllConfigs(t.Context(), "octocat/hello-world", "abc123")

	require.NoError(t, err)
	require.Len(t, result.ValidConfigs, 1)
	assert.Equal(t, "payments", result.ValidConfigs[0].Config.Database)
	assert.Equal(t, "apps/widgets/schema/payments", result.ValidConfigs[0].SchemaDir)
}

func TestFindAllConfigsTruncatedTreeSkipsMissingHintDir(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerTruncatedRepoWithSchemaSubtree(t, mux, []map[string]any{
		{"path": "schemabot.yaml", "type": "blob", "sha": "blob-config", "mode": "100644"},
	})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/schemabot.yaml", "database: widgets\ntype: mysql\n")

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ic.SetConfigDirHints(widgetsConfigDirHints(t, "apps/gone/schema", "apps/widgets/schema"))

	result, err := ic.FindAllConfigs(t.Context(), "octocat/hello-world", "abc123")

	require.NoError(t, err)
	require.Len(t, result.ValidConfigs, 1)
	assert.Equal(t, "widgets", result.ValidConfigs[0].Config.Database)
}

func TestFindAllConfigsTruncatedTreeFailsClosedWhenHintDirsHaveNoConfigs(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerTruncatedRepoWithSchemaSubtree(t, mux, []map[string]any{
		{"path": "main/widgets.sql", "type": "blob", "sha": "blob-sql", "mode": "100644"},
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ic.SetConfigDirHints(widgetsConfigDirHints(t, "apps/widgets/schema"))

	_, err := ic.FindAllConfigs(t.Context(), "octocat/hello-world", "abc123")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrGitTreeTruncated)
}

func TestFindAllConfigsTruncatedTreeFailsClosedWhenNoHintsForRepo(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerTruncatedRepoWithSchemaSubtree(t, mux, nil)

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ic.SetConfigDirHints(widgetsConfigDirHints(t))

	_, err := ic.FindAllConfigs(t.Context(), "octocat/hello-world", "abc123")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrGitTreeTruncated)
}

// A database that allows configs outside every probe-able directory (no
// allowed_dirs, a wildcard, or a repo-root entry) makes the hint list
// non-exhaustive: a partial probe could return a confidently wrong result, so
// discovery must keep the truncation error even when other hinted directories
// would have yielded configs.
func TestFindAllConfigsTruncatedTreeFailsClosedWhenHintsNotExhaustive(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerTruncatedRepoWithSchemaSubtree(t, mux, []map[string]any{
		{"path": "schemabot.yaml", "type": "blob", "sha": "blob-config", "mode": "100644"},
	})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/schemabot.yaml", "database: widgets\ntype: mysql\n")

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ic.SetConfigDirHints(func(repo string) ([]string, bool) {
		require.Equal(t, "octocat/hello-world", repo)
		return []string{"apps/widgets/schema"}, false
	})

	_, err := ic.FindAllConfigs(t.Context(), "octocat/hello-world", "abc123")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrGitTreeTruncated)
}

func TestFindAllConfigsTruncatedTreeReportsInvalidHintDirConfig(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerTruncatedRepoWithSchemaSubtree(t, mux, []map[string]any{
		{"path": "schemabot.yaml", "type": "blob", "sha": "blob-config", "mode": "100644"},
	})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/schemabot.yaml", "type: mysql\n")

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ic.SetConfigDirHints(widgetsConfigDirHints(t, "apps/widgets/schema"))

	result, err := ic.FindAllConfigs(t.Context(), "octocat/hello-world", "abc123")

	require.NoError(t, err)
	assert.Empty(t, result.ValidConfigs)
	require.Len(t, result.InvalidConfigs, 1)
	assert.Equal(t, "apps/widgets/schema/schemabot.yaml", result.InvalidConfigs[0].Path)
	assert.Contains(t, result.InvalidConfigs[0].Error, "database is required")
}

func TestFindConfigByDatabaseNameTruncatedTreeUsesConfigDirHints(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerPullRequest(t, mux, "abc123")
	registerPullRequestFiles(t, mux, []*gh.CommitFile{{
		Filename: new("docs/readme.md"),
		Status:   new("modified"),
	}})
	registerTruncatedRepoWithSchemaSubtree(t, mux, []map[string]any{
		{"path": "schemabot.yaml", "type": "blob", "sha": "blob-config", "mode": "100644"},
	})
	registerFileContent(t, mux, "/repos/octocat/hello-world/contents/apps/widgets/schema/schemabot.yaml", "database: widgets\ntype: mysql\n")

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ic.SetConfigDirHints(widgetsConfigDirHints(t, "apps/widgets/schema"))

	config, schemaDir, err := ic.FindConfigByDatabaseName(t.Context(), "octocat/hello-world", 1, "widgets")

	require.NoError(t, err)
	assert.Equal(t, "widgets", config.Database)
	assert.Equal(t, "apps/widgets/schema", schemaDir)
}

func TestFetchSchemaFilesFromTreeTruncatedRootUsesSchemaSubtree(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerTruncatedRepoWithSchemaSubtree(t, mux, []map[string]any{
		{"path": "schemabot.yaml", "type": "blob", "sha": "blob-config", "mode": "100644"},
		{"path": "main/widgets.sql", "type": "blob", "sha": "blob-sql", "mode": "100644"},
	})
	registerGitBlob(t, mux, "blob-sql", "CREATE TABLE `widgets` (`id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY);")

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	files, err := ic.fetchSchemaFilesFromTree(t.Context(), "octocat/hello-world", "abc123", "apps/widgets/schema")

	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.Equal(t, "apps/widgets/schema/main/widgets.sql", files[0].Path)
	assert.Equal(t, "widgets.sql", files[0].Name)
	assert.Contains(t, files[0].Content, "CREATE TABLE")
	assert.Contains(t, files[0].Content, "widgets")
}

func TestFetchSchemaFilesFromTreeTruncatedRootFailsClosedOnSubtreeSymlink(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerTruncatedRepoWithSchemaSubtree(t, mux, []map[string]any{
		{"path": "linked", "type": "blob", "sha": "blob-link", "mode": "120000"},
		{"path": "main/widgets.sql", "type": "blob", "sha": "blob-sql", "mode": "100644"},
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err := ic.fetchSchemaFilesFromTree(t.Context(), "octocat/hello-world", "abc123", "apps/widgets/schema")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "symlink")
}

func TestFetchSchemaFilesFromTreeTruncatedRootMissingSchemaDirReturnsNoFiles(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	registerTruncatedRepoWithSchemaSubtree(t, mux, nil)

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	files, err := ic.fetchSchemaFilesFromTree(t.Context(), "octocat/hello-world", "abc123", "apps/gone/schema")

	require.NoError(t, err)
	assert.Empty(t, files)
}

func TestFetchSchemaFilesFromTreeFailsClosedWhenSubtreeAlsoTruncated(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("recursive") == "1" {
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"truncated": true, "tree": []any{}}))
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": []any{
			map[string]any{"path": "apps", "type": "tree", "sha": "tree-huge"},
		}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/tree-huge", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("recursive") == "1" {
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"truncated": true, "tree": []any{}}))
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": []any{}}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err := ic.fetchSchemaFilesFromTree(t.Context(), "octocat/hello-world", "abc123", "apps")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrGitTreeTruncated)
}
