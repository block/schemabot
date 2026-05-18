package github

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	gh "github.com/google/go-github/v68/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestEnvironmentList_SimpleForm(t *testing.T) {
	yamlData := `
database: testdb
type: mysql
environments:
  - staging
  - production
`
	var config SchemabotConfig
	require.NoError(t, yaml.Unmarshal([]byte(yamlData), &config))
	assert.Equal(t, []string{"staging", "production"}, config.GetEnvironments())
}

func TestEnvironmentList_MapForm(t *testing.T) {
	yamlData := `
database: testdb
type: mysql
environments:
  staging:
    target: cluster-staging-001
  production:
    target: cluster-production-001
`
	var config SchemabotConfig
	require.NoError(t, yaml.Unmarshal([]byte(yamlData), &config))
	// Map form preserves YAML declaration order
	assert.Equal(t, []string{"staging", "production"}, config.GetEnvironments())
}

func TestEnvironmentList_MapFormEmptyTarget(t *testing.T) {
	yamlData := `
database: testdb
type: mysql
environments:
  staging:
    target: cluster-01
  production: {}
`
	var config SchemabotConfig
	require.NoError(t, yaml.Unmarshal([]byte(yamlData), &config))
	assert.Equal(t, []string{"staging", "production"}, config.GetEnvironments())
	assert.Equal(t, "cluster-01", config.GetTarget("staging"))
	assert.Equal(t, "testdb", config.GetTarget("production"))
}

func TestGetTarget_ExplicitTarget(t *testing.T) {
	config := SchemabotConfig{
		Database: "mydb",
		Environments: EnvironmentList{
			{Name: "staging", Target: "cluster-001"},
			{Name: "production", Target: "cluster-002"},
		},
	}
	assert.Equal(t, "cluster-001", config.GetTarget("staging"))
	assert.Equal(t, "cluster-002", config.GetTarget("production"))
}

func TestGetTarget_FallsBackToDatabase(t *testing.T) {
	config := SchemabotConfig{
		Database: "mydb",
		Environments: EnvironmentList{
			{Name: "staging"},
			{Name: "production"},
		},
	}
	assert.Equal(t, "mydb", config.GetTarget("staging"))
	assert.Equal(t, "mydb", config.GetTarget("production"))
	assert.Equal(t, "mydb", config.GetTarget("unknown"))
}

func TestGetTarget_EmptyEnvironments(t *testing.T) {
	config := SchemabotConfig{Database: "mydb"}
	assert.Equal(t, "mydb", config.GetTarget("staging"))
}

func TestGetEnvironments_Default(t *testing.T) {
	config := SchemabotConfig{Database: "mydb"}
	assert.Equal(t, []string{"staging"}, config.GetEnvironments())
}

func TestHasEnvironment_SimpleForm(t *testing.T) {
	config := SchemabotConfig{
		Database: "mydb",
		Environments: EnvironmentList{
			{Name: "staging"},
			{Name: "production"},
		},
	}
	assert.True(t, config.HasEnvironment("staging"))
	assert.True(t, config.HasEnvironment("production"))
	assert.False(t, config.HasEnvironment("unknown"))
}

func TestHasEnvironment_MapForm(t *testing.T) {
	yamlData := `
database: testdb
type: mysql
environments:
  staging:
    target: cluster-001
  production:
    target: cluster-002
`
	var config SchemabotConfig
	require.NoError(t, yaml.Unmarshal([]byte(yamlData), &config))
	assert.True(t, config.HasEnvironment("staging"))
	assert.True(t, config.HasEnvironment("production"))
	assert.False(t, config.HasEnvironment("dev"))
}

func TestFindAllConfigsForPRClassifiesGitHubUnavailable(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err := ic.FindAllConfigsForPR(t.Context(), "octocat/hello-world", 1)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrGitHubUnavailable)
}

func TestFindAllConfigsForPRDoesNotClassifyRateLimitAsGitHubUnavailable(t *testing.T) {
	client, mux := setupConfigTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.WriteHeader(http.StatusForbidden)
		_, err := w.Write([]byte(`{"message":"API rate limit exceeded"}`))
		require.NoError(t, err)
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err := ic.FindAllConfigsForPR(t.Context(), "octocat/hello-world", 1)
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrGitHubUnavailable))
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
