package webhook

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
)

const (
	onboardingAnchorSHA = "0123456789abcdef0123456789abcdef01234567"
	onboardingBaseSHA   = "fedcba9876543210fedcba9876543210fedcba98"
	onboardingHeadSHA   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

func registerOnboardingDiscovery(t *testing.T, mux *http.ServeMux, baseConfig, headConfig string) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.Reference{
			Ref: new("refs/heads/main"), Object: &gh.GitObject{Type: new("commit"), SHA: new(onboardingBaseSHA)},
		}))
	})
	registerTree := func(ref, config string) {
		mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/"+ref, func(w http.ResponseWriter, _ *http.Request) {
			entries := []map[string]any{}
			if config != "" {
				entries = append(entries, map[string]any{"path": "schema/schemabot.yaml", "type": "blob", "sha": "config-blob"})
			}
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": entries}))
		})
	}
	registerTree(onboardingBaseSHA, baseConfig)
	registerTree(onboardingHeadSHA, headConfig)
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema/schemabot.yaml", func(w http.ResponseWriter, r *http.Request) {
		content := headConfig
		if r.URL.Query().Get("ref") == onboardingBaseSHA {
			content = baseConfig
		}
		if content == "" {
			http.NotFound(w, r)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"type": "file", "encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(content)),
		}))
	})
}

func TestEvaluateOnboardingGateNotApplicableAfterConfigLands(t *testing.T) {
	client, mux := setupGitHubServer(t)
	config := "database: orders\ntype: mysql\n"
	registerOnboardingDiscovery(t, mux, config, config)
	h := &Handler{logger: testLogger()}

	result, err := h.evaluateOnboardingGate(t.Context(), ghclient.NewInstallationClient(client, testLogger()), "octocat/hello-world", onboardingHeadSHA, "main")

	require.NoError(t, err)
	assert.Equal(t, checkConclusionSuccess, result.conclusion)
	assert.Contains(t, result.summary, "Not applicable")
}

func TestEvaluateOnboardingGateRejectsIntroducedConfigWithoutAnchor(t *testing.T) {
	client, mux := setupGitHubServer(t)
	registerOnboardingDiscovery(t, mux, "", "database: orders\ntype: mysql\n")
	h := &Handler{logger: testLogger()}

	result, err := h.evaluateOnboardingGate(t.Context(), ghclient.NewInstallationClient(client, testLogger()), "octocat/hello-world", onboardingHeadSHA, "main")

	require.NoError(t, err)
	assert.Equal(t, checkConclusionFailure, result.conclusion)
	assert.Contains(t, result.summary, "legacy_baseline is required")
}

func TestEvaluateOnboardingGateAcceptsCurrentLegacyBaseline(t *testing.T) {
	client, mux := setupGitHubServer(t)
	config := `database: orders
type: mysql
legacy_baseline:
  version: 1
  base_commit: fedcba9876543210fedcba9876543210fedcba98
  legacy_paths:
    - service/db/changes
`
	registerOnboardingDiscovery(t, mux, "", config)
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/"+onboardingBaseSHA+"..."+onboardingBaseSHA, func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{Status: new("identical")}))
	})
	h := &Handler{logger: testLogger()}

	result, err := h.evaluateOnboardingGate(t.Context(), ghclient.NewInstallationClient(client, testLogger()), "octocat/hello-world", onboardingHeadSHA, "main")

	require.NoError(t, err)
	assert.Equal(t, checkConclusionSuccess, result.conclusion)
	assert.Contains(t, result.summary, "legacy paths are unchanged")
	assert.Contains(t, result.summary, "Production convergence is enforced independently")
}

func TestEvaluateOnboardingGateListsLegacyPathChanges(t *testing.T) {
	client, mux := setupGitHubServer(t)
	config := `database: orders
type: mysql
legacy_baseline:
  version: 1
  base_commit: 0123456789abcdef0123456789abcdef01234567
  legacy_paths:
    - service/db/changes
`
	registerOnboardingDiscovery(t, mux, "", config)
	changedSHA := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/"+onboardingAnchorSHA+"..."+onboardingBaseSHA, func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{
			Status: new("ahead"), TotalCommits: new(1),
			Commits: []*gh.RepositoryCommit{{SHA: &changedSHA}},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/commits", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]*gh.RepositoryCommit{
			{SHA: &changedSHA, Commit: &gh.Commit{Message: new("add legacy index")}},
			{SHA: new("older"), Commit: &gh.Commit{Message: new("older change")}},
		}))
	})
	h := &Handler{logger: testLogger()}

	result, err := h.evaluateOnboardingGate(t.Context(), ghclient.NewInstallationClient(client, testLogger()), "octocat/hello-world", onboardingHeadSHA, "main")

	require.NoError(t, err)
	assert.Equal(t, checkConclusionFailure, result.conclusion)
	assert.Contains(t, result.summary, "`bbbbbbbbbbbb`")
	assert.Contains(t, result.summary, "`service/db/changes`")
	assert.Contains(t, result.summary, "add legacy index")
}
