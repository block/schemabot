package webhook

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
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
	registerOnboardingConfigs(t, mux, baseConfig, headConfig)
}

func registerOnboardingConfigs(t *testing.T, mux *http.ServeMux, baseConfig, headConfig string) {
	t.Helper()
	registerTree := func(ref, config string) {
		mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/"+ref, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("recursive") == "" {
				require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Entries: onboardingLegacyEntries(ref)}))
				return
			}
			assert.Equal(t, onboardingHeadSHA, ref, "config discovery must use the PR head")
			entries := []map[string]any{}
			if config != "" {
				entries = append(entries, map[string]any{"path": "schema/schemabot.yaml", "type": "blob", "sha": "config-blob"})
			}
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": entries}))
		})
	}
	registerTree(onboardingBaseSHA, baseConfig)
	registerTree(onboardingHeadSHA, headConfig)
	for _, ref := range []string{onboardingAnchorSHA, "legacy-service", "legacy-db"} {
		mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/"+ref, func(w http.ResponseWriter, _ *http.Request) {
			require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Entries: onboardingLegacyEntries(ref)}))
		})
	}
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema/schemabot.yaml", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, onboardingHeadSHA, r.URL.Query().Get("ref"), "config reads must use the PR head")
		content := headConfig
		if content == "" {
			http.NotFound(w, r)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"type": "file", "encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(content)),
		}))
	})
}

func onboardingLegacyEntries(ref string) []*gh.TreeEntry {
	switch ref {
	case onboardingAnchorSHA, onboardingBaseSHA:
		return []*gh.TreeEntry{
			{Path: new("service"), Type: new("tree"), SHA: new("legacy-service")},
			{Path: new("db"), Type: new("tree"), SHA: new("legacy-db")},
		}
	case "legacy-service":
		return []*gh.TreeEntry{{Path: new("db"), Type: new("tree"), SHA: new("legacy-db")}}
	case "legacy-db":
		return []*gh.TreeEntry{{Path: new("changes"), Type: new("tree"), SHA: new("legacy-changes")}}
	default:
		return nil
	}
}

// A corrected head config is sufficient even when its base version is invalid.
func TestEvaluateOnboardingGateUsesCorrectedHeadConfig(t *testing.T) {
	client, mux := setupGitHubServer(t)
	registerOnboardingDiscovery(t, mux, "database: orders\n", "database: orders\ntype: mysql\n")
	h := &Handler{logger: testLogger()}
	result, err := h.evaluateOnboardingGate(t.Context(), ghclient.NewInstallationClient(client, testLogger()), "octocat/hello-world", onboardingHeadSHA, "main")
	require.NoError(t, err)
	assert.Equal(t, checkConclusionSuccess, result.conclusion)
	assert.Equal(t, "Not applicable: no database configures legacy verification.", result.summary)
}

func TestEvaluateOnboardingGateSkipsConfigWithoutLegacyMetadata(t *testing.T) {
	client, mux := setupGitHubServer(t)
	registerOnboardingDiscovery(t, mux, "", "database: orders\ntype: mysql\n")
	h := &Handler{logger: testLogger()}

	result, err := h.evaluateOnboardingGate(t.Context(), ghclient.NewInstallationClient(client, testLogger()), "octocat/hello-world", onboardingHeadSHA, "main")

	require.NoError(t, err)
	assert.Equal(t, checkConclusionSuccess, result.conclusion)
	assert.Equal(t, "Not applicable: no database configures legacy verification.", result.summary)
}

func TestEvaluateOnboardingGateRejectsMalformedLegacyMetadata(t *testing.T) {
	client, mux := setupGitHubServer(t)
	registerOnboardingDiscovery(t, mux, "", "database: orders\ntype: mysql\nlegacy_baseline: {}\n")
	h := &Handler{logger: testLogger()}

	result, err := h.evaluateOnboardingGate(t.Context(), ghclient.NewInstallationClient(client, testLogger()), "octocat/hello-world", onboardingHeadSHA, "main")

	require.NoError(t, err)
	assert.Equal(t, checkConclusionFailure, result.conclusion)
	assert.Contains(t, result.summary, "legacy_baseline.version")
}

// A PR can introduce databases with and without legacy verification. Opting out
// for one must not skip validation of another database's supplied metadata.
func TestEvaluateOnboardingGateMixedOptIn(t *testing.T) {
	for _, legacyPath := range []string{"db/changes", "db/typo"} {
		t.Run(legacyPath, func(t *testing.T) {
			client, mux := setupGitHubServer(t)
			configs := map[string]string{
				"new/schemabot.yaml":    "database: new\ntype: mysql\n",
				"legacy/schemabot.yaml": "database: legacy\ntype: mysql\nlegacy_baseline:\n  version: 1\n  base_commit: " + onboardingAnchorSHA + "\n  legacy_paths:\n    - " + legacyPath + "\n",
			}
			mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/{sha}", func(w http.ResponseWriter, r *http.Request) {
				ref := r.PathValue("sha")
				entries := onboardingLegacyEntries(ref)
				if r.URL.Query().Get("recursive") != "" {
					entries = nil
					if ref == onboardingHeadSHA {
						for _, configPath := range []string{"new/schemabot.yaml", "legacy/schemabot.yaml"} {
							entries = append(entries, &gh.TreeEntry{Path: &configPath, Type: new("blob"), SHA: new("config-blob")})
						}
					}
				}
				require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Entries: entries}))
			})
			mux.HandleFunc("GET /repos/octocat/hello-world/contents/{config...}", func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, onboardingHeadSHA, r.URL.Query().Get("ref"))
				content, found := configs[r.PathValue("config")]
				require.True(t, found)
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
					"type": "file", "encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(content)),
				}))
			})
			mux.HandleFunc("GET /repos/octocat/hello-world/compare/"+onboardingAnchorSHA+"..."+onboardingBaseSHA, func(w http.ResponseWriter, _ *http.Request) {
				require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{
					Status: new("ahead"), TotalCommits: new(1), Commits: []*gh.RepositoryCommit{{SHA: new("unrelated-change")}},
				}))
			})
			mux.HandleFunc("GET /repos/octocat/hello-world/commits", func(w http.ResponseWriter, _ *http.Request) {
				require.NoError(t, json.NewEncoder(w).Encode([]*gh.RepositoryCommit{}))
			})
			h := &Handler{logger: testLogger()}
			result, err := h.evaluateOnboardingGateAtBase(t.Context(), ghclient.NewInstallationClient(client, testLogger()), "octocat/hello-world", onboardingHeadSHA, onboardingBaseSHA)
			require.NoError(t, err)
			assert.NotContains(t, result.summary, "`new`")
			if legacyPath == "db/typo" {
				assert.Equal(t, checkConclusionFailure, result.conclusion)
				assert.Contains(t, result.summary, "does not exist at anchor")
				return
			}
			assert.Equal(t, checkConclusionSuccess, result.conclusion)
			assert.Contains(t, result.summary, "`legacy`: legacy paths still present on base")
		})
	}
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
	registerOnboardingDiscovery(t, mux, config, config)
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/"+onboardingBaseSHA+"..."+onboardingBaseSHA, func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{Status: new("identical")}))
	})
	h := &Handler{logger: testLogger()}

	result, err := h.evaluateOnboardingGate(t.Context(), ghclient.NewInstallationClient(client, testLogger()), "octocat/hello-world", onboardingHeadSHA, "main")

	require.NoError(t, err)
	assert.Equal(t, checkConclusionSuccess, result.conclusion)
	assert.Contains(t, result.summary, "legacy paths still present on base")
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
	registerOnboardingDiscovery(t, mux, config, config)
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

// Ordinary fold fixtures describe repositories with no legacy verification.
// Specific onboarding tests override these subtree routes with exact routes.
func registerExistingRepository(t *testing.T, mux *http.ServeMux) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/git/ref/heads/", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.Reference{Object: &gh.GitObject{SHA: new(onboardingBaseSHA)}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": []any{}}))
	})
}

func TestOnboardingGatesEveryPassingAggregate(t *testing.T) {
	for _, legacyMetadata := range []bool{false, true} {
		for _, perEnvironment := range []bool{false, true} {
			for _, noSchema := range []bool{false, true} {
				t.Run(fmt.Sprintf("legacy_metadata=%t/per_environment=%t/no_schema=%t", legacyMetadata, perEnvironment, noSchema), func(t *testing.T) {
					cfg := nonAggregateConfig()
					if perEnvironment {
						cfg.AllowedEnvironments = []string{"staging", "production"}
					}
					store := &foldCheckStore{}
					h, mux, client := newFoldHandler(t, cfg, store)
					serveHeadSHA(t, mux, onboardingHeadSHA)
					config := "database: orders\ntype: mysql\n"
					if legacyMetadata {
						config += "legacy_baseline:\n  version: 1\n  base_commit: " + onboardingBaseSHA + "\n  legacy_paths:\n    - never/existed/typo\n"
					}
					registerOnboardingDiscovery(t, mux, "", config)
					mux.HandleFunc("GET /repos/octocat/hello-world/compare/"+onboardingBaseSHA+"..."+onboardingBaseSHA, func(w http.ResponseWriter, _ *http.Request) {
						require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{Status: new("identical")}))
					})
					serveCheckRunCreate(t, mux)
					serveParticipantCheckRunsEmpty(t, mux, onboardingHeadSHA)
					if !noSchema {
						for _, env := range []string{"staging", "production"} {
							store.byPR = append(store.byPR, &storage.Check{
								Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: onboardingHeadSHA,
								Environment: env, DatabaseType: "mysql", DatabaseName: "orders",
								Status: checkStatusCompleted, Conclusion: checkConclusionSuccess,
							})
						}
					}
					for range 2 {
						store.upserted = nil
						if noSchema {
							require.NoError(t, h.postPassingAggregatesOnce(t.Context(), client, "octocat/hello-world", 1, onboardingHeadSHA))
						} else {
							_, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, onboardingHeadSHA)
							require.NoError(t, err)
						}
						require.Len(t, store.upserted, len(h.aggregateCheckTargetsForRepo("octocat/hello-world")))
						for _, check := range store.upserted {
							if !legacyMetadata {
								assert.Equal(t, checkConclusionSuccess, check.Conclusion)
								assert.Empty(t, check.BlockingReason)
								continue
							}
							assert.Equal(t, checkConclusionFailure, check.Conclusion)
							assert.Equal(t, onboardingVerificationBlock.blockingReason, check.BlockingReason)
							assert.Contains(t, check.ErrorMessage, "never/existed/typo does not exist at anchor")
						}
						// A subsequent plan/apply fold must re-verify the stored block.
						store.get = store.upserted[0]
					}
				})
			}
		}
	}
}

func TestOnboardingBaseFreshnessAndRetry(t *testing.T) {
	for _, legacyMetadata := range []bool{false, true} {
		for _, readFailure := range []bool{false, true} {
			t.Run(fmt.Sprintf("legacy_metadata=%t/read_failure=%t", legacyMetadata, readFailure), func(t *testing.T) {
				store := &foldCheckStore{}
				h, mux, client := newFoldHandler(t, nonAggregateConfig(), store)
				serveHeadSHA(t, mux, onboardingHeadSHA)
				config := "database: orders\ntype: mysql\n"
				baseConfig := config
				if legacyMetadata {
					baseConfig = ""
					config += "legacy_baseline:\n  version: 1\n  base_commit: " + onboardingBaseSHA + "\n  legacy_paths:\n    - db/changes\n"
				}
				registerOnboardingConfigs(t, mux, baseConfig, config)
				mux.HandleFunc("GET /repos/octocat/hello-world/compare/"+onboardingBaseSHA+"..."+onboardingBaseSHA, func(w http.ResponseWriter, _ *http.Request) {
					require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{Status: new("identical")}))
				})
				var recovered atomic.Bool
				var reads atomic.Int32
				mux.HandleFunc("GET /repos/octocat/hello-world/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
					sha := onboardingBaseSHA
					if reads.Add(1) > 1 && !recovered.Load() {
						if readFailure {
							http.Error(w, "private upstream error", http.StatusForbidden)
							return
						}
						sha = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
					}
					require.NoError(t, json.NewEncoder(w).Encode(gh.Reference{Object: &gh.GitObject{SHA: &sha}}))
				})
				var published []checkRunCapture
				mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
					var body checkRunCapture
					require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
					published = append(published, body)
					require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 555}))
				})
				serveParticipantCheckRunsEmpty(t, mux, onboardingHeadSHA)

				err := h.postPassingAggregatesOnce(t.Context(), client, "octocat/hello-world", 1, onboardingHeadSHA)
				require.Error(t, err)
				require.Len(t, published, 1)
				assert.Equal(t, aggregateCheckName, published[0].Name)
				assert.Equal(t, checkConclusionFailure, published[0].Conclusion)
				require.Len(t, store.upserted, 1)
				assert.Equal(t, onboardingVerificationBlock.blockingReason, store.upserted[0].BlockingReason)
				assert.NotContains(t, store.upserted[0].ErrorMessage, "private upstream error")

				// A no-schema retry has no database rows; its stored guard must
				// still drive a fresh evaluation and clear only after verification.
				store.get = store.upserted[0]
				store.byPR = []*storage.Check{store.get}
				recovered.Store(true)
				followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, onboardingHeadSHA)
				require.NoError(t, err)
				assert.Equal(t, aggregateFoldClearParticipantRefoldBudget, followUp)
				require.Len(t, published, 2)
				assert.Equal(t, checkConclusionSuccess, published[1].Conclusion)
				require.Len(t, store.upserted, 2)
				assert.Empty(t, store.upserted[1].BlockingReason)
			})
		}
	}
}

func TestOnboardingDoesNotPublishAfterHeadMoves(t *testing.T) {
	store := &foldCheckStore{}
	h, mux, client := newFoldHandler(t, &api.ServerConfig{}, store)
	registerOnboardingDiscovery(t, mux, "", "")
	var reads atomic.Int32
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		sha := onboardingHeadSHA
		if reads.Add(1) > 1 {
			sha = "new-head"
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"head": map[string]any{"sha": sha}, "base": map[string]any{"ref": "main"},
		}))
	})
	// A successful gate would attempt an unregistered check-run write and fail.
	err := h.upsertAggregateCheckRunOnce(t.Context(), client, "octocat/hello-world", 1, onboardingHeadSHA, nil, aggregateCheckName, aggregateSentinel)
	require.NoError(t, err)
	assert.Empty(t, store.upserted)
}

// Retained metadata keeps guarding legacy files until their deletion reaches
// the base branch. The PR head has already deleted the files in every case.
// Inconclusive base lookups block and recover through the aggregate retry path.
func TestOnboardingLegacyPathRetirementAtAggregatePublication(t *testing.T) {
	for _, tc := range []struct {
		name       string
		retired    bool
		changed    bool
		readStatus int
		truncated  bool
	}{
		{name: "active unchanged"},
		{name: "deletion in PR cannot hide a later legacy change", changed: true},
		{name: "deletion merged into base retires the path", retired: true, changed: true},
		{name: "base lookup forbidden", readStatus: http.StatusForbidden},
		{name: "base tree lookup not found is not path retirement", readStatus: http.StatusNotFound},
		{name: "base lookup unavailable", readStatus: http.StatusServiceUnavailable},
		{name: "base lookup truncated", truncated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &foldCheckStore{}
			h, mux, client := newFoldHandler(t, nonAggregateConfig(), store)
			serveHeadSHA(t, mux, onboardingHeadSHA)
			serveCheckRunCreate(t, mux)
			serveParticipantCheckRunsEmpty(t, mux, onboardingHeadSHA)
			var recovered atomic.Bool
			mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/{sha}", func(w http.ResponseWriter, r *http.Request) {
				ref := r.PathValue("sha")
				if ref == onboardingHeadSHA {
					require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Entries: []*gh.TreeEntry{
						{Path: new("schema/schemabot.yaml"), Type: new("blob"), SHA: new("config")},
					}}))
					return
				}
				assert.Empty(t, r.URL.Query().Get("recursive"), "the base and anchor are read only for exact legacy paths")
				if ref == onboardingBaseSHA {
					if tc.readStatus != 0 && !recovered.Load() {
						http.Error(w, "private upstream error", tc.readStatus)
						return
					}
					if tc.truncated && !recovered.Load() {
						require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Truncated: new(true)}))
						return
					}
					if tc.retired {
						require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Entries: []*gh.TreeEntry{}}))
						return
					}
				}
				require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Entries: onboardingLegacyEntries(ref)}))
			})
			mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema/schemabot.yaml", func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, onboardingHeadSHA, r.URL.Query().Get("ref"))
				config := "database: orders\ntype: mysql\nlegacy_baseline:\n  version: 1\n  base_commit: " + onboardingAnchorSHA + "\n  legacy_paths:\n    - db/changes\n"
				require.NoError(t, json.NewEncoder(w).Encode(map[string]string{
					"type": "file", "encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(config)),
				}))
			})
			mux.HandleFunc("GET /repos/octocat/hello-world/compare/"+onboardingAnchorSHA+"..."+onboardingBaseSHA, func(w http.ResponseWriter, _ *http.Request) {
				require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{
					Status: new("ahead"), TotalCommits: new(1), Commits: []*gh.RepositoryCommit{{SHA: new("change")}},
				}))
			})
			var historyReads atomic.Int32
			mux.HandleFunc("GET /repos/octocat/hello-world/commits", func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, onboardingBaseSHA, r.URL.Query().Get("sha"))
				assert.Equal(t, "db/changes", r.URL.Query().Get("path"))
				historyReads.Add(1)
				var commits []*gh.RepositoryCommit
				if tc.changed {
					commits = []*gh.RepositoryCommit{{SHA: new("change"), Commit: &gh.Commit{Message: new("add legacy index")}}}
				}
				require.NoError(t, json.NewEncoder(w).Encode(commits))
			})

			err := h.postPassingAggregatesOnce(t.Context(), client, "octocat/hello-world", 1, onboardingHeadSHA)
			require.Len(t, store.upserted, 1)
			if tc.readStatus != 0 || tc.truncated {
				require.ErrorIs(t, err, ghclient.ErrLegacyPathUnavailable)
				assert.Equal(t, checkConclusionFailure, store.upserted[0].Conclusion)
				assert.Equal(t, onboardingVerificationBlock.blockingReason, store.upserted[0].BlockingReason)
				assert.NotContains(t, store.upserted[0].ErrorMessage, "private upstream error")
				store.get = store.upserted[0]
				store.byPR = []*storage.Check{store.get}
				followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, onboardingHeadSHA)
				require.ErrorIs(t, err, ghclient.ErrLegacyPathUnavailable)
				assert.Equal(t, aggregateFoldScheduleParticipantRefold, followUp)
				recovered.Store(true)
				followUp, err = h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, onboardingHeadSHA)
				require.NoError(t, err)
				assert.Equal(t, aggregateFoldClearParticipantRefoldBudget, followUp)
				require.Len(t, store.upserted, 3)
				assert.Equal(t, checkConclusionSuccess, store.upserted[2].Conclusion)
				assert.Empty(t, store.upserted[2].BlockingReason)
				return
			}
			require.NoError(t, err)
			if tc.changed && !tc.retired {
				assert.Equal(t, checkConclusionFailure, store.upserted[0].Conclusion)
				assert.Contains(t, store.upserted[0].ErrorMessage, "add legacy index")
			} else {
				assert.Equal(t, checkConclusionSuccess, store.upserted[0].Conclusion)
				assert.Empty(t, store.upserted[0].BlockingReason)
			}
			if tc.retired {
				assert.Zero(t, historyReads.Load())
			} else {
				assert.Equal(t, int32(1), historyReads.Load())
			}
		})
	}
}
