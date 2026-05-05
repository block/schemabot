package webhook

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

func TestComputeAggregate(t *testing.T) {
	tests := []struct {
		name           string
		checks         []*storage.Check
		wantConclusion string
		wantStatus     string
	}{
		{
			name: "all success",
			checks: []*storage.Check{
				{Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
				{Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
			},
			wantConclusion: checkConclusionSuccess,
			wantStatus:     checkStatusCompleted,
		},
		{
			name: "any failure dominates",
			checks: []*storage.Check{
				{Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
				{Status: checkStatusCompleted, Conclusion: checkConclusionFailure},
				{Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired},
			},
			wantConclusion: checkConclusionFailure,
			wantStatus:     checkStatusCompleted,
		},
		{
			name: "action_required when no failure",
			checks: []*storage.Check{
				{Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
				{Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired},
			},
			wantConclusion: checkConclusionActionRequired,
			wantStatus:     checkStatusCompleted,
		},
		{
			name: "in_progress takes priority over conclusions",
			checks: []*storage.Check{
				{Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
				{Status: checkStatusInProgress, Conclusion: ""},
				{Status: checkStatusCompleted, Conclusion: checkConclusionFailure},
			},
			wantConclusion: "", // in_progress has no conclusion
			wantStatus:     checkStatusInProgress,
		},
		{
			name: "single check success",
			checks: []*storage.Check{
				{Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
			},
			wantConclusion: checkConclusionSuccess,
			wantStatus:     checkStatusCompleted,
		},
		{
			name: "single check action_required",
			checks: []*storage.Check{
				{Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired},
			},
			wantConclusion: checkConclusionActionRequired,
			wantStatus:     checkStatusCompleted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conclusion, status := computeAggregate(tt.checks)
			assert.Equal(t, tt.wantConclusion, conclusion)
			assert.Equal(t, tt.wantStatus, status)
		})
	}
}

func TestIsAggregateCheck(t *testing.T) {
	t.Run("global aggregate", func(t *testing.T) {
		aggregate := &storage.Check{
			Environment:  aggregateSentinel,
			DatabaseType: aggregateSentinel,
			DatabaseName: aggregateSentinel,
		}
		require.True(t, isAggregateCheck(aggregate))
	})

	t.Run("per-environment aggregate", func(t *testing.T) {
		aggregate := &storage.Check{
			Environment:  "staging",
			DatabaseType: aggregateSentinel,
			DatabaseName: aggregateSentinel,
		}
		require.True(t, isAggregateCheck(aggregate))
	})

	t.Run("per-database check", func(t *testing.T) {
		perDB := &storage.Check{
			Environment:  "staging",
			DatabaseType: "mysql",
			DatabaseName: "orders",
		}
		require.False(t, isAggregateCheck(perDB))
	})
}

func TestAggregateSummary(t *testing.T) {
	checks := []*storage.Check{
		{DatabaseName: "orders", Environment: "staging", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
		{DatabaseName: "orders", Environment: "production", Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired},
	}

	title, summary := aggregateSummary(checks, checkConclusionActionRequired)

	assert.Contains(t, title, "1 schema change(s) pending")
	assert.Contains(t, summary, "`orders`")
	assert.Contains(t, summary, "staging")
	assert.Contains(t, summary, "production")
	assert.Contains(t, summary, "Applied")
	assert.Contains(t, summary, "Pending")
}

func TestAggregateSummary_AllSuccess(t *testing.T) {
	checks := []*storage.Check{
		{DatabaseName: "orders", Environment: "staging", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
		{DatabaseName: "orders", Environment: "production", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
	}

	title, _ := aggregateSummary(checks, checkConclusionSuccess)
	assert.Equal(t, "All schema changes applied", title)
}

func TestConclusionEmoji(t *testing.T) {
	assert.Equal(t, "Applied", conclusionEmoji(checkStatusCompleted, checkConclusionSuccess))
	assert.Equal(t, "Failed", conclusionEmoji(checkStatusCompleted, checkConclusionFailure))
	assert.Equal(t, "Pending", conclusionEmoji(checkStatusCompleted, checkConclusionActionRequired))
	assert.Equal(t, "In progress", conclusionEmoji(checkStatusInProgress, ""))
	assert.Equal(t, "Cancelled", conclusionEmoji(checkStatusCompleted, checkConclusionNeutral))
}

func TestAggregateCheckNameForEnv(t *testing.T) {
	assert.Equal(t, "SchemaBot (staging)", aggregateCheckNameForEnv("staging"))
	assert.Equal(t, "SchemaBot (production)", aggregateCheckNameForEnv("production"))
	assert.Equal(t, "SchemaBot (sandbox)", aggregateCheckNameForEnv("sandbox"))
}

func TestFilterChecksByEnvironment(t *testing.T) {
	checks := []*storage.Check{
		{Environment: "staging", DatabaseName: "orders", DatabaseType: "mysql"},
		{Environment: "production", DatabaseName: "orders", DatabaseType: "mysql"},
		{Environment: "staging", DatabaseName: "users", DatabaseType: "mysql"},
		// Global aggregate (no allowed_environments)
		{Environment: aggregateSentinel, DatabaseType: aggregateSentinel, DatabaseName: aggregateSentinel},
		// Per-environment aggregate (with allowed_environments)
		{Environment: "staging", DatabaseType: aggregateSentinel, DatabaseName: aggregateSentinel},
	}

	t.Run("filters to staging only and excludes per-env aggregate", func(t *testing.T) {
		result := filterChecksByEnvironment(checks, "staging")
		require.Len(t, result, 2)
		assert.Equal(t, "orders", result[0].DatabaseName)
		assert.Equal(t, "users", result[1].DatabaseName)
	})

	t.Run("filters to production only", func(t *testing.T) {
		result := filterChecksByEnvironment(checks, "production")
		require.Len(t, result, 1)
		assert.Equal(t, "orders", result[0].DatabaseName)
	})

	t.Run("returns empty for unknown environment", func(t *testing.T) {
		result := filterChecksByEnvironment(checks, "sandbox")
		assert.Empty(t, result)
	})

	t.Run("excludes global aggregate checks", func(t *testing.T) {
		result := filterChecksByEnvironment(checks, aggregateSentinel)
		assert.Empty(t, result)
	})
}

func TestCheckPriorEnvViaGitHub(t *testing.T) {
	const (
		repo    = "octocat/hello-world"
		pr      = 1
		headSHA = "abc123"
	)

	// setupCheckRunServer creates a mock GitHub server with PR fetch and optional
	// comment capture, plus a check-runs endpoint that returns the given check runs.
	setupCheckRunServer := func(t *testing.T, checkRuns []map[string]any) (*Handler, chan string) {
		t.Helper()

		client, mux := setupGitHubServer(t)
		comments := make(chan string, 10)

		mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"head": map[string]any{"sha": headSHA, "ref": "feature"},
				"base": map[string]any{"sha": "base123", "ref": "main"},
				"user": map[string]any{"login": "testuser"},
			})
		})

		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
			var body struct {
				Body string `json:"body"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			comments <- body.Body
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})

		mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"total_count": len(checkRuns),
				"check_runs":  checkRuns,
			})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		h := &Handler{
			ghClient: factory,
			logger:   testLogger(),
		}

		return h, comments
	}

	t.Run("staging check success allows proceed", func(t *testing.T) {
		h, comments := setupCheckRunServer(t, []map[string]any{
			{"id": 1, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success"},
		})

		blocked := h.checkPriorEnvViaGitHub(t.Context(), repo, pr, "orders", "production", "staging", 12345)
		assert.False(t, blocked)

		select {
		case body := <-comments:
			t.Fatalf("unexpected comment posted: %s", body)
		default:
		}
	})

	t.Run("staging check pending blocks apply", func(t *testing.T) {
		h, _ := setupCheckRunServer(t, []map[string]any{
			{"id": 1, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "action_required"},
		})

		blocked := h.checkPriorEnvViaGitHub(t.Context(), repo, pr, "orders", "production", "staging", 12345)
		assert.True(t, blocked)
	})

	t.Run("no staging check allows proceed", func(t *testing.T) {
		h, _ := setupCheckRunServer(t, []map[string]any{})

		blocked := h.checkPriorEnvViaGitHub(t.Context(), repo, pr, "orders", "production", "staging", 12345)
		assert.False(t, blocked)
	})

	t.Run("staging check in progress blocks apply", func(t *testing.T) {
		h, _ := setupCheckRunServer(t, []map[string]any{
			{"id": 1, "name": "SchemaBot (staging)", "status": "in_progress", "conclusion": ""},
		})

		blocked := h.checkPriorEnvViaGitHub(t.Context(), repo, pr, "orders", "production", "staging", 12345)
		assert.True(t, blocked)
	})

	t.Run("GitHub API failure blocks apply (fail-closed)", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := make(chan string, 10)

		// PR fetch succeeds
		mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"head": map[string]any{"sha": headSHA, "ref": "feature"},
				"base": map[string]any{"sha": "base123", "ref": "main"},
				"user": map[string]any{"login": "testuser"},
			})
		})

		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
			var body struct {
				Body string `json:"body"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			comments <- body.Body
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})

		// Check runs endpoint returns a server error
		mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		h := &Handler{
			ghClient: factory,
			logger:   testLogger(),
		}

		blocked := h.checkPriorEnvViaGitHub(t.Context(), repo, pr, "orders", "production", "staging", 12345)
		assert.True(t, blocked, "GitHub API failure should block apply")

		select {
		case body := <-comments:
			assert.Contains(t, body, "Apply Blocked")
			assert.Contains(t, body, "staging")
		default:
			t.Fatal("expected a comment explaining the API failure")
		}
	})
}

func TestWebhookEnvironmentFiltering(t *testing.T) {
	t.Run("non-allowed environment silently ignored", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{
			AllowedEnvironments: []string{"staging"},
			Repos:               map[string]api.RepoConfig{},
		}, nil, testLogger())

		h := &Handler{
			service:  service,
			ghClient: factory,
			logger:   testLogger(),
		}

		// Plan targeting production should be silently ignored by this instance
		// because only staging is in allowed_environments.
		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot plan -e production",
			isPR:    true,
		}, nil)

		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "environment handled by another instance")
	})

	t.Run("allowed environment proceeds", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{
			AllowedEnvironments: []string{"staging"},
			Repos:               map[string]api.RepoConfig{},
		}, nil, testLogger())

		h := &Handler{
			service:  service,
			ghClient: factory,
			logger:   testLogger(),
		}

		// Plan for staging should proceed past the environment filter. It will fail
		// downstream because there's no schema config on GitHub, but the response
		// proves the environment filter did not block it.
		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot plan -e staging",
			isPR:    true,
		}, nil)

		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		// The plan command gets past the environment filter and enters the plan handler.
		// With no service/storage wired up fully, it responds with "plan started".
		assert.NotContains(t, rr.Body.String(), "environment handled by another instance")
	})

	t.Run("empty config allows all environments", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{
			Repos: map[string]api.RepoConfig{},
		}, nil, testLogger())

		h := &Handler{
			service:  service,
			ghClient: factory,
			logger:   testLogger(),
		}

		// Plan for production with no allowed_environments config should proceed
		// (empty config allows all environments).
		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot plan -e production",
			isPR:    true,
		}, nil)

		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		assert.NotContains(t, rr.Body.String(), "environment handled by another instance")
	})
}

//go:fix inline
func TestRespondToUnscoped(t *testing.T) {
	falseVal := false
	trueVal := true
	t.Run("help skipped when respond_to_unscoped is false", func(t *testing.T) {
		service := api.New(nil, &api.ServerConfig{
			RespondToUnscoped: &falseVal,
			Repos:             map[string]api.RepoConfig{},
		}, nil, testLogger())

		h := &Handler{
			service: service,
			logger:  testLogger(),
		}

		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot help",
			isPR:    true,
		}, nil)

		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "unscoped command skipped")
	})

	t.Run("invalid command skipped when respond_to_unscoped is false", func(t *testing.T) {
		service := api.New(nil, &api.ServerConfig{
			RespondToUnscoped: &falseVal,
			Repos:             map[string]api.RepoConfig{},
		}, nil, testLogger())

		h := &Handler{
			service: service,
			logger:  testLogger(),
		}

		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot foobar",
			isPR:    true,
		}, nil)

		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "unscoped command skipped")
	})

	t.Run("help responds when respond_to_unscoped is true", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{
			RespondToUnscoped: &trueVal,
			Repos:             map[string]api.RepoConfig{},
		}, nil, testLogger())

		h := &Handler{
			service:  service,
			ghClient: factory,
			logger:   testLogger(),
		}

		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot help",
			isPR:    true,
		}, nil)

		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "help posted")
	})
}

// httpResponseRecorder creates an httptest.ResponseRecorder.
func httpResponseRecorder() *httptest.ResponseRecorder {
	return httptest.NewRecorder()
}
