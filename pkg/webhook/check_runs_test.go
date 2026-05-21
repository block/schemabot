package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

type emptyStorage struct {
	storage.Storage
}

func (s *emptyStorage) Close() error {
	return nil
}

func (s *emptyStorage) Checks() storage.CheckStore {
	return &emptyCheckStore{}
}

func (s *emptyStorage) Applies() storage.ApplyStore {
	return &emptyApplyStore{}
}

type emptyCheckStore struct {
	storage.CheckStore
}

func (s *emptyCheckStore) Get(ctx context.Context, repo string, pr int, environment, dbType, database string) (*storage.Check, error) {
	return nil, nil
}

func (s *emptyCheckStore) GetByPR(ctx context.Context, repo string, pr int) ([]*storage.Check, error) {
	return nil, nil
}

type emptyApplyStore struct {
	storage.ApplyStore
}

func (s *emptyApplyStore) GetByPR(ctx context.Context, repo string, pr int) ([]*storage.Apply, error) {
	return nil, nil
}

type failingStorage struct {
	emptyStorage
}

func (s *failingStorage) Checks() storage.CheckStore {
	return &failingCheckStore{}
}

type failingCheckStore struct {
	storage.CheckStore
}

func (s *failingCheckStore) Get(ctx context.Context, repo string, pr int, environment, dbType, database string) (*storage.Check, error) {
	return nil, errors.New("storage read failed")
}

type sequenceStorage struct {
	emptyStorage
	checks *sequenceCheckStore
}

func (s *sequenceStorage) Checks() storage.CheckStore {
	return s.checks
}

type sequenceCheckStore struct {
	storage.CheckStore
	results []*storage.Check
	calls   int
}

func (s *sequenceCheckStore) Get(ctx context.Context, repo string, pr int, environment, dbType, database string) (*storage.Check, error) {
	s.calls++
	if len(s.results) == 0 {
		return nil, nil
	}
	check := s.results[0]
	s.results = s.results[1:]
	return check, nil
}

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

func TestCheckPriorEnvViaLocalFailsClosedOnStorageError(t *testing.T) {
	const (
		repo = "octocat/hello-world"
		pr   = 1
	)

	client, mux := setupGitHubServer(t)
	comments := make(chan string, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})

	installClient := ghclient.NewInstallationClient(client, testLogger())
	service := api.New(&failingStorage{}, &api.ServerConfig{}, nil, testLogger())
	t.Cleanup(func() { utils.CloseAndLog(service) })

	h := &Handler{
		service:                    service,
		ghClient:                   &fakeClientFactory{client: installClient},
		logger:                     testLogger(),
		priorEnvCheckMaxAttempts:   1,
		priorEnvCheckRetryInterval: time.Nanosecond,
	}

	blocked := h.checkPriorEnvViaLocal(t.Context(), repo, pr, "orders", "mysql", "production", "staging", 12345)
	assert.True(t, blocked, "storage read failure should block apply")

	select {
	case body := <-comments:
		assert.Contains(t, body, "Apply Blocked")
		assert.Contains(t, body, "Could not verify staging status")
		assert.Contains(t, body, "storage read failed")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for fail-closed comment")
	}
}

// TestCheckPriorEnvViaLocalMissingCheckBlocksWithActionableGuidance covers an
// apply to a later environment when this SchemaBot instance owns the required
// prior environment, but no stored check state exists for it. The apply must
// fail closed and tell the operator how to create the missing prior-environment
// status instead of suggesting a blind retry of the later apply.
func TestCheckPriorEnvViaLocalMissingCheckBlocksWithActionableGuidance(t *testing.T) {
	const (
		repo = "octocat/hello-world"
		pr   = 1
	)

	client, mux := setupGitHubServer(t)
	comments := make(chan string, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})

	installClient := ghclient.NewInstallationClient(client, testLogger())
	service := api.New(&emptyStorage{}, &api.ServerConfig{}, nil, testLogger())
	t.Cleanup(func() { utils.CloseAndLog(service) })

	h := &Handler{
		service:                    service,
		ghClient:                   &fakeClientFactory{client: installClient},
		logger:                     testLogger(),
		priorEnvCheckMaxAttempts:   1,
		priorEnvCheckRetryInterval: time.Nanosecond,
	}

	blocked := h.checkPriorEnvViaLocal(t.Context(), repo, pr, "orders", "mysql", "production", "staging", 12345)
	assert.True(t, blocked, "missing prior check should block apply")

	select {
	case body := <-comments:
		assert.Contains(t, body, "Apply Blocked")
		assert.Contains(t, body, "could not find a completed `staging` check")
		assert.Contains(t, body, "schemabot plan -e staging")
		assert.NotContains(t, body, "Retry the apply command")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for missing-check comment")
	}
}

// TestCheckPriorEnvViaLocalRetriesBeforeFailClosed covers the race where a
// later-environment apply starts just before the prior environment's local check
// state is visible in storage. SchemaBot should retry, accept the later success,
// and only use the missing-check fail-closed path if the state stays missing.
func TestCheckPriorEnvViaLocalRetriesBeforeFailClosed(t *testing.T) {
	const (
		repo = "octocat/hello-world"
		pr   = 1
	)

	client, mux := setupGitHubServer(t)
	comments := make(chan string, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})

	checks := &sequenceCheckStore{
		results: []*storage.Check{
			nil,
			{Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
		},
	}
	installClient := ghclient.NewInstallationClient(client, testLogger())
	service := api.New(&sequenceStorage{checks: checks}, &api.ServerConfig{}, nil, testLogger())
	t.Cleanup(func() { utils.CloseAndLog(service) })

	h := &Handler{
		service:                    service,
		ghClient:                   &fakeClientFactory{client: installClient},
		logger:                     testLogger(),
		priorEnvCheckMaxAttempts:   2,
		priorEnvCheckRetryInterval: time.Nanosecond,
	}

	blocked := h.checkPriorEnvViaLocal(t.Context(), repo, pr, "orders", "mysql", "production", "staging", 12345)
	assert.False(t, blocked, "retry should observe the prior environment success and allow apply")
	assert.Equal(t, 2, checks.calls)

	select {
	case body := <-comments:
		t.Fatalf("unexpected comment posted: %s", body)
	default:
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

	assert.Contains(t, title, "1 apply pending")
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
	assert.Equal(t, "All applies complete", title)
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

func TestLatestApplyByCheckKey(t *testing.T) {
	applies := []*storage.Apply{
		{
			ID:           1,
			Database:     "orders",
			DatabaseType: "mysql",
			Environment:  "staging",
			State:        state.Apply.Completed,
		},
		{
			ID:           2,
			Database:     "orders",
			DatabaseType: "mysql",
			Environment:  "staging",
			State:        state.Apply.Running,
		},
		{
			ID:           3,
			Database:     "orders",
			DatabaseType: "vitess",
			Environment:  "staging",
			State:        state.Apply.Completed,
		},
		{
			ID:           5,
			Database:     "users",
			DatabaseType: "mysql",
			Environment:  "staging",
			State:        state.Apply.Failed,
		},
		{
			ID:           4,
			Database:     "users",
			DatabaseType: "mysql",
			Environment:  "staging",
			State:        state.Apply.Completed,
		},
	}

	latest := latestApplyByCheckKey(applies)

	mysqlOrders := latest[applyCheckKey{environment: "staging", databaseType: "mysql", databaseName: "orders"}]
	require.NotNil(t, mysqlOrders)
	assert.Equal(t, state.Apply.Running, mysqlOrders.State)

	vitessOrders := latest[applyCheckKey{environment: "staging", databaseType: "vitess", databaseName: "orders"}]
	require.NotNil(t, vitessOrders)
	assert.Equal(t, state.Apply.Completed, vitessOrders.State)

	mysqlUsers := latest[applyCheckKey{environment: "staging", databaseType: "mysql", databaseName: "users"}]
	require.NotNil(t, mysqlUsers)
	assert.Equal(t, int64(5), mysqlUsers.ID)
	assert.Equal(t, state.Apply.Failed, mysqlUsers.State)
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
			ghClient:                   factory,
			logger:                     testLogger(),
			priorEnvCheckMaxAttempts:   1,
			priorEnvCheckRetryInterval: time.Nanosecond,
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

	t.Run("no staging check blocks apply", func(t *testing.T) {
		h, comments := setupCheckRunServer(t, []map[string]any{})

		blocked := h.checkPriorEnvViaGitHub(t.Context(), repo, pr, "orders", "production", "staging", 12345)
		assert.True(t, blocked)

		select {
		case body := <-comments:
			assert.Contains(t, body, "Apply Blocked")
			assert.Contains(t, body, "could not find a completed `staging` check")
			assert.Contains(t, body, "schemabot plan -e staging")
			assert.NotContains(t, body, "Retry the apply command")
		default:
			t.Fatal("expected a comment explaining the missing prior environment check")
		}
	})

	t.Run("staging check in progress blocks apply", func(t *testing.T) {
		h, _ := setupCheckRunServer(t, []map[string]any{
			{"id": 1, "name": "SchemaBot (staging)", "status": "in_progress", "conclusion": ""},
		})

		blocked := h.checkPriorEnvViaGitHub(t.Context(), repo, pr, "orders", "production", "staging", 12345)
		assert.True(t, blocked)
	})

	// This covers the cross-instance race where the production SchemaBot instance
	// checks GitHub before the staging instance's aggregate Check Run has become
	// visible. SchemaBot should retry briefly, accept the staging success, and
	// still fail closed if the check never appears.
	t.Run("missing staging check retries before allowing success", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := make(chan string, 10)
		checkCalls := 0

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

		mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
			checkCalls++
			checkRuns := []map[string]any{}
			if checkCalls > 1 {
				checkRuns = []map[string]any{
					{"id": 1, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success"},
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"total_count": len(checkRuns),
				"check_runs":  checkRuns,
			})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		h := &Handler{
			ghClient:                   &fakeClientFactory{client: installClient},
			logger:                     testLogger(),
			priorEnvCheckMaxAttempts:   2,
			priorEnvCheckRetryInterval: time.Nanosecond,
		}

		blocked := h.checkPriorEnvViaGitHub(t.Context(), repo, pr, "orders", "production", "staging", 12345)
		assert.False(t, blocked, "retry should observe the prior environment success and allow apply")
		assert.Equal(t, 2, checkCalls)

		select {
		case body := <-comments:
			t.Fatalf("unexpected comment posted: %s", body)
		default:
		}
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
			ghClient:                   factory,
			logger:                     testLogger(),
			priorEnvCheckMaxAttempts:   1,
			priorEnvCheckRetryInterval: time.Nanosecond,
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
	t.Run("non-allowed environment ignored with explicit response", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(&emptyStorage{}, &api.ServerConfig{
			AllowedEnvironments: []string{"staging"},
			Repos:               map[string]api.RepoConfig{},
		}, nil, testLogger())

		h := &Handler{
			service:  service,
			ghClient: factory,
			logger:   testLogger(),
		}

		// Plan targeting production should be ignored by this instance because
		// only staging is in allowed_environments.
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

		service := api.New(&emptyStorage{}, &api.ServerConfig{
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

		service := api.New(&emptyStorage{}, &api.ServerConfig{
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
