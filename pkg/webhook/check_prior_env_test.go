package webhook

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

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
		ghClients:                  ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
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
		ghClients:                  ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
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
		ghClients:                  ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
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

func TestCheckPriorEnvironmentsWithProductionOnlyServerConfigChecksStaging(t *testing.T) {
	const (
		repo    = "octocat/hello-world"
		pr      = 1
		headSHA = "abc123"
	)

	client, mux := setupGitHubServer(t)
	comments := make(chan string, 10)
	checkRunRequests := make(chan struct{}, 10)

	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"head": map[string]any{"sha": headSHA, "ref": "feature"},
			"base": map[string]any{"sha": "base123", "ref": "main"},
			"user": map[string]any{"login": "testuser"},
		})
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, r *http.Request) {
		checkRunRequests <- struct{}{}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total_count": 1,
			"check_runs": []map[string]any{
				{"id": 1, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "action_required", "app": map[string]any{"slug": "schemabot"}},
			},
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

	installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")
	service := api.New(&emptyStorage{}, &api.ServerConfig{
		AllowedEnvironments: []string{"production"},
		EnvironmentOrder:    []string{"staging", "production"},
		Databases: map[string]api.DatabaseConfig{
			"orders": {
				Type: "mysql",
				Environments: map[string]api.EnvironmentConfig{
					"production": {Deployment: "default", Target: "orders"},
				},
			},
		},
	}, nil, testLogger())
	t.Cleanup(func() { utils.CloseAndLog(service) })

	h := &Handler{
		service:                    service,
		ghClients:                  ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:                     testLogger(),
		priorEnvCheckMaxAttempts:   1,
		priorEnvCheckRetryInterval: time.Nanosecond,
	}

	blocked := h.checkPriorEnvironments(t.Context(), repo, pr,
		"orders", "mysql", "production", []string{"staging", "production"}, 12345, "testuser")
	assert.True(t, blocked, "production blocks when the environment list includes staging before production")

	select {
	case <-checkRunRequests:
	case <-time.After(2 * time.Second):
		t.Fatal("expected staging GitHub check lookup")
	}
	select {
	case body := <-comments:
		assert.Contains(t, body, "staging")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for staging block comment")
	}

	schemaResult := &ghclient.SchemaRequestResult{Database: "orders", Type: "mysql"}
	require.NoError(t, h.attachServerEnvironments(schemaResult, "production"))
	assert.Equal(t, []string{"production"}, schemaResult.Environments)

	blocked = h.checkPriorEnvironments(t.Context(), repo, pr,
		"orders", "mysql", "production", schemaResult.Environments, 12345, "testuser")
	assert.True(t, blocked, "production blocks on staging even when this server only has a production target for the database")

	select {
	case <-checkRunRequests:
	case <-time.After(2 * time.Second):
		t.Fatal("expected staging GitHub check lookup")
	}
	select {
	case body := <-comments:
		assert.Contains(t, body, "staging")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for staging block comment")
	}
}

// TestCheckPriorEnvironmentsCrossDeploymentAppTrust covers the split-deployment
// topology where each environment is owned by a separate SchemaBot deployment
// with its own GitHub App: the production deployment verifies staging through
// the "SchemaBot (staging)" aggregate Check Run, which was created by the
// staging deployment's App, not its own. The promotion gate must accept that
// sibling App's check when its slug is configured as trusted — and must keep
// failing closed when it is not, so an unconfigured deployment never trusts a
// check it cannot attribute to a SchemaBot App.
func TestCheckPriorEnvironmentsCrossDeploymentAppTrust(t *testing.T) {
	const (
		repo        = "octocat/hello-world"
		pr          = 1
		headSHA     = "abc123"
		ownSlug     = "schemabot-production"
		siblingSlug = "schemabot-staging"
	)

	productionScopedConfig := func() *api.ServerConfig {
		return &api.ServerConfig{
			AllowedEnvironments: []string{"production"},
			EnvironmentOrder:    []string{"staging", "production"},
			Databases: map[string]api.DatabaseConfig{
				"orders": {
					Type: "mysql",
					Environments: map[string]api.EnvironmentConfig{
						"production": {Deployment: "default", Target: "orders"},
					},
				},
			},
		}
	}

	setup := func(t *testing.T, installClient *ghclient.InstallationClient) (*Handler, chan string) {
		t.Helper()
		comments := make(chan string, 10)

		service := api.New(&emptyStorage{}, productionScopedConfig(), nil, testLogger())
		t.Cleanup(func() { utils.CloseAndLog(service) })

		return &Handler{
			service:                    service,
			ghClients:                  ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
			logger:                     testLogger(),
			priorEnvCheckMaxAttempts:   1,
			priorEnvCheckRetryInterval: time.Nanosecond,
		}, comments
	}

	registerGitHubEndpoints := func(t *testing.T, mux *http.ServeMux, comments chan string) {
		t.Helper()
		mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"head": map[string]any{"sha": headSHA, "ref": "feature"},
				"base": map[string]any{"sha": "base123", "ref": "main"},
				"user": map[string]any{"login": "testuser"},
			})
		})
		mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"total_count": 1,
				"check_runs": []map[string]any{
					{"id": 1, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": siblingSlug}},
				},
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
	}

	t.Run("trusted sibling app check satisfies the promotion gate", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), ownSlug, siblingSlug)
		h, comments := setup(t, installClient)
		registerGitHubEndpoints(t, mux, comments)

		blocked := h.checkPriorEnvironments(t.Context(), repo, pr,
			"orders", "mysql", "production", []string{"production"}, 12345, "testuser")

		assert.False(t, blocked, "a passing staging aggregate check from the trusted staging deployment App must allow production apply")
		select {
		case body := <-comments:
			t.Fatalf("unexpected comment posted: %s", body)
		default:
		}
	})

	t.Run("unconfigured sibling app check fails closed", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), ownSlug)
		h, comments := setup(t, installClient)
		registerGitHubEndpoints(t, mux, comments)

		blocked := h.checkPriorEnvironments(t.Context(), repo, pr,
			"orders", "mysql", "production", []string{"production"}, 12345, "testuser")

		assert.True(t, blocked, "a staging aggregate check from an unconfigured App must not satisfy the promotion gate")
		select {
		case body := <-comments:
			assert.Contains(t, body, "`schemabot-staging`")
			assert.Contains(t, body, "trusted-check-app-slugs")
			assert.NotContains(t, body, "could not find a completed `staging` check")
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for staging block comment")
		}
	})
}

// TestCheckPriorEnvironmentsScopedTargetMissingFromOrderFailsClosed covers a
// scoped SchemaBot instance (allowed_environments is configured) applying to an
// environment that this instance handles but that the operator omitted from the
// configured promotion order. The command reaches the gate because the target
// environment is allowed, yet the promotion order cannot place it among its
// prior environments. SchemaBot cannot determine which environments must be
// applied first, so it cannot enforce staging-first ordering. The apply must
// fail closed with a configuration error instead of falling back to this
// instance's local environment list, which would silently skip prior
// environments owned by other instances.
func TestCheckPriorEnvironmentsScopedTargetMissingFromOrderFailsClosed(t *testing.T) {
	const (
		repo = "octocat/hello-world"
		pr   = 1
	)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

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
	service := api.New(&emptyStorage{}, &api.ServerConfig{
		AllowedEnvironments: []string{"production", "canary"},
		EnvironmentOrder:    []string{"staging", "production"},
		Databases: map[string]api.DatabaseConfig{
			"orders": {
				Type: "mysql",
				Environments: map[string]api.EnvironmentConfig{
					"canary": {Deployment: "default", Target: "orders"},
				},
			},
		},
	}, nil, testLogger())
	t.Cleanup(func() { utils.CloseAndLog(service) })

	h := &Handler{
		service:                    service,
		ghClients:                  ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:                     testLogger(),
		priorEnvCheckMaxAttempts:   1,
		priorEnvCheckRetryInterval: time.Nanosecond,
	}

	blocked := h.checkPriorEnvironments(t.Context(), repo, pr,
		"orders", "mysql", "canary", []string{"canary"}, 12345, "testuser")
	assert.True(t, blocked, "scoped instance must fail closed when an allowed target environment is absent from the promotion order")

	select {
	case body := <-comments:
		assert.Contains(t, body, "Apply Blocked")
		assert.Contains(t, body, "`canary` is not in the configured promotion order")
		assert.Contains(t, body, "cannot enforce staging-first ordering")
		assert.Contains(t, body, "environment_order")
		assert.Contains(t, body, "`staging` → `production`")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for unlisted-environment config error comment")
	}

	dp := requirePromotionConfigErrorBlockDataPoint(t, reader)
	assert.Equal(t, int64(1), dp.Value, "the config-error block must increment the metric exactly once")
	assertStringAttr(t, dp.Attributes, "repository", repo)
	assertStringAttr(t, dp.Attributes, "database", "orders")
	assertStringAttr(t, dp.Attributes, "environment", "canary")
}

// requirePromotionConfigErrorBlockDataPoint collects metrics from the reader and
// returns the single data point for the promotion config-error block counter,
// failing the test if the metric is absent or has more than one data point.
func requirePromotionConfigErrorBlockDataPoint(t *testing.T, reader *sdkmetric.ManualReader) metricdata.DataPoint[int64] {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.promotion.config_error_blocks_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "promotion config error block metric must be an int64 sum")
			require.Len(t, sum.DataPoints, 1, "expected exactly one promotion config error block data point")
			return sum.DataPoints[0]
		}
	}

	t.Fatal("schemabot.promotion.config_error_blocks_total metric not found")
	return metricdata.DataPoint[int64]{}
}

func assertStringAttr(t *testing.T, set attribute.Set, key, want string) {
	t.Helper()
	got, ok := set.Value(attribute.Key(key))
	assert.True(t, ok, "metric data point must carry the %q attribute", key)
	assert.Equal(t, want, got.AsString(), "metric attribute %q", key)
}

// TestCheckPriorEnvironmentsScopedTargetInOrderAllowsApply covers a scoped
// SchemaBot instance applying to an environment that is in the configured
// promotion order, with all prior environments verified successfully. The
// staging-first config-error gate must not trip: the apply proceeds and no
// configuration-error comment is posted.
func TestCheckPriorEnvironmentsScopedTargetInOrderAllowsApply(t *testing.T) {
	const (
		repo    = "octocat/hello-world"
		pr      = 1
		headSHA = "abc123"
	)

	client, mux := setupGitHubServer(t)
	comments := make(chan string, 1)

	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"head": map[string]any{"sha": headSHA, "ref": "feature"},
			"base": map[string]any{"sha": "base123", "ref": "main"},
			"user": map[string]any{"login": "testuser"},
		})
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total_count": 1,
			"check_runs": []map[string]any{
				{"id": 1, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "schemabot"}},
			},
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

	installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")
	service := api.New(&emptyStorage{}, &api.ServerConfig{
		AllowedEnvironments: []string{"production"},
		EnvironmentOrder:    []string{"staging", "production"},
		Databases: map[string]api.DatabaseConfig{
			"orders": {
				Type: "mysql",
				Environments: map[string]api.EnvironmentConfig{
					"production": {Deployment: "default", Target: "orders"},
				},
			},
		},
	}, nil, testLogger())
	t.Cleanup(func() { utils.CloseAndLog(service) })

	h := &Handler{
		service:                    service,
		ghClients:                  ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:                     testLogger(),
		priorEnvCheckMaxAttempts:   1,
		priorEnvCheckRetryInterval: time.Nanosecond,
	}

	blocked := h.checkPriorEnvironments(t.Context(), repo, pr,
		"orders", "mysql", "production", []string{"production"}, 12345, "testuser")
	assert.False(t, blocked, "in-order target with a passing prior environment check should be allowed")

	select {
	case body := <-comments:
		t.Fatalf("unexpected comment posted: %s", body)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestCheckPriorEnvViaGitHub(t *testing.T) {
	const (
		repo    = "octocat/hello-world"
		pr      = 1
		headSHA = "abc123"
	)

	// setupCheckRunServer creates a mock GitHub server with PR fetch and optional
	// comment capture, plus a check-runs endpoint that returns the given check runs.
	setupCheckRunServer := func(t *testing.T, checkRuns []map[string]any, configs ...*api.ServerConfig) (*Handler, chan string) {
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

		installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")
		factory := &fakeClientFactory{client: installClient}
		config := &api.ServerConfig{}
		if len(configs) > 0 {
			config = configs[0]
		}
		service := api.New(&emptyStorage{}, config, nil, testLogger())
		t.Cleanup(func() { utils.CloseAndLog(service) })

		h := &Handler{
			service:                    service,
			ghClients:                  ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:                     testLogger(),
			priorEnvCheckMaxAttempts:   1,
			priorEnvCheckRetryInterval: time.Nanosecond,
		}

		return h, comments
	}

	t.Run("staging check success allows proceed", func(t *testing.T) {
		h, comments := setupCheckRunServer(t, []map[string]any{
			{"id": 1, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "schemabot"}},
		})

		blocked := h.checkPriorEnvViaGitHub(t.Context(), repo, pr, "orders", "production", "staging", 12345)
		assert.False(t, blocked)

		select {
		case body := <-comments:
			t.Fatalf("unexpected comment posted: %s", body)
		default:
		}
	})

	t.Run("custom check name success allows proceed", func(t *testing.T) {
		h, comments := setupCheckRunServer(t, []map[string]any{
			{"id": 1, "name": "SchemaBot X (staging)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "schemabot"}},
		}, &api.ServerConfig{GitHub: api.GitHubConfig{CheckName: "SchemaBot X"}})

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
			{"id": 1, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "action_required", "app": map[string]any{"slug": "schemabot"}},
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
			{"id": 1, "name": "SchemaBot (staging)", "status": "in_progress", "conclusion": "", "app": map[string]any{"slug": "schemabot"}},
		})

		blocked := h.checkPriorEnvViaGitHub(t.Context(), repo, pr, "orders", "production", "staging", 12345)
		assert.True(t, blocked)
	})

	// A repository contributor can create a GitHub Actions job whose name matches
	// the staging instance's aggregate Check Run. The promotion gate must only
	// trust Check Runs created by trusted SchemaBot GitHub Apps, so a passing
	// same-named run from another app blocks the production apply — and the
	// blocked comment names the untrusted app and points at the trust config
	// instead of suggesting a staging re-plan that cannot fix it.
	t.Run("same-named foreign-app success run blocks apply", func(t *testing.T) {
		h, comments := setupCheckRunServer(t, []map[string]any{
			{"id": 1, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "github-actions"}},
		})

		blocked := h.checkPriorEnvViaGitHub(t.Context(), repo, pr, "orders", "production", "staging", 12345)
		assert.True(t, blocked, "a foreign-app check run must not satisfy the promotion gate")

		select {
		case body := <-comments:
			assert.Contains(t, body, "Apply Blocked")
			assert.Contains(t, body, "`github-actions`")
			assert.Contains(t, body, "does not trust")
			assert.Contains(t, body, "trusted-check-app-slugs")
			assert.Contains(t, body, "will not resolve this")
			assert.NotContains(t, body, "could not find a completed `staging` check")
		default:
			t.Fatal("expected a comment explaining the untrusted prior environment check")
		}
	})

	// When SchemaBot does not know its own GitHub App slug it cannot verify
	// which app created the staging Check Run, so the promotion gate blocks the
	// production apply even though a same-named passing run exists.
	t.Run("unknown own app slug blocks apply", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		comments := make(chan string, 10)

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
			_ = json.NewEncoder(w).Encode(map[string]any{
				"total_count": 1,
				"check_runs": []map[string]any{
					{"id": 1, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "schemabot"}},
				},
			})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		h := &Handler{
			ghClients:                  ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
			logger:                     testLogger(),
			priorEnvCheckMaxAttempts:   1,
			priorEnvCheckRetryInterval: time.Nanosecond,
		}

		blocked := h.checkPriorEnvViaGitHub(t.Context(), repo, pr, "orders", "production", "staging", 12345)
		assert.True(t, blocked, "unverifiable check run ownership must block the promotion gate")

		select {
		case body := <-comments:
			assert.Contains(t, body, "Apply Blocked")
			assert.Contains(t, body, "staging")
		default:
			t.Fatal("expected a comment explaining the verification failure")
		}
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
					{"id": 1, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "schemabot"}},
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"total_count": len(checkRuns),
				"check_runs":  checkRuns,
			})
		})

		installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")
		h := &Handler{
			ghClients:                  ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
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

		installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")
		factory := &fakeClientFactory{client: installClient}

		h := &Handler{
			ghClients:                  ghclient.NewSingleClientSet(defaultAppName, factory),
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
