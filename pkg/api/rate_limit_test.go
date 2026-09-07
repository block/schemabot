package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/auth"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/testctx"
)

// newRateLimitedPullService configures two MySQL databases on one production
// deployment with the given pull budgets, and a data plane that answers every
// pull successfully, so a test can tell an admitted request (200) from a
// limited one (429). Callers are authenticated, the configuration under which
// each one carries an identity of its own.
func newRateLimitedPullService(t *testing.T, limits EndpointRateLimitConfig) (*Service, *mockTernClient) {
	t.Helper()
	return newRateLimitedPullServiceWithAuth(t, limits, AuthConfig{Type: "forward_auth"})
}

// newRateLimitedPullServiceWithAuth is newRateLimitedPullService with the auth
// configuration named, for tests that turn on what a refused caller is told.
func newRateLimitedPullServiceWithAuth(t *testing.T, limits EndpointRateLimitConfig, authCfg AuthConfig) (*Service, *mockTernClient) {
	t.Helper()
	client := &mockTernClient{
		pullSchemaResp: &ternv1.PullSchemaResponse{
			Database:    "orders",
			Type:        storage.DatabaseTypeMySQL,
			Environment: "production",
			Namespaces: map[string]*ternv1.PulledNamespace{
				"orders": {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
			},
			TableCount: 1,
		},
	}
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"orders": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"production": {Target: "orders-production", Deployment: "primary"},
				},
			},
			"payments": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"production": {Target: "payments-production", Deployment: "primary"},
				},
			},
		},
		TernDeployments: TernConfig{
			"primary": {"production": "tern.example.com:80"},
		},
		RateLimits: RateLimitsConfig{Pull: limits},
		Auth:       authCfg,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorage{}, cfg, map[string]tern.Client{"primary/production": client}, logger), client
}

// pullAs issues a pull for the given database as the given caller. An empty
// caller leaves the request unauthenticated.
func pullAs(t *testing.T, svc *Service, caller, database string) *httptest.ResponseRecorder {
	t.Helper()
	return pullAsInEnvironment(t, svc, caller, database, "production")
}

// pullAsInEnvironment is pullAs for a request naming an environment other than
// the configured one, the shape a caller can invent freely.
func pullAsInEnvironment(t *testing.T, svc *Service, caller, database, environment string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	ctx := t.Context()
	if caller != "" {
		ctx = auth.WithUser(ctx, &auth.User{Subject: caller})
	}
	body := `{"database":"` + database + `","environment":"` + environment + `","type":"mysql"}`
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/pull", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

// A caller that exhausts its pull budget is refused with a 429 that names the
// retryable error code and tells it how long to wait, in both the header and
// the body, so a client that only reads one of the two still learns the delay.
func TestPullRateLimitRefusesExhaustedCallerWith429(t *testing.T) {
	svc, client := newRateLimitedPullService(t, EndpointRateLimitConfig{
		PerCaller: RateLimitBudgetConfig{RequestsPerMinute: 60, Burst: 2},
	})

	for i := range 2 {
		w := pullAs(t, svc, "operator@example.com", "orders")
		require.Equal(t, http.StatusOK, w.Code, "request %d should be within the burst: %s", i, w.Body.String())
	}

	pullsBefore := len(client.pullSchemaReqs)
	w := pullAs(t, svc, "operator@example.com", "orders")
	require.Equal(t, http.StatusTooManyRequests, w.Code, w.Body.String())
	assert.Equal(t, "1", w.Header().Get("Retry-After"))

	var resp apitypes.ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, apitypes.ErrCodeRateLimited, resp.ErrorCode)
	assert.Equal(t, 1, resp.RetryAfterSeconds)
	assert.Contains(t, resp.Error, "too many pull requests from this caller")
	assert.True(t, apitypes.IsRetryableErrorCode(resp.ErrorCode), "a rate limit is retryable after the advertised delay")

	assert.Len(t, client.pullSchemaReqs, pullsBefore, "a limited request must not reach the data plane")
}

// A server that does not authenticate callers has no identities to tell them
// apart, so every client charges the same per-caller bucket. The refusal says
// so rather than blaming the operator reading it, who may have made only one of
// the requests that spent the budget, and who would otherwise go looking for a
// runaway loop in their own tooling instead of at the server's auth setting.
func TestPullRateLimitNamesTheSharedBudgetWhenCallersAreNotAuthenticated(t *testing.T) {
	svc, _ := newRateLimitedPullServiceWithAuth(t, EndpointRateLimitConfig{
		PerCaller: RateLimitBudgetConfig{RequestsPerMinute: 60, Burst: 1},
	}, AuthConfig{})

	require.Equal(t, http.StatusOK, pullAs(t, svc, "", "orders").Code)

	w := pullAs(t, svc, "", "payments")
	require.Equal(t, http.StatusTooManyRequests, w.Code, w.Body.String())

	var resp apitypes.ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Contains(t, resp.Error, "this server does not authenticate callers")
	assert.NotContains(t, resp.Error, "from this caller")
}

// The per-caller budget isolates callers: one client in a retry loop must not
// lock every other operator out of the endpoint.
func TestPullRateLimitIsolatesCallers(t *testing.T) {
	svc, _ := newRateLimitedPullService(t, EndpointRateLimitConfig{
		PerCaller: RateLimitBudgetConfig{RequestsPerMinute: 60, Burst: 1},
		PerTarget: RateLimitBudgetConfig{RequestsPerMinute: 6000, Burst: 100},
	})

	require.Equal(t, http.StatusOK, pullAs(t, svc, "noisy@example.com", "orders").Code)
	require.Equal(t, http.StatusTooManyRequests, pullAs(t, svc, "noisy@example.com", "orders").Code)

	w := pullAs(t, svc, "quiet@example.com", "orders")
	assert.Equal(t, http.StatusOK, w.Code, "a different caller has its own budget: %s", w.Body.String())
}

// The per-target budget protects one database from the aggregate of every
// caller reading it, and does not spill onto a different database.
func TestPullRateLimitBoundsTargetAcrossCallersAndIsolatesTargets(t *testing.T) {
	svc, _ := newRateLimitedPullService(t, EndpointRateLimitConfig{
		PerCaller: RateLimitBudgetConfig{RequestsPerMinute: 6000, Burst: 100},
		PerTarget: RateLimitBudgetConfig{RequestsPerMinute: 60, Burst: 2},
	})

	require.Equal(t, http.StatusOK, pullAs(t, svc, "caller-a@example.com", "orders").Code)
	require.Equal(t, http.StatusOK, pullAs(t, svc, "caller-b@example.com", "orders").Code)

	w := pullAs(t, svc, "caller-c@example.com", "orders")
	require.Equal(t, http.StatusTooManyRequests, w.Code, "the target budget is shared across callers")
	var resp apitypes.ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Contains(t, resp.Error, "too many pull requests for this database and environment")

	w = pullAs(t, svc, "caller-c@example.com", "payments")
	assert.Equal(t, http.StatusOK, w.Code, "a different database has its own budget: %s", w.Body.String())
}

// Rate limiting is off when the config disables it, so a deployment whose
// legitimate traffic does not fit the budgets has an escape hatch.
func TestPullRateLimitDisabledAdmitsEverything(t *testing.T) {
	disabled := false
	svc, _ := newRateLimitedPullService(t, EndpointRateLimitConfig{
		Enabled:   &disabled,
		PerCaller: RateLimitBudgetConfig{RequestsPerMinute: 60, Burst: 1},
		PerTarget: RateLimitBudgetConfig{RequestsPerMinute: 60, Burst: 1},
	})

	for i := range 10 {
		w := pullAs(t, svc, "operator@example.com", "orders")
		require.Equal(t, http.StatusOK, w.Code, "request %d should be admitted with limits disabled: %s", i, w.Body.String())
	}
}

// A malformed request is rejected before any budget is spent, so a client
// whose requests never name a target cannot exhaust a budget with them.
func TestPullRateLimitNotSpentOnMalformedRequests(t *testing.T) {
	svc, _ := newRateLimitedPullService(t, EndpointRateLimitConfig{
		PerCaller: RateLimitBudgetConfig{RequestsPerMinute: 60, Burst: 1},
		PerTarget: RateLimitBudgetConfig{RequestsPerMinute: 60, Burst: 1},
	})
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	ctx := auth.WithUser(t.Context(), &auth.User{Subject: "operator@example.com"})
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/pull", strings.NewReader(`{"environment":"production"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())

	w = pullAs(t, svc, "operator@example.com", "orders")
	assert.Equal(t, http.StatusOK, w.Code, "the rejected request must not have spent the budget: %s", w.Body.String())
}

// A request naming a database this server does not route still spends budget:
// resolving the route is server-side work, and a client looping on an unknown
// database is exactly the runaway the budget exists to bound.
func TestPullRateLimitSpentOnUnroutableRequests(t *testing.T) {
	svc, _ := newRateLimitedPullService(t, EndpointRateLimitConfig{
		PerCaller: RateLimitBudgetConfig{RequestsPerMinute: 60, Burst: 1},
		PerTarget: RateLimitBudgetConfig{RequestsPerMinute: 6000, Burst: 100},
	})

	w := pullAs(t, svc, "operator@example.com", "nonexistent")
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())

	w = pullAs(t, svc, "operator@example.com", "orders")
	assert.Equal(t, http.StatusTooManyRequests, w.Code, "the unroutable request spent the caller's budget")
}

// With API auth disabled there are no identities to tell callers apart, so
// every request shares the anonymous caller's budget and the per-caller lane
// acts as a process-wide budget.
func TestPullRateLimitChargesUnauthenticatedRequestsToOneBudget(t *testing.T) {
	svc, _ := newRateLimitedPullService(t, EndpointRateLimitConfig{
		PerCaller: RateLimitBudgetConfig{RequestsPerMinute: 60, Burst: 1},
		PerTarget: RateLimitBudgetConfig{RequestsPerMinute: 6000, Burst: 100},
	})

	require.Equal(t, http.StatusOK, pullAs(t, svc, "", "orders").Code)

	w := pullAs(t, svc, "", "payments")
	assert.Equal(t, http.StatusTooManyRequests, w.Code, "unauthenticated requests share one caller budget")
}

func TestPullRateLimitDefaults(t *testing.T) {
	cfg := &ServerConfig{}

	assert.True(t, cfg.PullRateLimitEnabled(), "rate limiting is on unless a deployment turns it off")
	assert.Equal(t, defaultPullPerCallerRequestsPerMinute, cfg.PullPerCallerRateLimit().RequestsPerMinute)
	assert.Equal(t, defaultPullPerCallerBurst, cfg.PullPerCallerRateLimit().Burst)
	assert.Equal(t, defaultPullPerTargetRequestsPerMinute, cfg.PullPerTargetRateLimit().RequestsPerMinute)
	assert.Equal(t, defaultPullPerTargetBurst, cfg.PullPerTargetRateLimit().Burst)

	// A partially configured budget keeps the default for the field it omits.
	cfg.RateLimits.Pull.PerTarget = RateLimitBudgetConfig{RequestsPerMinute: 5}
	assert.Equal(t, 5, cfg.PullPerTargetRateLimit().RequestsPerMinute)
	assert.Equal(t, defaultPullPerTargetBurst, cfg.PullPerTargetRateLimit().Burst)
}

// A negative budget is a typo, not a way to disable enforcement, so it fails
// validation at startup instead of silently admitting every request.
func TestValidateRejectsNegativeRateLimits(t *testing.T) {
	cases := []struct {
		name    string
		limits  RateLimitsConfig
		wantErr string
	}{
		{
			name:    "negative per-caller rate",
			limits:  RateLimitsConfig{Pull: EndpointRateLimitConfig{PerCaller: RateLimitBudgetConfig{RequestsPerMinute: -1}}},
			wantErr: "rate_limits.pull.per_caller.requests_per_minute must not be negative",
		},
		{
			name:    "negative per-caller burst",
			limits:  RateLimitsConfig{Pull: EndpointRateLimitConfig{PerCaller: RateLimitBudgetConfig{Burst: -5}}},
			wantErr: "rate_limits.pull.per_caller.burst must not be negative",
		},
		{
			name:    "negative per-target rate",
			limits:  RateLimitsConfig{Pull: EndpointRateLimitConfig{PerTarget: RateLimitBudgetConfig{RequestsPerMinute: -30}}},
			wantErr: "rate_limits.pull.per_target.requests_per_minute must not be negative",
		},
		{
			name:    "negative per-target burst",
			limits:  RateLimitsConfig{Pull: EndpointRateLimitConfig{PerTarget: RateLimitBudgetConfig{Burst: -1}}},
			wantErr: "rate_limits.pull.per_target.burst must not be negative",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &ServerConfig{
				Databases: map[string]DatabaseConfig{
					"orders": {
						Type: storage.DatabaseTypeMySQL,
						Environments: map[string]EnvironmentConfig{
							"production": {Target: "orders-production", Deployment: "primary"},
						},
					},
				},
				RateLimits: tc.limits,
			}
			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// The per-target key cannot collide across databases or environments, so one
// target's traffic is never charged to another's budget.
func TestTargetRateLimitKeyIsUnambiguous(t *testing.T) {
	assert.NotEqual(t, targetRateLimitKey("orders", "production"), targetRateLimitKey("orders-production", ""))
	assert.NotEqual(t, targetRateLimitKey("orders", "staging"), targetRateLimitKey("orders", "production"))
	assert.Equal(t, targetRateLimitKey("orders", "production"), targetRateLimitKey("orders", "production"))
}

// The environment on a pull comes from the request body, so a caller can
// invent one. The budget is still keyed on the name the request used, but the
// decision metric records only configured environments: an arbitrary string
// must never mint a time series per value.
func TestPullRateLimitMetricClampsCallerSuppliedEnvironment(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		cleanupCtx, cancel := testctx.Cleanup(t, 30*time.Second)
		defer cancel()
		require.NoError(t, mp.Shutdown(cleanupCtx))
	})

	svc, _ := newRateLimitedPullService(t, EndpointRateLimitConfig{
		PerCaller: RateLimitBudgetConfig{RequestsPerMinute: 60, Burst: 1},
		PerTarget: RateLimitBudgetConfig{RequestsPerMinute: 6000, Burst: 100},
	})

	// Two requests naming environments that exist nowhere in the config: the
	// first spends the caller's burst, the second is refused, so both an allow
	// and a limit decision are recorded for an unconfigured environment.
	pullAsInEnvironment(t, svc, "operator@example.com", "orders", "invented-one")
	w := pullAsInEnvironment(t, svc, "operator@example.com", "orders", "invented-two")
	require.Equal(t, http.StatusTooManyRequests, w.Code, w.Body.String())

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	environments := make(map[string]int)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.rate_limit_decisions.total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "the decision counter should be an int64 sum")
			for _, dp := range sum.DataPoints {
				environment, found := dp.Attributes.Value(attribute.Key("environment"))
				require.True(t, found, "every decision carries an environment attribute")
				environments[environment.Emit()]++
			}
		}
	}

	require.NotEmpty(t, environments, "the pulls should have recorded rate limit decisions")
	for environment := range environments {
		assert.Equal(t, "unconfigured", environment, "a caller-supplied environment must be clamped before it reaches a metric")
	}
}
