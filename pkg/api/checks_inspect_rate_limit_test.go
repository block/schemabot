package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/auth"
	ghclient "github.com/block/schemabot/pkg/github"
)

// inspectGitHubCalls counts every GitHub call an inspection makes, including
// resolving the installation client, so a refused request can be shown to
// have cost the installation nothing.
type inspectGitHubCalls struct {
	client      *inspectGitHubClient
	resolutions int
}

func (c *inspectGitHubCalls) total() int {
	return c.resolutions + c.client.prCalls + c.client.runCalls
}

// newRateLimitedInspectService serves the inspection with the given budget and
// a GitHub client that answers every read, so a test can tell an admitted
// inspection (200) from a limited one (429) and count what each one cost.
func newRateLimitedInspectService(t *testing.T, limits CallerRateLimitConfig, authCfg AuthConfig) (*Service, *inspectGitHubCalls) {
	t.Helper()
	cfg := inspectTestConfig()
	cfg.RateLimits = RateLimitsConfig{ChecksInspect: limits}
	cfg.Auth = authCfg
	svc := New(&inspectStorage{checks: &inspectCheckStore{}, applies: &inspectApplyStore{}},
		cfg, nil, slog.New(slog.DiscardHandler))

	calls := &inspectGitHubCalls{client: &inspectGitHubClient{
		prInfo: &ghclient.PullRequestInfo{HeadSHA: "abc123", State: "open"},
	}}
	svc.checksInspectClientFor = func(context.Context, *ServerConfig, string, *slog.Logger) (checksInspectClient, error) {
		calls.resolutions++
		return calls.client, nil
	}
	return svc, calls
}

// inspectAs inspects acme/store#7 as the given caller. An empty caller leaves
// the request unauthenticated.
func inspectAs(t *testing.T, svc *Service, caller string) *httptest.ResponseRecorder {
	t.Helper()
	return inspectQueryAs(t, svc, caller, "/api/checks/inspect?repo=acme/store&pull_request=7")
}

func inspectQueryAs(t *testing.T, svc *Service, caller, target string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	ctx := t.Context()
	if caller != "" {
		ctx = auth.WithUser(ctx, &auth.User{Subject: caller})
	}
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func TestChecksInspectRateLimitValidatesBeforeChargingCaller(t *testing.T) {
	svc, calls := newRateLimitedInspectService(t, CallerRateLimitConfig{
		PerCaller: RateLimitBudgetConfig{RequestsPerMinute: 60, Burst: 1},
	}, AuthConfig{})

	invalid := inspectQueryAs(t, svc, "", "/api/checks/inspect?repo=malformed&pull_request=7")
	require.Equal(t, http.StatusBadRequest, invalid.Code, invalid.Body.String())
	assert.Zero(t, calls.total(), "an invalid target must not reach GitHub")

	valid := inspectAs(t, svc, "")
	require.Equal(t, http.StatusOK, valid.Code, valid.Body.String())
	assert.Positive(t, calls.total(), "a valid target consumes the available token")

	limited := inspectAs(t, svc, "")
	assert.Equal(t, http.StatusTooManyRequests, limited.Code, limited.Body.String())
}

// A dashboard polling a pull request's check state is served while it stays
// inside its budget. The inspection that exceeds it is refused with a retryable
// 429 before any GitHub call is made, so a runaway poller cannot spend the
// installation quota the merge gate's own Check Run writes depend on.
func TestChecksInspectRateLimitRefusesExhaustedCallerBeforeCallingGitHub(t *testing.T) {
	svc, calls := newRateLimitedInspectService(t, CallerRateLimitConfig{
		PerCaller: RateLimitBudgetConfig{RequestsPerMinute: 60, Burst: 2},
	}, AuthConfig{Type: "forward_auth"})

	for i := range 2 {
		w := inspectAs(t, svc, "dashboard@example.com")
		require.Equal(t, http.StatusOK, w.Code, "inspection %d should be within the burst: %s", i, w.Body.String())
	}
	require.Equal(t, 2, calls.client.prCalls, "each admitted inspection reads the pull request")
	require.Positive(t, calls.client.runCalls, "each admitted inspection reads the expected Check Runs")

	callsBefore := calls.total()
	w := inspectAs(t, svc, "dashboard@example.com")
	require.Equal(t, http.StatusTooManyRequests, w.Code, w.Body.String())
	assert.Equal(t, "1", w.Header().Get("Retry-After"))

	var resp apitypes.ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, apitypes.ErrCodeRateLimited, resp.ErrorCode)
	assert.Equal(t, 1, resp.RetryAfterSeconds)
	assert.Contains(t, resp.Error, apitypes.ChecksInspectRateLimitCallerReason)
	assert.True(t, apitypes.IsRetryableErrorCode(resp.ErrorCode), "a rate limit is retryable after the advertised delay")

	assert.Equal(t, callsBefore, calls.total(), "a limited inspection must not reach GitHub")
}

// One caller in a polling loop must not lock every other operator out of the
// inspection.
func TestChecksInspectRateLimitIsolatesCallers(t *testing.T) {
	svc, _ := newRateLimitedInspectService(t, CallerRateLimitConfig{
		PerCaller: RateLimitBudgetConfig{RequestsPerMinute: 60, Burst: 1},
	}, AuthConfig{Type: "forward_auth"})

	require.Equal(t, http.StatusOK, inspectAs(t, svc, "noisy@example.com").Code)
	require.Equal(t, http.StatusTooManyRequests, inspectAs(t, svc, "noisy@example.com").Code)

	w := inspectAs(t, svc, "quiet@example.com")
	assert.Equal(t, http.StatusOK, w.Code, "a different caller has its own budget: %s", w.Body.String())
}

// A server that does not authenticate callers charges every inspection to one
// shared budget, and the refusal names that budget rather than the caller.
func TestChecksInspectRateLimitNamesTheSharedBudgetWhenCallersAreNotAuthenticated(t *testing.T) {
	svc, _ := newRateLimitedInspectService(t, CallerRateLimitConfig{
		PerCaller: RateLimitBudgetConfig{RequestsPerMinute: 60, Burst: 1},
	}, AuthConfig{})

	require.Equal(t, http.StatusOK, inspectAs(t, svc, "").Code)

	w := inspectAs(t, svc, "")
	require.Equal(t, http.StatusTooManyRequests, w.Code, w.Body.String())
	var resp apitypes.ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Contains(t, resp.Error, apitypes.ChecksInspectRateLimitSharedReason)
}

// Rate limiting is off when the config disables it, so a deployment whose
// legitimate polling does not fit the budget has an escape hatch.
func TestChecksInspectRateLimitDisabledAdmitsEverything(t *testing.T) {
	disabled := false
	svc, calls := newRateLimitedInspectService(t, CallerRateLimitConfig{
		Enabled:   &disabled,
		PerCaller: RateLimitBudgetConfig{RequestsPerMinute: 60, Burst: 1},
	}, AuthConfig{Type: "forward_auth"})

	for i := range 10 {
		w := inspectAs(t, svc, "dashboard@example.com")
		require.Equal(t, http.StatusOK, w.Code, "inspection %d should be admitted with limits disabled: %s", i, w.Body.String())
	}
	assert.Equal(t, 10, calls.client.prCalls)
}

func TestChecksInspectRateLimitDefaults(t *testing.T) {
	cfg := &ServerConfig{}

	assert.True(t, cfg.ChecksInspectRateLimitEnabled(), "rate limiting is on unless a deployment turns it off")
	// Pinned by value: the default is sized against the installation's GitHub
	// quota, so changing it is a decision to re-derive, not a refactor.
	assert.Equal(t, 6, cfg.ChecksInspectPerCallerRateLimit().RequestsPerMinute)
	assert.Equal(t, 10, cfg.ChecksInspectPerCallerRateLimit().Burst)

	svc := New(&inspectStorage{checks: &inspectCheckStore{}, applies: &inspectApplyStore{}},
		inspectTestConfig(), nil, slog.New(slog.DiscardHandler))
	assert.NotNil(t, svc.checksInspectLimiter, "an unconfigured server enforces the default budget")

	cfg.RateLimits.ChecksInspect.PerCaller = RateLimitBudgetConfig{Burst: 3}
	assert.Equal(t, defaultChecksInspectPerCallerRequestsPerMinute, cfg.ChecksInspectPerCallerRateLimit().RequestsPerMinute)
	assert.Equal(t, 3, cfg.ChecksInspectPerCallerRateLimit().Burst)
}
