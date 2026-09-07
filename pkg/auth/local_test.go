package auth

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalAuthorizer(t *testing.T) {
	token := strings.Repeat("a", 64)
	authorizer, err := NewLocalAuthorizer(token, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	for _, path := range []string{"/api/plan", "/api/apply", "/health", "/livez", "/webhook"} {
		t.Run(path, func(t *testing.T) {
			for _, header := range []string{"", "Bearer wrong", token} {
				request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, nil)
				request.Header.Set("Authorization", header)
				response := httptest.NewRecorder()
				authorizer.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { require.FailNow(t, "unauthenticated handler reached") })).ServeHTTP(response, request)
				assert.Equal(t, http.StatusUnauthorized, response.Code)
			}
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, nil)
			request.Header.Set("Authorization", "Bearer "+token)
			request.Header.Set("X-Forwarded-User", "claimed-human")
			response := httptest.NewRecorder()
			authorizer.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				subject, ok := AuthenticatedSubject(r.Context())
				assert.True(t, ok)
				assert.Equal(t, "local-runtime", subject)
				w.WriteHeader(http.StatusNoContent)
			})).ServeHTTP(response, request)
			assert.Equal(t, http.StatusNoContent, response.Code)
		})
	}
}

func TestLocalAuthorizerRequiresToken(t *testing.T) {
	for _, token := range []string{"", "short", strings.Repeat("a", 32) + "\n"} {
		_, err := NewLocalAuthorizer(token, nil)
		require.Error(t, err)
	}
}

// Local probes remain authenticated and counted even though hosted probes
// bypass authentication. Both admitted and denied requests stay observable.
func TestLocalProbeAuthMetrics(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	defer func() { otel.SetMeterProvider(previous); require.NoError(t, provider.Shutdown(t.Context())) }()
	token := strings.Repeat("a", 64)
	authorizer, err := NewLocalAuthorizer(token, nil)
	require.NoError(t, err)
	for _, header := range []string{"", "Bearer " + token} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health", nil)
		req.Header.Set("Authorization", header)
		response := httptest.NewRecorder()
		authorizer.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })).ServeHTTP(response, req)
		if header == "" {
			assert.Equal(t, http.StatusUnauthorized, response.Code)
		} else {
			assert.Equal(t, http.StatusOK, response.Code)
		}
	}
	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))
	decisions := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "schemabot.auth_decisions.total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, point := range sum.DataPoints {
				reason, ok := point.Attributes.Value(attribute.Key("reason"))
				require.True(t, ok)
				decisions[reason.AsString()] += point.Value
			}
		}
	}
	assert.Equal(t, int64(1), decisions["local_token_invalid"])
	assert.Equal(t, int64(1), decisions["local_token"])
}
