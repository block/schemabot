package auth

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
