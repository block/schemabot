package auth_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/auth"
)

func TestNoneAuthorizerSetsAnonymousUser(t *testing.T) {
	var captured *auth.User
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = auth.UserFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	handler := auth.NoneAuthorizer{}.Middleware(inner)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, captured)
	assert.Equal(t, "anonymous", captured.Subject)
}

func TestNoneAuthorizerPassesAllMethods(t *testing.T) {
	methods := []string{http.MethodGet, http.MethodPost, http.MethodDelete}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})

			handler := auth.NoneAuthorizer{}.Middleware(inner)
			req := httptest.NewRequestWithContext(t.Context(), method, "/api/plan", nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}
