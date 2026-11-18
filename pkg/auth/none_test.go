package auth_test

import (
	"bytes"
	"log/slog"
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

// A deployment running with auth disabled must still surface unauthenticated
// mutating traffic: each write is logged with the transport address, so a
// direct-to-pod write (e.g. through a port-forward) leaves an audit line
// instead of passing silently.
func TestNoneAuthorizerLogsWrites(t *testing.T) {
	var logs bytes.Buffer
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := auth.NoneAuthorizer{Logger: slog.New(slog.NewTextHandler(&logs, nil))}.Middleware(inner)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/apply", nil)
	req.RemoteAddr = "127.0.0.1:52000"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	logged := logs.String()
	assert.Contains(t, logged, "write operation allowed without authentication")
	assert.Contains(t, logged, "path=/api/apply")
	assert.Contains(t, logged, "remote_addr=127.0.0.1:52000")
}

// Reads are visibility traffic — they are counted in the auth-decision metric
// but not logged, so status polling cannot flood the log.
func TestNoneAuthorizerDoesNotLogReads(t *testing.T) {
	var logs bytes.Buffer
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := auth.NoneAuthorizer{Logger: slog.New(slog.NewTextHandler(&logs, nil))}.Middleware(inner)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, logs.String())
}

// Infrastructure paths (webhooks carry their own HMAC authentication; probes
// carry none by design) are outside the API tiers and are not logged even on
// POST.
func TestNoneAuthorizerDoesNotLogInfraPaths(t *testing.T) {
	var logs bytes.Buffer
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := auth.NoneAuthorizer{Logger: slog.New(slog.NewTextHandler(&logs, nil))}.Middleware(inner)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, logs.String())
}
