package auth_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/auth"
)

// verifiedSubject runs req through mw and returns the subject the wrapped
// handler saw as verified. It fails the test when the lane admits the request
// without marking its identity verified.
func verifiedSubject(t *testing.T, mw func(http.Handler) http.Handler, req *http.Request) string {
	t.Helper()
	var subject string
	var ok bool
	rec := httptest.NewRecorder()
	mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		subject, ok = auth.VerifiedSubject(r.Context())
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.True(t, ok, "the lane's identity must be marked verified")
	return subject
}

// Records that attribute work to a person name only a verified subject. A
// valid OIDC token and the local runtime token are both checked by the server,
// so the identity each lane establishes is verified, not merely authenticated.
// The forward-auth lanes are pinned in TestForwardAuth_IdentityProvenance.
func TestTokenLanesEstablishVerifiedIdentity(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		p := newTestOIDCProvider(t)
		authz := newAuthorizer(t, p, auth.OIDCConfig{Audience: "schemabot"})
		token := p.issueToken(t, "user@example.com", "schemabot", "groups", []string{"a"}, time.Now().Add(time.Hour))
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		assert.Equal(t, "user@example.com", verifiedSubject(t, authz.Middleware, req))
	})

	t.Run("local token", func(t *testing.T) {
		token := strings.Repeat("a", 64)
		authz, err := auth.NewLocalAuthorizer(token, slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/locks/acquire", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		// A forwarded header must not become the recorded identity on this lane.
		req.Header.Set("X-Forwarded-User", "claimed-human")

		assert.Equal(t, "local-runtime", verifiedSubject(t, authz.Middleware, req))
	})
}
