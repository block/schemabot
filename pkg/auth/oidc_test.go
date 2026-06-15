package auth_test

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/auth"
)

// testOIDCProvider is a minimal OIDC provider that serves discovery and JWKS
// endpoints and signs JWTs, for exercising token validation in tests.
type testOIDCProvider struct {
	server *httptest.Server
	key    *rsa.PrivateKey
	keyID  string
}

func newTestOIDCProvider(t *testing.T) *testOIDCProvider {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	p := &testOIDCProvider{key: key, keyID: "test-key-1"}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", p.handleDiscovery)
	mux.HandleFunc("/keys", p.handleJWKS)

	p.server = httptest.NewServer(mux)
	t.Cleanup(p.server.Close)

	return p
}

func (p *testOIDCProvider) handleDiscovery(w http.ResponseWriter, _ *http.Request) {
	discovery := map[string]string{
		"issuer":                 p.server.URL,
		"jwks_uri":               p.server.URL + "/keys",
		"authorization_endpoint": p.server.URL + "/authorize",
		"token_endpoint":         p.server.URL + "/token",
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(discovery); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (p *testOIDCProvider) handleJWKS(w http.ResponseWriter, _ *http.Request) {
	jwk := jose.JSONWebKey{
		Key:       &p.key.PublicKey,
		KeyID:     p.keyID,
		Algorithm: string(jose.RS256),
		Use:       "sig",
	}
	jwks := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{jwk}}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(jwks); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// issueToken signs a JWT with the groups value under the given claim name.
// groups is `any` so callers can supply a []string, a single string, or nil.
func (p *testOIDCProvider) issueToken(t *testing.T, subject, audience, claimName string, groups any, expiry time.Time) string {
	t.Helper()

	signer, err := jose.NewSigner(jose.SigningKey{
		Algorithm: jose.RS256,
		Key:       p.key,
	}, (&jose.SignerOptions{}).WithType("JWT").WithHeader(jose.HeaderKey("kid"), p.keyID))
	require.NoError(t, err)

	now := time.Now()
	claims := map[string]any{
		"iss":     p.server.URL,
		"sub":     subject,
		"aud":     audience,
		"iat":     now.Unix(),
		"exp":     expiry.Unix(),
		claimName: groups,
	}

	raw, err := jwt.Signed(signer).Claims(claims).Serialize()
	require.NoError(t, err)
	return raw
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func newAuthorizer(t *testing.T, p *testOIDCProvider, cfg auth.OIDCConfig) *auth.OIDCAuthorizer {
	t.Helper()
	if cfg.Issuer == "" {
		cfg.Issuer = p.server.URL
	}
	authz, err := auth.NewOIDCAuthorizer(t.Context(), cfg, testLogger())
	require.NoError(t, err)
	return authz
}

// okHandler records whether it ran and the user it saw.
func okHandler(ran *bool, captured **auth.User) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*ran = true
		if captured != nil {
			*captured = auth.UserFromContext(r.Context())
		}
		w.WriteHeader(http.StatusOK)
	})
}

func TestOIDCAuthorizerValidTokenAllowed(t *testing.T) {
	p := newTestOIDCProvider(t)
	authz := newAuthorizer(t, p, auth.OIDCConfig{Audience: "schemabot"})
	token := p.issueToken(t, "user@example.com", "schemabot", "groups", []string{"a", "b"}, time.Now().Add(time.Hour))

	var ran bool
	var captured *auth.User
	handler := authz.Middleware(okHandler(&ran, &captured))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, ran)
	require.NotNil(t, captured)
	assert.Equal(t, "user@example.com", captured.Subject)
	assert.Equal(t, []string{"a", "b"}, captured.Groups)
}

// A valid token grants access to write-method API endpoints too: tier/RBAC
// gating is not part of this authenticator yet.
func TestOIDCAuthorizerValidTokenAllowedForWriteMethods(t *testing.T) {
	p := newTestOIDCProvider(t)
	authz := newAuthorizer(t, p, auth.OIDCConfig{Audience: "schemabot"})
	token := p.issueToken(t, "user@example.com", "schemabot", "groups", nil, time.Now().Add(time.Hour))

	var ran bool
	handler := authz.Middleware(okHandler(&ran, nil))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/apply", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, ran)
}

func TestOIDCAuthorizerMissingTokenRejected(t *testing.T) {
	p := newTestOIDCProvider(t)
	authz := newAuthorizer(t, p, auth.OIDCConfig{Audience: "schemabot"})

	var ran bool
	handler := authz.Middleware(okHandler(&ran, nil))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Equal(t, "Bearer", rec.Header().Get("WWW-Authenticate"))
	assert.False(t, ran)
	var body map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	assert.Contains(t, body["error"], "invalid or missing authentication token")
}

func TestOIDCAuthorizerExpiredTokenRejected(t *testing.T) {
	p := newTestOIDCProvider(t)
	authz := newAuthorizer(t, p, auth.OIDCConfig{Audience: "schemabot"})
	token := p.issueToken(t, "user@example.com", "schemabot", "groups", nil, time.Now().Add(-time.Hour))

	var ran bool
	handler := authz.Middleware(okHandler(&ran, nil))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.False(t, ran)
}

func TestOIDCAuthorizerWrongAudienceRejected(t *testing.T) {
	p := newTestOIDCProvider(t)
	authz := newAuthorizer(t, p, auth.OIDCConfig{Audience: "schemabot"})
	token := p.issueToken(t, "user@example.com", "someone-else", "groups", nil, time.Now().Add(time.Hour))

	var ran bool
	handler := authz.Middleware(okHandler(&ran, nil))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.False(t, ran)
}

func TestOIDCAuthorizerCustomGroupsClaim(t *testing.T) {
	p := newTestOIDCProvider(t)
	authz := newAuthorizer(t, p, auth.OIDCConfig{Audience: "schemabot", GroupsClaim: "roles"})
	token := p.issueToken(t, "user@example.com", "schemabot", "roles", []string{"dba"}, time.Now().Add(time.Hour))

	var ran bool
	var captured *auth.User
	handler := authz.Middleware(okHandler(&ran, &captured))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, captured)
	assert.Equal(t, []string{"dba"}, captured.Groups)
}

func TestOIDCAuthorizerGroupsClaimAsSingleString(t *testing.T) {
	p := newTestOIDCProvider(t)
	authz := newAuthorizer(t, p, auth.OIDCConfig{Audience: "schemabot"})
	token := p.issueToken(t, "user@example.com", "schemabot", "groups", "solo", time.Now().Add(time.Hour))

	var ran bool
	var captured *auth.User
	handler := authz.Middleware(okHandler(&ran, &captured))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, captured)
	assert.Equal(t, []string{"solo"}, captured.Groups)
}

func TestNewOIDCAuthorizerRequiresAudience(t *testing.T) {
	p := newTestOIDCProvider(t)
	_, err := auth.NewOIDCAuthorizer(t.Context(), auth.OIDCConfig{Issuer: p.server.URL}, testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "audience")
}

func TestOIDCAuthorizerBypassesNonAPIPaths(t *testing.T) {
	p := newTestOIDCProvider(t)
	authz := newAuthorizer(t, p, auth.OIDCConfig{Audience: "schemabot"})

	for _, path := range []string{"/health", "/metrics", "/webhook", "/tern-health/cake/staging"} {
		t.Run(path, func(t *testing.T) {
			var ran bool
			handler := authz.Middleware(okHandler(&ran, nil))
			// No Authorization header — must still pass through.
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.True(t, ran)
		})
	}
}
