package auth_test

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
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
// endpoints for testing JWT validation.
type testOIDCProvider struct {
	server *httptest.Server
	key    *rsa.PrivateKey
	keyID  string
}

func newTestOIDCProvider(t *testing.T) *testOIDCProvider {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	p := &testOIDCProvider{
		key:   key,
		keyID: "test-key-1",
	}

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

// issueToken creates a signed JWT with the given claims.
func (p *testOIDCProvider) issueToken(t *testing.T, subject string, groups []string, expiry time.Time) string {
	t.Helper()

	signer, err := jose.NewSigner(jose.SigningKey{
		Algorithm: jose.RS256,
		Key:       p.key,
	}, (&jose.SignerOptions{}).WithType("JWT").WithHeader(jose.HeaderKey("kid"), p.keyID))
	require.NoError(t, err)

	now := time.Now()
	claims := map[string]any{
		"iss":    p.server.URL,
		"sub":    subject,
		"aud":    "schemabot",
		"iat":    now.Unix(),
		"exp":    expiry.Unix(),
		"groups": groups,
	}

	raw, err := jwt.Signed(signer).Claims(claims).Serialize()
	require.NoError(t, err)
	return raw
}

// issueTokenWithCustomClaim creates a signed JWT with groups under a custom claim name.
func (p *testOIDCProvider) issueTokenWithCustomClaim(t *testing.T, subject string, claimName string, groups []string, expiry time.Time) string {
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
		"aud":     "schemabot",
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

func TestOIDCAuthorizerValidToken(t *testing.T) {
	provider := newTestOIDCProvider(t)

	authz, err := auth.NewOIDCAuthorizer(t.Context(), auth.OIDCConfig{
		Issuer:     provider.server.URL,
		Audience:   "schemabot",
		AdminGroup: "schema-admins",
	}, testLogger())
	require.NoError(t, err)

	token := provider.issueToken(t, "user@example.com", []string{"schema-admins", "developers"}, time.Now().Add(time.Hour))

	var captured *auth.User
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = auth.UserFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	handler := authz.Middleware(inner)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, captured)
	assert.Equal(t, "user@example.com", captured.Subject)
	assert.Contains(t, captured.Groups, "schema-admins")
	assert.Contains(t, captured.Groups, "developers")
}

func TestOIDCAuthorizerMissingToken(t *testing.T) {
	provider := newTestOIDCProvider(t)

	authz, err := auth.NewOIDCAuthorizer(t.Context(), auth.OIDCConfig{
		Issuer:     provider.server.URL,
		Audience:   "schemabot",
		AdminGroup: "schema-admins",
	}, testLogger())
	require.NoError(t, err)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called")
	})

	handler := authz.Middleware(inner)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	var body map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	assert.Contains(t, body["error"], "invalid or missing authentication token")
}

func TestOIDCAuthorizerExpiredToken(t *testing.T) {
	provider := newTestOIDCProvider(t)

	authz, err := auth.NewOIDCAuthorizer(t.Context(), auth.OIDCConfig{
		Issuer:     provider.server.URL,
		Audience:   "schemabot",
		AdminGroup: "schema-admins",
	}, testLogger())
	require.NoError(t, err)

	token := provider.issueToken(t, "user@example.com", []string{"schema-admins"}, time.Now().Add(-time.Hour))

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called for expired token")
	})

	handler := authz.Middleware(inner)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestOIDCAuthorizerWriteEndpointRequiresAdminGroup(t *testing.T) {
	provider := newTestOIDCProvider(t)

	authz, err := auth.NewOIDCAuthorizer(t.Context(), auth.OIDCConfig{
		Issuer:     provider.server.URL,
		Audience:   "schemabot",
		AdminGroup: "schema-admins",
	}, testLogger())
	require.NoError(t, err)

	// Token without the admin group
	token := provider.issueToken(t, "reader@example.com", []string{"developers"}, time.Now().Add(time.Hour))

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called without admin group on write endpoint")
	})

	handler := authz.Middleware(inner)

	// POST to a write endpoint
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/plan", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)

	var body map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	assert.Contains(t, body["error"], "schema-admins")
}

func TestOIDCAuthorizerWriteEndpointAllowedWithAdminGroup(t *testing.T) {
	provider := newTestOIDCProvider(t)

	authz, err := auth.NewOIDCAuthorizer(t.Context(), auth.OIDCConfig{
		Issuer:     provider.server.URL,
		Audience:   "schemabot",
		AdminGroup: "schema-admins",
	}, testLogger())
	require.NoError(t, err)

	token := provider.issueToken(t, "admin@example.com", []string{"schema-admins"}, time.Now().Add(time.Hour))

	var called bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	handler := authz.Middleware(inner)

	writeEndpoints := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/api/plan"},
		{http.MethodPost, "/api/apply"},
		{http.MethodPost, "/api/cutover"},
		{http.MethodPost, "/api/revert"},
		{http.MethodPost, "/api/skip-revert"},
		{http.MethodPost, "/api/stop"},
		{http.MethodPost, "/api/start"},
		{http.MethodPost, "/api/volume"},
		{http.MethodDelete, "/api/locks"},
		{http.MethodPost, "/api/locks/acquire"},
		{http.MethodPost, "/api/settings"},
		{http.MethodPost, "/api/rollback/plan"},
	}

	for _, ep := range writeEndpoints {
		t.Run(fmt.Sprintf("%s %s", ep.method, ep.path), func(t *testing.T) {
			called = false
			req := httptest.NewRequestWithContext(t.Context(), ep.method, ep.path, nil)
			req.Header.Set("Authorization", "Bearer "+token)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.True(t, called, "handler should be called for admin user")
		})
	}
}

func TestOIDCAuthorizerReadEndpointAllowedWithAnyValidToken(t *testing.T) {
	provider := newTestOIDCProvider(t)

	authz, err := auth.NewOIDCAuthorizer(t.Context(), auth.OIDCConfig{
		Issuer:     provider.server.URL,
		Audience:   "schemabot",
		AdminGroup: "schema-admins",
	}, testLogger())
	require.NoError(t, err)

	// Token without admin group — should still work for read endpoints.
	token := provider.issueToken(t, "reader@example.com", []string{"developers"}, time.Now().Add(time.Hour))

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := authz.Middleware(inner)

	readEndpoints := []string{
		"/api/status",
		"/api/progress/mydb",
		"/api/progress/apply/apply-123",
		"/api/logs/mydb",
		"/api/logs",
		"/api/locks",
		"/api/locks/mydb/mysql",
		"/api/databases/mydb/environments",
		"/api/settings",
		"/api/settings/some-key",
		"/api/history/mydb",
	}

	for _, path := range readEndpoints {
		t.Run("GET "+path, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
			req.Header.Set("Authorization", "Bearer "+token)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestOIDCAuthorizerSkipsAuthForExcludedPaths(t *testing.T) {
	provider := newTestOIDCProvider(t)

	authz, err := auth.NewOIDCAuthorizer(t.Context(), auth.OIDCConfig{
		Issuer:     provider.server.URL,
		Audience:   "schemabot",
		AdminGroup: "schema-admins",
	}, testLogger())
	require.NoError(t, err)

	var called bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	handler := authz.Middleware(inner)

	excludedPaths := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/health"},
		{http.MethodGet, "/metrics"},
		{http.MethodPost, "/webhook"},
		{http.MethodPost, "/webhook/github"},
		{http.MethodGet, "/tern-health/default/staging"},
	}

	for _, ep := range excludedPaths {
		t.Run(fmt.Sprintf("%s %s", ep.method, ep.path), func(t *testing.T) {
			called = false
			// No Authorization header — these paths skip auth.
			req := httptest.NewRequestWithContext(t.Context(), ep.method, ep.path, nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.True(t, called, "handler should be called for excluded path")
		})
	}
}

func TestOIDCAuthorizerInvalidBearerScheme(t *testing.T) {
	provider := newTestOIDCProvider(t)

	authz, err := auth.NewOIDCAuthorizer(t.Context(), auth.OIDCConfig{
		Issuer:     provider.server.URL,
		Audience:   "schemabot",
		AdminGroup: "schema-admins",
	}, testLogger())
	require.NoError(t, err)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called")
	})

	handler := authz.Middleware(inner)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestOIDCAuthorizerCustomGroupsClaim(t *testing.T) {
	provider := newTestOIDCProvider(t)

	authz, err := auth.NewOIDCAuthorizer(t.Context(), auth.OIDCConfig{
		Issuer:      provider.server.URL,
		Audience:    "schemabot",
		AdminGroup:  "dba-team",
		GroupsClaim: "custom_groups",
	}, testLogger())
	require.NoError(t, err)

	token := provider.issueTokenWithCustomClaim(t, "admin@example.com", "custom_groups", []string{"dba-team"}, time.Now().Add(time.Hour))

	var captured *auth.User
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = auth.UserFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	handler := authz.Middleware(inner)

	// Test a write endpoint to verify the custom groups claim is used for authorization.
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/plan", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, captured)
	assert.Contains(t, captured.Groups, "dba-team")
}

func TestOIDCAuthorizerNoAudienceValidation(t *testing.T) {
	provider := newTestOIDCProvider(t)

	// No audience configured — should accept tokens regardless of aud claim.
	authz, err := auth.NewOIDCAuthorizer(t.Context(), auth.OIDCConfig{
		Issuer:     provider.server.URL,
		AdminGroup: "schema-admins",
	}, testLogger())
	require.NoError(t, err)

	token := provider.issueToken(t, "user@example.com", []string{"schema-admins"}, time.Now().Add(time.Hour))

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := authz.Middleware(inner)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestNewOIDCAuthorizerValidation(t *testing.T) {
	t.Run("missing issuer", func(t *testing.T) {
		_, err := auth.NewOIDCAuthorizer(t.Context(), auth.OIDCConfig{
			AdminGroup: "admins",
		}, testLogger())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "issuer")
	})

	t.Run("missing admin_group", func(t *testing.T) {
		_, err := auth.NewOIDCAuthorizer(t.Context(), auth.OIDCConfig{
			Issuer: "https://example.com",
		}, testLogger())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "admin_group")
	})
}

func TestOIDCAuthorizerTokenWithoutGroups(t *testing.T) {
	provider := newTestOIDCProvider(t)

	authz, err := auth.NewOIDCAuthorizer(t.Context(), auth.OIDCConfig{
		Issuer:     provider.server.URL,
		Audience:   "schemabot",
		AdminGroup: "schema-admins",
	}, testLogger())
	require.NoError(t, err)

	// Issue a token without any groups claim.
	signer, err := jose.NewSigner(jose.SigningKey{
		Algorithm: jose.RS256,
		Key:       provider.key,
	}, (&jose.SignerOptions{}).WithType("JWT").WithHeader(jose.HeaderKey("kid"), provider.keyID))
	require.NoError(t, err)

	claims := map[string]any{
		"iss": provider.server.URL,
		"sub": "nogroupuser@example.com",
		"aud": "schemabot",
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(time.Hour).Unix(),
	}
	token, err := jwt.Signed(signer).Claims(claims).Serialize()
	require.NoError(t, err)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := authz.Middleware(inner)

	// Read endpoint should succeed with no groups.
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Write endpoint should be denied — no groups means no admin group membership.
	req = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/plan", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusForbidden, rec.Code)
}
