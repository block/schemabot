package auth_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/auth"
)

const (
	trustedCIDR   = "192.0.2.0/24"
	trustedIPAddr = "192.0.2.10:443"
	untrustedAddr = "203.0.113.5:443"
	ingressSVID   = "spiffe://example.org/ns/ingress/sa/proxy"
)

// newForwardAuth builds a forward-auth authorizer around a handler that records
// the authenticated user, and returns both so tests can assert the response and
// the resolved identity.
func newForwardAuth(t *testing.T, cfg auth.ForwardAuthConfig) (http.Handler, *capturedUser) {
	t.Helper()
	authz, err := auth.NewForwardAuthAuthorizer(cfg, nil)
	require.NoError(t, err)
	captured := &capturedUser{}
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured.user = auth.UserFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	return authz.Middleware(inner), captured
}

type capturedUser struct {
	user *auth.User
}

func TestNewForwardAuthAuthorizer_RequiresTrustAnchor(t *testing.T) {
	_, err := auth.NewForwardAuthAuthorizer(auth.ForwardAuthConfig{
		WriteGroups: []string{"ops"},
	}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "trust anchor")
}

func TestNewForwardAuthAuthorizer_RejectsBadCIDR(t *testing.T) {
	_, err := auth.NewForwardAuthAuthorizer(auth.ForwardAuthConfig{
		TrustedProxyCIDRs: []string{"not-a-cidr"},
	}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "trusted_proxy_cidr")
}

func TestForwardAuth_UntrustedSourceDenied(t *testing.T) {
	handler, captured := newForwardAuth(t, auth.ForwardAuthConfig{
		TrustedProxyCIDRs: []string{trustedCIDR},
		WriteGroups:       []string{"ops"},
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	req.RemoteAddr = untrustedAddr
	req.Header.Set("X-Forwarded-User", "alice")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// A request that did not come through the trusted proxy is rejected before
	// any forwarded identity header is honored.
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Nil(t, captured.user)
}

func TestForwardAuth_TrustedCIDRReadAllowedForAnyUser(t *testing.T) {
	// With no read_groups configured, reads are open to any authenticated caller
	// arriving through the trusted proxy.
	handler, captured := newForwardAuth(t, auth.ForwardAuthConfig{
		TrustedProxyCIDRs: []string{trustedCIDR},
		WriteGroups:       []string{"ops"},
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	req.RemoteAddr = trustedIPAddr
	req.Header.Set("X-Forwarded-User", "alice")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, captured.user)
	assert.Equal(t, "alice", captured.user.Subject)
}

func TestForwardAuth_WriteRequiresWriteGroup(t *testing.T) {
	cfg := auth.ForwardAuthConfig{
		TrustedProxyCIDRs: []string{trustedCIDR},
		WriteGroups:       []string{"ops", "owners"},
	}

	t.Run("denied without write group", func(t *testing.T) {
		handler, captured := newForwardAuth(t, cfg)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/plan", nil)
		req.RemoteAddr = trustedIPAddr
		req.Header.Set("X-Forwarded-User", "alice")
		req.Header.Set("X-Forwarded-Groups", "readers,viewers")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Nil(t, captured.user)
	})

	t.Run("allowed with write group", func(t *testing.T) {
		handler, captured := newForwardAuth(t, cfg)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/plan", nil)
		req.RemoteAddr = trustedIPAddr
		req.Header.Set("X-Forwarded-User", "bob")
		req.Header.Set("X-Forwarded-Groups", "viewers,owners")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		require.NotNil(t, captured.user)
		assert.Equal(t, "bob", captured.user.Subject)
		assert.Equal(t, []string{"viewers", "owners"}, captured.user.Groups)
	})
}

func TestForwardAuth_SPIFFEOnlyMode(t *testing.T) {
	// SPIFFE-only (no CIDR) trusts a request purely by the SVID its XFCC carries —
	// the mesh sidecar mode. The source IP is irrelevant; only the SVID matters.
	cfg := auth.ForwardAuthConfig{
		TrustedProxySPIFFE: []string{ingressSVID},
		WriteGroups:        []string{"ops"},
	}

	t.Run("matching SVID is trusted regardless of source", func(t *testing.T) {
		handler, captured := newForwardAuth(t, cfg)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
		req.RemoteAddr = untrustedAddr
		req.Header.Set("X-Forwarded-Client-Cert", `URI=`+ingressSVID)
		req.Header.Set("X-Forwarded-User", "alice")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		require.NotNil(t, captured.user)
		assert.Equal(t, "alice", captured.user.Subject)
	})

	t.Run("missing or wrong SVID is not trusted", func(t *testing.T) {
		for _, xfcc := range []string{"", `URI=spiffe://example.org/ns/other/sa/attacker`} {
			handler, captured := newForwardAuth(t, cfg)
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
			req.RemoteAddr = trustedIPAddr
			if xfcc != "" {
				req.Header.Set("X-Forwarded-Client-Cert", xfcc)
			}
			req.Header.Set("X-Forwarded-User", "alice")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusUnauthorized, rec.Code)
			assert.Nil(t, captured.user)
		}
	})
}

func TestForwardAuth_TrustedViaSPIFFE(t *testing.T) {
	// CIDR + SPIFFE together is defense in depth: the request must both come from
	// a trusted source and carry a trusted SVID in XFCC.
	cfg := auth.ForwardAuthConfig{
		TrustedProxyCIDRs:  []string{trustedCIDR},
		TrustedProxySPIFFE: []string{ingressSVID},
		WriteGroups:        []string{"ops"},
	}

	t.Run("trusted source with matching SVID is trusted", func(t *testing.T) {
		handler, captured := newForwardAuth(t, cfg)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
		req.RemoteAddr = trustedIPAddr
		req.Header.Set("X-Forwarded-Client-Cert", `By=spiffe://example.org/ns/api/sa/schemabot;Hash=abc123;URI=`+ingressSVID)
		req.Header.Set("X-Forwarded-User", "alice")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		require.NotNil(t, captured.user)
		assert.Equal(t, "alice", captured.user.Subject)
	})

	t.Run("trusted source with wrong SVID is not trusted", func(t *testing.T) {
		handler, captured := newForwardAuth(t, cfg)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
		req.RemoteAddr = trustedIPAddr
		req.Header.Set("X-Forwarded-Client-Cert", `URI=spiffe://example.org/ns/other/sa/attacker`)
		req.Header.Set("X-Forwarded-User", "alice")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
		assert.Nil(t, captured.user)
	})

	t.Run("spoofed SVID from an untrusted source is not trusted", func(t *testing.T) {
		// A direct client outside the trusted CIDR cannot gain trust by forging
		// the XFCC header, because the transport gate fails first.
		handler, captured := newForwardAuth(t, cfg)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
		req.RemoteAddr = untrustedAddr
		req.Header.Set("X-Forwarded-Client-Cert", `URI=`+ingressSVID)
		req.Header.Set("X-Forwarded-User", "alice")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
		assert.Nil(t, captured.user)
	})
}

func TestForwardAuth_TrustedProxyWithoutUserDenied(t *testing.T) {
	handler, captured := newForwardAuth(t, auth.ForwardAuthConfig{
		TrustedProxyCIDRs: []string{trustedCIDR},
		WriteGroups:       []string{"ops"},
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	req.RemoteAddr = trustedIPAddr // trusted, but no user identity forwarded
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Nil(t, captured.user)
}

func TestForwardAuth_UnderscoreHeaderNotHonored(t *testing.T) {
	// A smuggled underscore variant (X_Forwarded_User) must not be read as the
	// identity: net/http does not fold it into the canonical dashed header.
	handler, captured := newForwardAuth(t, auth.ForwardAuthConfig{
		TrustedProxyCIDRs: []string{trustedCIDR},
		WriteGroups:       []string{"ops"},
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	req.RemoteAddr = trustedIPAddr
	req.Header.Set("X_Forwarded_User", "attacker")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Nil(t, captured.user)
}

func TestForwardAuth_ReadGroupsRestrictReads(t *testing.T) {
	cfg := auth.ForwardAuthConfig{
		TrustedProxyCIDRs: []string{trustedCIDR},
		ReadGroups:        []string{"users"},
		WriteGroups:       []string{"owners"},
	}

	t.Run("caller outside read and write groups is denied", func(t *testing.T) {
		handler, _ := newForwardAuth(t, cfg)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
		req.RemoteAddr = trustedIPAddr
		req.Header.Set("X-Forwarded-User", "alice")
		req.Header.Set("X-Forwarded-Groups", "strangers")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("read-group member is allowed", func(t *testing.T) {
		handler, _ := newForwardAuth(t, cfg)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
		req.RemoteAddr = trustedIPAddr
		req.Header.Set("X-Forwarded-User", "alice")
		req.Header.Set("X-Forwarded-Groups", "users")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("write-group member can always read", func(t *testing.T) {
		handler, _ := newForwardAuth(t, cfg)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
		req.RemoteAddr = trustedIPAddr
		req.Header.Set("X-Forwarded-User", "bob")
		req.Header.Set("X-Forwarded-Groups", "owners")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestForwardAuth_WriteDeniedWhenNoWriteGroupsConfigured(t *testing.T) {
	// An empty write_groups means no caller can write, even a trusted one.
	handler, _ := newForwardAuth(t, auth.ForwardAuthConfig{
		TrustedProxyCIDRs: []string{trustedCIDR},
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/plan", nil)
	req.RemoteAddr = trustedIPAddr
	req.Header.Set("X-Forwarded-User", "alice")
	req.Header.Set("X-Forwarded-Groups", "owners")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestForwardAuth_GroupsFromRepeatedAndDelimitedHeaders(t *testing.T) {
	handler, captured := newForwardAuth(t, auth.ForwardAuthConfig{
		TrustedProxyCIDRs: []string{trustedCIDR},
		WriteGroups:       []string{"owners"},
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/plan", nil)
	req.RemoteAddr = trustedIPAddr
	req.Header.Set("X-Forwarded-User", "bob")
	req.Header.Add("X-Forwarded-Groups", "viewers, readers")
	req.Header.Add("X-Forwarded-Groups", "owners")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, captured.user)
	assert.Equal(t, []string{"viewers", "readers", "owners"}, captured.user.Groups)
}

func TestForwardAuth_SkipsInfraPaths(t *testing.T) {
	// Health/metrics/webhook paths bypass the authorizer entirely.
	handler, _ := newForwardAuth(t, auth.ForwardAuthConfig{
		TrustedProxyCIDRs: []string{trustedCIDR},
		WriteGroups:       []string{"owners"},
	})

	for _, path := range []string{"/health", "/metrics", "/webhook"} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, nil)
		req.RemoteAddr = untrustedAddr // untrusted, but these paths skip auth
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equalf(t, http.StatusOK, rec.Code, "path %s should bypass auth", path)
	}
}
