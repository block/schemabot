package auth_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
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
	gatewaySVID   = "spiffe://example.org/ns/service-ingress/sa/gateway"
	callerSVID    = "spiffe://example.org/ns/reporting/sa/reporting"
)

// serviceCallerConfig returns a forward-auth config with both lanes enabled:
// the identity-header proxy for humans and a caller-forwarding gateway for
// read-only service callers.
func serviceCallerConfig() auth.ForwardAuthConfig {
	return auth.ForwardAuthConfig{
		TrustedProxySPIFFE:   []string{ingressSVID},
		WriteGroups:          []string{"ops"},
		TrustedGatewaySPIFFE: []string{gatewaySVID},
		ReadServiceSPIFFE:    []string{callerSVID},
	}
}

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

// newForwardAuthWithLogs is newForwardAuth with a logger that records the
// authorizer's output, so tests can assert on denial-log contents.
func newForwardAuthWithLogs(t *testing.T, cfg auth.ForwardAuthConfig) (http.Handler, *bytes.Buffer) {
	t.Helper()
	var logs bytes.Buffer
	authz, err := auth.NewForwardAuthAuthorizer(cfg, slog.New(slog.NewTextHandler(&logs, nil)))
	require.NoError(t, err)
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return authz.Middleware(inner), &logs
}

func TestForwardAuth_UntrustedProxyDenialLogsArrivingXFCCIdentities(t *testing.T) {
	// An operator triaging an untrusted-proxy denial needs to see which URI
	// identities were present in the XFCC header, so they can tell a missing
	// trust anchor apart from a caller that forwarded no identity. The log
	// carries the parsed URI values — never the raw header, which is untrusted
	// input on this path.
	handler, logs := newForwardAuthWithLogs(t, auth.ForwardAuthConfig{
		TrustedProxyCIDRs:  []string{trustedCIDR},
		TrustedProxySPIFFE: []string{ingressSVID},
		WriteGroups:        []string{"ops"},
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	req.RemoteAddr = trustedIPAddr
	req.Header.Set("X-Forwarded-Client-Cert",
		`Hash=abc123;URI=spiffe://example.org/ns/gateway/sa/other-gateway,By=spiffe://example.org/ns/schemabot/sa/schemabot;URI="spiffe://example.org/ns/caller/sa/service"`)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	logged := logs.String()
	assert.Contains(t, logged, "did not arrive through the trusted proxy")
	assert.Contains(t, logged, "spiffe://example.org/ns/gateway/sa/other-gateway")
	assert.Contains(t, logged, "spiffe://example.org/ns/caller/sa/service")
	assert.Contains(t, logged, "xfcc_uri_count=2")
	assert.NotContains(t, logged, "Hash=abc123")
}

func TestForwardAuth_UntrustedProxyDenialLogsZeroXFCCIdentities(t *testing.T) {
	// A denial with no XFCC header logs a zero identity count, telling the
	// operator no URI identity was forwarded at all — a routing or mesh
	// problem, not a trust-anchor mismatch.
	handler, logs := newForwardAuthWithLogs(t, auth.ForwardAuthConfig{
		TrustedProxyCIDRs: []string{trustedCIDR},
		WriteGroups:       []string{"ops"},
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	req.RemoteAddr = untrustedAddr
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, logs.String(), "xfcc_uri_count=0")
}

func TestForwardAuth_UntrustedProxyDenialClampsLoggedXFCCIdentities(t *testing.T) {
	// A hostile caller can stuff arbitrarily many URI values into a synthetic
	// XFCC header; the denial log clamps the listed identities while still
	// reporting the true total, so log lines stay bounded without hiding the
	// flood.
	handler, logs := newForwardAuthWithLogs(t, auth.ForwardAuthConfig{
		TrustedProxyCIDRs: []string{trustedCIDR},
		WriteGroups:       []string{"ops"},
	})

	elements := make([]string, 10)
	for i := range elements {
		elements[i] = fmt.Sprintf("URI=spiffe://example.org/ns/flood/sa/svc-%d", i)
	}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	req.RemoteAddr = untrustedAddr
	req.Header.Set("X-Forwarded-Client-Cert", strings.Join(elements, ","))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	logged := logs.String()
	assert.Contains(t, logged, "xfcc_uri_count=10")
	assert.Contains(t, logged, "spiffe://example.org/ns/flood/sa/svc-7")
	assert.NotContains(t, logged, "spiffe://example.org/ns/flood/sa/svc-8")
	assert.NotContains(t, logged, "spiffe://example.org/ns/flood/sa/svc-9")
}

func TestForwardAuth_UntrustedProxyDenialTruncatesOversizedXFCCIdentity(t *testing.T) {
	// The count clamp alone would still let a single synthetic multi-hundred-KB
	// URI value bloat every denial line; each logged value is also truncated to
	// a fixed byte bound, marked so the operator can tell it was cut.
	handler, logs := newForwardAuthWithLogs(t, auth.ForwardAuthConfig{
		TrustedProxyCIDRs: []string{trustedCIDR},
		WriteGroups:       []string{"ops"},
	})

	huge := "spiffe://example.org/ns/flood/sa/" + strings.Repeat("x", 4096)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	req.RemoteAddr = untrustedAddr
	req.Header.Set("X-Forwarded-Client-Cert", `URI="`+huge+`"`)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	logged := logs.String()
	assert.Contains(t, logged, "xfcc_uri_count=1")
	assert.Contains(t, logged, "spiffe://example.org/ns/flood/sa/x", "a recognizable prefix survives for triage")
	assert.Contains(t, logged, "…", "the truncation is marked")
	assert.NotContains(t, logged, huge, "the full oversized value is never logged")
	assert.Less(t, len(logged), 2048, "the denial line stays bounded")
}

func TestNewForwardAuthAuthorizer_GatewayRequiresReadServiceCallers(t *testing.T) {
	cfg := serviceCallerConfig()
	cfg.ReadServiceSPIFFE = nil
	_, err := auth.NewForwardAuthAuthorizer(cfg, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read_service_spiffe")
}

func TestNewForwardAuthAuthorizer_ReadServiceCallersRequireGateway(t *testing.T) {
	cfg := serviceCallerConfig()
	cfg.TrustedGatewaySPIFFE = nil
	_, err := auth.NewForwardAuthAuthorizer(cfg, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "trusted_gateway_spiffe")
}

func TestNewForwardAuthAuthorizer_RejectsGatewayLaneWithCIDROnlyTrust(t *testing.T) {
	// Under CIDR-only proxy trust the gateway lane can never authenticate
	// anyone — an in-CIDR gateway request is treated as the identity-header
	// proxy and an out-of-CIDR one is refused by the CIDR gate — so the
	// combination is a startup error, not a silently dead lane.
	cfg := serviceCallerConfig()
	cfg.TrustedProxySPIFFE = nil
	cfg.TrustedProxyCIDRs = []string{trustedCIDR}
	_, err := auth.NewForwardAuthAuthorizer(cfg, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SPIFFE-anchored proxy trust")
}

func TestNewForwardAuthAuthorizer_WhitespaceOnlyGatewayEntriesFailClosed(t *testing.T) {
	// Entries that trim to empty do not count as configuration: a gateway list
	// of blanks leaves the callers without a vouching gateway, which is the
	// same fail-closed startup error as omitting the list.
	cfg := serviceCallerConfig()
	cfg.TrustedGatewaySPIFFE = []string{"   "}
	_, err := auth.NewForwardAuthAuthorizer(cfg, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "trusted_gateway_spiffe")
}

func TestNewForwardAuthAuthorizer_RejectsProxyGatewayOverlap(t *testing.T) {
	// A single peer cannot be both the identity-header proxy and a
	// caller-forwarding gateway: the two lanes trust different headers, so an
	// overlapping SPIFFE ID is an ambiguous configuration that must fail at
	// startup rather than silently picking a lane.
	cfg := serviceCallerConfig()
	cfg.TrustedGatewaySPIFFE = []string{ingressSVID}
	_, err := auth.NewForwardAuthAuthorizer(cfg, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "both trusted_proxy_spiffe and trusted_gateway_spiffe")
}

func TestForwardAuth_GatewayServiceCallerReadAllowed(t *testing.T) {
	// A service calling as itself arrives through a caller-forwarding gateway:
	// the XFCC leaf is the gateway and the caller's verified SPIFFE ID rides in
	// the caller header. A listed caller gets read access with the SPIFFE ID as
	// its subject, and any user/groups headers on the request are ignored —
	// this lane never grants group-derived privileges.
	handler, captured := newForwardAuth(t, serviceCallerConfig())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases", nil)
	req.RemoteAddr = untrustedAddr
	req.Header.Set("X-Forwarded-Client-Cert", "URI="+gatewaySVID)
	req.Header.Set("X-Forwarded-Caller-Spiffe-Id", callerSVID)
	req.Header.Set("X-Forwarded-User", "mallory")
	req.Header.Set("X-Forwarded-Groups", "ops")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, captured.user)
	assert.Equal(t, callerSVID, captured.user.Subject)
	assert.Empty(t, captured.user.Groups)
}

func TestForwardAuth_GatewayServiceCallerCustomHeader(t *testing.T) {
	// A deployment whose gateway forwards the caller identity under a
	// different header name configures caller_spiffe_header; the default
	// header is then ignored.
	cfg := serviceCallerConfig()
	cfg.CallerSPIFFEHeader = "X-Custom-Caller-Id"
	handler, captured := newForwardAuth(t, cfg)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases", nil)
	req.RemoteAddr = untrustedAddr
	req.Header.Set("X-Forwarded-Client-Cert", "URI="+gatewaySVID)
	req.Header.Set("X-Custom-Caller-Id", callerSVID)
	req.Header.Set("X-Forwarded-Caller-Spiffe-Id", "spiffe://example.org/ns/other/sa/other")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, captured.user)
	assert.Equal(t, callerSVID, captured.user.Subject)
}

func TestForwardAuth_GatewayServiceCallerReadOnlyPOSTAllowed(t *testing.T) {
	// Read-only POST endpoints (the explicit readPaths set) are part of the
	// read tier, so a listed service caller can use them.
	handler, captured := newForwardAuth(t, serviceCallerConfig())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/pull", nil)
	req.RemoteAddr = untrustedAddr
	req.Header.Set("X-Forwarded-Client-Cert", "URI="+gatewaySVID)
	req.Header.Set("X-Forwarded-Caller-Spiffe-Id", callerSVID)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, captured.user)
	assert.Equal(t, callerSVID, captured.user.Subject)
}

func TestForwardAuth_GatewayServiceCallerUnlistedDenied(t *testing.T) {
	// A caller forwarded by a trusted gateway but absent from
	// read_service_spiffe is denied: gateway trust vouches for who the caller
	// is, not that the caller is allowed in.
	handler, captured := newForwardAuth(t, serviceCallerConfig())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases", nil)
	req.RemoteAddr = untrustedAddr
	req.Header.Set("X-Forwarded-Client-Cert", "URI="+gatewaySVID)
	req.Header.Set("X-Forwarded-Caller-Spiffe-Id", "spiffe://example.org/ns/other/sa/other")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Nil(t, captured.user)
}

func TestForwardAuth_GatewayServiceCallerWriteDenied(t *testing.T) {
	// Service callers are read-only by design: even a listed caller is denied
	// write-tier endpoints with an explicit read-only message, so an operator
	// triaging the denial sees a policy decision, not a trust failure.
	handler, captured := newForwardAuth(t, serviceCallerConfig())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/plan", nil)
	req.RemoteAddr = untrustedAddr
	req.Header.Set("X-Forwarded-Client-Cert", "URI="+gatewaySVID)
	req.Header.Set("X-Forwarded-Caller-Spiffe-Id", callerSVID)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Contains(t, rec.Body.String(), "read-only")
	assert.Nil(t, captured.user)
}

func TestForwardAuth_GatewayMissingCallerIdentityDenied(t *testing.T) {
	// A request from a trusted gateway with no forwarded caller identity is
	// denied rather than falling back to any other lane: the gateway vouched
	// for the transport but forwarded no caller, so there is nobody to
	// authorize.
	handler, captured := newForwardAuth(t, serviceCallerConfig())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases", nil)
	req.RemoteAddr = untrustedAddr
	req.Header.Set("X-Forwarded-Client-Cert", "URI="+gatewaySVID)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Nil(t, captured.user)
}

func TestForwardAuth_GatewayBlankCallerIdentityDenied(t *testing.T) {
	// A present-but-blank caller header is the same as no caller identity:
	// there is nobody to authorize, so the request is denied.
	handler, captured := newForwardAuth(t, serviceCallerConfig())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases", nil)
	req.RemoteAddr = untrustedAddr
	req.Header.Set("X-Forwarded-Client-Cert", "URI="+gatewaySVID)
	req.Header.Set("X-Forwarded-Caller-Spiffe-Id", "   ")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Nil(t, captured.user)
}

func TestForwardAuth_GatewayLaneGatedByTrustedCIDR(t *testing.T) {
	// Configured trusted_proxy_cidrs gate the gateway lane like everything
	// else: XFCC is a spoofable header, so a gateway SVID is honored only from
	// inside the trusted networks. From inside, the lane works as usual.
	cfg := serviceCallerConfig()
	cfg.TrustedProxyCIDRs = []string{trustedCIDR}

	t.Run("gateway SVID claimed from outside the CIDR is refused", func(t *testing.T) {
		handler, captured := newForwardAuth(t, cfg)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases", nil)
		req.RemoteAddr = untrustedAddr
		req.Header.Set("X-Forwarded-Client-Cert", "URI="+gatewaySVID)
		req.Header.Set("X-Forwarded-Caller-Spiffe-Id", callerSVID)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
		assert.Contains(t, rec.Body.String(), "trusted authenticating proxy",
			"the forged claim falls through to the standard untrusted-proxy denial")
		assert.Nil(t, captured.user)
	})

	t.Run("gateway caller from inside the CIDR is allowed", func(t *testing.T) {
		handler, captured := newForwardAuth(t, cfg)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases", nil)
		req.RemoteAddr = trustedIPAddr
		req.Header.Set("X-Forwarded-Client-Cert", "URI="+gatewaySVID)
		req.Header.Set("X-Forwarded-Caller-Spiffe-Id", callerSVID)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		require.NotNil(t, captured.user)
		assert.Equal(t, callerSVID, captured.user.Subject)
	})
}

func TestForwardAuth_ForgedCallerHeaderWithoutGatewayDenied(t *testing.T) {
	// A caller header on a request that did not arrive through a listed
	// gateway is spoofed input: the request falls through to the standard
	// untrusted-proxy denial no matter what identity the header claims.
	handler, captured := newForwardAuth(t, serviceCallerConfig())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases", nil)
	req.RemoteAddr = untrustedAddr
	req.Header.Set("X-Forwarded-Caller-Spiffe-Id", callerSVID)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Nil(t, captured.user)
}

func TestForwardAuth_TrustedProxyWinsOverCallerHeader(t *testing.T) {
	// The identity-header proxy always wins: a request that provably arrived
	// through it authenticates as the forwarded user, and a stray caller
	// header is ignored rather than switching the request to the service lane.
	handler, captured := newForwardAuth(t, serviceCallerConfig())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases", nil)
	req.RemoteAddr = untrustedAddr
	req.Header.Set("X-Forwarded-Client-Cert", "URI="+ingressSVID)
	req.Header.Set("X-Forwarded-User", "alice")
	req.Header.Set("X-Forwarded-Caller-Spiffe-Id", callerSVID)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, captured.user)
	assert.Equal(t, "alice", captured.user.Subject)
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
	// Health/webhook paths bypass the authorizer entirely.
	handler, _ := newForwardAuth(t, auth.ForwardAuthConfig{
		TrustedProxyCIDRs: []string{trustedCIDR},
		WriteGroups:       []string{"owners"},
	})

	for _, path := range []string{"/livez", "/health", "/webhook"} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, nil)
		req.RemoteAddr = untrustedAddr // untrusted, but these paths skip auth
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equalf(t, http.StatusOK, rec.Code, "path %s should bypass auth", path)
	}

	// /metrics is served on a dedicated listener outside the API handler, so it
	// no longer bypasses the authorizer on the API port.
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil)
	req.RemoteAddr = untrustedAddr
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code, "/metrics should not bypass auth")
}
