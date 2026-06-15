package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
)

// OIDCConfig holds configuration for the OIDC authorizer.
type OIDCConfig struct {
	// Issuer is the OIDC provider's issuer URL (e.g., "https://accounts.google.com").
	Issuer string

	// Audience is the expected audience (aud) claim, required to prevent a token
	// minted for another app sharing the issuer from being replayed against this
	// API. go-oidc accepts a token whose aud array contains this value.
	Audience string

	// GroupsClaim is the JWT claim containing group memberships (default: "groups").
	GroupsClaim string
}

// OIDCAuthorizer validates OIDC JWTs on incoming API requests using JWKS
// discovery to verify token signatures. It authenticates the caller and
// records their subject and group memberships in the request context.
//
// It does not yet make tier/role decisions: any request carrying a valid token
// is allowed, and unauthenticated requests are rejected. Tier and RBAC
// enforcement (read vs write, admin/operator groups) are layered on top of this
// authenticator in a follow-up.
type OIDCAuthorizer struct {
	verifier    *oidc.IDTokenVerifier
	groupsClaim string
	logger      *slog.Logger
}

// NewOIDCAuthorizer creates an OIDC authorizer that validates JWTs against the
// given issuer's JWKS endpoint. The JWKS keys are cached and refreshed
// automatically by the go-oidc library on key rotation.
func NewOIDCAuthorizer(ctx context.Context, cfg OIDCConfig, logger *slog.Logger) (*OIDCAuthorizer, error) {
	if cfg.Issuer == "" {
		return nil, fmt.Errorf("OIDC issuer is required")
	}
	// Audience is required: skipping the aud check would accept a token minted
	// for any other app sharing the issuer. Tolerating audience-less tokens for
	// non-spec providers is a deliberate future opt-in, not the default.
	if cfg.Audience == "" {
		return nil, fmt.Errorf("OIDC audience is required")
	}

	groupsClaim := cfg.GroupsClaim
	if groupsClaim == "" {
		groupsClaim = "groups"
	}

	provider, err := oidc.NewProvider(ctx, cfg.Issuer)
	if err != nil {
		return nil, fmt.Errorf("discover OIDC provider %s: %w", cfg.Issuer, err)
	}

	// ClientID with SkipClientIDCheck=false makes go-oidc require the token's
	// aud to contain cfg.Audience (it handles aud as a string or an array).
	verifierConfig := &oidc.Config{ClientID: cfg.Audience}

	return &OIDCAuthorizer{
		verifier:    provider.Verifier(verifierConfig),
		groupsClaim: groupsClaim,
		logger:      logger,
	}, nil
}

// Middleware validates the Bearer token on API requests and records the
// authenticated user in the request context. Non-API paths that authenticate
// themselves (webhooks via HMAC) or are unauthenticated infrastructure
// endpoints (health, metrics) bypass validation. Any request with a valid token
// is allowed through; tier/RBAC enforcement is added on top of this seam.
func (a *OIDCAuthorizer) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipAuth(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		user, err := a.authenticate(r.Context(), r)
		if err != nil {
			a.logger.Warn("authentication failed", "path", r.URL.Path, "error", err)
			writeAuthError(w, http.StatusUnauthorized, "invalid or missing authentication token")
			return
		}

		ctx := WithUser(r.Context(), user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// authenticate extracts the Bearer token from the Authorization header, verifies
// it against the OIDC provider's JWKS, and returns the authenticated user with
// their group memberships.
func (a *OIDCAuthorizer) authenticate(ctx context.Context, r *http.Request) (*User, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return nil, fmt.Errorf("missing Authorization header")
	}

	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return nil, fmt.Errorf("authorization header must use Bearer scheme")
	}
	rawToken := parts[1]

	idToken, err := a.verifier.Verify(ctx, rawToken)
	if err != nil {
		return nil, fmt.Errorf("verify token: %w", err)
	}

	groups, err := a.extractGroups(idToken)
	if err != nil {
		return nil, fmt.Errorf("extract groups from token: %w", err)
	}

	return &User{
		Subject: idToken.Subject,
		Groups:  groups,
	}, nil
}

// extractGroups reads the configured groups claim. Providers emit it as either
// a JSON array of strings or a single string, so both are accepted. A missing
// claim means the caller has no group memberships.
func (a *OIDCAuthorizer) extractGroups(token *oidc.IDToken) ([]string, error) {
	var claims map[string]json.RawMessage
	if err := token.Claims(&claims); err != nil {
		return nil, fmt.Errorf("parse token claims: %w", err)
	}

	raw, ok := claims[a.groupsClaim]
	if !ok {
		return nil, nil
	}

	var groups []string
	if err := json.Unmarshal(raw, &groups); err == nil {
		return groups, nil
	}
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		return []string{single}, nil
	}
	return nil, fmt.Errorf("parse %s claim as string or string array", a.groupsClaim)
}

// skipAuth reports paths that bypass OIDC authentication: webhooks have their
// own HMAC authentication, and health/metrics are unauthenticated
// infrastructure endpoints.
func skipAuth(path string) bool {
	switch {
	case path == "/health":
		return true
	case path == "/metrics":
		return true
	case strings.HasPrefix(path, "/webhook"):
		return true
	case strings.HasPrefix(path, "/tern-health/"):
		return true
	default:
		return false
	}
}

// writeAuthError writes a JSON error response for an authentication failure.
func writeAuthError(w http.ResponseWriter, status int, message string) {
	if status == http.StatusUnauthorized {
		// RFC 6750: signal a Bearer auth challenge so clients can detect it.
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	resp := map[string]string{"error": message}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("failed to write auth error response", "error", err)
	}
}
