package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
)

// OIDCConfig holds configuration for the OIDC authorizer.
type OIDCConfig struct {
	// Issuer is the OIDC provider's issuer URL (e.g., "https://accounts.google.com").
	Issuer string

	// Audience is the expected audience claim. If empty, audience is not validated.
	Audience string

	// AdminGroup is the group required for write operations (e.g., "schema-admins").
	AdminGroup string

	// GroupsClaim is the JWT claim containing group memberships (default: "groups").
	GroupsClaim string
}

// OIDCAuthorizer validates OIDC JWT tokens on incoming API requests.
// It uses JWKS discovery to verify token signatures and checks group
// memberships for write authorization.
type OIDCAuthorizer struct {
	verifier    *oidc.IDTokenVerifier
	adminGroup  string
	groupsClaim string
	logger      *slog.Logger
}

// NewOIDCAuthorizer creates a new OIDC authorizer that validates JWTs against
// the given issuer's JWKS endpoint. The JWKS keys are cached and refreshed
// automatically by the go-oidc library on key rotation.
func NewOIDCAuthorizer(ctx context.Context, cfg OIDCConfig, logger *slog.Logger) (*OIDCAuthorizer, error) {
	if cfg.Issuer == "" {
		return nil, fmt.Errorf("OIDC issuer is required")
	}
	if cfg.AdminGroup == "" {
		return nil, fmt.Errorf("OIDC admin_group is required")
	}

	groupsClaim := cfg.GroupsClaim
	if groupsClaim == "" {
		groupsClaim = "groups"
	}

	provider, err := oidc.NewProvider(ctx, cfg.Issuer)
	if err != nil {
		return nil, fmt.Errorf("discover OIDC provider %s: %w", cfg.Issuer, err)
	}

	verifierConfig := &oidc.Config{
		// If no audience is configured, skip audience validation.
		// Some OIDC providers issue tokens without a specific audience for
		// device auth flows.
		SkipClientIDCheck: cfg.Audience == "",
		ClientID:          cfg.Audience,
	}

	return &OIDCAuthorizer{
		verifier:    provider.Verifier(verifierConfig),
		adminGroup:  cfg.AdminGroup,
		groupsClaim: groupsClaim,
		logger:      logger,
	}, nil
}

// Middleware returns HTTP middleware that validates OIDC JWT tokens.
// Requests to paths that bypass auth (webhooks, health, metrics) are passed
// through without validation. Read endpoints require a valid token. Write
// endpoints additionally require admin_group membership.
func (a *OIDCAuthorizer) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip auth for paths that have their own authentication (webhooks use
		// HMAC) or are unauthenticated by design (health, metrics).
		if skipAuth(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		// Extract and validate the Bearer token.
		user, err := a.authenticate(r.Context(), r)
		if err != nil {
			a.logger.Warn("authentication failed", "path", r.URL.Path, "error", err)
			writeAuthError(w, http.StatusUnauthorized, "invalid or missing authentication token")
			return
		}

		// Check authorization for write endpoints.
		if isWriteEndpoint(r.Method, r.URL.Path) {
			if !a.hasAdminGroup(user) {
				a.logger.Warn("authorization denied for write endpoint",
					"path", r.URL.Path,
					"subject", user.Subject,
					"required_group", a.adminGroup,
				)
				writeAuthError(w, http.StatusForbidden, fmt.Sprintf("write access requires membership in group %q", a.adminGroup))
				return
			}
		}

		ctx := WithUser(r.Context(), user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// authenticate extracts the Bearer token from the Authorization header,
// verifies it against the OIDC provider's JWKS, and returns the authenticated
// user with their group memberships.
func (a *OIDCAuthorizer) authenticate(ctx context.Context, r *http.Request) (*User, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return nil, fmt.Errorf("missing Authorization header")
	}

	// Must be "Bearer <token>"
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return nil, fmt.Errorf("authorization header must use Bearer scheme")
	}
	rawToken := parts[1]

	// Verify the JWT against the OIDC provider's JWKS.
	idToken, err := a.verifier.Verify(ctx, rawToken)
	if err != nil {
		return nil, fmt.Errorf("verify token: %w", err)
	}

	// Extract groups from the configured claim.
	groups, err := a.extractGroups(idToken)
	if err != nil {
		return nil, fmt.Errorf("extract groups from token: %w", err)
	}

	return &User{
		Subject: idToken.Subject,
		Groups:  groups,
	}, nil
}

// extractGroups reads the groups claim from the token. The claim may contain
// a JSON array of strings. If the claim is missing, an empty slice is returned.
func (a *OIDCAuthorizer) extractGroups(token *oidc.IDToken) ([]string, error) {
	var claims map[string]json.RawMessage
	if err := token.Claims(&claims); err != nil {
		return nil, fmt.Errorf("parse token claims: %w", err)
	}

	raw, ok := claims[a.groupsClaim]
	if !ok {
		// Groups claim not present — the user has no group memberships.
		return nil, nil
	}

	var groups []string
	if err := json.Unmarshal(raw, &groups); err != nil {
		return nil, fmt.Errorf("parse %s claim as string array: %w", a.groupsClaim, err)
	}

	return groups, nil
}

// hasAdminGroup checks if the user is a member of the configured admin group.
func (a *OIDCAuthorizer) hasAdminGroup(user *User) bool {
	return slices.Contains(user.Groups, a.adminGroup)
}

// skipAuth returns true for paths that bypass OIDC authentication.
// Webhooks have their own HMAC authentication. Health and metrics are
// unauthenticated infrastructure endpoints.
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

// isWriteEndpoint returns true for API endpoints that modify state and require
// admin group membership. Read-only GET endpoints only require a valid token.
func isWriteEndpoint(method, path string) bool {
	// All POST and DELETE requests to the API are write operations.
	if method == http.MethodPost || method == http.MethodDelete {
		return strings.HasPrefix(path, "/api/")
	}
	return false
}

// writeAuthError writes a JSON error response for authentication/authorization failures.
func writeAuthError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	resp := map[string]string{"error": message}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("failed to write auth error response", "error", err)
	}
}
