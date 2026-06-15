// Package auth provides authentication and authorization middleware for the
// SchemaBot API. It currently ships an allow-all authorizer (NoneAuthorizer),
// which lets every request through with a synthetic anonymous user, used when
// auth is not configured — local development, self-hosted setups, and today's
// deployments. OIDC JWT validation is layered on top of this seam.
package auth

import "context"

type contextKey struct{}

// User represents an authenticated caller extracted from a request.
type User struct {
	// Subject is the unique identifier for the user (e.g., "user@example.com").
	Subject string

	// Groups are the group memberships from the token's groups claim.
	Groups []string
}

// WithUser returns a new context with the given user attached.
func WithUser(ctx context.Context, user *User) context.Context {
	return context.WithValue(ctx, contextKey{}, user)
}

// UserFromContext returns the authenticated user from the context, or nil if
// no user is present.
func UserFromContext(ctx context.Context) *User {
	user, _ := ctx.Value(contextKey{}).(*User)
	return user
}
