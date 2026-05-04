// Package auth provides authentication and authorization middleware for the
// SchemaBot API. It supports OIDC JWT validation against any standards-compliant
// identity provider (Google, Auth0, Keycloak, etc.) and a no-op mode for local
// development where all requests are allowed.
package auth

import "context"

type contextKey struct{}

// User represents an authenticated user extracted from a JWT token.
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
