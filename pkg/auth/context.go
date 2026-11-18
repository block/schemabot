// Package auth provides authentication and authorization middleware for the
// SchemaBot API. It currently ships an allow-all authorizer (NoneAuthorizer),
// which lets every request through with a synthetic anonymous user, used when
// auth is not configured — local development, self-hosted setups, and today's
// deployments. OIDC JWT validation is layered on top of this seam.
package auth

import "context"

// AnonymousSubject is the subject NoneAuthorizer assigns when API auth is
// disabled. It is a placeholder, not a real identity, so callers that attribute
// actions to an authenticated user must not treat it as one.
const AnonymousSubject = "anonymous"

type contextKey struct{}
type verifiedContextKey struct{}

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

// WithVerifiedUser returns a new context with a user whose identity was
// established by an authenticator's verified lane.
func WithVerifiedUser(ctx context.Context, user *User) context.Context {
	ctx = WithUser(ctx, user)
	return context.WithValue(ctx, verifiedContextKey{}, true)
}

// UserFromContext returns the authenticated user from the context, or nil if
// no user is present.
func UserFromContext(ctx context.Context) *User {
	user, _ := ctx.Value(contextKey{}).(*User)
	return user
}

// AuthenticatedSubject returns the caller's subject and true only when the
// request carries a real authenticated identity — a user is present and is not
// the synthetic anonymous user NoneAuthorizer sets when auth is disabled. Use it
// to attribute an action to the authenticated caller without falling back to the
// anonymous placeholder.
func AuthenticatedSubject(ctx context.Context) (string, bool) {
	u := UserFromContext(ctx)
	if u == nil || u.Subject == "" || u.Subject == AnonymousSubject {
		return "", false
	}
	return u.Subject, true
}

// VerifiedSubject returns the caller's subject only when an authenticator
// established the identity through a verified lane.
func VerifiedSubject(ctx context.Context) (string, bool) {
	if verified, _ := ctx.Value(verifiedContextKey{}).(bool); !verified {
		return "", false
	}
	return AuthenticatedSubject(ctx)
}
