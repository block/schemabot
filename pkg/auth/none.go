package auth

import "net/http"

// NoneAuthorizer is an allow-all authorizer: it lets every request through and
// sets a synthetic anonymous user in the request context. Used when auth is not
// configured (local development, self-hosted, today's deployments).
type NoneAuthorizer struct{}

// Middleware passes all requests through with an anonymous user in context.
func (NoneAuthorizer) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := WithUser(r.Context(), &User{Subject: "anonymous"})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
