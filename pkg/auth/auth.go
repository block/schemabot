package auth

import "net/http"

// Authorizer provides HTTP middleware that authenticates and authorizes
// incoming requests. Implementations set the authenticated User in the
// request context for downstream handlers.
type Authorizer interface {
	// Middleware returns HTTP middleware that authenticates requests.
	// It sets the authenticated User in the request context on success.
	// On failure, it writes an HTTP error response and does not call next.
	Middleware(next http.Handler) http.Handler
}
