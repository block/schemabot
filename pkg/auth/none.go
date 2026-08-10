package auth

import (
	"log/slog"
	"net/http"
)

// NoneAuthorizer is an allow-all authorizer: it lets every request through and
// sets a synthetic anonymous user in the request context. Used when auth is not
// configured (local development, self-hosted, deployments with no
// authenticating proxy in front of the API).
//
// Allow-all does not mean invisible: every API request is still recorded in
// the auth-decision metric (reason "auth_disabled"), and each write operation
// is logged, so a deployment running without authentication has an alertable
// signal for unauthenticated mutating traffic — e.g. a direct port-forward to
// the pod — instead of silence.
type NoneAuthorizer struct {
	// Logger receives the per-write log lines. Nil falls back to slog.Default.
	Logger *slog.Logger
}

// Middleware passes all requests through with an anonymous user in context,
// recording an auth-decision metric for every API request and logging each
// write operation.
func (a NoneAuthorizer) Middleware(next http.Handler) http.Handler {
	logger := a.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := WithUser(r.Context(), &User{Subject: AnonymousSubject})
		if !skipAuth(r.URL.Path) {
			tier := TierForRequest(r.Method, r.URL.Path)
			authDecision(r, tier, "allow", "auth_disabled")
			if tier == TierWrite {
				logger.Warn("auth disabled: write operation allowed without authentication",
					"method", r.Method, "path", r.URL.Path, "remote_addr", r.RemoteAddr)
			}
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
