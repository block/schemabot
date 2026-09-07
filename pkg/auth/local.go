package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
)

// LocalAuthorizer authenticates possession of a private local runtime token.
// It identifies the credential, not a human: agents running as the same OS
// user have the same access. Normal plan and consent gates still apply.
type LocalAuthorizer struct {
	digest [sha256.Size]byte
	logger *slog.Logger
}

// NewLocalAuthorizer requires a token containing at least 32 non-whitespace
// characters. The caller must generate it from a cryptographically random source.
func NewLocalAuthorizer(token string, logger *slog.Logger) (*LocalAuthorizer, error) {
	if len(token) < 32 || strings.ContainsAny(token, " \t\r\n") {
		return nil, fmt.Errorf("local runtime token must contain at least 32 characters without whitespace")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &LocalAuthorizer{digest: sha256.Sum256([]byte(token)), logger: logger}, nil
}

// Middleware protects every route, including probes, with the local credential.
func (a *LocalAuthorizer) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		supplied := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		digest := sha256.Sum256([]byte(supplied))
		valid := strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") && subtle.ConstantTimeCompare(digest[:], a.digest[:]) == 1
		tier := TierForRequest(r.Method, r.URL.Path)
		if !valid {
			authDecision(r, tier, "deny", "local_token_invalid")
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "local runtime authentication required", http.StatusUnauthorized)
			return
		}
		authDecision(r, tier, "allow", "local_token")
		if tier == TierWrite {
			a.logger.Info("local runtime write authorized", "method", r.Method, "path", r.URL.Path, "subject", "local-runtime")
		}
		next.ServeHTTP(w, r.WithContext(WithUser(r.Context(), &User{Subject: "local-runtime"})))
	})
}
