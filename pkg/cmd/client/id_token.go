package client

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"
)

// idTokenExpiry reads the bearer credential's refresh hint, not a verified
// identity. The server must still verify the signature, issuer, audience and
// expiry in pkg/auth/oidc.go. Never substitute the access token's lifetime.
func idTokenExpiry(raw string) (time.Time, error) {
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return time.Time{}, errors.New("id_token must have three JWT segments")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, errors.New("id_token payload is not valid base64url")
	}
	var claims struct {
		Exp *float64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		// Do not include parsing errors: they can contain credential claims.
		return time.Time{}, errors.New("id_token payload must contain a valid numeric exp claim")
	}
	if claims.Exp == nil {
		return time.Time{}, errors.New("id_token must contain a positive exp claim")
	}
	seconds := *claims.Exp
	if seconds <= 0 {
		return time.Time{}, errors.New("id_token must contain a positive exp claim")
	}
	// NumericDate permits fractional and exponent notation. Cache whole seconds
	// like the server's OIDC verifier; truncation refreshes conservatively.
	// Leave room for the offset between Go's zero time and the Unix epoch.
	maxUnixSeconds := float64(math.MaxInt64 + (time.Time{}).Unix())
	if seconds >= maxUnixSeconds {
		return time.Time{}, errors.New("id_token exp is outside the supported time range")
	}
	expiry := time.Unix(int64(seconds), 0)
	// The local clock only schedules refresh. A fast client clock must not
	// reject login; the server's verifier decides whether the token is valid.
	return expiry, nil
}
