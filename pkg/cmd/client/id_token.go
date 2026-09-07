package client

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// idTokenExpiry reads the bearer credential's refresh hint, not a verified
// identity. The server must still verify the signature, issuer, audience and
// expiry (AZ-3). Never substitute the access token's independent lifetime.
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
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		// Do not include parsing errors: they can contain credential claims.
		return time.Time{}, errors.New("id_token payload must contain a valid integer exp claim")
	}
	if claims.Exp <= 0 {
		return time.Time{}, errors.New("id_token must contain a positive exp claim")
	}
	expiry := time.Unix(claims.Exp, 0)
	if !expiry.After(time.Now()) {
		return time.Time{}, errors.New("id_token has already expired")
	}
	return expiry, nil
}
