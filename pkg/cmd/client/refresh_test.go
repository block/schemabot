package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRefreshToken(t *testing.T) {
	t.Run("exchanges refresh token for a new id token", func(t *testing.T) {
		f := newFakeOIDC(t)
		result, err := RefreshToken(t.Context(), LoginConfig{Issuer: f.issuer(), ClientID: "cli-client"}, "test-refresh-token")
		require.NoError(t, err)
		assert.Equal(t, f.refreshedIDToken, result.IDToken)
		assert.Equal(t, f.refreshToken, result.RefreshToken)
		assert.False(t, result.Expiry.IsZero())
	})

	t.Run("validates required input before any network call", func(t *testing.T) {
		_, err := RefreshToken(t.Context(), LoginConfig{ClientID: "c"}, "r")
		assert.ErrorContains(t, err, "issuer")
		_, err = RefreshToken(t.Context(), LoginConfig{Issuer: "https://i"}, "r")
		assert.ErrorContains(t, err, "client ID")
		_, err = RefreshToken(t.Context(), LoginConfig{Issuer: "https://i", ClientID: "c"}, "")
		assert.ErrorContains(t, err, "refresh token")
	})
}

func TestResolveBearerToken(t *testing.T) {
	t.Run("flag and env take precedence and never refresh", func(t *testing.T) {
		writeConfig(t, &Config{Profiles: map[string]Profile{"default": {Token: "from-profile"}}}, 0o600)
		t.Setenv("SCHEMABOT_TOKEN", "")
		tok, err := ResolveBearerToken(t.Context(), "from-flag", "", "")
		require.NoError(t, err)
		assert.Equal(t, "from-flag", tok)

		t.Setenv("SCHEMABOT_TOKEN", "from-env")
		tok, err = ResolveBearerToken(t.Context(), "", "", "")
		require.NoError(t, err)
		assert.Equal(t, "from-env", tok)
	})

	t.Run("unexpired token is returned as-is", func(t *testing.T) {
		t.Setenv("SCHEMABOT_TOKEN", "")
		writeConfig(t, &Config{
			DefaultProfile: "default",
			Profiles: map[string]Profile{"default": {
				Endpoint:    "https://schemabot.example",
				Token:       "still-valid",
				TokenExpiry: time.Now().Add(time.Hour).Unix(),
			}},
		}, 0o600)
		tok, err := ResolveBearerToken(t.Context(), "", "", "")
		require.NoError(t, err)
		assert.Equal(t, "still-valid", tok)
	})

	t.Run("expired token is refreshed and persisted", func(t *testing.T) {
		t.Setenv("SCHEMABOT_TOKEN", "")
		f := newFakeOIDC(t)
		writeConfig(t, &Config{
			DefaultProfile: "default",
			Profiles: map[string]Profile{"default": {
				Endpoint:     "https://schemabot.example",
				Token:        "stale-token",
				RefreshToken: "old-refresh-token",
				TokenExpiry:  time.Now().Add(-time.Hour).Unix(),
				OIDC:         &OIDCLogin{Issuer: f.issuer(), ClientID: "cli-client"},
			}},
		}, 0o600)

		tok, err := ResolveBearerToken(t.Context(), "", "", "")
		require.NoError(t, err)
		assert.Equal(t, f.refreshedIDToken, tok)

		// The rotated ID token, the rotated refresh token, and a fresh expiry are
		// all persisted for the next command.
		reloaded, err := LoadConfig()
		require.NoError(t, err)
		assert.Equal(t, f.refreshedIDToken, reloaded.Profiles["default"].Token)
		assert.Equal(t, f.refreshToken, reloaded.Profiles["default"].RefreshToken)
		assert.Greater(t, reloaded.Profiles["default"].TokenExpiry, time.Now().Unix())
	})

	t.Run("expired token with no refresh token returns stale token and a warning", func(t *testing.T) {
		t.Setenv("SCHEMABOT_TOKEN", "")
		writeConfig(t, &Config{
			DefaultProfile: "default",
			Profiles: map[string]Profile{"default": {
				Endpoint:    "https://schemabot.example",
				Token:       "stale-token",
				TokenExpiry: time.Now().Add(-time.Hour).Unix(),
			}},
		}, 0o600)

		tok, err := ResolveBearerToken(t.Context(), "", "", "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expired")
		assert.Equal(t, "stale-token", tok, "stale token is still returned so the command can run and re-login can fix it")
	})
}
