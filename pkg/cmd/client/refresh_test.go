package client

import (
	"fmt"
	"os"
	"path/filepath"
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
		assert.Equal(t, f.refreshedIDExpiry, result.Expiry.Unix())
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
				Token:        testIDToken(`{"exp":1}`),
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
		assert.Equal(t, f.refreshedIDExpiry, reloaded.Profiles["default"].TokenExpiry)
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
		assert.Contains(t, err.Error(), "check the local clock")
		assert.Equal(t, "stale-token", tok, "stale token is still returned so the command can run and re-login can fix it")
	})
}

// refreshSeen reports how many refresh_token grants the fake provider served
// and the client ID presented with the last one.
func (f *fakeOIDC) refreshSeen() (int, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refreshCalls, f.refreshedFor
}

// A refresh token is renewed at the issuer and for the client it was issued
// by. When `schemabot login --issuer --client-id` recorded that pair on the
// profile, refresh goes there even though the profile's oidc settings name a
// different provider, and it works on a profile with no oidc settings at all.
// A profile saved without the pair keeps refreshing with its oidc settings.
func TestResolveBearerTokenRefreshesAtTokenIssuer(t *testing.T) {
	t.Run("recorded pair wins over the oidc settings", func(t *testing.T) {
		t.Setenv("SCHEMABOT_TOKEN", "")
		t.Setenv("SCHEMABOT_PROFILE", "")
		configured, loggedIn := newFakeOIDC(t), newFakeOIDC(t)
		writeConfig(t, &Config{Profiles: map[string]Profile{"default": {
			Endpoint:      "https://schemabot.example",
			Token:         testIDToken(`{"exp":1}`),
			RefreshToken:  "flag-refresh-token",
			TokenIssuer:   loggedIn.issuer(),
			TokenClientID: "flag-client",
			OIDC:          &OIDCLogin{Issuer: configured.issuer(), ClientID: "cli-client"},
		}}}, 0o600)

		tok, err := ResolveBearerToken(t.Context(), "", "", "")
		require.NoError(t, err)
		assert.Equal(t, loggedIn.refreshedIDToken, tok)

		calls, clientID := loggedIn.refreshSeen()
		assert.Equal(t, 1, calls)
		assert.Equal(t, "flag-client", clientID)
		calls, _ = configured.refreshSeen()
		assert.Zero(t, calls, "the refresh token must not be sent to an issuer that did not issue it")

		// The renewed tokens still came from the recorded pair, so it is kept.
		reloaded, err := LoadConfig()
		require.NoError(t, err)
		saved := reloaded.Profiles["default"]
		assert.Equal(t, loggedIn.refreshedIDToken, saved.Token)
		assert.Equal(t, loggedIn.refreshToken, saved.RefreshToken)
		assert.Equal(t, loggedIn.issuer(), saved.TokenIssuer)
		assert.Equal(t, "flag-client", saved.TokenClientID)
	})

	t.Run("recorded pair refreshes a profile without oidc settings", func(t *testing.T) {
		t.Setenv("SCHEMABOT_TOKEN", "")
		t.Setenv("SCHEMABOT_PROFILE", "")
		loggedIn := newFakeOIDC(t)
		writeConfig(t, &Config{Profiles: map[string]Profile{"default": {
			Endpoint:      "https://schemabot.example",
			Token:         testIDToken(`{"exp":1}`),
			RefreshToken:  "flag-refresh-token",
			TokenIssuer:   loggedIn.issuer(),
			TokenClientID: "flag-client",
		}}}, 0o600)

		tok, err := ResolveBearerToken(t.Context(), "", "", "")
		require.NoError(t, err)
		assert.Equal(t, loggedIn.refreshedIDToken, tok)
		calls, clientID := loggedIn.refreshSeen()
		assert.Equal(t, 1, calls)
		assert.Equal(t, "flag-client", clientID)
	})

	t.Run("profile without the recorded pair refreshes with its oidc settings", func(t *testing.T) {
		t.Setenv("SCHEMABOT_TOKEN", "")
		t.Setenv("SCHEMABOT_PROFILE", "")
		configured := newFakeOIDC(t)
		home := t.TempDir()
		t.Setenv("HOME", home)
		dir := filepath.Join(home, ".schemabot")
		require.NoError(t, os.MkdirAll(dir, 0o700))
		saved := fmt.Sprintf(`profiles:
  default:
    endpoint: https://schemabot.example
    token: %s
    refresh_token: old-refresh-token
    oidc:
      issuer: %s
      client_id: cli-client
`, testIDToken(`{"exp":1}`), configured.issuer())
		require.NoError(t, os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(saved), 0o600))

		tok, err := ResolveBearerToken(t.Context(), "", "", "")
		require.NoError(t, err)
		assert.Equal(t, configured.refreshedIDToken, tok)
		calls, clientID := configured.refreshSeen()
		assert.Equal(t, 1, calls)
		assert.Equal(t, "cli-client", clientID)

		reloaded, err := LoadConfig()
		require.NoError(t, err)
		assert.Empty(t, reloaded.Profiles["default"].TokenIssuer)
		assert.Empty(t, reloaded.Profiles["default"].TokenClientID)
	})
}

func TestRefreshPreservesConcurrentConfigEdits(t *testing.T) {
	for _, replaceSession := range []bool{false, true} {
		t.Run(fmt.Sprint(replaceSession), func(t *testing.T) {
			t.Setenv("SCHEMABOT_TOKEN", "")
			t.Setenv("SCHEMABOT_PROFILE", "")
			f := newFakeOIDC(t)
			started, release := make(chan struct{}), make(chan struct{})
			f.beforeRefresh = func() { close(started); <-release }
			writeConfig(t, &Config{Profiles: map[string]Profile{"default": {Endpoint: "https://example.test", Token: testIDToken(`{"exp":1}`), RefreshToken: "old", OIDC: &OIDCLogin{Issuer: f.issuer(), ClientID: "cli-client"}}}}, 0600)
			done := make(chan error, 1)
			go func() { _, err := ResolveBearerToken(t.Context(), "", "", "default"); done <- err }()
			<-started
			cfg, err := LoadConfig()
			require.NoError(t, err)
			cfg.Profiles["other"] = Profile{Endpoint: "https://other.example"}
			if replaceSession {
				p := cfg.Profiles["default"]
				p.RefreshToken = "new-session"
				cfg.Profiles["default"] = p
			}
			err = SaveConfig(cfg)
			close(release)
			require.NoError(t, err)
			err = <-done
			if replaceSession {
				require.ErrorContains(t, err, "newer profile was preserved")
			} else {
				require.NoError(t, err)
			}
			saved, err := LoadConfig()
			require.NoError(t, err)
			require.Contains(t, saved.Profiles, "other")
			if replaceSession {
				require.Equal(t, "new-session", saved.Profiles["default"].RefreshToken)
			} else {
				require.Equal(t, f.refreshToken, saved.Profiles["default"].RefreshToken)
			}
		})
	}
}
