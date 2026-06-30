package commands

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeIDToken builds a syntactically valid (unsigned) JWT carrying the given
// claims, for exercising display-only claim parsing.
func fakeIDToken(t *testing.T, claims map[string]any) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	payload, err := json.Marshal(claims)
	require.NoError(t, err)
	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + "."
}

func TestUserFromIDToken(t *testing.T) {
	t.Run("prefers email", func(t *testing.T) {
		tok := fakeIDToken(t, map[string]any{"email": "dev@example.com", "name": "Dev", "sub": "abc"})
		assert.Equal(t, "dev@example.com", userFromIDToken(tok))
	})
	t.Run("falls back to name then sub", func(t *testing.T) {
		assert.Equal(t, "Dev", userFromIDToken(fakeIDToken(t, map[string]any{"name": "Dev", "sub": "abc"})))
		assert.Equal(t, "abc", userFromIDToken(fakeIDToken(t, map[string]any{"sub": "abc"})))
	})
	t.Run("malformed token yields empty", func(t *testing.T) {
		assert.Empty(t, userFromIDToken("not-a-jwt"))
		assert.Empty(t, userFromIDToken(""))
	})
}

func TestResolveLoginConfig(t *testing.T) {
	t.Run("flags override profile", func(t *testing.T) {
		profile := &client.Profile{OIDC: &client.OIDCLogin{Issuer: "https://profile.example", ClientID: "profile-client", RedirectPort: 1111}}
		got, err := resolveLoginConfig("https://flag.example", "flag-client", 2222, profile)
		require.NoError(t, err)
		assert.Equal(t, "https://flag.example", got.Issuer)
		assert.Equal(t, "flag-client", got.ClientID)
		assert.Equal(t, 2222, got.RedirectPort)
	})

	t.Run("profile used when flags empty", func(t *testing.T) {
		profile := &client.Profile{OIDC: &client.OIDCLogin{Issuer: "https://profile.example", ClientID: "profile-client", RedirectPort: 1111}}
		got, err := resolveLoginConfig("", "", 0, profile)
		require.NoError(t, err)
		assert.Equal(t, "https://profile.example", got.Issuer)
		assert.Equal(t, "profile-client", got.ClientID)
		assert.Equal(t, 1111, got.RedirectPort)
	})

	t.Run("default redirect port when none set", func(t *testing.T) {
		profile := &client.Profile{OIDC: &client.OIDCLogin{Issuer: "https://profile.example", ClientID: "profile-client"}}
		got, err := resolveLoginConfig("", "", 0, profile)
		require.NoError(t, err)
		assert.Equal(t, defaultRedirectPort, got.RedirectPort)
	})

	t.Run("missing issuer errors", func(t *testing.T) {
		profile := &client.Profile{OIDC: &client.OIDCLogin{ClientID: "profile-client"}}
		_, err := resolveLoginConfig("", "", 0, profile)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "issuer")
	})

	t.Run("missing client ID errors", func(t *testing.T) {
		profile := &client.Profile{OIDC: &client.OIDCLogin{Issuer: "https://profile.example"}}
		_, err := resolveLoginConfig("", "", 0, profile)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "client ID")
	})

	t.Run("no oidc config and no flags errors with both", func(t *testing.T) {
		_, err := resolveLoginConfig("", "", 0, &client.Profile{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "issuer")
		assert.Contains(t, err.Error(), "client ID")
	})
}

// Logging in caches the ID token on the resolved profile so later commands
// authenticate without a manually supplied --token, and the resolved OIDC
// settings flow from the profile into the login flow.
func TestLoginCmdRunCachesToken(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("SCHEMABOT_PROFILE", "")

	seed := &client.Config{
		DefaultProfile: "default",
		Profiles: map[string]client.Profile{
			"default": {
				Endpoint: "https://schemabot.example",
				OIDC:     &client.OIDCLogin{Issuer: "https://issuer.example", ClientID: "cli-client", RedirectPort: 9999},
			},
		},
	}
	require.NoError(t, client.SaveConfig(seed))

	idToken := fakeIDToken(t, map[string]any{"email": "dev@example.com"})
	var gotCfg client.LoginConfig
	cmd := &LoginCmd{
		loginFn: func(_ context.Context, lc client.LoginConfig, _ client.BrowserOpener) (*client.LoginResult, error) {
			gotCfg = lc
			return &client.LoginResult{IDToken: idToken}, nil
		},
		openBrowser: func(string) error { return nil },
	}

	require.NoError(t, cmd.Run(&Globals{}))

	assert.Equal(t, "https://issuer.example", gotCfg.Issuer)
	assert.Equal(t, "cli-client", gotCfg.ClientID)
	assert.Equal(t, 9999, gotCfg.RedirectPort)

	reloaded, err := client.LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, idToken, reloaded.Profiles["default"].Token)
}

// Without OIDC settings on the profile and no flags, login fails with a clear
// configuration error rather than opening a browser.
func TestLoginCmdRunMissingOIDCErrors(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("SCHEMABOT_PROFILE", "")
	require.NoError(t, client.SaveConfig(&client.Config{
		DefaultProfile: "default",
		Profiles:       map[string]client.Profile{"default": {Endpoint: "https://schemabot.example"}},
	}))

	browserOpened := false
	cmd := &LoginCmd{
		loginFn: func(context.Context, client.LoginConfig, client.BrowserOpener) (*client.LoginResult, error) {
			t.Fatal("login should not be attempted when OIDC is unconfigured")
			return nil, nil
		},
		openBrowser: func(string) error { browserOpened = true; return nil },
	}

	err := cmd.Run(&Globals{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OIDC")
	assert.False(t, browserOpened)
}

// Logging in against a profile that does not exist fails clearly instead of
// caching a token on a dangling profile with no endpoint.
func TestLoginCmdRunUnknownProfileErrors(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("SCHEMABOT_PROFILE", "")
	require.NoError(t, client.SaveConfig(&client.Config{
		Profiles: map[string]client.Profile{"other": {Endpoint: "https://other.example"}},
	}))

	browserOpened := false
	cmd := &LoginCmd{
		loginFn: func(context.Context, client.LoginConfig, client.BrowserOpener) (*client.LoginResult, error) {
			t.Fatal("login should not be attempted for an unconfigured profile")
			return nil, nil
		},
		openBrowser: func(string) error { browserOpened = true; return nil },
	}

	err := cmd.Run(&Globals{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not configured")
	assert.False(t, browserOpened)
}
