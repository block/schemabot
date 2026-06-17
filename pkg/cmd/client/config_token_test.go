package client

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// writeConfig writes a CLI config into an isolated HOME so resolution goes
// through the normal config path without touching the developer's real config.
func writeConfig(t *testing.T, cfg *Config, mode os.FileMode) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	dir := filepath.Join(home, ".schemabot")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	data, err := yaml.Marshal(cfg)
	require.NoError(t, err)
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	// Force the exact mode with Chmod, which (unlike WriteFile) is not masked by
	// the process umask, so the permission checks are exercised deterministically.
	require.NoError(t, os.Chmod(path, mode))
}

func TestResolveToken(t *testing.T) {
	t.Run("flag takes precedence over env and profile", func(t *testing.T) {
		writeConfig(t, &Config{Profiles: map[string]Profile{"default": {Token: "from-profile"}}}, 0o600)
		t.Setenv("SCHEMABOT_TOKEN", "from-env")
		tok, err := ResolveToken("from-flag", "", "")
		require.NoError(t, err)
		assert.Equal(t, "from-flag", tok)
	})

	t.Run("env takes precedence over profile", func(t *testing.T) {
		writeConfig(t, &Config{Profiles: map[string]Profile{"default": {Token: "from-profile"}}}, 0o600)
		t.Setenv("SCHEMABOT_TOKEN", "from-env")
		tok, err := ResolveToken("", "", "")
		require.NoError(t, err)
		assert.Equal(t, "from-env", tok)
	})

	t.Run("falls back to profile token", func(t *testing.T) {
		writeConfig(t, &Config{
			DefaultProfile: "default",
			Profiles:       map[string]Profile{"default": {Endpoint: "https://sb.example.com", Token: "from-profile"}},
		}, 0o600)
		t.Setenv("SCHEMABOT_TOKEN", "")
		tok, err := ResolveToken("", "", "")
		require.NoError(t, err)
		assert.Equal(t, "from-profile", tok)
	})

	t.Run("empty when nothing is configured", func(t *testing.T) {
		writeConfig(t, &Config{Profiles: map[string]Profile{}}, 0o600)
		t.Setenv("SCHEMABOT_TOKEN", "")
		tok, err := ResolveToken("", "", "")
		require.NoError(t, err)
		assert.Empty(t, tok)
	})

	t.Run("profile token withheld when endpoint flag overrides to another server", func(t *testing.T) {
		writeConfig(t, &Config{
			DefaultProfile: "default",
			Profiles:       map[string]Profile{"default": {Endpoint: "https://sb.example.com", Token: "from-profile"}},
		}, 0o600)
		t.Setenv("SCHEMABOT_TOKEN", "")
		tok, err := ResolveToken("", "https://other.example.com", "")
		require.NoError(t, err)
		assert.Empty(t, tok)
	})

	t.Run("profile token withheld when SCHEMABOT_ENDPOINT overrides to another server", func(t *testing.T) {
		writeConfig(t, &Config{
			DefaultProfile: "default",
			Profiles:       map[string]Profile{"default": {Endpoint: "https://sb.example.com", Token: "from-profile"}},
		}, 0o600)
		t.Setenv("SCHEMABOT_TOKEN", "")
		t.Setenv("SCHEMABOT_ENDPOINT", "https://other.example.com")
		tok, err := ResolveToken("", "", "")
		require.NoError(t, err)
		assert.Empty(t, tok)
	})

	t.Run("profile token kept when endpoint override matches the profile endpoint", func(t *testing.T) {
		writeConfig(t, &Config{
			DefaultProfile: "default",
			Profiles:       map[string]Profile{"default": {Endpoint: "https://sb.example.com", Token: "from-profile"}},
		}, 0o600)
		t.Setenv("SCHEMABOT_TOKEN", "")
		tok, err := ResolveToken("", "https://sb.example.com/", "")
		require.NoError(t, err)
		assert.Equal(t, "from-profile", tok)
	})

	t.Run("explicit token wins even when endpoint is overridden", func(t *testing.T) {
		writeConfig(t, &Config{
			DefaultProfile: "default",
			Profiles:       map[string]Profile{"default": {Endpoint: "https://sb.example.com", Token: "from-profile"}},
		}, 0o600)
		t.Setenv("SCHEMABOT_TOKEN", "")
		tok, err := ResolveToken("from-flag", "https://other.example.com", "")
		require.NoError(t, err)
		assert.Equal(t, "from-flag", tok)
	})
}

func TestLoadConfigRejectsInsecurePermsWhenTokenPresent(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission bits are synthetic on Windows")
	}
	writeConfig(t, &Config{Profiles: map[string]Profile{"default": {Token: "secret-token"}}}, 0o644)

	_, err := LoadConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "accessible by other users")
}

func TestLoadConfigRejectsInsecurePermsWhenTokenInNonDefaultProfile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission bits are synthetic on Windows")
	}
	// The credential check guards the file, not a single profile: a token cached
	// in any profile makes the whole config a credential file.
	writeConfig(t, &Config{
		DefaultProfile: "default",
		Profiles: map[string]Profile{
			"default": {Endpoint: "https://sb.example.com"},
			"prod":    {Endpoint: "https://prod.example.com", Token: "secret-token"},
		},
	}, 0o644)

	_, err := LoadConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "accessible by other users")
}

func TestLoadConfigAllowsInsecurePermsWithoutToken(t *testing.T) {
	// A token-less config carries no secret, so loose permissions must not
	// block ordinary endpoint resolution (backward compatible).
	writeConfig(t, &Config{Profiles: map[string]Profile{"default": {Endpoint: "https://sb.example.com"}}}, 0o644)

	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, "https://sb.example.com", cfg.Profiles["default"].Endpoint)
}

func TestLoadConfigAllowsSecurePermsWithToken(t *testing.T) {
	writeConfig(t, &Config{Profiles: map[string]Profile{"default": {Token: "secret-token"}}}, 0o600)

	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, "secret-token", cfg.Profiles["default"].Token)
}
