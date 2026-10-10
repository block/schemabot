package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveProfileSelection(t *testing.T) {
	for _, tt := range []struct {
		name, flag, env, configured, want string
		source                            ProfileSource
		explicit                          bool
	}{
		{name: "fallback", want: "default", source: ProfileSourceFallback},
		{name: "configured default", configured: "team", want: "team", source: ProfileSourceConfig},
		{name: "environment wins", env: "sandbox", configured: "team", want: "sandbox", source: ProfileSourceEnvironment, explicit: true},
		{name: "flag wins", flag: "chosen", env: "sandbox", configured: "team", want: "chosen", source: ProfileSourceFlag, explicit: true},
		{name: "explicit default", flag: "default", want: "default", source: ProfileSourceFlag, explicit: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SCHEMABOT_PROFILE", tt.env)
			cfg := &Config{DefaultProfile: tt.configured}
			got := ResolveProfile(cfg, tt.flag)
			require.Equal(t, ProfileSelection{Name: tt.want, Source: tt.source}, got)
			require.Equal(t, tt.explicit, got.Explicit())
			require.Equal(t, tt.want, ResolveProfileName(cfg, tt.flag))
		})
	}
}

// Local runtime selection has a stricter policy, covered in commands/local_selection_test.go.
func TestProfileConsumersSelection(t *testing.T) {
	for _, tt := range []struct {
		name, flag, env, configured, wantName string
		exists, wantError, fallbackExists     bool
	}{
		{name: "missing fallback"},
		{name: "missing configured default", configured: "selected", fallbackExists: true},
		{name: "missing environment", env: "selected", wantError: true},
		{name: "missing flag", flag: "selected", wantError: true},
		{name: "missing explicit default", flag: "default", wantError: true},
		{name: "existing fallback", exists: true, wantName: "default"},
		{name: "existing configured default", configured: "selected", exists: true, wantName: "selected"},
		{name: "existing environment", env: "selected", configured: "missing", exists: true, wantName: "selected"},
		{name: "existing flag wins", flag: "selected", env: "missing", exists: true, wantName: "selected"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			t.Setenv("SCHEMABOT_TOKEN", "")
			t.Setenv("SCHEMABOT_ENDPOINT", "")
			t.Setenv("SCHEMABOT_PROFILE", tt.env)
			cfg := &Config{DefaultProfile: tt.configured, Profiles: map[string]Profile{}}
			profiles := map[string]Profile{
				"selected": {Endpoint: "https://selected.example.test", Token: "selected-token"},
				"default":  {Endpoint: "https://default.example.test", Token: "default-token"},
			}
			want := profiles[tt.wantName]
			if tt.exists {
				cfg.Profiles = profiles
			}
			// Keep a fallback available when the configured profile is missing:
			// ignoring the configured name must not return the fallback's credentials.
			if tt.fallbackExists {
				cfg.Profiles["default"] = profiles["default"]
			}
			require.NoError(t, SaveConfig(cfg))
			token, tokenErr := ResolveBearerToken(t.Context(), "", "", tt.flag)
			got, err := GetProfile(tt.flag)
			if tt.wantError {
				require.ErrorContains(t, err, "unknown profile")
				require.Nil(t, got)
				require.ErrorContains(t, tokenErr, "unknown profile")
				require.Empty(t, token)
			} else {
				require.NoError(t, err)
				require.NoError(t, tokenErr)
				if tt.exists {
					require.Equal(t, &want, got)
					require.Equal(t, want.Token, token)
				} else {
					require.Equal(t, &Profile{}, got)
					require.Empty(t, token)
				}
			}
		})
	}
}

func TestProfileSelectionPolicies(t *testing.T) {
	for _, tt := range []struct {
		name                 string
		source               ProfileSource
		explicit, configured bool
	}{
		{"unspecified", ProfileSourceUnspecified, true, true},
		{"fallback", ProfileSourceFallback, false, false},
		{"flag", ProfileSourceFlag, true, true},
		{"environment", ProfileSourceEnvironment, true, true},
		{"config", ProfileSourceConfig, false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			selection := ProfileSelection{Source: tt.source}
			require.Equal(t, tt.explicit, selection.Explicit())
			require.Equal(t, tt.configured, selection.Configured())
		})
	}
}
