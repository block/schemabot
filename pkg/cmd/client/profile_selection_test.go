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

func TestGetProfileSelection(t *testing.T) {
	for _, tt := range []struct {
		name, flag, env, configured string
		exists, wantError           bool
	}{
		{name: "missing fallback"},
		{name: "missing configured default", configured: "selected"},
		{name: "missing environment", env: "selected", wantError: true},
		{name: "missing flag", flag: "selected", wantError: true},
		{name: "missing explicit default", flag: "default", wantError: true},
		{name: "existing fallback", exists: true},
		{name: "existing configured default", configured: "selected", exists: true},
		{name: "existing environment", env: "selected", configured: "missing", exists: true},
		{name: "existing flag wins", flag: "selected", env: "missing", exists: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			t.Setenv("SCHEMABOT_PROFILE", tt.env)
			cfg := &Config{DefaultProfile: tt.configured, Profiles: map[string]Profile{}}
			want := Profile{Endpoint: "https://example.test"}
			if tt.exists {
				cfg.Profiles["selected"] = want
				cfg.Profiles["default"] = want
			}
			require.NoError(t, SaveConfig(cfg))
			got, err := GetProfile(tt.flag)
			if tt.wantError {
				require.ErrorContains(t, err, "unknown profile")
				require.Nil(t, got)
			} else {
				require.NoError(t, err)
				if tt.exists {
					require.Equal(t, &want, got)
				} else {
					require.Equal(t, &Profile{}, got)
				}
			}
		})
	}
}
