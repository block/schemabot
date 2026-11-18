package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpdateConfigMergesAfterConcurrentSave(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	calls := 0
	err := UpdateConfig(t.Context(), func(cfg *Config) error {
		calls++
		if calls == 1 {
			concurrent, err := LoadConfig()
			require.NoError(t, err)
			concurrent.Profiles["other"] = Profile{Endpoint: "https://other.example"}
			require.NoError(t, SaveConfig(concurrent))
		}
		cfg.Profiles["chosen"] = Profile{Endpoint: "https://chosen.example"}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	saved, err := LoadConfig()
	require.NoError(t, err)
	require.Equal(t, "https://other.example", saved.Profiles["other"].Endpoint)
	require.Equal(t, "https://chosen.example", saved.Profiles["chosen"].Endpoint)
}
