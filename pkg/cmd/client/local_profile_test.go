package client

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegisterLocalProfilePreservesConnections(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	cfg := &Config{DefaultProfile: "remote", Profiles: map[string]Profile{"remote": {Endpoint: "https://example.test", Token: "secret"}}}
	require.NoError(t, SaveConfig(cfg))
	changed, err := RegisterLocalProfile("project", "shared")
	require.NoError(t, err)
	require.True(t, changed)
	changed, err = RegisterLocalProfile("project", "shared")
	require.NoError(t, err)
	require.False(t, changed)
	_, err = RegisterLocalProfile("remote", "shared")
	require.ErrorContains(t, err, "different connection")
	_, err = RegisterLocalProfile("project", "other")
	require.ErrorContains(t, err, "different connection")
	saved, err := LoadConfig()
	require.NoError(t, err)
	require.Equal(t, "remote", saved.DefaultProfile)
	require.Equal(t, cfg.Profiles["remote"], saved.Profiles["remote"])
	require.Equal(t, Profile{LocalRuntime: "shared"}, saved.Profiles["project"])
}

func TestSaveConfigRefusesStaleSnapshot(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	first, err := LoadConfig()
	require.NoError(t, err)
	stale, err := LoadConfig()
	require.NoError(t, err)
	first.Profiles["one"] = Profile{Endpoint: "https://one.example"}
	require.NoError(t, SaveConfig(first))
	stale.Profiles["two"] = Profile{Endpoint: "https://two.example"}
	require.ErrorIs(t, SaveConfig(stale), ErrConfigChanged)
	// A successful save updates its revision for the next save.
	first.Profiles["three"] = Profile{Endpoint: "https://three.example"}
	require.NoError(t, SaveConfig(first))
	saved, err := LoadConfig()
	require.NoError(t, err)
	require.Len(t, saved.Profiles, 2)
	require.Contains(t, saved.Profiles, "one")
	require.Contains(t, saved.Profiles, "three")
	path, err := ConfigPath()
	require.NoError(t, err)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	temps, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".config-*"))
	require.NoError(t, err)
	require.Empty(t, temps)
}

func TestRegisterLocalProfileRejectsInvalidRuntimeBeforeWriting(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	_, err := RegisterLocalProfile("project", "../other")
	require.Error(t, err)
	_, err = os.Stat(filepath.Join(home, ".schemabot"))
	require.True(t, os.IsNotExist(err))
}

func TestConcurrentLocalProfileRegistration(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	start := make(chan struct{})
	errs := make(chan error, 8)
	for i := range 8 {
		go func() { <-start; _, err := RegisterLocalProfile(fmt.Sprintf("project-%d", i), "shared"); errs <- err }()
	}
	close(start)
	for range 8 {
		require.NoError(t, <-errs)
	}
	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.Len(t, cfg.Profiles, 8)
}
