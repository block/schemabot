package commands

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/api"
	"gopkg.in/yaml.v3"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadPrivateLocalFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("private-token"), 0600))
	data, err := readPrivateLocalFile(path)
	require.NoError(t, err)
	assert.Equal(t, "private-token", string(data))
	for _, mode := range []os.FileMode{0644, 0640, 0620, 0610, 0604, 0602, 0601} {
		require.NoError(t, os.Chmod(path, mode))
		_, err = readPrivateLocalFile(path)
		require.ErrorContains(t, err, "must be private", "mode %#o", mode)
	}

	_, err = readPrivateLocalFile(t.TempDir())
	require.ErrorContains(t, err, "regular file")
}

func TestLocalServeRejectsAmbiguousConfiguration(t *testing.T) {
	for _, data := range []string{
		"storage: {}\nunknown_field: {}\n",
		"storage: {}\n---\nstorage: {}\n",
	} {
		path := filepath.Join(t.TempDir(), "runtime.yaml")
		require.NoError(t, os.WriteFile(path, []byte(data), 0600))
		cmd := LocalServeCmd{Config: path}
		err := cmd.Run(t.Context(), &Globals{})
		require.ErrorContains(t, err, "configuration")
	}
}

// Private local configuration gets the same repository normalization and
// collision checks as hosted configuration, before any server can start.
func TestLocalConfigUsesSharedParser(t *testing.T) {
	cfg := api.ServerConfig{Storage: api.StorageConfig{DSN: "root@tcp(127.0.0.1:3306)/state"}, Databases: map[string]api.DatabaseConfig{
		"app": {Type: "mysql", AllowedRepos: []string{"Example/App"}, Environments: map[string]api.EnvironmentConfig{"development": {DSN: "root@tcp(127.0.0.1:3306)/app"}}},
	}, Repos: map[string]api.RepoConfig{"Example/App": {}}}
	dir := t.TempDir()
	configPath, tokenPath := filepath.Join(dir, "runtime.yaml"), filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(tokenPath, []byte(strings.Repeat("a", 64)+"\n"), 0600))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { assert.NoError(t, listener.Close()) }()
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	t.Setenv("LOG_LEVEL", "debug")
	data, err := yaml.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(configPath, data, 0600))
	cmd := LocalServeCmd{Config: configPath, TokenFile: tokenPath, Listen: listener.Addr().String()}
	err = cmd.Run(ctx, &Globals{})
	require.ErrorContains(t, err, "listen for local runtime")
	assert.True(t, slog.Default().Enabled(ctx, slog.LevelDebug))
	assert.IsType(t, &slog.JSONHandler{}, slog.Default().Handler())
	cfg.Repos["example/app"] = api.RepoConfig{}
	data, err = yaml.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(configPath, data, 0600))
	require.ErrorContains(t, cmd.Run(ctx, &Globals{}), "canonicalize")
}
