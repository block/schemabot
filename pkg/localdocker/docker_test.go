package localdocker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestSampleRequiresLocalDocker(t *testing.T) {
	for _, endpoint := range []string{"unix:///var/run/docker.sock", "npipe:////./pipe/docker_engine", "tcp://127.0.0.1:2376", "tcp://[::1]:2376"} {
		require.True(t, IsLocalEndpoint(endpoint), endpoint)
	}
	for _, endpoint := range []string{"ssh://production.example.com", "tcp://production.example.com:2376", "tcp://127.0.0.1.attacker.example:2376", ""} {
		require.False(t, IsLocalEndpoint(endpoint), endpoint)
	}
}

func TestDockerErrorExplainsFailureWithoutCredentials(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "docker"), []byte("#!/bin/sh\necho \"disk full: $MYSQL_ROOT_PASSWORD\" >&2\nexit 1\n"), 0700))
	t.Setenv("PATH", dir)
	_, err := Run(t.Context(), []string{"MYSQL_ROOT_PASSWORD=private-password"}, "create")
	require.ErrorContains(t, err, "disk full")
	require.ErrorContains(t, err, "[redacted]")
	require.NotContains(t, err.Error(), "private-password")
}

func TestDockerErrorTruncationPreservesUTF8(t *testing.T) {
	dir := t.TempDir()
	diagnostic := strings.Repeat("a", 2047) + "数据库"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "docker"), []byte("#!/bin/sh\necho '"+diagnostic+"' >&2\nexit 1\n"), 0700))
	t.Setenv("PATH", dir)
	_, err := Run(t.Context(), nil, "info")
	require.Error(t, err)
	require.True(t, utf8.ValidString(err.Error()))
	require.Contains(t, err.Error(), "…")
}
