package commands

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadPrivateLocalFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("private-token"), 0600))
	data, err := readPrivateLocalFile(path)
	require.NoError(t, err)
	assert.Equal(t, "private-token", string(data))
	require.NoError(t, os.Chmod(path, 0644))
	_, err = readPrivateLocalFile(path)
	require.ErrorContains(t, err, "must be private")
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
