package localstorage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareDoesNotReplaceRegisteredStorage(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "runtime.yaml"), []byte("existing"), 0600))
	_, err := Prepare(t.Context(), dir)
	require.ErrorContains(t, err, "already has storage")
	require.NoFileExists(t, filepath.Join(dir, "docker-storage.password"))
}
func TestResumeWithoutManagedStorageNeedsNoDocker(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	require.NoError(t, Resume(t.Context(), t.TempDir()))
}
func TestStorageCredentialPublicationPreservesFirstValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secret")
	require.NoError(t, writeOnce(path, []byte("first")))
	require.NoError(t, writeOnce(path, []byte("second")))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "first", string(data))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
}
func TestStorageOwnership(t *testing.T) {
	var c container
	c.Config.Labels = map[string]string{ownerLabel: "owned"}
	require.ErrorContains(t, owned(c, "other", "volume"), "not owned")
	require.ErrorContains(t, owned(c, "owned", "volume"), "volume")
	c.Mounts = append(c.Mounts, struct{ Name, Destination string }{"volume", "/var/lib/mysql"})
	require.NoError(t, owned(c, "owned", "volume"))
	_, err := endpoint(c)
	require.ErrorContains(t, err, "loopback-only")
}
