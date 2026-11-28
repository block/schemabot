package ui

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Escape-emitting and prompting decisions hinge on this check, so it must say
// yes only for a character device a person can be at: a regular file is where
// a redirected pull lands, and the null device is the input cron, CI, and a
// container started without a terminal give a command.
func TestIsTerminal(t *testing.T) {
	t.Run("a character device is a terminal", func(t *testing.T) {
		f, err := os.Open("/dev/zero")
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, f.Close()) })
		assert.True(t, IsTerminal(f))
	})

	t.Run("the null device is not a terminal", func(t *testing.T) {
		f, err := os.Open(os.DevNull)
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, f.Close()) })
		assert.False(t, IsTerminal(f))
	})

	t.Run("a regular file is not a terminal", func(t *testing.T) {
		f, err := os.Create(filepath.Join(t.TempDir(), "schema.sql"))
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, f.Close()) })
		assert.False(t, IsTerminal(f))
	})

	t.Run("a closed file reports no terminal rather than failing", func(t *testing.T) {
		f, err := os.Open("/dev/zero")
		require.NoError(t, err)
		require.NoError(t, f.Close())
		assert.False(t, IsTerminal(f))
	})
}
