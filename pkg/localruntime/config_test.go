package localruntime

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpdateConfigRefusesActiveChanges(t *testing.T) {
	m := Manager{Dir: filepath.Join(t.TempDir(), "shared")}
	original := []byte("original config")
	changed, err := m.UpdateConfig(func(data []byte) ([]byte, error) {
		require.Nil(t, data)
		return original, nil
	})
	require.NoError(t, err)
	require.True(t, changed)
	lease, available, err := lock(m.Dir)
	require.NoError(t, err)
	require.True(t, available)
	t.Cleanup(func() { require.NoError(t, lease.Close()) })
	changed, err = m.UpdateConfig(func(data []byte) ([]byte, error) { return data, nil })
	require.NoError(t, err)
	require.False(t, changed)
	_, err = m.UpdateConfig(func(data []byte) ([]byte, error) {
		require.Equal(t, original, data)
		data[0] = 'X'
		return data, nil
	})
	require.ErrorContains(t, err, "configuration changes require validated registration")
	after, err := ReadPrivate(filepath.Join(m.Dir, "runtime.yaml"))
	require.NoError(t, err)
	require.Equal(t, original, after)
}
