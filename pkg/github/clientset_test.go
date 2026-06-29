package github

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubFactory struct{}

func (stubFactory) ForInstallation(int64) (*InstallationClient, error) { return nil, nil }

func (stubFactory) InstallationIDForRepo(context.Context, string) (int64, error) { return 0, nil }

func TestClientSet_For(t *testing.T) {
	t.Run("returns registered factory", func(t *testing.T) {
		f := stubFactory{}
		cs := NewSingleClientSet("default", f)
		got, err := cs.For("default")
		require.NoError(t, err)
		assert.Equal(t, f, got)
	})

	t.Run("unknown app name fails closed", func(t *testing.T) {
		cs := NewSingleClientSet("default", stubFactory{})
		_, err := cs.For("other")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `no GitHub App client for "other"`)
	})

	t.Run("empty set fails closed", func(t *testing.T) {
		var cs ClientSet
		_, err := cs.For("default")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no GitHub App clients configured")
	})

	t.Run("registered nil factory fails closed instead of panicking", func(t *testing.T) {
		// Guards against NewHandler(svc, nil, ...) — the nil factory must
		// surface as an error from For, not as a nil-pointer panic when a
		// later caller invokes ForInstallation on it.
		cs := NewSingleClientSet("default", nil)
		_, err := cs.For("default")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `GitHub App client for "default" is nil`)
	})
}
