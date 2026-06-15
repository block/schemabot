package api_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
)

func TestAuthConfigValidate(t *testing.T) {
	t.Run("empty type is valid", func(t *testing.T) {
		require.NoError(t, (&api.AuthConfig{}).Validate())
	})

	t.Run("none type is valid", func(t *testing.T) {
		require.NoError(t, (&api.AuthConfig{Type: "none"}).Validate())
	})

	t.Run("unknown type is rejected", func(t *testing.T) {
		err := (&api.AuthConfig{Type: "ldap"}).Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown auth type")
	})
}
