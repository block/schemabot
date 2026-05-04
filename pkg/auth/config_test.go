package auth_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
)

func TestAuthConfigValidation(t *testing.T) {
	t.Run("empty type is valid", func(t *testing.T) {
		cfg := api.AuthConfig{}
		require.NoError(t, cfg.Validate())
	})

	t.Run("none type is valid", func(t *testing.T) {
		cfg := api.AuthConfig{Type: "none"}
		require.NoError(t, cfg.Validate())
	})

	t.Run("oidc requires issuer", func(t *testing.T) {
		cfg := api.AuthConfig{
			Type:       "oidc",
			AdminGroup: "admins",
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "issuer")
	})

	t.Run("oidc requires admin_group", func(t *testing.T) {
		cfg := api.AuthConfig{
			Type:   "oidc",
			Issuer: "https://accounts.google.com",
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "admin_group")
	})

	t.Run("valid oidc config", func(t *testing.T) {
		cfg := api.AuthConfig{
			Type:       "oidc",
			Issuer:     "https://accounts.google.com",
			AdminGroup: "schema-admins",
		}
		require.NoError(t, cfg.Validate())
	})

	t.Run("unknown type", func(t *testing.T) {
		cfg := api.AuthConfig{Type: "ldap"}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown auth type")
	})
}
