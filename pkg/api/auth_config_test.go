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

	t.Run("oidc requires issuer", func(t *testing.T) {
		err := (&api.AuthConfig{Type: "oidc"}).Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "issuer")
	})

	t.Run("oidc with issuer and audience is valid", func(t *testing.T) {
		require.NoError(t, (&api.AuthConfig{Type: "oidc", Issuer: "https://issuer.example.com", Audience: "schemabot"}).Validate())
	})

	t.Run("oidc requires audience", func(t *testing.T) {
		err := (&api.AuthConfig{Type: "oidc", Issuer: "https://issuer.example.com"}).Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "audience")
	})

	t.Run("oidc whitespace-only issuer is rejected", func(t *testing.T) {
		err := (&api.AuthConfig{Type: "oidc", Issuer: "   "}).Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "issuer")
	})

	t.Run("oidc issuer with surrounding whitespace is rejected", func(t *testing.T) {
		err := (&api.AuthConfig{Type: "oidc", Issuer: " https://issuer.example.com "}).Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "whitespace")
	})
}
