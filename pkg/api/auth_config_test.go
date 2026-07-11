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

	t.Run("unknown type message lists forward_auth", func(t *testing.T) {
		err := (&api.AuthConfig{Type: "ldap"}).Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "forward_auth")
	})

	t.Run("forward_auth requires a trust anchor", func(t *testing.T) {
		err := (&api.AuthConfig{Type: "forward_auth"}).Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "trust anchor")
	})

	t.Run("forward_auth spiffe-only is valid", func(t *testing.T) {
		require.NoError(t, (&api.AuthConfig{Type: "forward_auth", ForwardAuth: api.ForwardAuthSettings{
			TrustedProxySPIFFE: []string{"spiffe://example.org/ns/ingress/sa/proxy"},
			WriteGroups:        []string{"owners"},
		}}).Validate())
	})

	t.Run("forward_auth rejects a malformed cidr", func(t *testing.T) {
		err := (&api.AuthConfig{Type: "forward_auth", ForwardAuth: api.ForwardAuthSettings{
			TrustedProxyCIDRs: []string{"not-a-cidr"},
		}}).Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CIDR")
	})

	t.Run("forward_auth with a cidr is valid", func(t *testing.T) {
		require.NoError(t, (&api.AuthConfig{Type: "forward_auth", ForwardAuth: api.ForwardAuthSettings{
			TrustedProxyCIDRs: []string{"127.0.0.1/32"},
			WriteGroups:       []string{"owners"},
		}}).Validate())
	})

	t.Run("forward_auth with spiffe and a cidr is valid", func(t *testing.T) {
		require.NoError(t, (&api.AuthConfig{Type: "forward_auth", ForwardAuth: api.ForwardAuthSettings{
			TrustedProxyCIDRs:  []string{"127.0.0.1/32"},
			TrustedProxySPIFFE: []string{"spiffe://example.org/ns/ingress/sa/proxy"},
			WriteGroups:        []string{"owners"},
		}}).Validate())
	})
}
