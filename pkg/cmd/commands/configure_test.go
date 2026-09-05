package commands

import (
	"testing"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/stretchr/testify/assert"
)

// Reconfiguring a profile's endpoint must not log the operator out unless the
// endpoint actually changed: a cached token belongs to the server that issued
// it, so it survives a same-endpoint reconfigure and is dropped when the
// endpoint moves. The oidc settings are the operator's own configuration and
// survive either way.
func TestReconfiguredProfile(t *testing.T) {
	oidc := &client.OIDCLogin{Issuer: "https://issuer.example.com", ClientID: "schemabot-cli", RedirectPort: 8765}
	loggedIn := client.Profile{
		Endpoint:     "https://schemabot.example.com",
		Token:        "id-token",
		RefreshToken: "refresh-token",
		TokenExpiry:  1_900_000_000,
		OIDC:         oidc,
	}

	t.Run("same endpoint keeps the cached login and oidc settings", func(t *testing.T) {
		profile, loginCleared := reconfiguredProfile(loggedIn, loggedIn.Endpoint)

		assert.False(t, loginCleared)
		assert.Equal(t, loggedIn, profile)
	})

	t.Run("new endpoint drops the cached login but keeps oidc settings", func(t *testing.T) {
		profile, loginCleared := reconfiguredProfile(loggedIn, "https://other.example.com")

		assert.True(t, loginCleared)
		assert.Equal(t, "https://other.example.com", profile.Endpoint)
		assert.Empty(t, profile.Token)
		assert.Empty(t, profile.RefreshToken)
		assert.Zero(t, profile.TokenExpiry)
		assert.Equal(t, oidc, profile.OIDC)
	})

	t.Run("new endpoint without a cached login reports nothing cleared", func(t *testing.T) {
		existing := client.Profile{Endpoint: "https://schemabot.example.com", OIDC: oidc}
		profile, loginCleared := reconfiguredProfile(existing, "https://other.example.com")

		assert.False(t, loginCleared)
		assert.Equal(t, client.Profile{Endpoint: "https://other.example.com", OIDC: oidc}, profile)
	})

	t.Run("fresh profile records only the endpoint", func(t *testing.T) {
		profile, loginCleared := reconfiguredProfile(client.Profile{}, "http://localhost:13370")

		assert.False(t, loginCleared)
		assert.Equal(t, client.Profile{Endpoint: "http://localhost:13370"}, profile)
	})
}
