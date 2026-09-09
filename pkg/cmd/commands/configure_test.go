package commands

import (
	"io"
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	t.Run("the same endpoint spelled differently keeps the cached login", func(t *testing.T) {
		profile, loginCleared := reconfiguredProfile(loggedIn, "https://SchemaBot.Example.com/")

		assert.False(t, loginCleared)
		assert.Equal(t, "id-token", profile.Token)
		assert.Equal(t, "refresh-token", profile.RefreshToken)
		assert.Equal(t, "https://SchemaBot.Example.com/", profile.Endpoint,
			"the endpoint is stored as the operator typed it; only the comparison normalizes")
	})
}

// The endpoint comparison decides whether a cached token survives a
// reconfigure, and the operator retypes the endpoint at the prompt. Spellings
// the URL grammar treats as the same address must compare equal, so nobody is
// signed out of a server they never left; everything else must compare
// different, since a token is only ever valid at the server that issued it.
func TestSameEndpoint(t *testing.T) {
	cases := []struct {
		name string
		a    string
		b    string
		same bool
	}{
		{"identical", "https://schemabot.example.com", "https://schemabot.example.com", true},
		{"trailing slash", "https://schemabot.example.com", "https://schemabot.example.com/", true},
		{"trailing slash on a path", "https://example.com/schemabot/", "https://example.com/schemabot", true},
		{"host case", "https://SchemaBot.Example.com", "https://schemabot.example.com", true},
		{"scheme case", "HTTPS://schemabot.example.com", "https://schemabot.example.com", true},
		{"different host", "https://schemabot.example.com", "https://other.example.com", false},
		{"different port", "http://localhost:13370", "http://localhost:13371", false},
		{"port added", "https://schemabot.example.com", "https://schemabot.example.com:8443", false},
		{"different scheme", "http://schemabot.example.com", "https://schemabot.example.com", false},
		{"different path", "https://example.com/schemabot", "https://example.com/other", false},
		{"path case", "https://example.com/SchemaBot", "https://example.com/schemabot", false},
		{"unparseable compares exactly", "://not a url", "://also not a url", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.same, sameEndpoint(tc.a, tc.b))
			assert.Equal(t, tc.same, sameEndpoint(tc.b, tc.a), "the comparison must not depend on argument order")
		})
	}
}

// Mutate configuration after the prompt's defaults are loaded but before the
// user submits their answer, without relying on goroutine timing.
type configurePromptInput struct {
	beforeRead func()
	reader     io.Reader
}

func (r *configurePromptInput) Read(p []byte) (int, error) {
	if r.beforeRead != nil {
		r.beforeRead()
		r.beforeRead = nil
	}
	return r.reader.Read(p)
}

func TestConfigurePreservesConcurrentChanges(t *testing.T) {
	for _, change := range []string{"endpoint", "oidc", "runtime", "deleted", "created", "token", "other-profile"} {
		t.Run(change, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			cfg := &client.Config{Profiles: map[string]client.Profile{}}
			if change != "created" {
				cfg.Profiles["default"] = client.Profile{Endpoint: "https://old.example", Token: "old-token"}
			}
			require.NoError(t, client.SaveConfig(cfg))
			var concurrent client.Profile
			cmd := ConfigureSetupCmd{input: &configurePromptInput{reader: strings.NewReader("\n"), beforeRead: func() {
				latest, err := client.LoadConfig()
				require.NoError(t, err)
				concurrent = latest.Profiles["default"]
				switch change {
				case "endpoint", "created":
					concurrent.Endpoint = "https://new.example"
					concurrent.Token = "new-token"
				case "oidc":
					concurrent.OIDC = &client.OIDCLogin{Issuer: "https://issuer.example", ClientID: "new-client"}
				case "runtime":
					concurrent.LocalRuntime = "project"
				case "token":
					concurrent.Token = "refreshed-token"
				case "other-profile":
					latest.Profiles["other"] = client.Profile{Endpoint: "https://other.example"}
				}
				latest.Profiles["default"] = concurrent
				if change == "deleted" {
					delete(latest.Profiles, "default")
				}
				require.NoError(t, client.SaveConfig(latest))
			}}}
			err := cmd.Run(&Globals{})
			if change == "token" || change == "other-profile" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "newer settings were preserved")
			}
			saved, err := client.LoadConfig()
			require.NoError(t, err)
			if change == "deleted" {
				require.NotContains(t, saved.Profiles, "default")
			} else {
				require.Equal(t, concurrent, saved.Profiles["default"])
			}
			if change == "other-profile" {
				require.Contains(t, saved.Profiles, "other")
			}
		})
	}
}
