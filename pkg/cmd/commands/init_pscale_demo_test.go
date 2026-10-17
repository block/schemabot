package commands

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/localsetup"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/muesli/termenv"
	"github.com/stretchr/testify/require"
)

// The demo drives the production wizard with a fictional provider account.
// Only external authentication, API responses, and connection probes are fixtures;
// selection, permission review, credential persistence, and rendering are real.
// Generate: SCHEMABOT_PSCALE_DEMO=/tmp/pscale-scenes.json go test ./pkg/cmd/commands -run '^TestInitPScaleDemo$' -count=1
func TestInitPScaleDemo(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DATABASE_URL", "")
	t.Setenv("PLANETSCALE_TOKEN", "")
	m := newInitWizard(&InitCmd{SchemaDir: t.TempDir()}, "shop", io.Discard)
	m.ctx = t.Context()
	m.renderer.SetColorProfile(termenv.TrueColor)
	m.renderer.SetHasDarkBackground(false)
	m.Update(tea.WindowSizeMsg{Width: 88, Height: 24})
	type scene struct {
		Screen   string  `json:"screen"`
		Duration float64 `json:"duration"`
		Caption  string  `json:"caption"`
		Stage    int     `json:"stage"`
	}
	var scenes []scene
	capture := func(duration float64, caption string, stage int) {
		scenes = append(scenes, scene{m.View(), duration, caption, stage})
	}
	capture(.8, "Choose your database engine.", 0)
	wizardKey(m, tea.KeyUp)
	capture(1.2, "Choose Vitess on PlanetScale.", 0)
	wizardKey(m, tea.KeyEnter)
	capture(2.5, "Let the PlanetScale CLI set up access.", 0)
	// Browser authorization belongs to pscale. Inject its successful completion,
	// then use the real adapter for every subsequent provider operation.
	m.pscale.phase = "login"
	m.pscale.busy = true
	capture(2, "Sign in through pscale auth login in your browser.", 0)
	m.pscale.client.run = func(_ context.Context, args ...string) ([]byte, error) {
		switch strings.Join(args[:2], " ") {
		case "org list":
			return []byte(`[{"name":"personal"},{"name":"acme"}]`), nil
		case "database list":
			return []byte(`[{"name":"analytics","kind":"mysql"},{"name":"shop","kind":"mysql"}]`), nil
		case "service-token create":
			return []byte(`{"id":"demo-token","token":"fictional-secret"}`), nil
		case "service-token add-access":
			return []byte(`[]`), nil
		case "password create":
			return []byte(`{"id":"demo-password","access_host_url":"aws.connect.psdb.cloud","username":"demo-user","plain_text":"fictional-password"}`), nil
		default:
			require.FailNow(t, "unexpected demo command", "%v", args)
			return nil, nil
		}
	}
	m.check = func(_ context.Context, engine, dsn string) error {
		require.Equal(t, "vitess", engine)
		require.Contains(t, dsn, "/shop?")
		return nil
	}
	m.checkAPI = func(_ context.Context, target localsetup.Target) error {
		require.Equal(t, "acme", target.Organization)
		require.Equal(t, "shop", target.Database)
		return nil
	}
	m.discover = func(_ context.Context, target localsetup.Target) ([]string, error) {
		require.Equal(t, "shop", target.Database)
		return []string{"shop"}, nil
	}
	m.Update(initPScaleMsg{phase: "login"})
	orgs, err := m.pscale.client.organizations(t.Context())
	require.NoError(t, err)
	m.Update(initPScaleMsg{phase: "organizations", names: orgs})
	capture(1, "Choose the organization you signed into.", 1)
	wizardKey(m, tea.KeyDown)
	capture(1.5, "Choose the organization you signed into.", 1)
	wizardKey(m, tea.KeyEnter)
	dbs, err := m.pscale.client.databases(t.Context(), m.pscale.org)
	require.NoError(t, err)
	m.Update(initPScaleMsg{phase: "databases", names: dbs})
	capture(1, "Choose an existing Vitess database.", 1)
	wizardKey(m, tea.KeyDown)
	capture(1.5, "Choose an existing Vitess database.", 1)
	wizardKey(m, tea.KeyEnter)
	capture(6, "Review the scoped permissions before creating credentials.", 2)
	wizardKey(m, tea.KeyEnter)
	capture(2, "Create scoped credentials and save them privately.", 2)
	credentials, err := m.pscale.client.provision(t.Context(), m.pscale.org, m.pscale.database)
	require.NoError(t, err)
	m.Update(initPScaleMsg{phase: "create", credentials: credentials})
	m.Update(m.checkConnection()())
	capture(2, "Database connected. No connection string to copy.", 3)
	m.Update(initConnectionAdvanceMsg{generation: m.generation, step: stepDSN})
	capture(3, "Choose local Docker storage for plans and progress.", 3)
	wizardKey(m, tea.KeyEnter)
	m.Update(m.checkConnection()())
	capture(2, "PlanetScale API access is verified too.", 3)
	m.Update(initConnectionAdvanceMsg{generation: m.generation, step: stepAPIToken})
	m.Update(m.discoverNamespaces()())
	require.Equal(t, len(m.fields), m.step)
	require.Contains(t, m.View(), "Review your setup")
	capture(3, "Ready to import and verify your schema.", 3)
	for _, s := range scenes {
		require.NotContains(t, s.Screen, "fictional-secret")
		require.NotContains(t, s.Screen, "fictional-password")
	}
	if path := os.Getenv("SCHEMABOT_PSCALE_DEMO"); path != "" {
		data, err := json.Marshal(scenes)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, data, 0600))
	}
}
