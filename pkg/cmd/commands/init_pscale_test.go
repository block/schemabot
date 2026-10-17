package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/block/mysql"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/stretchr/testify/require"
)

// Guided setup scopes API access to one database, uses read-only SQL access,
// and saves both secrets without putting either secret in subprocess arguments.
func TestInitPScaleProvision(t *testing.T) {
	var calls [][]string
	saved := map[string]string{}
	p := initPScale{run: func(_ context.Context, args ...string) ([]byte, error) {
		calls = append(calls, args)
		require.NotContains(t, strings.Join(args, " "), "secret")
		switch strings.Join(args[:2], " ") {
		case "service-token create":
			return []byte(`{"id":"token-id","token":"token-secret"}`), nil
		case "service-token add-access":
			return []byte(`[]`), nil
		case "password create":
			return []byte(`{"id":"password-id","access_host_url":"aws.connect.psdb.cloud","username":"user","plain_text":"password-secret"}`), nil
		default:
			return nil, fmt.Errorf("unexpected call")
		}
	}, save: func(purpose, value string) (string, error) {
		saved[purpose] = value
		return "file:/private/" + purpose, nil
	}}
	result, err := p.provision(t.Context(), "example", "shop")
	require.NoError(t, err)
	require.Len(t, calls, 3)
	require.Equal(t, append(append([]string{"service-token", "add-access", "token-id"}, initPlanetScalePermissions...), "--database", "shop", "--org", "example"), calls[1])
	require.Equal(t, []string{"--org", "example", "--role", "reader"}, calls[2][5:])
	require.Len(t, saved, 2)
	require.Equal(t, "token-id:token-secret", saved[strings.TrimPrefix(result.token, "file:/private/")])
	cfg, err := mysql.ParseDSN(saved[strings.TrimPrefix(result.dsn, "file:/private/")])
	require.NoError(t, err)
	require.Equal(t, "shop", cfg.DBName)
	require.Equal(t, "aws.connect.psdb.cloud:3306", cfg.Addr)
	require.Equal(t, "password-secret", cfg.Passwd)
	require.Equal(t, "true", cfg.TLSConfig)
}

// If permission assignment fails, revoke the newly created token even if the
// initiating context was cancelled, and never proceed to create a SQL password.
func TestInitPScalePartialFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var calls [][]string
	p := initPScale{run: func(ctx context.Context, args ...string) ([]byte, error) {
		calls = append(calls, args)
		switch strings.Join(args[:2], " ") {
		case "service-token create":
			return []byte(`{"id":"created-token","token":"secret"}`), nil
		case "service-token add-access":
			cancel()
			return nil, fmt.Errorf("permission denied")
		case "service-token delete":
			require.NoError(t, ctx.Err())
			require.Equal(t, "created-token", args[2])
			return nil, nil
		default:
			require.FailNow(t, "unexpected command", "%v", args)
			return nil, nil
		}
	}}
	_, err := p.provision(ctx, "example", "shop")
	require.ErrorContains(t, err, "permission denied")
	require.Len(t, calls, 3)
}

func TestInitPScaleDatabasePagination(t *testing.T) {
	var pages []string
	p := initPScale{run: func(_ context.Context, args ...string) ([]byte, error) {
		pages = append(pages, args[5])
		if args[5] == "1" {
			dbs := make([]map[string]string, 100)
			for i := range dbs {
				dbs[i] = map[string]string{"name": fmt.Sprintf("pg%d", i), "kind": "postgresql"}
			}
			return json.Marshal(dbs)
		}
		return []byte(`[{"name":"shop","kind":"mysql"},{"name":"other","kind":"postgresql"}]`), nil
	}}
	names, err := p.databases(t.Context(), "example")
	require.NoError(t, err)
	require.Equal(t, []string{"1", "2"}, pages)
	require.Equal(t, []string{"shop"}, names)
}

// Manual credentials remain available without installing or running pscale.
func TestInitPScaleManualPath(t *testing.T) {
	m := newInitWizard(&InitCmd{Type: "vitess"}, "default", io.Discard)
	require.Equal(t, stepPlanetScale, m.step)
	require.Contains(t, m.View(), "Set up with PlanetScale CLI")
	require.Contains(t, m.View(), "Enter credentials manually")
	wizardKey(m, tea.KeyDown)
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, stepName, m.step)
	wizardKey(m, tea.KeyShiftTab)
	require.Equal(t, stepPlanetScale, m.step)
}

// Account selection does not create credentials. The creation command is only
// returned after the user confirms the scope and local credential storage.
func TestInitPScaleReviewGate(t *testing.T) {
	m := newInitWizard(&InitCmd{Type: "vitess"}, "default", io.Discard)
	m.pscale.phase = "organizations"
	m.Update(initPScaleMsg{phase: "organizations", names: []string{"example"}})
	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	require.NotNil(t, cmd)
	require.Equal(t, "example", m.pscale.org)
	m.Update(initPScaleMsg{phase: "databases", names: []string{"shop"}})
	m.pscale.phase = "databases"
	_, cmd = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	require.Nil(t, cmd)
	require.Equal(t, "review", m.pscale.phase)
	require.Contains(t, m.View(), "example / shop")
	require.Contains(t, m.View(), "read-only password")
	require.Contains(t, m.View(), "enter create credentials")
	_, cmd = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	require.NotNil(t, cmd)
	require.True(t, m.pscale.busy)
}

func TestInitPScaleCompletedUsesSavedReferences(t *testing.T) {
	m := newInitWizard(&InitCmd{Type: "vitess"}, "default", io.Discard)
	m.pscale.org = "example"
	m.pscale.database = "shop"
	m.Update(initPScaleMsg{phase: "create", credentials: initPScaleCredentials{dsn: "file:/private/database", token: "file:/private/token"}})
	require.Equal(t, stepDSN, m.step)
	require.Equal(t, "shop", m.fields[stepName].value)
	require.Equal(t, "example", m.fields[stepOrganization].value)
	require.Equal(t, "file:/private/database", m.fields[stepDSN].value)
	require.Equal(t, "file:/private/token", m.fields[stepAPIToken].value)
	require.True(t, m.hidden(stepName))
	require.True(t, m.hidden(stepOrganization))
}

// Failed private persistence revokes both newly created credentials, never an
// existing user token or password. A cleanup failure remains visible to the user.
func TestInitPScaleSaveFailureCleanup(t *testing.T) {
	var deletes []string
	p := initPScale{run: func(_ context.Context, args ...string) ([]byte, error) {
		switch strings.Join(args[:2], " ") {
		case "service-token create":
			return []byte(`{"id":"new-token","token":"secret"}`), nil
		case "service-token add-access":
			return nil, nil
		case "password create":
			return []byte(`{"id":"new-password","access_host_url":"aws.connect.psdb.cloud","username":"user","plain_text":"secret"}`), nil
		case "password delete":
			deletes = append(deletes, args[4])
			return nil, nil
		case "service-token delete":
			deletes = append(deletes, args[2])
			return nil, fmt.Errorf("network unavailable")
		default:
			require.FailNow(t, "unexpected command", "%v", args)
			return nil, nil
		}
	}, save: func(string, string) (string, error) { return "", fmt.Errorf("disk full") }}
	_, err := p.provision(t.Context(), "example", "shop")
	require.ErrorContains(t, err, "could not save the service token privately")
	require.ErrorContains(t, err, "remove credentials named schemabot-")
	require.NotContains(t, err.Error(), "secret")
	require.Equal(t, []string{"new-password", "new-token"}, deletes)
}

func TestInitPScaleCommandUsesUserLogin(t *testing.T) {
	t.Setenv("PLANETSCALE_SERVICE_TOKEN", "secret")
	t.Setenv("PLANETSCALE_SERVICE_TOKEN_ID", "token-id")
	t.Setenv("PLANETSCALE_API_TOKEN", "other-secret")
	cmd := initPScaleCommand(t.Context(), "org", "list")
	for _, entry := range cmd.Env {
		require.False(t, strings.HasPrefix(entry, "PLANETSCALE_SERVICE_TOKEN="))
		require.False(t, strings.HasPrefix(entry, "PLANETSCALE_SERVICE_TOKEN_ID="))
		require.False(t, strings.HasPrefix(entry, "PLANETSCALE_API_TOKEN="))
	}
	require.Equal(t, []string{"pscale", "org", "list"}, cmd.Args)
}
