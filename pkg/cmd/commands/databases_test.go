package commands

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

func TestWriteDatabaseList(t *testing.T) {
	var out bytes.Buffer
	err := writeDatabaseList(&out, &apitypes.DatabaseListResponse{
		Databases: []*apitypes.DatabaseResponse{
			{
				Database: "accounts",
				Type:     "vitess",
				Environments: []*apitypes.DatabaseEnvironmentResponse{
					{Environment: "production", Deployments: []string{"sled"}},
				},
			},
			{
				Database: "orders",
				Type:     "mysql",
				Environments: []*apitypes.DatabaseEnvironmentResponse{
					{Environment: "production", Deployments: []string{"pie"}},
					{Environment: "staging"},
				},
			},
		},
	}, "", "")

	require.NoError(t, err)
	output := out.String()
	assert.Contains(t, output, "DATABASE")
	assert.Contains(t, output, "TYPE")
	assert.Contains(t, output, "ENVIRONMENTS")
	assert.Contains(t, output, "DEPLOYMENTS")
	assert.Contains(t, output, "accounts")
	assert.Contains(t, output, "vitess")
	assert.Contains(t, output, "production: sled")
	assert.Contains(t, output, "orders")
	assert.Contains(t, output, "mysql")
	assert.Contains(t, output, "production, staging")
	assert.Contains(t, output, "production: pie")
}

func TestDatabasesCommandRunFetchesAndRendersDatabases(t *testing.T) {
	var gotType, gotName string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/api/databases", r.URL.Path)
		gotType = r.URL.Query().Get("type")
		gotName = r.URL.Query().Get("name")
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(apitypes.DatabaseListResponse{
			Databases: []*apitypes.DatabaseResponse{
				{
					Database: "orders",
					Type:     "mysql",
					Environments: []*apitypes.DatabaseEnvironmentResponse{
						{Environment: "production", Deployments: []string{"pie"}},
					},
				},
			},
		}))
	}))
	t.Cleanup(server.Close)

	cmd := &DatabasesCmd{Type: "mysql", Name: " ord "}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(&Globals{Endpoint: server.URL})
	})

	require.NoError(t, runErr)
	assert.Equal(t, "mysql", gotType)
	assert.Equal(t, "ord", gotName, "the name filter is trimmed before it reaches the server")
	assert.Contains(t, output, "DATABASE")
	assert.Contains(t, output, "orders")
	assert.Contains(t, output, "mysql")
	assert.Contains(t, output, "production")
	assert.Contains(t, output, "production: pie")
}

func TestWriteDatabaseListEmpty(t *testing.T) {
	var out bytes.Buffer
	err := writeDatabaseList(&out, &apitypes.DatabaseListResponse{}, "", "")

	require.NoError(t, err)
	assert.Equal(t, "No databases configured.\n", out.String())
}

func TestWriteDatabaseListEmptyWithNameFilter(t *testing.T) {
	var out bytes.Buffer
	err := writeDatabaseList(&out, &apitypes.DatabaseListResponse{}, "omnibus", "")

	require.NoError(t, err)
	assert.Equal(t, "No databases match --name \"omnibus\".\n", out.String())
}

// The app column stays out of the table until some database reports an app, so
// a deployment that has not adopted the key is not given a column of dashes.
func TestWriteDatabaseListOmitsAppColumnWhenNoDatabaseHasAnApp(t *testing.T) {
	var out bytes.Buffer
	err := writeDatabaseList(&out, &apitypes.DatabaseListResponse{
		Databases: []*apitypes.DatabaseResponse{
			{Database: "orders", Type: "mysql"},
		},
	}, "", "")

	require.NoError(t, err)
	assert.NotContains(t, out.String(), "APP")
}

// One application's shards are returned together and each names the app it
// belongs to, which is what lets a caller group a family without inferring the
// grouping from the database names.
func TestWriteDatabaseListShowsAppColumn(t *testing.T) {
	var out bytes.Buffer
	err := writeDatabaseList(&out, &apitypes.DatabaseListResponse{
		Databases: []*apitypes.DatabaseResponse{
			{
				Database: "orders_001",
				App:      "orders",
				Type:     "mysql",
				Environments: []*apitypes.DatabaseEnvironmentResponse{
					{Environment: "production", Deployments: []string{"pie"}},
				},
			},
			{Database: "legacy", Type: "mysql"},
		},
	}, "", "")

	require.NoError(t, err)
	output := out.String()
	assert.Contains(t, output, "APP")
	assert.Contains(t, output, "orders_001")
	assert.Contains(t, output, "orders")
	assert.Regexp(t, `legacy\s+-\s+mysql`, output, "a database with no app renders a dash, not an empty cell")
}

func TestWriteDatabaseListEmptyWithAppFilter(t *testing.T) {
	var out bytes.Buffer
	err := writeDatabaseList(&out, &apitypes.DatabaseListResponse{}, "", "orders")

	require.NoError(t, err)
	assert.Equal(t, "No databases match --app \"orders\".\n", out.String())
}

// An empty result names every filter the caller supplied, not just the first
// one checked, so the message describes the query that actually ran.
func TestWriteDatabaseListEmptyWithBothFilters(t *testing.T) {
	var out bytes.Buffer
	err := writeDatabaseList(&out, &apitypes.DatabaseListResponse{}, "omni", "orders")

	require.NoError(t, err)
	assert.Equal(t, "No databases match --name \"omni\" and --app \"orders\".\n", out.String())
}

func TestDatabasesCommandSendsAppFilter(t *testing.T) {
	var gotApp string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotApp = r.URL.Query().Get("app")
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(apitypes.DatabaseListResponse{
			Databases: []*apitypes.DatabaseResponse{{Database: "orders_001", App: "orders", Type: "mysql"}},
		}))
	}))
	t.Cleanup(server.Close)

	cmd := &DatabasesCmd{App: " orders "}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(&Globals{Endpoint: server.URL})
	})

	require.NoError(t, runErr)
	assert.Equal(t, "orders", gotApp, "the app filter is trimmed before it reaches the server")
	assert.Contains(t, output, "APP")
	assert.Contains(t, output, "orders_001")
}
