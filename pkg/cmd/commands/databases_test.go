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
	})

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
	var gotType string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/api/databases", r.URL.Path)
		gotType = r.URL.Query().Get("type")
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

	cmd := &DatabasesCmd{Type: "mysql"}
	var runErr error
	output := captureStdout(func() {
		runErr = cmd.Run(&Globals{Endpoint: server.URL})
	})

	require.NoError(t, runErr)
	assert.Equal(t, "mysql", gotType)
	assert.Contains(t, output, "DATABASE")
	assert.Contains(t, output, "orders")
	assert.Contains(t, output, "mysql")
	assert.Contains(t, output, "production")
	assert.Contains(t, output, "production: pie")
}

func TestWriteDatabaseListEmpty(t *testing.T) {
	var out bytes.Buffer
	err := writeDatabaseList(&out, &apitypes.DatabaseListResponse{})

	require.NoError(t, err)
	assert.Equal(t, "No databases configured.\n", out.String())
}

func TestValidateDatabaseListType(t *testing.T) {
	assert.NoError(t, validateDatabaseListType(""))
	assert.NoError(t, validateDatabaseListType("mysql"))
	assert.NoError(t, validateDatabaseListType("vitess"))
	assert.NoError(t, validateDatabaseListType("strata"))

	err := validateDatabaseListType("postgres")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--type must be")
}
