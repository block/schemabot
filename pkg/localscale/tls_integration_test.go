//go:build integration

package localscale_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/planetscale"
	"github.com/block/schemabot/pkg/localscale"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
)

// TestMTLS_PlanAndApply verifies that the PlanetScale engine can connect to
// LocalScale branch proxies over mutual TLS using RegisterMTLS.
func TestMTLS_PlanAndApply(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Start a LocalScale container with mTLS enabled on branch proxies.
	lsc, err := localscale.RunContainer(ctx, localscale.ContainerConfig{
		Orgs: map[string]localscale.ContainerOrgConfig{
			"test-org": {Databases: map[string]localscale.ContainerDatabaseConfig{
				"testdb": {Keyspaces: []localscale.ContainerKeyspaceConfig{
					{Name: "testkeyspace", Shards: 1},
				}},
			}},
		},
		BranchTLSMode: "mtls",
	})
	require.NoError(t, err, "start LocalScale container with mTLS")
	t.Cleanup(func() { _ = lsc.Terminate(ctx) })

	// Seed VSchema so vtgate routes queries.
	vschema := []byte(`{"sharded": false}`)
	require.NoError(t, lsc.SeedVSchema(ctx, "test-org", "testdb", "testkeyspace", vschema))

	// Seed an initial table.
	require.NoError(t, lsc.SeedDDL(ctx, "test-org", "testdb", "testkeyspace",
		"CREATE TABLE users (id bigint NOT NULL PRIMARY KEY, name varchar(255)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"))

	// Fetch TLS certs from the container and write to temp files.
	certs, err := lsc.GetTLSCerts(ctx)
	require.NoError(t, err, "fetch TLS certs")
	require.NotEmpty(t, certs.CACert, "CA cert should be populated")
	require.NotEmpty(t, certs.ClientCert, "client cert should be populated for mTLS")
	require.NotEmpty(t, certs.ClientKey, "client key should be populated for mTLS")

	certDir := t.TempDir()
	caPath := filepath.Join(certDir, "ca.pem")
	clientCertPath := filepath.Join(certDir, "client-cert.pem")
	clientKeyPath := filepath.Join(certDir, "client-key.pem")
	require.NoError(t, os.WriteFile(caPath, []byte(certs.CACert), 0o600))
	require.NoError(t, os.WriteFile(clientCertPath, []byte(certs.ClientCert), 0o600))
	require.NoError(t, os.WriteFile(clientKeyPath, []byte(certs.ClientKey), 0o600))

	// Register mTLS config with the Go MySQL driver. The engine uses it
	// automatically for all branch connections.
	require.NoError(t, planetscale.RegisterMTLS(planetscale.MTLSConfig{
		CABundlePath:   caPath,
		ClientCertPath: clientCertPath,
		ClientKeyPath:  clientKeyPath,
	}), "RegisterMTLS")

	// Create PlanetScale engine pointing at LocalScale.
	psEngine := planetscale.NewWithClient(logger, func(tokenName, tokenValue string) (psclient.PSClient, error) {
		return psclient.NewPSClientWithBaseURL(tokenName, tokenValue, lsc.URL())
	})

	// Plan: add a column to the users table.
	schemaFiles := schema.SchemaFiles{
		"testkeyspace": &schema.Namespace{
			Files: map[string]string{
				"users.sql": "CREATE TABLE users (id bigint NOT NULL PRIMARY KEY, name varchar(255), email varchar(255)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			},
		},
	}

	// Get the edge address for vtgate.
	edgeAddr := getEdgeAddr(t, lsc.URL(), "test-org/testdb")

	planResult, err := psEngine.Plan(ctx, &engine.PlanRequest{
		Database:    "testdb",
		SchemaFiles: schemaFiles,
		Credentials: &engine.Credentials{
			Organization: "test-org",
			TokenName:    "test",
			TokenValue:   "test",
			DSN:          fmt.Sprintf("root@tcp(%s)/testkeyspace?interpolateParams=true", edgeAddr),
		},
	})
	require.NoError(t, err, "Plan")
	assert.False(t, planResult.NoChanges, "should detect column addition")

	// Apply: the engine creates a branch, connects over mTLS, and applies DDL.
	_, err = psEngine.Apply(ctx, &engine.ApplyRequest{
		Database:    "testdb",
		Changes:     planResult.Changes,
		SchemaFiles: schemaFiles,
		Credentials: &engine.Credentials{
			Organization: "test-org",
			TokenName:    "test",
			TokenValue:   "test",
			DSN:          fmt.Sprintf("root@tcp(%s)/testkeyspace?interpolateParams=true", edgeAddr),
		},
	})
	require.NoError(t, err, "Apply should succeed over mTLS")

	// Verify: check that the deploy request was created (branch DDL applied).
	psClient, err := psclient.NewPSClientWithBaseURL("test", "test", lsc.URL())
	require.NoError(t, err)
	drs, err := psClient.ListDeployRequests(ctx, &ps.ListDeployRequestsRequest{
		Organization: "test-org",
		Database:     "testdb",
	})
	require.NoError(t, err)
	require.NotEmpty(t, drs, "should have at least one deploy request")
}

// getEdgeAddr fetches the edge proxy address from LocalScale's admin API.
func getEdgeAddr(t *testing.T, baseURL, key string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/admin/edges", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	var edges map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&edges))
	addr, ok := edges[key]
	require.True(t, ok, "edge address for %s not found in %v", key, edges)
	return addr
}
