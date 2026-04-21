//go:build integration

package localscale_test

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/localscale"
	"github.com/block/schemabot/pkg/psclient"
)

// TestMTLS_BranchConnection verifies that LocalScale branch proxies enforce
// mutual TLS when BranchTLSMode is "mtls". Tests the LocalScale TLS feature
// directly via MySQL connections to branch proxies.
func TestMTLS_BranchConnection(t *testing.T) {
	ctx := t.Context()

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

	require.NoError(t, lsc.SeedVSchema(ctx, "test-org", "testdb", "testkeyspace", []byte(`{"sharded": false}`)))
	require.NoError(t, lsc.SeedDDL(ctx, "test-org", "testdb", "testkeyspace",
		"CREATE TABLE users (id bigint NOT NULL PRIMARY KEY, name varchar(255)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"))

	// Get TLS certs from container
	certs, err := lsc.GetTLSCerts(ctx)
	require.NoError(t, err, "fetch TLS certs")
	require.NotEmpty(t, certs.CACert, "CA cert should be populated")
	require.NotEmpty(t, certs.ClientCert, "client cert should be populated for mTLS")
	require.NotEmpty(t, certs.ClientKey, "client key should be populated for mTLS")

	// Write certs to temp files
	certDir := t.TempDir()
	caPath := filepath.Join(certDir, "ca.pem")
	clientCertPath := filepath.Join(certDir, "client-cert.pem")
	clientKeyPath := filepath.Join(certDir, "client-key.pem")
	require.NoError(t, os.WriteFile(caPath, []byte(certs.CACert), 0o600))
	require.NoError(t, os.WriteFile(clientCertPath, []byte(certs.ClientCert), 0o600))
	require.NoError(t, os.WriteFile(clientKeyPath, []byte(certs.ClientKey), 0o600))

	// Create branch and get credentials
	client, err := psclient.NewPSClientWithBaseURL("test", "test", lsc.URL())
	require.NoError(t, err)

	_, err = client.CreateBranch(ctx, &ps.CreateDatabaseBranchRequest{
		Organization: "test-org",
		Database:     "testdb",
		Name:         "tls-test-branch",
		ParentBranch: "main",
	})
	require.NoError(t, err)

	pw, err := client.CreateBranchPassword(ctx, &ps.DatabaseBranchPasswordRequest{
		Organization: "test-org",
		Database:     "testdb",
		Branch:       "tls-test-branch",
		Role:         "admin",
		TTL:          3600,
	})
	require.NoError(t, err)
	require.NotEmpty(t, pw.Hostname, "branch password should have hostname")

	// Register mTLS config with Go MySQL driver
	caCert, err := os.ReadFile(caPath)
	require.NoError(t, err)
	caCertPool := x509.NewCertPool()
	require.True(t, caCertPool.AppendCertsFromPEM(caCert))

	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	require.NoError(t, err)

	tlsCfg := &tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{clientCert},
	}
	require.NoError(t, mysql.RegisterTLSConfig("test-mtls", tlsCfg))

	// Connect to branch proxy via mTLS
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/testkeyspace?tls=test-mtls&interpolateParams=true",
		pw.Username, pw.PlainText, pw.Hostname)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	err = db.PingContext(ctx)
	require.NoError(t, err, "should connect to branch via mTLS")

	// Verify we can query
	var result int
	err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 1, result)
}
