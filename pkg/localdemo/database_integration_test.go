//go:build integration

package localdemo

import (
	"context"
	"database/sql"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// Both sample engines import seeded tables and retain user edits when setup is
// retried. The container publishes only on loopback and is owned by its project.
func TestSampleDatabaseLifecycle(t *testing.T) {
	for _, engine := range []string{"mysql", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			project := t.TempDir()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			name, err := sampleName(project, engine)
			require.NoError(t, err)
			t.Cleanup(func() {
				// Teardown runs after t.Context is cancelled. Keep cleanup independent.
				ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
				defer cancel()
				listed, err := exec.CommandContext(ctx, "docker", "container", "ls", "-a", "--filter", "name=^/"+name+"$", "--format", "{{.ID}}").Output()
				require.NoError(t, err)
				if strings.TrimSpace(string(listed)) == "" {
					return
				}
				require.NoError(t, exec.CommandContext(ctx, "docker", "rm", "-fv", name).Run())
			})
			// The version only selects the LocalScale image a Vitess sample
			// runs, and this test covers the two SQL engines.
			sample, err := Ensure(ctx, project, engine, "", Progress{})
			require.NoError(t, err)
			var db *sql.DB
			if engine == "mysql" {
				db, err = mysqlconn.Open(sample.DSN)
			} else {
				db, err = postgresconn.Open(sample.DSN)
			}
			require.NoError(t, err)
			defer utils.CloseAndLog(db)
			require.NoError(t, db.PingContext(ctx))
			var email string
			require.NoError(t, db.QueryRowContext(ctx, "SELECT email FROM customers WHERE id=1").Scan(&email))
			require.Equal(t, "alex@example.com", email)
			var status string
			require.NoError(t, db.QueryRowContext(ctx, "SELECT status FROM orders WHERE id=1").Scan(&status))
			require.Equal(t, "pending", status)
			_, err = db.ExecContext(ctx, "UPDATE customers SET email='changed@example.com' WHERE id=1")
			require.NoError(t, err)
			require.NoError(t, db.Close())
			require.NoError(t, exec.CommandContext(ctx, "docker", "stop", "-t", "1", sample.Name).Run())
			c, err := inspect(ctx, sample.Name)
			require.NoError(t, err)
			// NetworkSettings may omit stopped bindings; inspect the configured host port.
			port, err := run(ctx, nil, "inspect", "--format", "{{range .HostConfig.PortBindings}}{{(index . 0).HostPort}}{{end}}", sample.Name)
			require.NoError(t, err)
			require.False(t, c.State.Running)
			occupied, err := new(net.ListenConfig).Listen(ctx, "tcp4", net.JoinHostPort("127.0.0.1", strings.TrimSpace(string(port))))
			require.NoError(t, err)
			_, startErr := Ensure(ctx, project, engine, "", Progress{})
			require.NoError(t, occupied.Close())
			require.ErrorContains(t, startErr, "keep the container to preserve your data")
			same, err := Ensure(ctx, project, engine, "", Progress{})
			require.NoError(t, err)
			require.Equal(t, sample, same)
			// Reconnect with the original saved DSN, without retaining a killed session.
			if engine == "mysql" {
				db, err = mysqlconn.Open(sample.DSN)
			} else {
				db, err = postgresconn.Open(sample.DSN)
			}
			require.NoError(t, err)
			defer utils.CloseAndLog(db)
			require.NoError(t, db.QueryRowContext(ctx, "SELECT email FROM customers WHERE id=1").Scan(&email))
			require.Equal(t, "changed@example.com", email)
		})
	}
}

// vitessSampleDeadline bounds the whole Vitess sample: pulling the LocalScale
// image, starting vtcombo with its own mysqld and one tablet per shard, and
// seeding the keyspace. It is the deadline sample setup itself allows.
const vitessSampleDeadline = 10 * time.Minute

// A Vitess sample is usable the moment setup returns: its proxy ports are
// published on the host, the cluster knows which host port each one answers
// on, and the DSN it hands back reaches vtgate. Those are what an apply needs —
// it reads shards and schema over vtgate and runs its DDL over a branch proxy
// from the same range — so a sample that published only its API would plan and
// then fail every apply.
func TestVitessSampleIsReachableFromTheHost(t *testing.T) {
	project := t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), vitessSampleDeadline)
	defer cancel()
	name, err := sampleName(project, "vitess")
	require.NoError(t, err)
	storageName, err := sampleName(project, "vitess-storage")
	require.NoError(t, err)
	t.Cleanup(func() {
		// Teardown runs after t.Context is cancelled. Keep cleanup independent.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 2*time.Minute)
		defer cancel()
		for _, container := range []string{name, storageName} {
			listed, err := exec.CommandContext(ctx, "docker", "container", "ls", "-a", "--filter", "name=^/"+container+"$", "--format", "{{.ID}}").Output()
			require.NoError(t, err)
			if strings.TrimSpace(string(listed)) == "" {
				continue
			}
			require.NoError(t, exec.CommandContext(ctx, "docker", "rm", "-fv", container).Run())
		}
	})
	sample, err := Ensure(ctx, project, "vitess", "", Progress{})
	require.NoError(t, err)
	require.Equal(t, sampleOrganization, sample.Organization)
	require.Equal(t, sampleVitessDatabase, sample.Namespace)

	// The API and the DSN are different addresses: one is the PlanetScale
	// endpoint, the other the keyspace's vtgate.
	require.NotEqual(t, sample.APIURL, sample.DSN)
	require.NoError(t, localscaleHealthy(ctx, sample.APIURL))
	cfg, err := mysql.ParseDSN(sample.DSN)
	require.NoError(t, err)
	require.Equal(t, sampleVitessDatabase, cfg.DBName)
	require.True(t, strings.HasPrefix(cfg.Addr, "127.0.0.1:"), "vtgate is published on loopback, got %s", cfg.Addr)

	// Every proxy port is published on loopback, and the cluster was told which
	// host port each one answers on, so the addresses it advertises are dialable.
	c, err := inspect(ctx, sample.Name)
	require.NoError(t, err)
	ports, err := sampleProxyPortMap(c)
	require.NoError(t, err)
	require.Len(t, ports, sampleProxyPorts)
	advertised, err := sampleVtgateDSN(ctx, sample.APIURL, ports)
	require.NoError(t, err)
	require.Equal(t, sample.DSN, advertised)

	db, err := mysqlconn.Open(sample.DSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(ctx))
	rows, err := db.QueryContext(ctx, "SHOW TABLES")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	var tables []string
	for rows.Next() {
		var table string
		require.NoError(t, rows.Scan(&table))
		tables = append(tables, table)
	}
	require.NoError(t, rows.Err())
	require.ElementsMatch(t, []string{"customers", "orders"}, tables)
}
