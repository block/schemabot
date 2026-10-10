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

	"github.com/block/schemabot/pkg/localdocker"
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
			sample, err := Ensure(ctx, project, engine)
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
			port, err := localdocker.Run(ctx, nil, "inspect", "--format", "{{range .HostConfig.PortBindings}}{{(index . 0).HostPort}}{{end}}", sample.Name)
			require.NoError(t, err)
			require.False(t, c.State.Running)
			occupied, err := new(net.ListenConfig).Listen(ctx, "tcp4", net.JoinHostPort("127.0.0.1", strings.TrimSpace(string(port))))
			require.NoError(t, err)
			_, startErr := Ensure(ctx, project, engine)
			require.NoError(t, occupied.Close())
			require.ErrorContains(t, startErr, "keep the container to preserve your data")
			same, err := Ensure(ctx, project, engine)
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
