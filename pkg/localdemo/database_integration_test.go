//go:build integration

package localdemo

import (
	"context"
	"database/sql"
	"os/exec"
	"testing"
	"time"

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
			sample, err := Ensure(ctx, project, engine)
			require.NoError(t, err)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
				defer cancel()
				require.NoError(t, exec.CommandContext(ctx, "docker", "rm", "-fv", sample.Name).Run())
			})
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
			_, err = db.ExecContext(ctx, "UPDATE customers SET email='changed@example.com' WHERE id=1")
			require.NoError(t, err)
			same, err := Ensure(ctx, project, engine)
			require.NoError(t, err)
			require.Equal(t, sample, same)
			require.NoError(t, db.QueryRowContext(ctx, "SELECT email FROM customers WHERE id=1").Scan(&email))
			require.Equal(t, "changed@example.com", email)
		})
	}
}
