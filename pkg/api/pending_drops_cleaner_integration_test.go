//go:build integration

package api

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/block/schemabot/pkg/pendingdrops"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/testutil"
)

// The service-level pending drops cleaner starts a scheduled background loop
// and runs an immediate cleanup pass, so expired quarantined tables are dropped
// without waiting for the next interval.
func TestStartPendingDropsCleanerDropsExpiredTable(t *testing.T) {
	ctx := t.Context()

	container, err := mysql.Run(ctx,
		"mysql:8.0",
		mysql.WithDatabase("schemabot_test"),
		mysql.WithUsername("root"),
		mysql.WithPassword("test"),
	)
	require.NoError(t, err, "start mysql")
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate mysql container: %v", err)
		}
	})

	dsn, err := testutil.ContainerConnectionString(ctx, container, "parseTime=true")
	require.NoError(t, err, "container connection string")

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "open mysql")
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(ctx), "ping mysql")

	expired := pendingdrops.TableName("schemabot_test", "scheduled_drop", time.Now().Add(-48*time.Hour))
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", pendingdrops.Database))
	require.NoError(t, err, "create pending drops database")
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE `%s`.`%s` (id INT PRIMARY KEY)", pendingdrops.Database, expired))
	require.NoError(t, err, "create expired quarantined table")

	svc := New(nil, &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"schemabot_test": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: dsn},
				},
			},
		},
		PendingDrops: PendingDropsConfig{Retention: "24h"},
	}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	cleanerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	svc.StartPendingDropsCleaner(cleanerCtx)
	t.Cleanup(svc.StopPendingDropsCleaner)

	require.Eventually(t, func() bool {
		var count int
		err := db.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
			pendingdrops.Database, expired,
		).Scan(&count)
		require.NoError(t, err, "query expired quarantined table")
		return count == 0
	}, 10*time.Second, 100*time.Millisecond, "scheduled cleaner should drop expired quarantined table")
}
