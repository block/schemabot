//go:build integration

package tern

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/inventory"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/testutil"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
)

// A partially completed rollout has the desired schema on the mapped sandbox
// cluster but not the live cluster. Both consume identical canonical SQL, plan
// independently in rollout order, and converge without touching the sandbox's
// canonical-name decoy.
func TestOverrideMultiTargetPartialRollout(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	live, err := mysql.Run(ctx, "mysql:8.0", mysql.WithDatabase("testdb"), mysql.WithUsername("root"), mysql.WithPassword("test"), testutil.MySQLTmpfsDatadir())
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:usetesting // Cleanup runs after the test context is cancelled.
		defer cancel()
		require.NoError(t, live.Terminate(cleanupCtx))
	})
	liveDSN, err := testutil.ContainerConnectionString(ctx, live, "parseTime=true", "interpolateParams=true", "multiStatements=true")
	require.NoError(t, err)
	const canonical = "override_rollout_orders"
	const sandbox = "override_rollout_sandbox"
	desired := "CREATE TABLE `widgets` (`id` bigint unsigned NOT NULL, `value` varchar(255) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	original := "CREATE TABLE `widgets` (`id` bigint unsigned NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	backend, err := inventory.NewStaticResolver(inventory.StaticConfig{Targets: map[string]inventory.StaticTarget{
		"cluster-002": {DatabaseType: "mysql", DSN: dsnWithoutDatabase(t, sharedDSN)},
		"cluster-001": {DatabaseType: "mysql", DSN: dsnWithoutDatabase(t, liveDSN)},
	}})
	require.NoError(t, err)
	resolver, err := inventory.NewOverrideResolver(backend, []inventory.TargetOverride{{Environment: "production", Target: "cluster-002", DatabaseType: "mysql", SchemaOverrides: map[string]string{canonical: sandbox}}}, slog.Default())
	require.NoError(t, err)
	for _, member := range []struct {
		target, dsn, physical string
		converged             bool
	}{
		{"cluster-002", sharedDSN, sandbox, true}, {"cluster-001", liveDSN, canonical, false},
	} {
		t.Run(member.target, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			db, err := sql.Open("block-mysql", member.dsn)
			require.NoError(t, err)
			defer utils.CloseAndLog(db)
			require.NoError(t, db.PingContext(ctx))
			for _, schema := range []string{canonical, sandbox} {
				_, err = db.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+schema+"`")
				require.NoError(t, err)
				_, err = db.ExecContext(ctx, "CREATE DATABASE `"+schema+"`")
				require.NoError(t, err)
			}
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:usetesting // Cleanup runs after the test context is cancelled.
				defer cancel()
				cleanupDB, err := sql.Open("block-mysql", member.dsn)
				require.NoError(t, err)
				defer utils.CloseAndLog(cleanupDB)
				require.NoError(t, cleanupDB.PingContext(cleanupCtx))
				for _, schema := range []string{canonical, sandbox} {
					_, err = cleanupDB.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS `"+schema+"`")
					require.NoError(t, err)
				}
			})
			connection, err := db.Conn(ctx)
			require.NoError(t, err)
			defer utils.CloseAndLog(connection)
			_, err = connection.ExecContext(ctx, "USE `"+canonical+"`")
			require.NoError(t, err)
			_, err = connection.ExecContext(ctx, original)
			require.NoError(t, err)
			if member.converged {
				_, err = connection.ExecContext(ctx, "USE `"+sandbox+"`")
				require.NoError(t, err)
				_, err = connection.ExecContext(ctx, desired)
				require.NoError(t, err)
			}
			setupStorageSchema(t, member.dsn)
			target, err := resolver.ResolveTarget(ctx, inventory.Request{Environment: "production", Target: member.target, DatabaseType: "mysql"})
			require.NoError(t, err)
			store := createStorage(t, member.dsn)
			t.Cleanup(func() { utils.CloseAndLog(store) })
			client, err := NewLocalClient(LocalConfig{Database: "rollout_orders", Type: "mysql", TargetDSN: target.DSN, SchemaOverrides: target.SchemaOverrides}, store, slog.Default())
			require.NoError(t, err)
			defer utils.CloseAndLog(client)
			request := &ternv1.PlanRequest{Type: "mysql", Database: "rollout_orders", Environment: "production", SchemaFiles: map[string]*ternv1.SchemaFiles{canonical: {Files: map[string]string{"widgets.sql": desired}}}}
			plan, err := client.Plan(ctx, request)
			require.NoError(t, err)
			if member.converged {
				assert.Empty(t, plan.Changes)
			} else {
				require.NotEmpty(t, plan.Changes)
				apply, err := client.Apply(ctx, &ternv1.ApplyRequest{PlanId: plan.PlanId, Environment: "production"})
				require.NoError(t, err)
				require.True(t, apply.Accepted)
				startTestOperator(t, store, client, apply.ApplyId)
				waitForApplyComplete(t, client, ctx, apply.ApplyId)
				plan, err = client.Plan(ctx, request)
				require.NoError(t, err)
				assert.Empty(t, plan.Changes)
			}
			var count int
			err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'widgets' AND COLUMN_NAME = 'value'", member.physical).Scan(&count)
			require.NoError(t, err)
			assert.Equal(t, 1, count)
			if member.converged {
				err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'widgets' AND COLUMN_NAME = 'value'", canonical).Scan(&count)
				require.NoError(t, err)
				assert.Zero(t, count)
			}
		})
	}
}
