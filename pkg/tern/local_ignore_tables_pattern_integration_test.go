//go:build integration

package tern

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// An application keeps queued work in a family of tables it creates at
// runtime, one per configured trigger, and the repository withholds the family
// with a pattern entry. The plan withholds and discloses the members on the
// target, records the pattern as written, and the apply runs under that
// record: a member the application creates between plan and apply is withheld
// too, so the apply changes only the declared table and every member of the
// family keeps its rows.
func TestLocalClient_IgnoreTablesPatternWithholdsRuntimeTablesThroughApply(t *testing.T) {
	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTestTables(t, dsn)
	cleanupTasks(t, dsn)

	ctx := t.Context()
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err)
	// Registered before the drops, so the handle closes after they run.
	t.Cleanup(func() { utils.CloseAndLog(db) })

	familyTables := []string{"relay_1_feed", "relay_2_feed"}
	const dropFamily = "DROP TABLE IF EXISTS `relay_1_feed`, `relay_2_feed`"
	_, err = db.ExecContext(ctx, dropFamily)
	require.NoError(t, err, "drop feed tables left by an earlier run")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		_, err := db.ExecContext(cleanupCtx, dropFamily)
		assert.NoError(t, err, "drop feed tables")
	})

	_, err = db.ExecContext(ctx, "CREATE TABLE users (id INT PRIMARY KEY)")
	require.NoError(t, err)
	// Built before the family exists, so no schema file declares a member.
	schemaFiles := buildSchemaWithAllTables(t, dsn, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})
	createFeed := func(name string) {
		t.Helper()
		_, err := db.ExecContext(ctx, "CREATE TABLE `"+name+"` (id BIGINT PRIMARY KEY, payload VARCHAR(64))")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "INSERT INTO `"+name+"` VALUES (1, 'queued'), (2, 'queued')")
		require.NoError(t, err)
	}
	createFeed("relay_1_feed")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	client, err := NewLocalClient(LocalConfig{Database: "testdb", Type: "mysql", TargetDSN: dsn}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	const pattern = `/^relay_\d+_feed$/`
	planResp, err := client.Plan(ctx, &ternv1.PlanRequest{
		Type:         "mysql",
		Database:     "testdb",
		SchemaFiles:  map[string]*ternv1.SchemaFiles{"testdb": {Files: schemaFiles}},
		IgnoreTables: []string{pattern},
	})
	require.NoError(t, err)

	require.Len(t, planResp.ExemptTables, 1)
	assert.Equal(t, []string{"relay_1_feed"}, planResp.ExemptTables[0].Tables)
	assert.Equal(t, engine.ExemptReasonIgnoreTables, planResp.ExemptTables[0].Reason)
	var planned []string
	for _, change := range planResp.Changes {
		for _, tc := range change.TableChanges {
			planned = append(planned, tc.TableName+" "+tc.ChangeType.String())
		}
	}
	assert.Equal(t, []string{"users CHANGE_TYPE_ALTER"}, planned, "only the declared table's change is planned")

	stored, err := stor.Plans().Get(ctx, planResp.PlanId)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, []string{pattern}, stored.IgnoreTables(), "the plan records the pattern, not the names it matched here")

	// The application creates the next member before the apply runs.
	createFeed("relay_2_feed")

	applyResp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      planResp.PlanId,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted, "apply rejected: %s", applyResp.ErrorMessage)
	startTestOperator(t, stor, client, applyResp.ApplyId)
	waitForApplyComplete(t, client, ctx, applyResp.ApplyId)

	var emailColumns int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'testdb' AND TABLE_NAME = 'users' AND COLUMN_NAME = 'email'").Scan(&emailColumns))
	assert.Equal(t, 1, emailColumns, "the reviewed change was applied")
	for _, name := range familyTables {
		var rows int
		require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+name+"`").Scan(&rows), name)
		assert.Equal(t, 2, rows, "%s keeps its queued rows", name)
	}
}
