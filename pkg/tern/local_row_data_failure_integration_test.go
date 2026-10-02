//go:build integration

package tern

import (
	"database/sql"
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
)

// An apply that makes a column NOT NULL while an existing row holds NULL in it
// can never succeed as planned: every copy attempt writes the same NULL row into
// the same definition. The first attempt must fail the apply permanently, with
// the operator-facing reason naming the NULL, rather than pausing it as
// failed_retryable for a recovery budget that would restart the copy from zero
// on each attempt and fail identically. A single drive settles the apply, so a
// retryable pause would be observable here as failed_retryable.
func TestLocalClient_Apply_RowDataFailureIsPermanent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err, "open target database")
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(ctx), "ping target database")

	dropRowDataFailureTables(t, db)
	_, err = db.ExecContext(ctx, "CREATE TABLE `row_data_failure` (`id` INT NOT NULL, `amount` BIGINT NULL, PRIMARY KEY (`id`))")
	require.NoError(t, err, "create row_data_failure")
	_, err = db.ExecContext(ctx, "INSERT INTO `row_data_failure` (`id`, `amount`) VALUES (1, 10), (2, NULL), (3, 30)")
	require.NoError(t, err, "seed row_data_failure with a NULL amount")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err, "create local client")
	defer utils.CloseAndLog(client)

	planResp, err := client.Plan(ctx, &ternv1.PlanRequest{
		Type:     "mysql",
		Database: "testdb",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"testdb": {Files: buildSchemaWithAllTables(t, dsn, map[string]string{
				"row_data_failure": "CREATE TABLE `row_data_failure` (`id` INT NOT NULL, `amount` BIGINT NOT NULL, PRIMARY KEY (`id`))",
			})},
		},
	})
	require.NoError(t, err, "plan NOT NULL change")
	require.NotEmpty(t, planResp.PlanId, "plan id")

	applyResp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      planResp.PlanId,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err, "apply NOT NULL change")
	require.True(t, applyResp.Accepted, "apply should be accepted: %s", applyResp.ErrorMessage)

	driveQueuedApply(t, stor, client, applyResp.ApplyId)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, applyResp.ApplyId)
	require.NoError(t, err, "load apply")
	require.NotNil(t, apply, "apply %s", applyResp.ApplyId)
	assert.Equal(t, state.Apply.Failed, apply.State)
	assert.Zero(t, apply.Attempt, "a permanent failure spends no recovery attempt")
	assert.Contains(t, apply.ErrorMessage, "(error 1048)")

	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err, "load tasks")
	require.Len(t, tasks, 1)
	assert.Equal(t, "row_data_failure", tasks[0].TableName)
	assert.Equal(t, state.Task.Failed, tasks[0].State)
	assert.Zero(t, tasks[0].Attempt, "a permanent failure spends no recovery attempt")
	assert.Contains(t, tasks[0].ErrorMessage, "A row held NULL in a column that cannot be null")
	assert.Contains(t, tasks[0].ErrorMessage, "(error 1048)")

	var nullRows int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `row_data_failure` WHERE `amount` IS NULL").Scan(&nullRows), "count NULL amounts")
	assert.Equal(t, 1, nullRows, "the failed change leaves the table's data untouched")

	dropRowDataFailureTables(t, db)
}

// dropRowDataFailureTables removes the test table and the shadow and checkpoint
// tables the failed copy leaves beside it. The integration suite shares one
// MySQL container, and a later plan built from every table in the database
// would otherwise pick them up.
func dropRowDataFailureTables(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, table := range []string{"row_data_failure", "_row_data_failure_new", "_row_data_failure_chkpnt"} {
		_, err := db.ExecContext(t.Context(), "DROP TABLE IF EXISTS `"+table+"`")
		require.NoError(t, err, "drop %s", table)
	}
}
