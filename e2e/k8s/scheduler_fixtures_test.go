//go:build e2e

package k8s

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func waitForIndex(t *testing.T, dsn, tableName, indexName string, timeout time.Duration) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(t.Context()))

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		rows, err := db.QueryContext(t.Context(), fmt.Sprintf("SHOW INDEX FROM `%s` WHERE Key_name = ?", tableName), indexName)
		if err == nil {
			found := rows.Next()
			require.NoError(t, rows.Close())
			if found {
				return
			}
		} else {
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}

	var tblName, createStmt string
	_ = db.QueryRowContext(t.Context(), fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)).Scan(&tblName, &createStmt)
	require.Failf(t, "timeout", "timeout waiting for index %s on %s, last query error: %v, table structure: %s", indexName, tableName, lastErr, createStmt)
}

func markApplyHeartbeatStale(t *testing.T, dsn, applyID, storageName string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(t.Context()))

	result, err := db.ExecContext(t.Context(),
		"UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE apply_identifier = ?",
		applyID)
	require.NoError(t, err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rowsAffected, "expected to mark one %s apply heartbeat stale", storageName)
}

func markDataPlaneHeartbeatStale(t *testing.T, applyID string) {
	t.Helper()
	markApplyHeartbeatStale(t, storageDSNs(t)[0], applyID, "data-plane")
}

func markControlPlaneHeartbeatStale(t *testing.T, applyID string) {
	t.Helper()
	markApplyHeartbeatStale(t, testutil.SchemabotDSN(t), applyID, "control-plane")
}

func waitForApplyExternalID(t *testing.T, applyID string, timeout time.Duration) string {
	t.Helper()
	db, err := sql.Open("mysql", testutil.SchemabotDSN(t))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(t.Context()))

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		var externalID string
		err = db.QueryRowContext(t.Context(),
			"SELECT external_id FROM applies WHERE apply_identifier = ?",
			applyID,
		).Scan(&externalID)
		if err == nil && externalID != "" {
			return externalID
		}
		lastErr = err
		time.Sleep(300 * time.Millisecond)
	}
	require.Failf(t, "timeout", "timeout waiting for control-plane apply %s to reference the data-plane apply, last error: %v", applyID, lastErr)
	return ""
}

type runningIndexApply struct {
	Endpoint         string
	TargetDSN        string
	TableName        string
	ApplyID          string
	DataPlaneApplyID string
}

func startRunningIndexAddApply(t *testing.T, tablePrefix string) runningIndexApply {
	t.Helper()
	ep, dsn := testutil.Endpoint(t), testutil.TernStagingDSN(t)
	tableName := testutil.UniqueTableName(tablePrefix)

	testutil.CreateTestTableWithCleanup(t, dsn, tableName, fmt.Sprintf(
		`CREATE TABLE %s (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, account_id BIGINT NOT NULL, event_type VARCHAR(100) NOT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`, tableName),
		storageDSNs(t)...)

	testutil.SeedRows(t, dsn, tableName, "account_id, event_type",
		"FLOOR(1 + RAND() * 100000), ELT(FLOOR(1 + RAND() * 5), 'type_a', 'type_b', 'type_c', 'type_d', 'type_e')", 500000)

	schemaFiles := map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, account_id BIGINT NOT NULL, event_type VARCHAR(100) NOT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, KEY idx_account_created (account_id, created_at)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, tableName),
	}

	_, applyID := testutil.PlanAndApply(t, ep, "testapp", "mysql", "staging", schemaFiles, nil, deployOpts)
	dataPlaneApplyID := waitForApplyExternalID(t, applyID, testutil.PollDeadline)

	// The index add keeps Spirit observable long enough for the test to replace
	// one tier while the other tier continues running.
	testutil.WaitForState(t, ep, applyID, state.Apply.Running, testutil.PollDeadline)

	return runningIndexApply{
		Endpoint:         ep,
		TargetDSN:        dsn,
		TableName:        tableName,
		ApplyID:          applyID,
		DataPlaneApplyID: dataPlaneApplyID,
	}
}
