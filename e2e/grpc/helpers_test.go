//go:build e2e

package grpc

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Environment Helpers
// =============================================================================

func grpcSchemabotURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("E2E_SCHEMABOT_URL")
	require.NotEmpty(t, url, "E2E_SCHEMABOT_URL environment variable not set")
	return url
}

func grpcSchemabotMySQLDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("E2E_SCHEMABOT_MYSQL_DSN")
	require.NotEmpty(t, dsn, "E2E_SCHEMABOT_MYSQL_DSN environment variable not set")
	return dsn
}

func grpcTernMySQLDSN(t *testing.T, env string) string {
	t.Helper()
	var key string
	switch env {
	case "staging":
		key = "E2E_TERN_STAGING_MYSQL_DSN"
	case "production":
		key = "E2E_TERN_PRODUCTION_MYSQL_DSN"
	default:
		require.Failf(t, "unknown environment", "%s", env)
	}
	dsn := os.Getenv(key)
	require.NotEmptyf(t, dsn, "%s environment variable not set", key)
	return dsn
}

// =============================================================================
// HTTP Helpers
// =============================================================================

func grpcPost(t *testing.T, path string, body any) *http.Response {
	t.Helper()
	baseURL := grpcSchemabotURL(t)
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		require.NoError(t, err, "marshal request body")
		bodyReader = bytes.NewReader(data)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bodyReader)
	require.NoError(t, err, "create request")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoErrorf(t, err, "POST %s", path)
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func grpcGet(t *testing.T, path string) *http.Response {
	t.Helper()
	baseURL := grpcSchemabotURL(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
	require.NoError(t, err, "create request")
	resp, err := http.DefaultClient.Do(req)
	require.NoErrorf(t, err, "GET %s", path)
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func grpcDecodeJSON(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer utils.CloseAndLog(resp.Body)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read response body")
	require.NoErrorf(t, json.Unmarshal(body, v), "decode response JSON, body: %s", string(body))
}

// =============================================================================
// API Helpers
// =============================================================================

type grpcPlanResponse struct {
	PlanID       string             `json:"plan_id"`
	Engine       string             `json:"engine"`
	Changes      []grpcSchemaChange `json:"changes"`
	LintWarnings []grpcLintWarning  `json:"lint_warnings"`
	Errors       []string           `json:"errors"`
}

// FlatTables returns a flat list of all table changes across all namespaces.
func (r grpcPlanResponse) FlatTables() []grpcTableChange {
	var tables []grpcTableChange
	for _, sc := range r.Changes {
		tables = append(tables, sc.TableChanges...)
	}
	return tables
}

type grpcLintWarning struct {
	Message  string `json:"message"`
	Table    string `json:"table"`
	Severity string `json:"severity"`
}

type grpcSchemaChange struct {
	Namespace    string            `json:"namespace"`
	TableChanges []grpcTableChange `json:"table_changes,omitempty"`
}

type grpcTableChange struct {
	TableName  string `json:"table_name"`
	DDL        string `json:"ddl"`
	ChangeType string `json:"change_type"`
}

type grpcApplyResponse struct {
	Accepted     bool   `json:"accepted"`
	ErrorMessage string `json:"error_message,omitempty"`
	ApplyID      string `json:"apply_id,omitempty"`
}

type grpcProgressResponse struct {
	State        string              `json:"state"`
	Engine       string              `json:"engine"`
	Tables       []grpcTableProgress `json:"tables"`
	ErrorMessage string              `json:"error_message,omitempty"`
	Summary      string              `json:"summary,omitempty"`
}

type grpcTableProgress struct {
	TableName       string `json:"table_name"`
	DDL             string `json:"ddl"`
	Status          string `json:"status"`
	PercentComplete int32  `json:"percent_complete"`
}

type grpcSimpleResponse struct {
	Accepted     bool   `json:"accepted"`
	ErrorMessage string `json:"error_message,omitempty"`
}

type grpcVolumeResponse struct {
	Accepted       bool   `json:"accepted"`
	ErrorMessage   string `json:"error_message,omitempty"`
	PreviousVolume int32  `json:"previous_volume"`
	NewVolume      int32  `json:"new_volume"`
}

func grpcPlan(t *testing.T, database, env string, schemaFiles map[string]string) grpcPlanResponse {
	t.Helper()

	// Schema files format matches CLI: map[filename] -> content, wrapped in "default" keyspace
	body := map[string]any{
		"database":    database,
		"environment": env,
		"type":        "mysql",
		"schema_files": map[string]any{
			"default": map[string]any{
				"files": schemaFiles,
			},
		},
	}
	resp := grpcPost(t, "/api/plan", body)
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.Failf(t, "plan failed", "status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	var result grpcPlanResponse
	grpcDecodeJSON(t, resp, &result)
	return result
}

func grpcApply(t *testing.T, planID, database, env string, opts map[string]string) grpcApplyResponse {
	t.Helper()
	body := map[string]any{
		"plan_id":     planID,
		"database":    database,
		"environment": env,
	}
	if opts != nil {
		body["options"] = opts
	}
	resp := grpcPost(t, "/api/apply", body)
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.Failf(t, "apply failed", "status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	var result grpcApplyResponse
	grpcDecodeJSON(t, resp, &result)
	return result
}

func grpcProgress(t *testing.T, database, env string) grpcProgressResponse {
	t.Helper()
	path := fmt.Sprintf("/api/progress/%s?environment=%s", database, env)
	resp := grpcGet(t, path)
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.Failf(t, "progress failed", "status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	var result grpcProgressResponse
	grpcDecodeJSON(t, resp, &result)
	return result
}

func grpcProgressByApplyID(t *testing.T, applyID string) grpcProgressResponse {
	t.Helper()
	path := fmt.Sprintf("/api/progress/apply/%s", applyID)
	resp := grpcGet(t, path)
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.Failf(t, "progress by apply_id failed", "status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	var result grpcProgressResponse
	grpcDecodeJSON(t, resp, &result)
	return result
}

func grpcWaitForState(t *testing.T, database, env, state string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		prog := grpcProgress(t, database, env)
		lastState = prog.State
		if strings.Contains(strings.ToUpper(prog.State), strings.ToUpper(state)) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.Failf(t, "timeout", "waiting for state %q, last state: %s", state, lastState)
}

func grpcWaitForAnyState(t *testing.T, database, env string, states []string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		prog := grpcProgress(t, database, env)
		lastState = prog.State
		for _, s := range states {
			if strings.Contains(strings.ToUpper(prog.State), strings.ToUpper(s)) {
				return prog.State
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.Failf(t, "timeout", "waiting for any of states %v, last state: %s", states, lastState)
	return ""
}

// grpcEnsureNoActiveChange cleans up any active schema change for the given database.
func grpcEnsureNoActiveChange(t *testing.T, database, env string) {
	t.Helper()

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		prog := grpcProgress(t, database, env)
		upper := strings.ToUpper(prog.State)

		// No active change — includes STATE_COMPLETED because in gRPC mode,
		// completed tasks are terminal and don't hold locks. The Tern service's
		// SkipRevert requires a non-terminal task, so we clear storage directly instead.
		if upper == "" || upper == "STATE_NO_ACTIVE_CHANGE" || strings.Contains(upper, "NO_ACTIVE") ||
			strings.Contains(upper, "COMPLETED") || strings.Contains(upper, "REVERT_WINDOW") || strings.Contains(upper, "REVERTABLE") {
			// Clean up Tern storage so the next test starts with a fresh state
			grpcClearTernStorage(t, env)
			grpcClearSchemabotState(t)
			return
		}

		// Failed - clear all state
		if strings.Contains(upper, "FAILED") {
			grpcClearTernStorage(t, env)
			grpcClearSchemabotState(t)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// Stopped - start it so it can reach completion
		if strings.Contains(upper, "STOPPED") {
			resp := grpcPost(t, "/api/start", map[string]string{
				"database":    database,
				"environment": env,
			})
			_ = resp.Body.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		// Waiting for cutover - trigger it
		if strings.Contains(upper, "WAITING_FOR_CUTOVER") {
			resp := grpcPost(t, "/api/cutover", map[string]string{
				"database":    database,
				"environment": env,
			})
			_ = resp.Body.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		// Running state - wait
		time.Sleep(1 * time.Second)
	}
	require.Fail(t, "could not ensure no active schema change within 60s")
}

// grpcClearSchemabotState truncates SchemaBot state tables to recover from stuck states.
func grpcClearSchemabotState(t *testing.T) {
	t.Helper()
	dsn := grpcSchemabotMySQLDSN(t)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Logf("warning: could not open schemabot db to clear state: %v", err)
		return
	}
	defer utils.CloseAndLog(db)

	rows, err := db.QueryContext(context.Background(), "SHOW TABLES") //nolint:usetesting // cleanup utility
	if err != nil {
		return
	}
	defer utils.CloseAndLog(rows)

	var tables []string
	for rows.Next() {
		var table string
		_ = rows.Scan(&table)
		tables = append(tables, table)
	}

	for _, table := range tables {
		_, _ = db.ExecContext(context.Background(), "DELETE FROM `"+table+"`") //nolint:usetesting // cleanup utility
	}
}

// grpcClearTernStorage clears the Tern service's storage tables (in the tern DB, not testapp).
// This removes completed tasks/applies so the Tern service returns STATE_NO_ACTIVE_CHANGE for progress.
func grpcClearTernStorage(t *testing.T, env string) {
	t.Helper()
	// The Tern MySQL DSN points to testapp; we need the tern database on the same instance.
	testappDSN := grpcTernMySQLDSN(t, env)
	ternDSN := strings.Replace(testappDSN, "/testapp", "/tern", 1)

	db, err := sql.Open("mysql", ternDSN)
	if err != nil {
		t.Logf("warning: could not open tern storage db (%s): %v", env, err)
		return
	}
	defer utils.CloseAndLog(db)

	rows, err := db.QueryContext(context.Background(), "SHOW TABLES") //nolint:usetesting // cleanup utility
	if err != nil {
		return
	}
	defer utils.CloseAndLog(rows)

	var tables []string
	for rows.Next() {
		var table string
		_ = rows.Scan(&table)
		tables = append(tables, table)
	}

	for _, table := range tables {
		_, _ = db.ExecContext(context.Background(), "DELETE FROM `"+table+"`") //nolint:usetesting // cleanup utility
	}
}

// =============================================================================
// Table / Data Helpers
// =============================================================================

// grpcCreateTestTable creates a table on the Tern MySQL for the given environment.
// Cleanup is registered via t.Cleanup.
func grpcCreateTestTable(t *testing.T, env, tableName, ddl string) {
	t.Helper()
	dsn := grpcTernMySQLDSN(t, env)
	db, err := sql.Open("mysql", dsn)
	require.NoErrorf(t, err, "open tern mysql (%s)", env)

	_, err = db.ExecContext(t.Context(), ddl)
	require.NoErrorf(t, err, "create table %s on %s", tableName, env)
	_ = db.Close()

	t.Cleanup(func() {
		db2, err := sql.Open("mysql", dsn)
		if err != nil {
			return
		}
		defer utils.CloseAndLog(db2)
		for _, suffix := range []string{"_new", "_old", "_chkpnt", ""} {
			name := tableName
			if suffix != "" {
				name = "_" + tableName + suffix
			}
			_, _ = db2.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+name+"`") //nolint:usetesting // cleanup
		}
	})
}

// grpcSeedRows inserts test data using efficient SQL cross-joins.
func grpcSeedRows(t *testing.T, env, tableName, columns, valueTemplate string, rowCount int) {
	t.Helper()
	dsn := grpcTernMySQLDSN(t, env)
	db, err := sql.Open("mysql", dsn)
	require.NoErrorf(t, err, "open tern mysql (%s)", env)
	defer utils.CloseAndLog(db)

	seqGen := `(SELECT @row := @row + 1 as seq FROM
		(SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a`

	if rowCount >= 100 {
		seqGen += `, (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b`
	}
	if rowCount >= 1000 {
		seqGen += `, (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c`
	}
	if rowCount >= 10000 {
		seqGen += `, (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) d`
	}
	if rowCount >= 100000 {
		seqGen += `, (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) e`
	}
	seqGen += `, (SELECT @row := 0) r) nums`

	query := fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM %s LIMIT %d`,
		tableName, columns, valueTemplate, seqGen, rowCount)

	_, err = db.ExecContext(t.Context(), query)
	require.NoErrorf(t, err, "seed %s on %s", tableName, env)
}

// grpcColumnExists checks if a column exists in a table on the Tern MySQL.
func grpcColumnExists(t *testing.T, env, tableName, columnName string) bool {
	t.Helper()
	dsn := grpcTernMySQLDSN(t, env)
	db, err := sql.Open("mysql", dsn)
	require.NoErrorf(t, err, "open tern mysql (%s)", env)
	defer utils.CloseAndLog(db)

	var count int
	err = db.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM information_schema.COLUMNS
		 WHERE TABLE_SCHEMA = 'testapp' AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
		tableName, columnName,
	).Scan(&count)
	require.NoErrorf(t, err, "check column %s.%s", tableName, columnName)
	return count > 0
}

// =============================================================================
// Status Helpers
// =============================================================================

type grpcStatusResponse struct {
	ActiveCount int                    `json:"active_count"`
	Applies     []grpcStatusApplyEntry `json:"applies"`
}

type grpcStatusApplyEntry struct {
	ApplyID     string `json:"apply_id"`
	Database    string `json:"database"`
	Environment string `json:"environment"`
	State       string `json:"state"`
	Engine      string `json:"engine"`
	Caller      string `json:"caller"`
	StartedAt   string `json:"started_at,omitempty"`
	CompletedAt string `json:"completed_at,omitempty"`
	UpdatedAt   string `json:"updated_at"`
}

func grpcStatus(t *testing.T) grpcStatusResponse {
	t.Helper()
	resp := grpcGet(t, "/api/status")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "status endpoint returned non-200")
	var result grpcStatusResponse
	grpcDecodeJSON(t, resp, &result)
	return result
}

// grpcWaitForStatusState polls the /api/status endpoint until the given
// apply's state matches (or stops matching) the expected state.
// Set negate to true to wait until the state is NOT the expected state.
func grpcWaitForStatusState(t *testing.T, applyID, expectedState string, negate bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		s := grpcStatus(t)
		for i := range s.Applies {
			if s.Applies[i].ApplyID == applyID {
				lastState = s.Applies[i].State
				matched := state.IsState(lastState, expectedState)
				if (!negate && matched) || (negate && !matched) {
					return
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	if negate {
		require.Failf(t, "timeout", "status still shows %q for apply %s, expected NOT %q", lastState, applyID, expectedState)
	} else {
		require.Failf(t, "timeout", "status shows %q for apply %s, expected %q", lastState, applyID, expectedState)
	}
}

// uniqueGRPCTableName generates a unique table name for test isolation.
func uniqueGRPCTableName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano()%100000)
}
