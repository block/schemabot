//go:build e2e

package local

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/e2eutil"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

var cli e2eutil.CLIFinder

func schemabotURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("E2E_SCHEMABOT_URL")
	require.NotEmpty(t, url, "E2E_SCHEMABOT_URL environment variable not set")
	return url
}

func mysqlDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("E2E_MYSQL_DSN")
	if dsn == "" {
		t.Skip("E2E_MYSQL_DSN environment variable not set")
	}
	return dsn
}

func testappStagingDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("E2E_TESTAPP_STAGING_DSN")
	if dsn == "" {
		t.Skip("E2E_TESTAPP_STAGING_DSN environment variable not set")
	}
	return dsn
}

func testappProductionDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("E2E_TESTAPP_PRODUCTION_DSN")
	if dsn == "" {
		t.Skip("E2E_TESTAPP_PRODUCTION_DSN environment variable not set")
	}
	return dsn
}

func runCLI(t *testing.T, binPath string, args ...string) string {
	t.Helper()
	out, err := runCLIWithError(t, binPath, args...)
	require.NoErrorf(t, err, "CLI command failed\nOutput: %s", out)
	return out
}

func runCLIWithError(t *testing.T, binPath string, args ...string) (string, error) {
	t.Helper()
	return e2eutil.RunCLIWithErrorInDir(t, binPath, "", args...)
}

func newSchemaDir(t *testing.T) string {
	t.Helper()
	return e2eutil.NewSchemaDirForDB(t, "testapp")
}

func openTestappStaging(t *testing.T) *sql.DB {
	t.Helper()
	dsn := testappStagingDSN(t)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "open testapp staging db")
	t.Cleanup(func() { utils.CloseAndLog(db) })
	return db
}

func assertNotContains(t *testing.T, output, unexpected string) {
	t.Helper()
	stripped := e2eutil.StripANSI(output)
	assert.NotContains(t, stripped, unexpected, "expected output to NOT contain %q, got:\n%s", unexpected, output)
}

func buildCLI(t *testing.T) string {
	t.Helper()
	// Walk up from cwd to find the repo root (directory containing go.mod).
	// This works in worktrees where the e2e/local directory may be deeply nested.
	wd, err := os.Getwd()
	require.NoError(t, err)
	return cli.FindOrBuild(t, findModuleRoot(t, wd), "./pkg/cmd")
}

// findModuleRoot walks up from dir until it finds a go.mod file.
// This works correctly in both the repo root and git worktrees.
func findModuleRoot(t *testing.T, start string) string {
	t.Helper()
	dir := start
	for {
		_, err := os.Stat(filepath.Join(dir, "go.mod"))
		if err == nil {
			return dir
		}
		if !os.IsNotExist(err) {
			t.Fatalf("stat %s/go.mod: %v", dir, err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find go.mod in any parent of %s", start)
		}
		dir = parent
	}
}

// runE2EComposeCommand executes docker compose against the e2e stack from the
// repository root, so tests can control real e2e services without depending on
// the current package working directory.
func runE2EComposeCommand(t *testing.T, args ...string) string {
	t.Helper()

	wd, err := os.Getwd()
	require.NoError(t, err)
	root := findModuleRoot(t, wd)
	composeFile := filepath.Join(root, "deploy/e2e/docker-compose.yml")
	cmdArgs := append([]string{"compose", "-f", composeFile}, args...)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", cmdArgs...)
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "docker %s failed\nOutput: %s", strings.Join(cmdArgs, " "), string(output))
	return string(output)
}

// crashE2ESchemaBotServer kills only the SchemaBot server process. The backing
// MySQL containers stay up, preserving apply/task rows for operator recovery.
func crashE2ESchemaBotServer(t *testing.T) {
	t.Helper()
	runE2EComposeCommand(t, "kill", "-s", "SIGKILL", "schemabot")
}

// startE2ESchemaBotServer starts the SchemaBot service and waits until the
// health endpoint is ready before the test resumes polling apply state.
func startE2ESchemaBotServer(t *testing.T, endpoint string) {
	t.Helper()
	runE2EComposeCommand(t, "up", "-d", "schemabot")
	waitForSchemaBotHealth(t, endpoint, 30*time.Second)
}

func waitForSchemaBotHealth(t *testing.T, endpoint string, timeout time.Duration) {
	t.Helper()

	var lastErr error
	testutil.Poll(t, timeout, 500*time.Millisecond,
		func() bool {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/health", nil)
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				lastErr = err
				return false
			}
			statusCode := resp.StatusCode
			require.NoError(t, resp.Body.Close())
			if statusCode == http.StatusOK {
				return true
			}
			lastErr = fmt.Errorf("health returned status %d", statusCode)
			return false
		},
		func() string {
			return fmt.Sprintf("SchemaBot did not become healthy within %s: %v", timeout, lastErr)
		},
	)
}

func waitForTableInProgress(t *testing.T, binPath, schemaDir, endpoint, applyID, tableName string, timeout time.Duration) {
	t.Helper()
	var lastOut string
	testutil.Poll(t, timeout, 300*time.Millisecond,
		func() bool {
			out, _ := e2eutil.RunCLIWithErrorInDir(t, binPath, schemaDir, "progress",
				applyID,
				"--endpoint", endpoint,
				"--watch=false",
			)
			lastOut = out
			return strings.Contains(e2eutil.StripANSI(out), tableName)
		},
		func() string {
			return fmt.Sprintf("timeout waiting for table %s in progress, last output: %s", tableName, lastOut)
		},
	)
}

func ensureNoActiveChange(t *testing.T, endpoint string) {
	t.Helper()

	// Start clean — clear any stale state from previous tests
	clearSchemaBotState(t)

	deadline := time.Now().Add(30 * time.Second)
	var lastState string
	for time.Now().Before(deadline) {
		result, err := client.GetStatus(endpoint)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		// Check if any apply is active for testapp/staging
		s := state.NoActiveChange
		var applyID string
		for _, a := range result.Applies {
			if a.Database == "testapp" && a.Environment == "staging" {
				s = a.State
				applyID = a.ApplyID
				break
			}
		}
		lastState = s

		if s == state.NoActiveChange || s == state.Apply.Completed {
			return
		}

		if s == state.Apply.Failed {
			log.Printf("Found failed schema change (%s), clearing SchemaBot state...", applyID)
			clearSchemaBotState(t)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if s == state.Apply.Stopped {
			log.Printf("Found stopped schema change (%s), restarting...", applyID)
			resp, err := client.CallStartAPI(endpoint, "staging", applyID)
			if err != nil {
				log.Printf("Start failed for %s: %v", applyID, err)
			} else if !resp.Accepted {
				log.Printf("Start not accepted for %s: %s", applyID, resp.ErrorMessage)
			}
			time.Sleep(1 * time.Second)
			continue
		}

		if s == state.Apply.WaitingForCutover {
			// Cutover is async — triggers sentinel drop, Spirit completes in background.
			// Poll loop will pick up completion on next iteration.
			log.Printf("Found schema change waiting for cutover (%s), cutting over...", applyID)
			resp, err := client.CallCutoverAPI(endpoint, "staging", applyID)
			if err != nil {
				log.Printf("Cutover failed for %s: %v", applyID, err)
			} else if !resp.Accepted {
				log.Printf("Cutover not accepted for %s: %s", applyID, resp.ErrorMessage)
			}
			time.Sleep(1 * time.Second)
			continue
		}

		time.Sleep(500 * time.Millisecond)
	}
	require.Failf(t, "timeout", "could not ensure no active schema change within 30s, last API state: %s", lastState)
}

func clearSchemaBotState(t *testing.T) {
	t.Helper()
	// Reset LocalScale first so any Vitess deploy still executing from a prior
	// test is cancelled before its SchemaBot rows are deleted. Clearing storage
	// first deletes rows out from under an in-flight operator drive, which keeps
	// that driver busy unwinding a lost lease and can starve the next test's
	// apply of a free driver.
	resetLocalScaleState(t)
	clearSchemaBotStorage(t)
}

func clearSchemaBotStorage(t *testing.T) {
	t.Helper()
	clearSchemaBotStateImpl()
}

func clearSchemaBotStateImpl() {
	schemabotDSN := os.Getenv("E2E_MYSQL_DSN")
	if schemabotDSN == "" {
		return
	}
	db, err := sql.Open("mysql", schemabotDSN)
	if err != nil {
		return
	}
	defer utils.CloseAndLog(db)

	rows, err := db.QueryContext(context.Background(), "SHOW TABLES")
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
		_, _ = db.ExecContext(context.Background(), "DELETE FROM `"+table+"`")
	}
	log.Printf("Cleared %d schemabot state tables", len(tables))
}

func markApplyHeartbeatStale(t *testing.T, applyID string) {
	t.Helper()

	db, err := sql.Open("mysql", mysqlDSN(t))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(t.Context()))

	result, err := db.ExecContext(t.Context(),
		"UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE apply_identifier = ?",
		applyID)
	require.NoError(t, err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rowsAffected, "expected to mark one apply heartbeat stale")
}

// resetLocalScaleState resets LocalScale through its admin API so active deploy
// execution and Vitess state are cancelled before metadata is cleared.
func resetLocalScaleState(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	err := resetLocalScaleStateWithContext(ctx)
	require.NoError(t, err, "reset LocalScale state")
}

// resetLocalScaleStateOrFatal is the TestMain variant (no *testing.T).
func resetLocalScaleStateOrFatal() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	err := resetLocalScaleStateWithContext(ctx)
	cancel()
	if err != nil {
		log.Fatalf("reset LocalScale state: %v", err)
	}
}

func resetLocalScaleStateWithContext(ctx context.Context) error {
	localscaleURL := os.Getenv("LOCALSCALE_URL")
	if localscaleURL == "" {
		return fmt.Errorf("LOCALSCALE_URL not set")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, localscaleURL+"/admin/reset-state", strings.NewReader("{}"))
	if err != nil {
		return fmt.Errorf("create reset request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute reset request: %w", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read reset response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("reset state status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func uniqueSchemaIdentifier(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

func uniqueTableName(prefix string) string {
	return uniqueSchemaIdentifier(prefix)
}

func createTestTable(t *testing.T, tableName, ddlStmt string) {
	t.Helper()
	dsn := testappStagingDSN(t)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "open db")
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(t.Context(), ddlStmt)
	require.NoErrorf(t, err, "create table %s", tableName)

	t.Cleanup(func() {
		db2, err := sql.Open("mysql", dsn)
		if err != nil {
			return
		}
		defer utils.CloseAndLog(db2)
		ctx, cancel := testutil.CleanupContext(30 * time.Second)
		defer cancel()
		for _, suffix := range []string{"_new", "_old", "_chkpnt", ""} {
			name := tableName
			if suffix != "" {
				name = "_" + tableName + suffix
			}
			_, _ = db2.ExecContext(ctx, "DROP TABLE IF EXISTS `"+name+"`")
		}
	})
}

func dropTestTable(t *testing.T, tableName string) {
	t.Helper()
	dsn := testappStagingDSN(t)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return
	}
	defer utils.CloseAndLog(db)
	_, _ = db.ExecContext(t.Context(), "DROP TABLE IF EXISTS _"+tableName+"_new")
	_, _ = db.ExecContext(t.Context(), "DROP TABLE IF EXISTS _"+tableName+"_old")
	_, _ = db.ExecContext(t.Context(), "DROP TABLE IF EXISTS _"+tableName+"_chkpnt")
	_, _ = db.ExecContext(t.Context(), "DROP TABLE IF EXISTS "+tableName)
}

func writeBaseFixtureSchemas(t *testing.T, schemaDir string) {
	t.Helper()
	dsn := testappStagingDSN(t)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return
	}
	defer utils.CloseAndLog(db)

	baseTables := []string{"users", "orders", "products"}
	for _, name := range baseTables {
		var tableName, createStmt string
		err := db.QueryRowContext(t.Context(), fmt.Sprintf("SHOW CREATE TABLE `%s`", name)).Scan(&tableName, &createStmt)
		if err == nil && createStmt != "" {
			e2eutil.WriteFile(t, filepath.Join(schemaDir, name+".sql"), createStmt+";")
		}
	}
}

func writeExistingTablesSchema(t *testing.T, schemaDir string) {
	t.Helper()
	dsn := testappStagingDSN(t)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return
	}
	defer utils.CloseAndLog(db)

	tables, err := table.LoadSchemaFromDB(t.Context(), db, table.WithoutUnderscoreTables)
	if err != nil {
		return
	}

	for _, ts := range tables {
		e2eutil.WriteFile(t, filepath.Join(schemaDir, ts.Name+".sql"), ts.Schema+";")
	}
}

func waitForIndex(t *testing.T, db *sql.DB, tableName, indexName string, timeout time.Duration) {
	t.Helper()
	testutil.Poll(t, timeout, 500*time.Millisecond,
		func() bool {
			rows, err := db.QueryContext(t.Context(), fmt.Sprintf("SHOW INDEX FROM %s WHERE Key_name = ?", tableName), indexName)
			if err != nil {
				return false
			}
			found := rows.Next()
			_ = rows.Close()
			return found
		},
		func() string {
			var tblName, createStmt string
			_ = db.QueryRowContext(t.Context(), fmt.Sprintf("SHOW CREATE TABLE %s", tableName)).Scan(&tblName, &createStmt)
			return fmt.Sprintf("timeout waiting for index %s on %s, table structure: %s", indexName, tableName, createStmt)
		},
	)
}

func seedTestRows(t *testing.T, db *sql.DB, tableName string, columns string, valueTemplate string, rowCount int) {
	t.Helper()

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
	if rowCount > 10000 {
		seqGen += `, (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) e`
	}
	seqGen += `, (SELECT @row := 0) r) nums`

	query := fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM %s LIMIT %d`,
		tableName, columns, valueTemplate, seqGen, rowCount)

	_, err := db.ExecContext(t.Context(), query)
	require.NoErrorf(t, err, "seed %s", tableName)
}
