//go:build e2e

package local

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
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

func waitForTableInProgress(t *testing.T, binPath, schemaDir, endpoint, dbName, env, tableName string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastOut string
	for time.Now().Before(deadline) {
		out, _ := e2eutil.RunCLIWithErrorInDir(t, binPath, schemaDir, "progress",
			"--database", dbName,
			"-e", env,
			"--endpoint", endpoint,
			"--watch=false",
		)
		lastOut = out
		if strings.Contains(e2eutil.StripANSI(out), tableName) {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	require.Failf(t, "timeout", "timeout waiting for table %s in progress, last output: %s", tableName, lastOut)
}

func ensureNoActiveChange(t *testing.T, endpoint string) {
	t.Helper()

	// Start clean — clear any stale state from previous tests
	clearSchemaBotState(t)

	deadline := time.Now().Add(30 * time.Second)
	var lastState string
	for time.Now().Before(deadline) {
		result, err := testutil.FetchProgress(endpoint, "testapp", "staging")
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		s := result.State
		lastState = s
		applyID := result.ApplyID

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
			resp, err := client.CallStartAPI(endpoint, "testapp", "staging", applyID)
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
			resp, err := client.CallCutoverAPI(endpoint, "testapp", "staging", applyID)
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
	if t != nil {
		t.Helper()
	}
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

func uniqueTableName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
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
		for _, suffix := range []string{"_new", "_old", "_chkpnt", ""} {
			name := tableName
			if suffix != "" {
				name = "_" + tableName + suffix
			}
			_, _ = db2.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+name+"`") //nolint:usetesting // runs after test context cancelled
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
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rows, err := db.QueryContext(t.Context(), fmt.Sprintf("SHOW INDEX FROM %s WHERE Key_name = ?", tableName), indexName)
		if err == nil {
			if rows.Next() {
				_ = rows.Close()
				return
			}
			_ = rows.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	var tblName, createStmt string
	_ = db.QueryRowContext(t.Context(), fmt.Sprintf("SHOW CREATE TABLE %s", tableName)).Scan(&tblName, &createStmt)
	require.Failf(t, "timeout", "timeout waiting for index %s on %s, table structure: %s", indexName, tableName, createStmt)
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
