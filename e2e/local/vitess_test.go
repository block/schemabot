//go:build e2e

package local

import (
	"encoding/json"
	"fmt"
	"maps"
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
)

// =============================================================================
// Vitess/PlanetScale Engine E2E Tests
//
// These test the PlanetScale engine flow against LocalScale + real Vitess.
// They run as part of `make test-e2e-local` when LocalScale is in the docker-compose.
//
// Database: testapp-vitess (type: vitess)
// Environments: staging, production
// Keyspaces: testapp (unsharded, 1 shard), testapp_sharded (2 shards)
//
// Test isolation: Tests that CREATE new tables use unique names and are fully
// isolated. Tests that ALTER existing tables must restore the original schema
// via a cleanup apply to prevent drift between tests.
// =============================================================================

const vitessDB = "testapp-vitess"

func vitessAvailable(t *testing.T) {
	t.Helper()
	if os.Getenv("LOCALSCALE_URL") == "" {
		t.Skip("LOCALSCALE_URL not set (LocalScale not running)")
	}
}

// newVitessSchemaDir creates a temp schema directory with schemabot.yaml and
// the given SQL/JSON files organized by keyspace subdirectory.
func newVitessSchemaDir(t *testing.T, sqlFiles map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	e2eutil.WriteFile(t, filepath.Join(dir, "schemabot.yaml"), "database: "+vitessDB+"\ntype: vitess\n")
	for path, content := range sqlFiles {
		fullPath := filepath.Join(dir, path)
		require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0o755))
		e2eutil.WriteFile(t, fullPath, content)
	}
	return dir
}

// vitessApplyAndWait runs apply in log mode and waits for completion.
func vitessApplyAndWait(t *testing.T, schemaDir, env string, extraArgs ...string) string {
	t.Helper()
	start := time.Now()
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	args := []string{"apply", "-s", ".", "-e", env, "--endpoint", endpoint, "-y", "-o", "log", "--allow-unsafe"}
	args = append(args, extraArgs...)
	t.Logf("vitessApplyAndWait: starting CLI apply (elapsed=%s)", time.Since(start))
	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, args...)
	t.Logf("vitessApplyAndWait: CLI returned (elapsed=%s)", time.Since(start))
	e2eutil.AssertContains(t, out, "Apply started")

	applyID := extractApplyIDFromLog(out)
	require.NotEmpty(t, applyID)
	t.Logf("vitessApplyAndWait: waiting for completion apply_id=%s (elapsed=%s)", applyID, time.Since(start))
	waitForApplyState(t, endpoint, applyID, state.Apply.Completed, testutil.PollDeadline)
	t.Logf("vitessApplyAndWait: done (elapsed=%s)", time.Since(start))
	return out
}

// vitessBaseSchema reads the canonical Vitess schema files from examples/.
// This ensures the test schema always matches the source of truth.
func vitessBaseSchema() map[string]string {
	baseDir := "examples/vitess/schema"
	// Find the repo root by looking for go.mod
	dir, _ := os.Getwd()
	for dir != "/" {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			baseDir = filepath.Join(dir, "examples/vitess/schema")
			break
		}
		dir = filepath.Dir(dir)
	}

	files := make(map[string]string)
	keyspaces := []string{"testapp", "testapp_sharded"}
	for _, ks := range keyspaces {
		entries, err := os.ReadDir(filepath.Join(baseDir, ks))
		if err != nil {
			// Keyspace directory may not exist in the schema dir
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			data, err := os.ReadFile(filepath.Join(baseDir, ks, e.Name()))
			if err != nil {
				panic(fmt.Sprintf("failed to read schema file %s/%s: %v", ks, e.Name(), err))
			}
			files[ks+"/"+e.Name()] = string(data)
		}
	}
	return files
}

// vitessSchemaWithOverrides returns the full base schema with specific files overridden.
// This ensures all keyspace tables are present so the differ doesn't generate DROPs.
func vitessSchemaWithOverrides(overrides map[string]string) map[string]string {
	files := vitessBaseSchema()
	maps.Copy(files, overrides)
	return files
}

// localscaleAdminPost delegates to the shared e2eutil helper.
func localscaleAdminPost(t *testing.T, endpoint string, body string) ([]byte, error) {
	t.Helper()
	return e2eutil.LocalScaleAdminPost(t, os.Getenv("LOCALSCALE_URL"), endpoint, body)
}

// vitessResetVSchema seeds the base VSchema directly via LocalScale admin endpoint.
// This ensures vtgate's VSchema matches the examples/vitess/schema files regardless
// of what previous tests may have applied.
func vitessResetVSchema(t *testing.T) {
	t.Helper()
	baseFiles := vitessBaseSchema()
	for _, ks := range []string{"testapp", "testapp_sharded"} {
		vschemaContent, ok := baseFiles[ks+"/vschema.json"]
		if !ok {
			continue
		}
		for _, org := range []string{"localscale-staging", "localscale-production"} {
			body := fmt.Sprintf(`{"org":%q,"database":%q,"keyspace":%q,"vschema":%s}`,
				org, vitessDB, ks, vschemaContent)
			_, err := localscaleAdminPost(t, "/admin/seed-vschema", body)
			if err != nil {
				t.Logf("reset vschema for %s/%s: %v", org, ks, err)
			}
		}
	}
}

// vitessRestoreBaseSchema resets the Vitess schema to the canonical base state
// using direct DDL via admin endpoints, bypassing the deploy request lifecycle.
// Drops extra tables, indexes, and columns that tests added (~1s vs ~10s for a full apply).
func vitessRestoreBaseSchema(t *testing.T, _ string) {
	t.Helper()
	start := time.Now()
	clearSchemaBotState(t)
	t.Logf("vitessRestoreBaseSchema: clearState done (elapsed=%s)", time.Since(start))
	vitessResetVSchema(t)
	t.Logf("vitessRestoreBaseSchema: resetVSchema done (elapsed=%s)", time.Since(start))
	vitessCleanupSchema(t)
	t.Logf("vitessRestoreBaseSchema: cleanup done (elapsed=%s)", time.Since(start))
}

// baseSchemaTableNames returns the expected table names per keyspace,
// derived from the example schema files (source of truth).
func baseSchemaTableNames() map[string][]string {
	files := vitessBaseSchema()
	result := make(map[string][]string)
	for path := range files {
		parts := strings.SplitN(path, "/", 2)
		if len(parts) != 2 {
			continue
		}
		ks := parts[0]
		name := strings.TrimSuffix(parts[1], ".sql")
		name = strings.TrimSuffix(name, ".json") // skip vschema.json
		if strings.HasSuffix(parts[1], ".json") {
			continue
		}
		result[ks] = append(result[ks], name)
	}
	return result
}

// vitessCleanupSchema drops extra tables and non-base indexes via direct DDL
// on vtgate, restoring the schema to a clean state for the next test.
// Uses the example schema files as the source of truth for which tables should exist.
func vitessCleanupSchema(t *testing.T) {
	t.Helper()
	localscaleURL := os.Getenv("LOCALSCALE_URL")
	if localscaleURL == "" {
		t.Log("LOCALSCALE_URL not set, skipping vitess schema cleanup")
		return
	}

	expected := baseSchemaTableNames()
	for _, org := range []string{"localscale-staging", "localscale-production"} {
		for ks, expectedTables := range expected {
			expectedSet := make(map[string]bool, len(expectedTables))
			for _, name := range expectedTables {
				expectedSet[name] = true
			}

			// Drop extra tables
			tables := vitessAdminQuery(t, localscaleURL, org, ks, "SHOW TABLES")
			for _, row := range tables {
				if len(row) == 0 {
					continue
				}
				if !expectedSet[row[0]] {
					vitessAdminDDL(t, localscaleURL, org, ks, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", row[0]))
				}
			}

			// Drop non-base indexes from base tables (tests add indexes that must be cleaned)
			for _, tbl := range expectedTables {
				indexes := vitessAdminQuery(t, localscaleURL, org, ks, fmt.Sprintf("SHOW INDEX FROM `%s`", tbl))
				baseIdxSet := baseTableIndexes(ks, tbl)
				seen := make(map[string]bool)
				for _, idx := range indexes {
					if len(idx) < 3 {
						continue
					}
					idxName := idx[2] // Key_name column
					if seen[idxName] || baseIdxSet[idxName] {
						seen[idxName] = true
						continue
					}
					seen[idxName] = true
					vitessAdminDDL(t, localscaleURL, org, ks, fmt.Sprintf("ALTER TABLE `%s` DROP INDEX `%s`", tbl, idxName))
				}

				// Drop non-base columns
				cols := vitessAdminQuery(t, localscaleURL, org, ks, fmt.Sprintf("SHOW COLUMNS FROM `%s`", tbl))
				baseColSet := baseTableColumns(ks, tbl)
				for _, col := range cols {
					if len(col) == 0 {
						continue
					}
					if !baseColSet[col[0]] {
						vitessAdminDDL(t, localscaleURL, org, ks, fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN `%s`", tbl, col[0]))
					}
				}
			}
		}
	}
}

// baseTableIndexes returns the expected index names for a table, parsed from the schema files.
func baseTableIndexes(keyspace, table string) map[string]bool {
	files := vitessBaseSchema()
	content, ok := files[keyspace+"/"+table+".sql"]
	if !ok {
		return map[string]bool{"PRIMARY": true}
	}
	result := map[string]bool{"PRIMARY": true}
	// Parse KEY `name` patterns from CREATE TABLE
	for line := range strings.SplitSeq(content, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "KEY ") || strings.HasPrefix(line, "UNIQUE KEY ") || strings.HasPrefix(line, "INDEX ") {
			// Extract index name from KEY `name` or UNIQUE KEY `name`
			start := strings.Index(line, "`")
			end := strings.Index(line[start+1:], "`")
			if start >= 0 && end >= 0 {
				result[line[start+1:start+1+end]] = true
			}
		}
	}
	return result
}

// baseTableColumns returns the expected column names for a table, parsed from the schema files.
func baseTableColumns(keyspace, table string) map[string]bool {
	files := vitessBaseSchema()
	content, ok := files[keyspace+"/"+table+".sql"]
	if !ok {
		return nil
	}
	result := make(map[string]bool)
	for line := range strings.SplitSeq(content, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "`") {
			end := strings.Index(line[1:], "`")
			if end >= 0 {
				result[line[1:1+end]] = true
			}
		}
	}
	return result
}

// vitessAdminQuery runs a query via the LocalScale vtgate-exec admin endpoint.
func vitessAdminQuery(t *testing.T, localscaleURL, org, keyspace, query string) [][]string {
	t.Helper()
	_ = localscaleURL // preserved for call-site compatibility; localscaleAdminPost reads the env var
	body := fmt.Sprintf(`{"org":%q,"database":%q,"keyspace":%q,"query":%q}`,
		org, vitessDB, keyspace, query)
	respBody, err := localscaleAdminPost(t, "/admin/vtgate-exec", body)
	require.NoError(t, err, "vtgate-exec query %s", query)
	var result struct {
		Rows [][]string `json:"rows"`
	}
	require.NoError(t, json.NewDecoder(strings.NewReader(string(respBody))).Decode(&result))
	return result.Rows
}

// vitessAdminDDL runs a DDL statement via the LocalScale seed-ddl admin endpoint.
// Best-effort: logs failures (e.g., dropping a non-existent index) without failing the test.
func vitessAdminDDL(t *testing.T, localscaleURL, org, keyspace, ddl string) {
	t.Helper()
	_ = localscaleURL // preserved for call-site compatibility; localscaleAdminPost reads the env var
	body := fmt.Sprintf(`{"org":%q,"database":%q,"keyspace":%q,"statements":[%q]}`,
		org, vitessDB, keyspace, ddl)
	_, err := localscaleAdminPost(t, "/admin/seed-ddl", body)
	if err != nil {
		t.Logf("vitessAdminDDL: %s: %v (non-fatal)", ddl, err)
	}
}

// extractApplyIDFromLog extracts the apply ID from log mode output.
// Handles both "Apply started: apply-xxx" text and "apply_id=apply-xxx" logfmt.
func extractApplyIDFromLog(output string) string {
	for line := range strings.SplitSeq(output, "\n") {
		line = strings.TrimSpace(line)
		// Log mode: "Apply started: apply-xxx"
		if after, ok := strings.CutPrefix(line, "Apply started: "); ok {
			return after
		}
		// Logfmt: apply_id=apply-xxx
		if _, after, ok := strings.Cut(line, "apply_id="); ok {
			rest := after
			if before, _, ok := strings.Cut(rest, " "); ok {
				return before
			}
			return rest
		}
		// JSON mode fallback
		if strings.HasPrefix(line, "{") {
			var result struct {
				ApplyID string `json:"apply_id"`
			}
			if json.Unmarshal([]byte(line), &result) == nil && result.ApplyID != "" {
				return result.ApplyID
			}
		}
	}
	return ""
}

// --- Plan Tests ---

func TestVitess_Plan_Header(t *testing.T) {
	vitessAvailable(t)
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	tableName := uniqueTableName("vhdr")
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp/" + tableName + ".sql": fmt.Sprintf(`CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, tableName),
	}))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan",
		"-s", ".", "-e", "staging", "--endpoint", endpoint)

	e2eutil.AssertContains(t, out, "Vitess Schema Change Plan")
	e2eutil.AssertContains(t, out, vitessDB)
	assert.NotContains(t, out, "Schema name")
}

func TestVitess_Plan_JSON(t *testing.T) {
	vitessAvailable(t)
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	tableName := uniqueTableName("vjson")
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp/" + tableName + ".sql": fmt.Sprintf(`CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, tableName),
	}))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan",
		"-s", ".", "-e", "staging", "--endpoint", endpoint, "--json")

	var result map[string]*json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(out), &result))

	var staging struct {
		Engine string `json:"engine"`
	}
	stagingRaw, ok := result["staging"]
	require.True(t, ok, "expected 'staging' key in plan output")
	require.NoError(t, json.Unmarshal(*stagingRaw, &staging))
	assert.Equal(t, "PlanetScale", staging.Engine)
}

func TestVitess_Plan_CreateTable(t *testing.T) {
	vitessAvailable(t)
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	tableName := uniqueTableName("vplan")
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp/" + tableName + ".sql": fmt.Sprintf(`CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, tableName),
	}))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan",
		"-s", ".", "-e", "staging", "--endpoint", endpoint)

	e2eutil.AssertContains(t, out, "CREATE TABLE")
	e2eutil.AssertContains(t, out, tableName)
}

// --- Apply Tests ---

func TestVitess_Apply_CreateTable_Unsharded(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)
	defer vitessRestoreBaseSchema(t, "staging")

	tableName := uniqueTableName("vcreate")
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp/" + tableName + ".sql": fmt.Sprintf(`CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, tableName),
	}))

	out := vitessApplyAndWait(t, schemaDir, "staging")

	e2eutil.AssertContains(t, out, "Table started")
	e2eutil.AssertContains(t, out, tableName)
	e2eutil.AssertContains(t, out, "Apply completed")
}

func TestVitess_Apply_AddIndex_Sharded(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)
	defer vitessRestoreBaseSchema(t, "staging")

	indexName := fmt.Sprintf("idx_e2e_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/orders.sql": fmt.Sprintf(`CREATE TABLE `+"`orders`"+` (
    `+"`id`"+` bigint unsigned NOT NULL,
    `+"`user_id`"+` bigint unsigned NOT NULL,
    `+"`total_cents`"+` bigint NOT NULL,
    `+"`status`"+` varchar(100) NOT NULL DEFAULT 'pending',
    `+"`created_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP,
    `+"`updated_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`+"`id`"+`),
    KEY `+"`idx_user_id`"+` (`+"`user_id`"+`),
    KEY `+"`idx_status`"+` (`+"`status`"+`),
    KEY `+"`idx_created_at`"+` (`+"`created_at`"+`),
    KEY `+"`%s`"+` (`+"`total_cents`"+`)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;`, indexName),
	}))

	out := vitessApplyAndWait(t, schemaDir, "staging")

	e2eutil.AssertContains(t, out, "ADD INDEX")
	e2eutil.AssertContains(t, out, indexName)
	e2eutil.AssertContains(t, out, "Apply completed")
	// Sharded table (2 shards) should show shard progress
	assert.Contains(t, out, "shards=2")
}

func TestVitess_Apply_AddColumn_Sharded(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)

	defer vitessRestoreBaseSchema(t, "staging")

	colName := fmt.Sprintf("col_e2e_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": fmt.Sprintf(`CREATE TABLE `+"`users`"+` (
  `+"`id`"+` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `+"`email`"+` varchar(255) NOT NULL,
  `+"`full_name`"+` varchar(255) NULL,
  `+"`%s`"+` varchar(100) NULL,
  `+"`created_at`"+` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `+"`idx_email`"+` (`+"`email`"+`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, colName),
	}))

	out := vitessApplyAndWait(t, schemaDir, "staging")

	e2eutil.AssertContains(t, out, "ADD COLUMN")
	e2eutil.AssertContains(t, out, colName)
	e2eutil.AssertContains(t, out, "Apply completed")

	// Verify instant DDL was used — ADD COLUMN NULL is instant in MySQL 8.4.
	// LocalScale detects instant eligibility at deploy request diff time by
	// testing ALGORITHM=INSTANT on a scratch database.
	applyID := extractApplyIDFromLog(out)
	endpoint := schemabotURL(t)
	resp, err := client.GetProgress(endpoint, applyID)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Tables, "expected table progress")
	for _, tbl := range resp.Tables {
		assert.True(t, tbl.IsInstant, "ADD COLUMN NULL should use instant DDL for table %s", tbl.TableName)
		assert.Equal(t, int32(100), tbl.PercentComplete, "instant DDL should show 100%% for table %s", tbl.TableName)
	}
}

func TestVitess_Apply_ConsecutiveApplies(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)
	defer vitessRestoreBaseSchema(t, "staging")

	// First apply: add index
	idx1 := fmt.Sprintf("idx_c1_%d", time.Now().UnixMilli()%100000)
	schemaDir1 := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": fmt.Sprintf(`CREATE TABLE `+"`users`"+` (
  `+"`id`"+` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `+"`email`"+` varchar(255) NOT NULL,
  `+"`full_name`"+` varchar(255) NULL,
  `+"`created_at`"+` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `+"`idx_email`"+` (`+"`email`"+`),
  INDEX `+"`%s`"+` (`+"`full_name`"+`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, idx1),
	}))

	out1 := vitessApplyAndWait(t, schemaDir1, "staging")
	e2eutil.AssertContains(t, out1, "Apply completed")

	clearSchemaBotState(t)

	// Second apply immediately after — tests that previous deploy's revert window
	// is auto-completed and VReplication streams are cleaned up.
	idx2 := fmt.Sprintf("idx_c2_%d", time.Now().UnixMilli()%100000)
	schemaDir2 := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": fmt.Sprintf(`CREATE TABLE `+"`users`"+` (
  `+"`id`"+` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `+"`email`"+` varchar(255) NOT NULL,
  `+"`full_name`"+` varchar(255) NULL,
  `+"`created_at`"+` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `+"`idx_email`"+` (`+"`email`"+`),
  INDEX `+"`%s`"+` (`+"`full_name`"+`),
  INDEX `+"`%s`"+` (`+"`created_at`"+`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, idx1, idx2),
	}))

	out2 := vitessApplyAndWait(t, schemaDir2, "staging")
	e2eutil.AssertContains(t, out2, "Apply completed")
}

func TestVitess_Apply_ShardProgress(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)

	defer vitessRestoreBaseSchema(t, "staging")

	indexName := fmt.Sprintf("idx_sp_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/orders.sql": fmt.Sprintf(`CREATE TABLE `+"`orders`"+` (
    `+"`id`"+` bigint unsigned NOT NULL,
    `+"`user_id`"+` bigint unsigned NOT NULL,
    `+"`total_cents`"+` bigint NOT NULL,
    `+"`status`"+` varchar(100) NOT NULL DEFAULT 'pending',
    `+"`created_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP,
    `+"`updated_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`+"`id`"+`),
    KEY `+"`idx_user_id`"+` (`+"`user_id`"+`),
    KEY `+"`idx_status`"+` (`+"`status`"+`),
    KEY `+"`idx_created_at`"+` (`+"`created_at`"+`),
    KEY `+"`%s`"+` (`+"`total_cents`"+`)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;`, indexName),
	}))

	out := vitessApplyAndWait(t, schemaDir, "staging")

	e2eutil.AssertContains(t, out, "Apply completed")
	// testapp_sharded has 2 shards — verify shard progress appears
	assert.Contains(t, out, "shards=2", "expected per-shard progress for 2-shard keyspace")
}

func TestVitess_Apply_LogMode_Lifecycle(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)

	defer vitessRestoreBaseSchema(t, "staging")

	indexName := fmt.Sprintf("idx_lm_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/products.sql": fmt.Sprintf(`CREATE TABLE `+"`products`"+` (
    `+"`id`"+` bigint unsigned NOT NULL,
    `+"`name`"+` varchar(255) NOT NULL,
    `+"`description`"+` text NULL,
    `+"`price_cents`"+` bigint NOT NULL,
    `+"`sku`"+` varchar(100) NOT NULL,
    `+"`created_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP,
    `+"`updated_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`+"`id`"+`),
    KEY `+"`idx_name`"+` (`+"`name`"+`),
    UNIQUE KEY `+"`idx_sku`"+` (`+"`sku`"+`),
    KEY `+"`idx_price`"+` (`+"`price_cents`"+`),
    KEY `+"`%s`"+` (`+"`name`"+`, `+"`price_cents`"+`)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;`, indexName),
	}))

	out := vitessApplyAndWait(t, schemaDir, "staging")

	// Verify log mode lifecycle events
	e2eutil.AssertContains(t, out, "Table started")
	e2eutil.AssertContains(t, out, "Apply completed")
	e2eutil.AssertContains(t, out, "keyspace=testapp_sharded")
}

func TestVitess_Apply_Production(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)
	defer vitessRestoreBaseSchema(t, "production")

	tableName := uniqueTableName("vprod")
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp/" + tableName + ".sql": fmt.Sprintf(`CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    label VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, tableName),
	}))

	out := vitessApplyAndWait(t, schemaDir, "production")

	e2eutil.AssertContains(t, out, "Table started")
	e2eutil.AssertContains(t, out, tableName)
	e2eutil.AssertContains(t, out, "Apply completed")
}

// --- Plan-only Tests ---

func TestVitess_Plan_NoChanges(t *testing.T) {
	vitessAvailable(t)
	vitessRestoreBaseSchema(t, "staging")
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	schemaDir := newVitessSchemaDir(t, vitessBaseSchema())
	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan",
		"-s", ".", "-e", "staging", "--endpoint", endpoint)

	e2eutil.AssertContains(t, out, "No schema changes detected")
}

func TestVitess_Plan_AddIndex(t *testing.T) {
	vitessAvailable(t)
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	indexName := fmt.Sprintf("idx_plan_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": fmt.Sprintf(`CREATE TABLE `+"`users`"+` (
  `+"`id`"+` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `+"`email`"+` varchar(255) NOT NULL,
  `+"`full_name`"+` varchar(255) NULL,
  `+"`created_at`"+` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `+"`idx_email`"+` (`+"`email`"+`),
  INDEX `+"`%s`"+` (`+"`full_name`"+`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, indexName),
	}))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan",
		"-s", ".", "-e", "staging", "--endpoint", endpoint)

	e2eutil.AssertContains(t, out, "ADD INDEX")
	e2eutil.AssertContains(t, out, indexName)
	e2eutil.AssertContains(t, out, "1 table to alter")
}

func TestVitess_Plan_AddColumn(t *testing.T) {
	vitessAvailable(t)
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	colName := fmt.Sprintf("col_plan_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": fmt.Sprintf(`CREATE TABLE `+"`users`"+` (
  `+"`id`"+` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `+"`email`"+` varchar(255) NOT NULL,
  `+"`full_name`"+` varchar(255) NULL,
  `+"`%s`"+` text NULL,
  `+"`created_at`"+` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `+"`idx_email`"+` (`+"`email`"+`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, colName),
	}))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan",
		"-s", ".", "-e", "staging", "--endpoint", endpoint)

	e2eutil.AssertContains(t, out, "ADD COLUMN")
	e2eutil.AssertContains(t, out, colName)
	e2eutil.AssertContains(t, out, "1 table to alter")
}

func TestVitess_Plan_MultiKeyspace(t *testing.T) {
	vitessAvailable(t)
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	tableName := uniqueTableName("vmulti")
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		// Change in sharded keyspace
		"testapp_sharded/users.sql": `CREATE TABLE ` + "`users`" + ` (
  ` + "`id`" + ` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ` + "`email`" + ` varchar(255) NOT NULL,
  ` + "`full_name`" + ` varchar(255) NULL,
  ` + "`created_at`" + ` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX ` + "`idx_email`" + ` (` + "`email`" + `),
  INDEX ` + "`idx_created_at`" + ` (` + "`created_at`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`,
		// New table in unsharded keyspace
		"testapp/" + tableName + ".sql": fmt.Sprintf(`CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    label VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, tableName),
	}))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan",
		"-s", ".", "-e", "staging", "--endpoint", endpoint)

	// Both keyspaces should appear in plan
	e2eutil.AssertContains(t, out, "ADD INDEX")
	e2eutil.AssertContains(t, out, "CREATE TABLE")
	e2eutil.AssertContains(t, out, tableName)
}

func TestVitess_Plan_Deduplication(t *testing.T) {
	vitessAvailable(t)
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	// Add an index — this change is the same on both envs since
	// neither env has this index. Note: earlier tests may have modified
	// production, so we use an index change that's additive.
	indexName := fmt.Sprintf("idx_dedup_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": fmt.Sprintf(`CREATE TABLE `+"`users`"+` (
  `+"`id`"+` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `+"`email`"+` varchar(255) NOT NULL,
  `+"`full_name`"+` varchar(255) NULL,
  `+"`created_at`"+` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `+"`idx_email`"+` (`+"`email`"+`),
  INDEX `+"`%s`"+` (`+"`full_name`"+`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, indexName),
	}))

	// Plan for staging only, then all envs — compare
	outStaging := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan",
		"-s", ".", "-e", "staging", "--endpoint", endpoint)
	e2eutil.AssertContains(t, outStaging, "ADD INDEX")
	e2eutil.AssertContains(t, outStaging, "Staging")

	// Plan without env should show both or dedup
	outAll := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan",
		"-s", ".", "--endpoint", endpoint)
	e2eutil.AssertContains(t, outAll, "ADD INDEX")
	e2eutil.AssertContains(t, outAll, indexName)
}

func TestVitess_Plan_UnsafeBlocked(t *testing.T) {
	vitessAvailable(t)
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	// Remove the idx_email index from users — this is a DROP INDEX (unsafe)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": `CREATE TABLE ` + "`users`" + ` (
  ` + "`id`" + ` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ` + "`email`" + ` varchar(255) NOT NULL,
  ` + "`full_name`" + ` varchar(255) NULL,
  ` + "`created_at`" + ` timestamp NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`,
	}))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan",
		"-s", ".", "-e", "staging", "--endpoint", endpoint)

	e2eutil.AssertContains(t, out, "DROP INDEX")
	e2eutil.AssertContains(t, out, "Unsafe Changes Detected")
}

// --- Apply: CREATE TABLE (sharded + sequence + VSchema) ---

func TestVitess_Apply_CreateTable_Sharded_WithSequence(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)
	defer vitessRestoreBaseSchema(t, "staging")

	tableName := uniqueTableName("vshrd")
	seqName := tableName + "_seq"
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		// Sharded table
		"testapp_sharded/" + tableName + ".sql": fmt.Sprintf(`CREATE TABLE `+"`%s`"+` (
    `+"`id`"+` bigint unsigned NOT NULL,
    `+"`user_id`"+` bigint unsigned NOT NULL,
    `+"`amount`"+` bigint NOT NULL DEFAULT 0,
    `+"`created_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`+"`id`"+`),
    KEY `+"`idx_user_id`"+` (`+"`user_id`"+`)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;`, tableName),
		// Sequence table in unsharded keyspace
		"testapp/" + seqName + ".sql": fmt.Sprintf(`CREATE TABLE `+"`%s`"+` (
    `+"`id`"+` int unsigned NOT NULL DEFAULT '0',
    `+"`next_id`"+` bigint unsigned,
    `+"`cache`"+` bigint unsigned,
    PRIMARY KEY (`+"`id`"+`)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci,
  COMMENT 'vitess_sequence';`, seqName),
		// Updated VSchema with new table
		"testapp_sharded/vschema.json": fmt.Sprintf(`{
  "sharded": true,
  "vindexes": {"hash": {"type": "hash"}},
  "tables": {
    "users": {"column_vindexes": [{"column": "id", "name": "hash"}], "auto_increment": {"column": "id", "sequence": "users_seq"}},
    "orders": {"column_vindexes": [{"column": "user_id", "name": "hash"}], "auto_increment": {"column": "id", "sequence": "orders_seq"}},
    "products": {"column_vindexes": [{"column": "id", "name": "hash"}], "auto_increment": {"column": "id", "sequence": "products_seq"}},
    "%s": {"column_vindexes": [{"column": "user_id", "name": "hash"}], "auto_increment": {"column": "id", "sequence": "%s"}}
  }
}`, tableName, seqName),
		"testapp/vschema.json": fmt.Sprintf(`{
  "tables": {
    "users_seq": {"type": "sequence"},
    "orders_seq": {"type": "sequence"},
    "products_seq": {"type": "sequence"},
    "%s": {"type": "sequence"}
  }
}`, seqName),
	}))

	out := vitessApplyAndWait(t, schemaDir, "staging")

	e2eutil.AssertContains(t, out, "Table started")
	e2eutil.AssertContains(t, out, tableName)
	e2eutil.AssertContains(t, out, seqName)
	e2eutil.AssertContains(t, out, "Apply completed")
	// Sharded table should show 2 shards, sequence should show 1
	assert.Contains(t, out, "shards=2")
	assert.Contains(t, out, "shards=1")
}

// --- VSchema-only changes ---

func TestVitess_Plan_VSchemaOnly(t *testing.T) {
	vitessAvailable(t)
	defer vitessRestoreBaseSchema(t, "staging")
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/vschema.json": `{"sharded":true,"vindexes":{"hash":{"type":"hash"},"xxhash":{"type":"xxhash"}},"tables":{"users":{"column_vindexes":[{"column":"id","name":"hash"}],"auto_increment":{"column":"id","sequence":"users_seq"}},"orders":{"column_vindexes":[{"column":"user_id","name":"hash"}],"auto_increment":{"column":"id","sequence":"orders_seq"}},"products":{"column_vindexes":[{"column":"id","name":"hash"}],"auto_increment":{"column":"id","sequence":"products_seq"}}}}`,
	}))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan",
		"-s", ".", "-e", "staging", "--endpoint", endpoint)

	e2eutil.AssertContains(t, out, "VSchema")
	e2eutil.AssertContains(t, out, "xxhash")
	assert.NotContains(t, out, "No schema changes detected")
}

func TestVitess_Apply_VSchemaOnly(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)
	defer vitessRestoreBaseSchema(t, "staging")

	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/vschema.json": `{"sharded":true,"vindexes":{"hash":{"type":"hash"},"xxhash":{"type":"xxhash"}},"tables":{"users":{"column_vindexes":[{"column":"id","name":"hash"}],"auto_increment":{"column":"id","sequence":"users_seq"}},"orders":{"column_vindexes":[{"column":"user_id","name":"hash"}],"auto_increment":{"column":"id","sequence":"orders_seq"}},"products":{"column_vindexes":[{"column":"id","name":"hash"}],"auto_increment":{"column":"id","sequence":"products_seq"}}}}`,
	}))

	out := vitessApplyAndWait(t, schemaDir, "staging")
	e2eutil.AssertContains(t, out, "Apply completed")
}

func TestVitess_Apply_VSchemaOnly_MultiKeyspace(t *testing.T) {
	vitessAvailable(t)
	vitessResetVSchema(t)
	clearSchemaBotState(t)
	defer vitessRestoreBaseSchema(t, "staging")

	// VSchema changes in both keyspaces: add xxhash to sharded, add audit_seq to unsharded
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/vschema.json": `{"sharded":true,"vindexes":{"hash":{"type":"hash"},"xxhash":{"type":"xxhash"}},"tables":{"users":{"column_vindexes":[{"column":"id","name":"hash"}],"auto_increment":{"column":"id","sequence":"users_seq"}},"orders":{"column_vindexes":[{"column":"user_id","name":"hash"}],"auto_increment":{"column":"id","sequence":"orders_seq"}},"products":{"column_vindexes":[{"column":"id","name":"hash"}],"auto_increment":{"column":"id","sequence":"products_seq"}}}}`,
		"testapp/vschema.json":         `{"tables":{"users_seq":{"type":"sequence"},"orders_seq":{"type":"sequence"},"products_seq":{"type":"sequence"},"audit_seq":{"type":"sequence"}}}`,
	}))

	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	// Plan should show both keyspaces
	planOut := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan",
		"-s", ".", "-e", "staging", "--endpoint", endpoint)
	e2eutil.AssertContains(t, planOut, "── testapp_sharded ──")
	e2eutil.AssertContains(t, planOut, "── testapp ──")
	e2eutil.AssertContains(t, planOut, "~ VSchema:")

	// Apply should complete
	out := vitessApplyAndWait(t, schemaDir, "staging")
	e2eutil.AssertContains(t, out, "Apply completed")
}

// --- Apply: VSchema with DDL ---

func TestVitess_Apply_VSchemaWithDDL(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)
	defer vitessRestoreBaseSchema(t, "staging")

	// VSchema change paired with a DDL change. Pure VSchema-only changes
	// are not yet detected by the plan differ — this tests that VSchema
	// is propagated when DDL changes are present.
	indexName := fmt.Sprintf("idx_vs_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": fmt.Sprintf(`CREATE TABLE `+"`users`"+` (
  `+"`id`"+` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `+"`email`"+` varchar(255) NOT NULL,
  `+"`full_name`"+` varchar(255) NULL,
  `+"`created_at`"+` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `+"`idx_email`"+` (`+"`email`"+`),
  INDEX `+"`%s`"+` (`+"`full_name`"+`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, indexName),
		"testapp_sharded/vschema.json": `{
  "sharded": true,
  "vindexes": {
    "hash": {"type": "hash"},
    "xxhash": {"type": "xxhash"}
  },
  "tables": {
    "users": {
      "column_vindexes": [{"column": "id", "name": "hash"}],
      "auto_increment": {"column": "id", "sequence": "users_seq"}
    },
    "orders": {
      "column_vindexes": [{"column": "user_id", "name": "hash"}],
      "auto_increment": {"column": "id", "sequence": "orders_seq"}
    },
    "products": {
      "column_vindexes": [{"column": "id", "name": "hash"}],
      "auto_increment": {"column": "id", "sequence": "products_seq"}
    }
  }
}`,
	}))

	out := vitessApplyAndWait(t, schemaDir, "staging")
	e2eutil.AssertContains(t, out, "Apply completed")
	e2eutil.AssertContains(t, out, indexName)
}

// --- Apply: Unsafe (DROP) ---

func TestVitess_Apply_DropIndex_BlockedWithoutFlag(t *testing.T) {
	vitessAvailable(t)
	vitessRestoreBaseSchema(t, "staging")
	defer vitessRestoreBaseSchema(t, "staging")
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	// First add an index
	indexName := fmt.Sprintf("idx_drop_%d", time.Now().UnixMilli()%100000)
	addSchema := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": fmt.Sprintf(`CREATE TABLE `+"`users`"+` (
  `+"`id`"+` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `+"`email`"+` varchar(255) NOT NULL,
  `+"`full_name`"+` varchar(255) NULL,
  `+"`created_at`"+` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `+"`idx_email`"+` (`+"`email`"+`),
  INDEX `+"`%s`"+` (`+"`full_name`"+`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, indexName),
	}))
	vitessApplyAndWait(t, addSchema, "staging")
	clearSchemaBotState(t)

	// Now try to drop it without --allow-unsafe — should be blocked
	dropSchema := newVitessSchemaDir(t, vitessBaseSchema())
	out, err := e2eutil.RunCLIWithErrorInDir(t, binPath, dropSchema, "apply",
		"-s", ".", "-e", "staging", "--endpoint", endpoint, "-y", "-o", "log")
	t.Logf("DROP INDEX apply output:\n%s", out)
	require.Error(t, err, "expected apply to fail without --allow-unsafe")
	assert.Contains(t, out, "Unsafe Changes Detected")
}

func TestVitess_Apply_DropTable_WithVSchema(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	// Step 1: Create a sharded table + sequence + VSchema entry
	tableName := uniqueTableName("vdrop")
	seqName := tableName + "_seq"
	createSchema := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/" + tableName + ".sql": fmt.Sprintf(
			"CREATE TABLE `%s` (`id` bigint unsigned NOT NULL, `data` varchar(255), PRIMARY KEY (`id`)) ENGINE InnoDB, CHARSET utf8mb4, COLLATE utf8mb4_0900_ai_ci;", tableName),
		"testapp/" + seqName + ".sql": fmt.Sprintf(
			"CREATE TABLE `%s` (`id` int unsigned NOT NULL DEFAULT '0', `next_id` bigint unsigned, `cache` bigint unsigned, PRIMARY KEY (`id`)) ENGINE InnoDB, CHARSET utf8mb4, COLLATE utf8mb4_0900_ai_ci, COMMENT 'vitess_sequence';", seqName),
		"testapp_sharded/vschema.json": fmt.Sprintf(
			`{"sharded":true,"vindexes":{"hash":{"type":"hash"}},"tables":{"users":{"column_vindexes":[{"column":"id","name":"hash"}],"auto_increment":{"column":"id","sequence":"users_seq"}},"orders":{"column_vindexes":[{"column":"user_id","name":"hash"}],"auto_increment":{"column":"id","sequence":"orders_seq"}},"products":{"column_vindexes":[{"column":"id","name":"hash"}],"auto_increment":{"column":"id","sequence":"products_seq"}},"%s":{"column_vindexes":[{"column":"id","name":"hash"}],"auto_increment":{"column":"id","sequence":"%s"}}}}`, tableName, seqName),
		"testapp/vschema.json": fmt.Sprintf(
			`{"tables":{"users_seq":{"type":"sequence"},"orders_seq":{"type":"sequence"},"products_seq":{"type":"sequence"},"%s":{"type":"sequence"}}}`, seqName),
	}))
	vitessApplyAndWait(t, createSchema, "staging")
	clearSchemaBotState(t)

	// Step 2: Plan the DROP — base schema without the new table, sequence, or VSchema entries
	dropSchema := newVitessSchemaDir(t, vitessBaseSchema())

	planOut := e2eutil.RunCLIInDir(t, binPath, dropSchema, "plan",
		"-s", ".", "-e", "staging", "--endpoint", endpoint)
	e2eutil.AssertContains(t, planOut, "DROP TABLE")
	e2eutil.AssertContains(t, planOut, tableName)
	e2eutil.AssertContains(t, planOut, "Unsafe Changes Detected")
	// VSchema diff should show the removed table entry
	e2eutil.AssertContains(t, planOut, "VSchema")

	// Step 3: Apply the DROP with --allow-unsafe
	clearSchemaBotState(t)
	out := vitessApplyAndWait(t, dropSchema, "staging")
	e2eutil.AssertContains(t, out, "Apply completed")
}

// --- Progress & Engine Tests ---

func TestVitess_Apply_DeployRequestURL(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)

	defer vitessRestoreBaseSchema(t, "staging")

	indexName := fmt.Sprintf("idx_dr_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/orders.sql": fmt.Sprintf(`CREATE TABLE `+"`orders`"+` (
    `+"`id`"+` bigint unsigned NOT NULL,
    `+"`user_id`"+` bigint unsigned NOT NULL,
    `+"`total_cents`"+` bigint NOT NULL,
    `+"`status`"+` varchar(100) NOT NULL DEFAULT 'pending',
    `+"`created_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP,
    `+"`updated_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`+"`id`"+`),
    KEY `+"`idx_user_id`"+` (`+"`user_id`"+`),
    KEY `+"`idx_status`"+` (`+"`status`"+`),
    KEY `+"`idx_created_at`"+` (`+"`created_at`"+`),
    KEY `+"`%s`"+` (`+"`total_cents`"+`)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;`, indexName),
	}))

	out := vitessApplyAndWait(t, schemaDir, "staging")

	e2eutil.AssertContains(t, out, "Apply completed")
	// Deploy request URL should appear in log output
	assert.Contains(t, out, "deploy-requests/", "expected deploy request URL in log output")
}

func TestVitess_Apply_SetupPhases(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)

	defer vitessRestoreBaseSchema(t, "staging")

	indexName := fmt.Sprintf("idx_sp2_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/products.sql": fmt.Sprintf(`CREATE TABLE `+"`products`"+` (
    `+"`id`"+` bigint unsigned NOT NULL,
    `+"`name`"+` varchar(255) NOT NULL,
    `+"`description`"+` text NULL,
    `+"`price_cents`"+` bigint NOT NULL,
    `+"`sku`"+` varchar(100) NOT NULL,
    `+"`created_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP,
    `+"`updated_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`+"`id`"+`),
    KEY `+"`idx_name`"+` (`+"`name`"+`),
    UNIQUE KEY `+"`idx_sku`"+` (`+"`sku`"+`),
    KEY `+"`idx_price`"+` (`+"`price_cents`"+`),
    KEY `+"`%s`"+` (`+"`created_at`"+`, `+"`price_cents`"+`)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;`, indexName),
	}))

	out := vitessApplyAndWait(t, schemaDir, "staging")

	e2eutil.AssertContains(t, out, "Apply completed")
	// Setup phase messages should appear during the pending phase
	assert.Contains(t, out, "Setting up branch", "expected 'Setting up branch' message during setup")
	assert.Contains(t, out, "Deploy request", "expected 'Deploy request' message during setup")
}

func TestVitess_Progress_DeployRequestMetadata(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)

	defer vitessRestoreBaseSchema(t, "staging")

	endpoint := schemabotURL(t)
	indexName := fmt.Sprintf("idx_pm_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/orders.sql": fmt.Sprintf(`CREATE TABLE `+"`orders`"+` (
    `+"`id`"+` bigint unsigned NOT NULL,
    `+"`user_id`"+` bigint unsigned NOT NULL,
    `+"`total_cents`"+` bigint NOT NULL,
    `+"`status`"+` varchar(100) NOT NULL DEFAULT 'pending',
    `+"`created_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP,
    `+"`updated_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`+"`id`"+`),
    KEY `+"`idx_user_id`"+` (`+"`user_id`"+`),
    KEY `+"`idx_status`"+` (`+"`status`"+`),
    KEY `+"`idx_created_at`"+` (`+"`created_at`"+`),
    KEY `+"`%s`"+` (`+"`total_cents`"+`)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;`, indexName),
	}))

	// Start apply without watching (returns immediately)
	binPath := buildCLI(t)
	applyOut := e2eutil.RunCLI(t, binPath, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y", "-o", "log", "--no-watch", "--allow-unsafe",
	)
	applyID := extractApplyIDFromLog(applyOut)
	require.NotEmpty(t, applyID, "expected apply ID in output")

	// Poll progress API until completion, checking for deploy_request_url along the way
	var foundURL bool
	var finalState string
	deadline := time.Now().Add(testutil.PollDeadline)
	for time.Now().Before(deadline) {
		resp, err := client.GetProgress(endpoint, applyID)
		if err == nil {
			if !foundURL && resp.Metadata != nil {
				if url := resp.Metadata["deploy_request_url"]; url != "" {
					foundURL = true
					assert.Contains(t, url, "deploy-requests/", "expected deploy request URL in metadata")
				}
			}
			if state.IsTerminalApplyState(resp.State) {
				finalState = resp.State
				break
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	assert.True(t, foundURL, "expected deploy_request_url in progress metadata during apply")
	require.NotEmpty(t, finalState, "apply did not reach terminal state within poll deadline")
	assert.Equal(t, state.Apply.Completed, finalState)
}

func TestVitess_Apply_Timestamps(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)

	defer vitessRestoreBaseSchema(t, "staging")

	endpoint := schemabotURL(t)
	indexName := fmt.Sprintf("idx_ts_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/orders.sql": fmt.Sprintf(`CREATE TABLE `+"`orders`"+` (
    `+"`id`"+` bigint unsigned NOT NULL,
    `+"`user_id`"+` bigint unsigned NOT NULL,
    `+"`total_cents`"+` bigint NOT NULL,
    `+"`status`"+` varchar(100) NOT NULL DEFAULT 'pending',
    `+"`created_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP,
    `+"`updated_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`+"`id`"+`),
    KEY `+"`idx_user_id`"+` (`+"`user_id`"+`),
    KEY `+"`idx_status`"+` (`+"`status`"+`),
    KEY `+"`idx_created_at`"+` (`+"`created_at`"+`),
    KEY `+"`%s`"+` (`+"`total_cents`"+`)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;`, indexName),
	}))

	// Start apply without watching
	binPath := buildCLI(t)
	applyOut := e2eutil.RunCLI(t, binPath, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y", "-o", "log", "--no-watch", "--allow-unsafe",
	)
	applyID := extractApplyIDFromLog(applyOut)
	require.NotEmpty(t, applyID, "expected apply ID in output")

	// Poll until completion
	waitForApplyState(t, endpoint, applyID, state.Apply.Completed, testutil.PollDeadline)

	// Verify timestamps on completed apply
	resp, err := client.GetProgress(endpoint, applyID)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Completed, resp.State)

	// Apply-level started_at should be populated and before completed_at
	assert.NotEmpty(t, resp.StartedAt, "apply started_at should be populated")
	assert.NotEmpty(t, resp.CompletedAt, "apply completed_at should be populated")
	if resp.StartedAt != "" && resp.CompletedAt != "" {
		startedAt, err := time.Parse(time.RFC3339, resp.StartedAt)
		require.NoError(t, err, "parse started_at")
		completedAt, err := time.Parse(time.RFC3339, resp.CompletedAt)
		require.NoError(t, err, "parse completed_at")
		assert.True(t, startedAt.Before(completedAt) || startedAt.Equal(completedAt),
			"started_at (%s) should be <= completed_at (%s)", resp.StartedAt, resp.CompletedAt)
	}

	// Table progress should exist with per-table timestamps
	require.NotEmpty(t, resp.Tables, "expected at least one table in progress")
	for _, tbl := range resp.Tables {
		assert.NotEmpty(t, tbl.Status, "table %s should have a status", tbl.TableName)
		assert.NotEmpty(t, tbl.StartedAt, "table %s should have started_at", tbl.TableName)
		assert.NotEmpty(t, tbl.CompletedAt, "table %s should have completed_at", tbl.TableName)
	}
}

// --- Revert behavior tests ---

func TestVitess_Apply_RevertWindow(t *testing.T) {
	// Verify that applies without --skip-revert correctly reach the revert window state.
	vitessAvailable(t)
	clearSchemaBotState(t)

	defer vitessRestoreBaseSchema(t, "staging")

	indexName := fmt.Sprintf("idx_rv_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/orders.sql": fmt.Sprintf(`CREATE TABLE `+"`orders`"+` (
    `+"`id`"+` bigint unsigned NOT NULL,
    `+"`user_id`"+` bigint unsigned NOT NULL,
    `+"`total_cents`"+` bigint NOT NULL,
    `+"`status`"+` varchar(100) NOT NULL DEFAULT 'pending',
    `+"`created_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP,
    `+"`updated_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`+"`id`"+`),
    KEY `+"`idx_user_id`"+` (`+"`user_id`"+`),
    KEY `+"`idx_status`"+` (`+"`status`"+`),
    KEY `+"`idx_created_at`"+` (`+"`created_at`"+`),
    KEY `+"`%s`"+` (`+"`total_cents`"+`)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;`, indexName),
	}))

	// Apply without --skip-revert (revert window enabled by default).
	// Use --no-watch without -o log so the CLI exits after starting the apply.
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)
	applyOut := e2eutil.RunCLI(t, binPath, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y", "--no-watch", "--allow-unsafe",
	)
	applyID := extractApplyIDFromLog(applyOut)
	require.NotEmpty(t, applyID, "expected apply ID in output")

	// Poll until we see revert_window (not completed — that means auto-skip fired)
	var sawRevertWindow bool
	deadline := time.Now().Add(testutil.PollDeadline)
	for time.Now().Before(deadline) {
		resp, err := client.GetProgress(endpoint, applyID)
		if err == nil {
			if resp.State == state.Apply.RevertWindow {
				sawRevertWindow = true
				break
			}
			if resp.State == state.Apply.Completed {
				break
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.True(t, sawRevertWindow, "expected revert_window state (revert enabled by default), but apply jumped to completed")

	// Clean up: skip-revert to finalize
	_, _ = client.CallSkipRevertAPI(endpoint, vitessDB, "staging")
	waitForApplyState(t, endpoint, applyID, state.Apply.Completed, testutil.PollDeadline)
}

// --- Cancel Tests ---

func TestVitess_Apply_Cancel(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)
	defer vitessRestoreBaseSchema(t, "staging")

	endpoint := schemabotURL(t)
	// Use defer_cutover so the apply holds at waiting_for_cutover — a stable
	// state we can reliably cancel from without racing against completion.
	indexName := fmt.Sprintf("idx_cancel_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/orders.sql": fmt.Sprintf(`CREATE TABLE `+"`orders`"+` (
    `+"`id`"+` bigint unsigned NOT NULL,
    `+"`user_id`"+` bigint unsigned NOT NULL,
    `+"`total_cents`"+` bigint NOT NULL,
    `+"`status`"+` varchar(100) NOT NULL DEFAULT 'pending',
    `+"`created_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP,
    `+"`updated_at`"+` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`+"`id`"+`),
    KEY `+"`idx_user_id`"+` (`+"`user_id`"+`),
    KEY `+"`idx_status`"+` (`+"`status`"+`),
    KEY `+"`idx_created_at`"+` (`+"`created_at`"+`),
    KEY `+"`%s`"+` (`+"`total_cents`"+`)
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_0900_ai_ci;`, indexName),
	}))

	// Apply with defer_cutover so it pauses at waiting_for_cutover
	binPath := buildCLI(t)
	applyOut := e2eutil.RunCLI(t, binPath, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y", "-o", "log", "--no-watch", "--allow-unsafe", "--defer-cutover",
	)
	applyID := extractApplyIDFromLog(applyOut)
	require.NotEmpty(t, applyID, "expected apply ID in output")

	// Wait for waiting_for_cutover — deterministic pause point
	waitForApplyState(t, endpoint, applyID, state.Apply.WaitingForCutover, testutil.PollDeadline)

	// Cancel from the stable waiting_for_cutover state
	t.Logf("calling stop API: endpoint=%s database=%s applyID=%s", endpoint, vitessDB, applyID)
	stopResult, err := client.CallStopAPI(endpoint, vitessDB, "staging", applyID)
	require.NoError(t, err, "stop/cancel API call")
	t.Logf("stop API returned: accepted=%v stopped=%d skipped=%d error=%q",
		stopResult.Accepted, stopResult.StoppedCount, stopResult.SkippedCount, stopResult.ErrorMessage)
	require.True(t, stopResult.Accepted, "stop should be accepted: %s", stopResult.ErrorMessage)

	// Verify it reaches cancelled state (not stopped)
	waitForApplyState(t, endpoint, applyID, state.Apply.Cancelled, testutil.PollDeadline)

	// Verify status shows cancelled
	resp, err := client.GetProgress(endpoint, applyID)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Cancelled, resp.State)
}

// extractApplyID is defined in apply_wait_test.go
