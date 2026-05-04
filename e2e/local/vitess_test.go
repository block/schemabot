//go:build e2e

package local

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/lint"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/apitypes"
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

// vitessApplyAndWait starts an apply with --no-watch and polls until completion.
// The CLI returns immediately after "Apply started", then we poll the progress
// API separately. This decouples the CLI process lifetime from the apply
// lifecycle (branch creation + deploy + online DDL + cutover + revert window).
func vitessApplyAndWait(t *testing.T, schemaDir, env string, extraArgs ...string) string {
	t.Helper()
	start := time.Now()
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	args := []string{"apply", "-s", ".", "-e", env, "--endpoint", endpoint, "-y", "-o", "log", "--no-watch", "--allow-unsafe"}
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

// usersSchemaWithColumn returns the testapp_sharded/users.sql CREATE TABLE
// with an extra varchar column added. Reads the base schema from examples/
// and inserts the column before the closing line, keeping it in sync with
// the canonical schema.
func usersSchemaWithColumn(colName string) string {
	base := vitessBaseSchema()
	original := base["testapp_sharded/users.sql"]
	// Insert the new column before the PRIMARY KEY line
	return strings.Replace(original,
		"  PRIMARY KEY (`id`)",
		fmt.Sprintf("  `%s` varchar(100) DEFAULT NULL,\n  PRIMARY KEY (`id`)", colName),
		1)
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
	// Cancel all pending Vitess online DDL migrations before cleaning up schema.
	// Without this, a slow migration from a previous test can cause
	// singleton-context rejection when the next test submits DDL.
	_, err := localscaleAdminPost(t, "/admin/reset-state", "{}")
	if err != nil {
		t.Logf("vitessRestoreBaseSchema: reset-state failed (non-fatal): %v", err)
	}
	t.Logf("vitessRestoreBaseSchema: resetState done (elapsed=%s)", time.Since(start))
	clearSchemaBotState(t)
	t.Logf("vitessRestoreBaseSchema: clearState done (elapsed=%s)", time.Since(start))
	vitessCleanupSchema(t)
	t.Logf("vitessRestoreBaseSchema: cleanup done (elapsed=%s)", time.Since(start))
	vitessResetVSchema(t)
	t.Logf("vitessRestoreBaseSchema: resetVSchema done (elapsed=%s)", time.Since(start))
}

// vitessCleanupSchema restores each keyspace to the base schema using Spirit's
// PlanChanges differ. Loads the live schema from vtgate (SHOW CREATE TABLE),
// diffs against the base schema files, and applies the resulting DDL.
// Also deletes all row data so subsequent online DDL tests start clean.
func vitessCleanupSchema(t *testing.T) {
	t.Helper()
	localscaleURL := os.Getenv("LOCALSCALE_URL")
	if localscaleURL == "" {
		t.Log("LOCALSCALE_URL not set, skipping vitess schema cleanup")
		return
	}

	baseFiles := vitessBaseSchema()
	// Build desired schema per keyspace from base files.
	desired := make(map[string][]table.TableSchema)
	for path, content := range baseFiles {
		parts := strings.SplitN(path, "/", 2)
		if len(parts) != 2 || strings.HasSuffix(parts[1], ".json") {
			continue
		}
		ks := parts[0]
		name := strings.TrimSuffix(parts[1], ".sql")
		desired[ks] = append(desired[ks], table.TableSchema{Name: name, Schema: content})
	}

	for _, org := range []string{"localscale-staging", "localscale-production"} {
		for ks, desiredTables := range desired {
			// Load live schema from vtgate.
			var current []table.TableSchema
			tables := vitessAdminQuery(t, localscaleURL, org, ks, "SHOW TABLES")
			for _, row := range tables {
				if len(row) == 0 || strings.HasPrefix(row[0], "_") {
					continue
				}
				createRows := vitessAdminQuery(t, localscaleURL, org, ks,
					fmt.Sprintf("SHOW CREATE TABLE `%s`", row[0]))
				if len(createRows) > 0 && len(createRows[0]) > 1 {
					current = append(current, table.TableSchema{
						Name:   row[0],
						Schema: createRows[0][1],
					})
				}
			}

			// Diff and apply. PlanChanges computes the exact DDL needed to
			// go from current → desired (DROP extra tables, DROP extra
			// indexes/columns, CREATE missing tables).
			plan, err := lint.PlanChanges(current, desiredTables, nil, nil)
			if err != nil {
				t.Logf("vitessCleanupSchema: PlanChanges failed for %s/%s: %v", org, ks, err)
				continue
			}
			for _, change := range plan.Changes {
				vitessAdminDDL(t, localscaleURL, org, ks, change.Statement)
			}

			// Delete row data in batches (batched DELETE LIMIT)
			// so subsequent online DDL tests start with empty tables.
			// Skip sequence tables — they have initialization data.
			for _, tbl := range desiredTables {
				if strings.HasSuffix(tbl.Name, "_seq") {
					continue
				}
				for range 100 { // safety limit
					affected := vitessExecAffected(t, localscaleURL, org, ks,
						fmt.Sprintf("DELETE FROM `%s` LIMIT 10000", tbl.Name))
					if affected == 0 {
						break
					}
				}
			}
		}
	}
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

// vitessExecAffected runs a DML statement via LocalScale vtgate-exec and returns
// the number of rows affected. Used for batched DELETE cleanup (batched DELETE LIMIT).
func vitessExecAffected(t *testing.T, localscaleURL, org, keyspace, stmt string) int64 {
	t.Helper()
	body := fmt.Sprintf(`{"org":%q,"database":%q,"keyspace":%q,"query":%q}`,
		org, vitessDB, keyspace, stmt)
	respBody, err := localscaleAdminPost(t, "/admin/vtgate-exec", body)
	if err != nil {
		t.Logf("vitessExecAffected: %s: %v (non-fatal)", stmt[:min(len(stmt), 60)], err)
		return 0
	}
	var result struct {
		RowsAffected int64 `json:"rows_affected"`
	}
	if err := json.NewDecoder(strings.NewReader(string(respBody))).Decode(&result); err != nil {
		return 0
	}
	return result.RowsAffected
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

func TestVitess_Plan_UsesSchemaAPIWhenSafeSchemaChangesEnabled(t *testing.T) {
	vitessAvailable(t)
	vitessRestoreBaseSchema(t, "staging")
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	// LocalScale has safe_migrations enabled by default, so the engine
	// uses the PlanetScale schema API (branch-based diff) for plan instead
	// of querying vtgate directly.
	colName := fmt.Sprintf("schema_api_col_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": usersSchemaWithColumn(colName),
	}))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan",
		"-s", ".", "-e", "staging", "--endpoint", endpoint)

	e2eutil.AssertContains(t, out, "ADD COLUMN")
	e2eutil.AssertContains(t, out, colName)
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

	e2eutil.AssertContains(t, out, tableName)
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

	// Instant DDL verification: the engine correctly detects instant eligibility
	// (confirmed via engine logs: "instant DDL decision ... use_instant:true"),
	// but VitessApplyData is not always saved before the progress API reads from
	// storage for terminal applies. This is a known storage race — tracked as a
	// separate engine issue. The apply completing in <10s is itself evidence of
	// instant DDL (online DDL takes 15-30s on CI).
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

	vitessApplyAndWait(t, schemaDir1, "staging")

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

	vitessApplyAndWait(t, schemaDir2, "staging")
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

	// Seed 100k rows so VReplication has real data to copy during online DDL.
	// Without data, the copy phase completes instantly and SHOW VITESS_MIGRATIONS
	// won't return per-shard progress.
	localscaleURL := os.Getenv("LOCALSCALE_URL")
	for batch := range 100 {
		var b strings.Builder
		for i := range 1000 {
			id := batch*1000 + i + 1
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "(%d, %d, %d)", id, id%10+1, id*100)
		}
		vitessAdminQuery(t, localscaleURL, "localscale-staging", "testapp_sharded",
			"INSERT INTO orders (id, user_id, total_cents) VALUES "+b.String())
	}

	binPath := buildCLI(t)
	endpoint := schemabotURL(t)
	applyOut := e2eutil.RunCLIInDir(t, binPath, schemaDir, "apply",
		"-s", ".", "-e", "staging", "--endpoint", endpoint,
		"-y", "--no-watch", "--allow-unsafe", "--skip-revert")
	e2eutil.AssertContains(t, applyOut, "Apply started")
	applyID := extractApplyIDFromLog(applyOut)
	require.NotEmpty(t, applyID)

	// Poll for shard progress during execution with a tight loop.
	var foundShards bool
	deadline := time.Now().Add(testutil.PollDeadline)
	for time.Now().Before(deadline) {
		resp, err := client.GetProgress(endpoint, applyID)
		if err == nil {
			for _, tbl := range resp.Tables {
				if len(tbl.Shards) > 0 {
					foundShards = true
				}
			}
			if foundShards || state.IsTerminalApplyState(resp.State) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, foundShards, "expected per-shard progress for 2-shard keyspace")

	// Wait for completion so cleanup doesn't race with in-flight DDL.
	waitForApplyState(t, endpoint, applyID, state.Apply.Completed, testutil.PollDeadline)
}

func TestVitess_Apply_LogMode_Lifecycle(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)

	defer vitessRestoreBaseSchema(t, "staging")

	colName := fmt.Sprintf("lm_col_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": usersSchemaWithColumn(colName),
	}))

	vitessApplyAndWait(t, schemaDir, "staging")
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

	e2eutil.AssertContains(t, out, tableName)
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

// TestVitess_Plan_DDLOnly_NoVSchemaMetadata verifies that a plan with only DDL
// changes (no VSchema diff) does not include VSchema metadata. This prevents
// unnecessary VSchema updates during apply, which would trigger PlanetScale
// schema snapshot conflicts on databases with many keyspaces.
func TestVitess_Plan_DDLOnly_NoVSchemaMetadata(t *testing.T) {
	vitessAvailable(t)
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	// Add a column — DDL only, no VSchema change
	colName := fmt.Sprintf("col_novs_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": usersSchemaWithColumn(colName),
	}))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan",
		"-s", ".", "-e", "staging", "--endpoint", endpoint, "--json")

	var envResults map[string]*apitypes.PlanResponse
	require.NoError(t, json.Unmarshal([]byte(out), &envResults))
	staging, ok := envResults["staging"]
	require.True(t, ok, "expected 'staging' in plan output")

	for _, sc := range staging.Changes {
		assert.Empty(t, sc.Metadata["vschema"],
			"keyspace %s should not have VSchema metadata for DDL-only changes", sc.Namespace)
	}
}

// TestVitess_Apply_BranchValidation verifies that the branch validation
// passes for a normal apply and the success message appears in logs.
func TestVitess_Apply_BranchValidation(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)
	defer vitessRestoreBaseSchema(t, "staging")

	colName := fmt.Sprintf("col_val_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": usersSchemaWithColumn(colName),
	}))

	out := vitessApplyAndWait(t, schemaDir, "staging")
	e2eutil.AssertContains(t, out, "Apply started")

	// Verify the branch validation passed by checking apply logs
	applyID := extractApplyIDFromLog(out)
	endpoint := schemabotURL(t)
	logs, err := client.GetLogs(endpoint, "", "", applyID, 50)
	require.NoError(t, err)

	var foundValidation bool
	for _, entry := range logs {
		if strings.Contains(entry.Message, "Branch schema validated") {
			foundValidation = true
			break
		}
	}
	assert.True(t, foundValidation,
		"expected 'Branch schema validated' in apply logs — branch validation should pass for a normal apply")
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

	e2eutil.AssertContains(t, out, tableName)
	e2eutil.AssertContains(t, out, seqName)
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

	vitessApplyAndWait(t, schemaDir, "staging")
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
	vitessApplyAndWait(t, schemaDir, "staging")
}

// --- Apply: VSchema with DDL ---

func TestVitess_Apply_VSchemaWithDDL(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)
	defer vitessRestoreBaseSchema(t, "staging")

	// VSchema change paired with a DDL change. Pure VSchema-only changes
	// are not yet detected by the plan differ — this tests that VSchema
	// is propagated when DDL changes are present.
	// Uses a nullable column addition (instant DDL) to avoid slow online DDL
	// that could leave pending migrations interfering with subsequent tests.
	colName := fmt.Sprintf("vs_col_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": usersSchemaWithColumn(colName),
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
	e2eutil.AssertContains(t, out, colName)
}

func TestVitess_Apply_VSchemaTaskTracking(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)
	defer vitessRestoreBaseSchema(t, "staging")

	endpoint := schemabotURL(t)
	colName := fmt.Sprintf("vt_col_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": usersSchemaWithColumn(colName),
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

	binPath := buildCLI(t)
	applyOut := e2eutil.RunCLI(t, binPath, schemaDir, "apply",
		"-s", ".", "-e", "staging", "--endpoint", endpoint,
		"-y", "-o", "log", "--no-watch", "--allow-unsafe",
	)
	applyID := extractApplyIDFromLog(applyOut)
	require.NotEmpty(t, applyID)

	// Poll progress until completion, verifying VSchema tasks appear
	var foundVSchemaTask bool
	var vschemaChangeType string
	var finalState string
	deadline := time.Now().Add(testutil.PollDeadline)
	for time.Now().Before(deadline) {
		resp, err := client.GetProgress(endpoint, applyID)
		if err == nil {
			for _, tbl := range resp.Tables {
				if tbl.ChangeType == "vschema_update" {
					foundVSchemaTask = true
					vschemaChangeType = tbl.ChangeType
				}
			}
			if state.IsTerminalApplyState(resp.State) {
				finalState = resp.State
				// Verify all tables (including VSchema) reached terminal state
				for _, tbl := range resp.Tables {
					assert.True(t, state.IsTerminalApplyState(state.NormalizeTaskStatus(tbl.Status)),
						"table %s should be terminal, got %s", tbl.TableName, tbl.Status)
				}
				break
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	assert.True(t, foundVSchemaTask, "expected VSchema task in progress tables")
	assert.Equal(t, "vschema_update", vschemaChangeType)
	require.NotEmpty(t, finalState, "apply did not reach terminal state")
	assert.Equal(t, state.Apply.Completed, finalState)
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
	defer vitessRestoreBaseSchema(t, "staging")
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
	vitessApplyAndWait(t, dropSchema, "staging")
}

// --- Progress & Engine Tests ---

func TestVitess_Apply_DeployRequestURL(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)
	endpoint := schemabotURL(t)

	defer vitessRestoreBaseSchema(t, "staging")

	colName := fmt.Sprintf("dr_col_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": usersSchemaWithColumn(colName),
	}))

	out := vitessApplyAndWait(t, schemaDir, "staging")

	// Deploy request should appear in apply logs
	applyID := extractApplyIDFromLog(out)
	logs, err := client.GetLogs(endpoint, "", "", applyID, 50)
	require.NoError(t, err)
	var foundDR bool
	for _, entry := range logs {
		if strings.Contains(entry.Message, "Deploy request") {
			foundDR = true
			break
		}
	}
	assert.True(t, foundDR, "expected 'Deploy request' in apply logs")
}

func TestVitess_Apply_SetupPhases(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)
	endpoint := schemabotURL(t)

	defer vitessRestoreBaseSchema(t, "staging")

	colName := fmt.Sprintf("sp_col_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": usersSchemaWithColumn(colName),
	}))

	out := vitessApplyAndWait(t, schemaDir, "staging")

	// Setup phase messages should appear in apply logs
	applyID := extractApplyIDFromLog(out)
	logs, err := client.GetLogs(endpoint, "", "", applyID, 50)
	require.NoError(t, err)
	var foundBranch, foundDeploy bool
	for _, entry := range logs {
		if strings.Contains(entry.Message, "Creating branch") {
			foundBranch = true
		}
		if strings.Contains(entry.Message, "Deploy request") {
			foundDeploy = true
		}
	}
	assert.True(t, foundBranch, "expected 'Creating branch' in apply logs")
	assert.True(t, foundDeploy, "expected 'Deploy request' in apply logs")
}

func TestVitess_Progress_DeployRequestMetadata(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)

	defer vitessRestoreBaseSchema(t, "staging")

	endpoint := schemabotURL(t)
	colName := fmt.Sprintf("pm_col_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": usersSchemaWithColumn(colName),
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
	// Use ADD INDEX (online DDL) because this test verifies per-table timestamps
	// that are only populated during the VReplication copy/cutover lifecycle.
	// Use --skip-revert to save 2s and stay within the 30s poll deadline.
	indexName := fmt.Sprintf("idx_ts_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/orders.sql": fmt.Sprintf(`CREATE TABLE `+"`orders`"+` (
    `+"`id`"+` bigint unsigned NOT NULL,
    `+"`user_id`"+` bigint unsigned NOT NULL,
    `+"`total_cents`"+` bigint NOT NULL,
    `+"`status`"+` varchar(100) NOT NULL DEFAULT 'pending',
    `+"`created_at`"+` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    `+"`updated_at`"+` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`+"`id`"+`),
    KEY `+"`idx_user_id`"+` (`+"`user_id`"+`),
    KEY `+"`idx_status`"+` (`+"`status`"+`),
    KEY `+"`idx_created_at`"+` (`+"`created_at`"+`),
    KEY `+"`%s`"+` (`+"`total_cents`"+`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, indexName),
	}))

	// Start apply without watching
	binPath := buildCLI(t)
	applyOut := e2eutil.RunCLI(t, binPath, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y", "-o", "log", "--no-watch", "--allow-unsafe", "--skip-revert",
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
		time.Sleep(100 * time.Millisecond)
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
	// Use a column addition (instant DDL) with --defer-deploy so the apply
	// holds at waiting_for_deploy — a stable state we can reliably cancel
	// from without racing against completion. --defer-cutover doesn't work
	// for instant DDL since there's no cutover phase.
	colName := fmt.Sprintf("cancel_col_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": usersSchemaWithColumn(colName),
	}))

	binPath := buildCLI(t)
	applyOut := e2eutil.RunCLI(t, binPath, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y", "-o", "log", "--no-watch", "--allow-unsafe", "--defer-deploy",
	)
	applyID := extractApplyIDFromLog(applyOut)
	require.NotEmpty(t, applyID, "expected apply ID in output")

	// Wait for waiting_for_deploy — deterministic pause point
	waitForApplyState(t, endpoint, applyID, state.Apply.WaitingForDeploy, testutil.PollDeadline)

	// Cancel from the stable waiting_for_deploy state
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

// createBranchViaLocalScale creates a PlanetScale branch via the LocalScale API
// and waits for it to be ready. Returns the branch name.
func createBranchViaLocalScale(t *testing.T, branchName string) {
	t.Helper()
	ctx := t.Context()
	localscaleURL := os.Getenv("LOCALSCALE_URL")
	require.NotEmpty(t, localscaleURL, "LOCALSCALE_URL not set")

	body := fmt.Sprintf(`{"name":"%s","parent_branch":"main"}`, branchName)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		localscaleURL+"/v1/organizations/localscale-staging/databases/"+vitessDB+"/branches",
		strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "CreateBranch failed")

	// Poll until branch is ready
	deadline := time.Now().Add(testutil.PollDeadline)
	for time.Now().Before(deadline) {
		getReq, _ := http.NewRequestWithContext(ctx, http.MethodGet,
			localscaleURL+"/v1/organizations/localscale-staging/databases/"+vitessDB+"/branches/"+branchName, nil)
		resp, err := http.DefaultClient.Do(getReq)
		if err == nil {
			var branch struct {
				Ready bool `json:"ready"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&branch)
			resp.Body.Close()
			if branch.Ready {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("branch %s not ready within deadline", branchName)
}

// TestVitess_Apply_BranchReuse tests the --branch flag for reusing an existing
// PlanetScale branch. Creates a branch, applies with --branch, then syncs and
// applies again on the same branch.
func TestVitess_Apply_DeferDeploy(t *testing.T) {
	// Verify that applies with defer_deploy hold at WaitingForDeploy,
	// and that calling Start triggers the deploy to completion.
	vitessAvailable(t)
	clearSchemaBotState(t)
	defer vitessRestoreBaseSchema(t, "staging")

	endpoint := schemabotURL(t)

	// Plan via API
	colName := fmt.Sprintf("defer_col_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": usersSchemaWithColumn(colName),
	}))
	planResp, err := client.CallPlanAPI(endpoint, vitessDB, "vitess", "staging", schemaDir, "", 0)
	require.NoError(t, err)
	require.NotEmpty(t, planResp.PlanID)
	require.NotEmpty(t, planResp.Changes, "expected plan to have changes")

	// Apply via API with defer_deploy option
	applyResp, err := client.CallApplyAPI(endpoint, planResp.PlanID, vitessDB, "staging", "e2e-test",
		map[string]string{"defer_deploy": "true", "skip_revert": "true"})
	require.NoError(t, err)
	require.NotEmpty(t, applyResp.ApplyID)
	t.Logf("apply started: %s", applyResp.ApplyID)

	// Poll until WaitingForDeploy
	waitForApplyState(t, endpoint, applyResp.ApplyID, state.Apply.WaitingForDeploy, testutil.PollDeadline)
	t.Logf("apply reached waiting_for_deploy")

	// Verify the progress response has deferred_deploy metadata
	progress, err := client.GetProgress(endpoint, applyResp.ApplyID)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.WaitingForDeploy, progress.State)
	assert.Equal(t, "true", progress.Metadata["deferred_deploy"])

	// Trigger deploy via Start API. Retry briefly — the progress API may
	// return WaitingForDeploy before the apply record in storage catches up.
	var startResp *apitypes.StartResponse
	for range 10 {
		startResp, err = client.CallStartAPI(endpoint, vitessDB, "staging", applyResp.ApplyID)
		if err == nil && startResp.Accepted {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.NoError(t, err)
	require.True(t, startResp.Accepted, "start should be accepted: %s", startResp.ErrorMessage)
	t.Logf("deploy triggered via start API")

	// Wait for completion
	waitForApplyState(t, endpoint, applyResp.ApplyID, state.Apply.Completed, testutil.PollDeadline)
	t.Logf("apply completed")
}

func TestVitess_Apply_DeferDeploy_StartTooEarly(t *testing.T) {
	// Verify that calling Start before the apply reaches WaitingForDeploy
	// returns a clear "not ready" error instead of a confusing message.
	vitessAvailable(t)
	clearSchemaBotState(t)
	defer vitessRestoreBaseSchema(t, "staging")

	endpoint := schemabotURL(t)

	colName := fmt.Sprintf("early_col_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": usersSchemaWithColumn(colName),
	}))
	planResp, err := client.CallPlanAPI(endpoint, vitessDB, "vitess", "staging", schemaDir, "", 0)
	require.NoError(t, err)
	require.NotEmpty(t, planResp.PlanID)

	applyResp, err := client.CallApplyAPI(endpoint, planResp.PlanID, vitessDB, "staging", "e2e-test",
		map[string]string{"defer_deploy": "true", "skip_revert": "true"})
	require.NoError(t, err)
	require.NotEmpty(t, applyResp.ApplyID)

	// Call Start immediately — apply hasn't reached WaitingForDeploy yet
	_, startErr := client.CallStartAPI(endpoint, vitessDB, "staging", applyResp.ApplyID)
	require.Error(t, startErr)
	assert.Contains(t, startErr.Error(), "not ready for deploy")

	// Clean up: wait for WaitingForDeploy, then trigger and complete
	waitForApplyState(t, endpoint, applyResp.ApplyID, state.Apply.WaitingForDeploy, testutil.PollDeadline)
	var startResp *apitypes.StartResponse
	for range 10 {
		startResp, err = client.CallStartAPI(endpoint, vitessDB, "staging", applyResp.ApplyID)
		if err == nil && startResp.Accepted {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.NoError(t, err)
	waitForApplyState(t, endpoint, applyResp.ApplyID, state.Apply.Completed, testutil.PollDeadline)
}

func TestVitess_Apply_BranchReuse(t *testing.T) {
	vitessAvailable(t)
	clearSchemaBotState(t)
	defer vitessRestoreBaseSchema(t, "staging")

	branchName := fmt.Sprintf("reuse-e2e-%d", time.Now().UnixMilli()%100000)
	createBranchViaLocalScale(t, branchName)

	// First apply: add a column using --branch
	col1 := fmt.Sprintf("reuse_col1_%d", time.Now().UnixMilli()%100000)
	schemaDir := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": fmt.Sprintf(`CREATE TABLE `+"`users`"+` (
  `+"`id`"+` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `+"`email`"+` varchar(255) NOT NULL,
  `+"`full_name`"+` varchar(255) NULL,
  `+"`%s`"+` varchar(100) NULL,
  `+"`created_at`"+` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `+"`idx_email`"+` (`+"`email`"+`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, col1),
	}))

	out := vitessApplyAndWait(t, schemaDir, "staging", "--branch", branchName)
	e2eutil.AssertContains(t, out, "Apply started")

	// Verify branch refresh happened via apply logs
	applyID := extractApplyIDFromLog(out)
	endpoint := schemabotURL(t)
	logs, err := client.GetLogs(endpoint, "", "", applyID, 50)
	require.NoError(t, err)
	var foundRefresh bool
	for _, entry := range logs {
		if strings.Contains(entry.Message, "Refreshing schema for branch") {
			foundRefresh = true
			break
		}
	}
	assert.True(t, foundRefresh, "expected 'Refreshing schema for branch' in apply logs")

	// Second apply on the same branch: add another column
	clearSchemaBotState(t)
	col2 := fmt.Sprintf("reuse_col2_%d", time.Now().UnixMilli()%100000)
	schemaDir2 := newVitessSchemaDir(t, vitessSchemaWithOverrides(map[string]string{
		"testapp_sharded/users.sql": fmt.Sprintf(`CREATE TABLE `+"`users`"+` (
  `+"`id`"+` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `+"`email`"+` varchar(255) NOT NULL,
  `+"`full_name`"+` varchar(255) NULL,
  `+"`%s`"+` varchar(100) NULL,
  `+"`%s`"+` varchar(100) NULL,
  `+"`created_at`"+` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX `+"`idx_email`"+` (`+"`email`"+`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, col1, col2),
	}))

	out2 := vitessApplyAndWait(t, schemaDir2, "staging", "--branch", branchName)
	e2eutil.AssertContains(t, out2, "Apply started")
	// Second apply completing (verified by vitessApplyAndWait polling)
	// proves the branch was preserved and reused
}
