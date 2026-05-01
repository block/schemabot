package e2eutil

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// LocalScaleAdminPost sends a POST request to a LocalScale admin endpoint.
// The localscaleURL should be the base URL (e.g., "http://localhost:15387").
func LocalScaleAdminPost(t *testing.T, localscaleURL, endpoint, body string) ([]byte, error) {
	t.Helper()
	if localscaleURL == "" {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", localscaleURL+endpoint, strings.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request for %s: %w", endpoint, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response from %s: %w", endpoint, err)
	}
	if resp.StatusCode != http.StatusOK {
		return respBody, fmt.Errorf("POST %s status %d: %s", endpoint, resp.StatusCode, respBody)
	}
	return respBody, nil
}

// LocalScaleVtgateQuery runs a query via the LocalScale vtgate-exec admin endpoint.
func LocalScaleVtgateQuery(t *testing.T, localscaleURL, org, database, keyspace, query string) [][]string {
	t.Helper()
	body := fmt.Sprintf(`{"org":%q,"database":%q,"keyspace":%q,"query":%q}`,
		org, database, keyspace, query)
	respBody, err := LocalScaleAdminPost(t, localscaleURL, "/admin/vtgate-exec", body)
	if err != nil {
		t.Fatalf("vtgate-exec query %s: %v", query, err)
	}
	var result struct {
		Rows [][]string `json:"rows"`
	}
	if err := json.NewDecoder(strings.NewReader(string(respBody))).Decode(&result); err != nil {
		t.Fatalf("decode vtgate-exec response: %v", err)
	}
	return result.Rows
}

// LocalScaleSeedDDL runs DDL statements via the LocalScale seed-ddl admin endpoint.
// Best-effort: logs failures without failing the test.
func LocalScaleSeedDDL(t *testing.T, localscaleURL, org, database, keyspace, ddl string) {
	t.Helper()
	body := fmt.Sprintf(`{"org":%q,"database":%q,"keyspace":%q,"statements":[%q]}`,
		org, database, keyspace, ddl)
	_, err := LocalScaleAdminPost(t, localscaleURL, "/admin/seed-ddl", body)
	if err != nil {
		t.Logf("LocalScaleSeedDDL: %s: %v (non-fatal)", ddl, err)
	}
}

// LocalScaleSeedVSchema seeds VSchema via the LocalScale admin endpoint.
func LocalScaleSeedVSchema(t *testing.T, localscaleURL, org, database, keyspace, vschemaJSON string) {
	t.Helper()
	body := fmt.Sprintf(`{"org":%q,"database":%q,"keyspace":%q,"vschema":%s}`,
		org, database, keyspace, vschemaJSON)
	_, err := LocalScaleAdminPost(t, localscaleURL, "/admin/seed-vschema", body)
	if err != nil {
		t.Logf("LocalScaleSeedVSchema: %s/%s: %v (non-fatal)", org, keyspace, err)
	}
}

// LocalScaleMetadataQuery sends a SQL query to the LocalScale metadata endpoint.
func LocalScaleMetadataQuery(localscaleURL, query string) error {
	if localscaleURL == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	body := strings.NewReader(fmt.Sprintf(`{"query":%q}`, query))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, localscaleURL+"/admin/metadata-query", body)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}

// LocalScaleResetDeployRequests marks all pending deploy requests as closed.
// Preserves auto-increment counters so new DRs get unique numbers.
func LocalScaleResetDeployRequests(localscaleURL string) error {
	return LocalScaleMetadataQuery(localscaleURL,
		"UPDATE localscale_deploy_requests SET deployment_state = 'complete', deployed = FALSE, cancelled = TRUE WHERE deployment_state != 'complete'")
}

// LocalScaleCleanupSchema drops extra tables, indexes, and columns from a
// Vitess keyspace, restoring it to the expected base state. Uses direct DDL
// via admin endpoints for speed (~1s vs ~10s for a full apply).
func LocalScaleCleanupSchema(t *testing.T, localscaleURL, org, database, keyspace string, expectedTables map[string]TableSchema) {
	t.Helper()
	if localscaleURL == "" {
		return
	}

	expectedNames := make(map[string]bool, len(expectedTables))
	for name := range expectedTables {
		expectedNames[name] = true
	}

	// Drop extra tables
	tables := LocalScaleVtgateQuery(t, localscaleURL, org, database, keyspace, "SHOW TABLES")
	for _, row := range tables {
		if len(row) == 0 {
			continue
		}
		if !expectedNames[row[0]] {
			LocalScaleSeedDDL(t, localscaleURL, org, database, keyspace,
				fmt.Sprintf("DROP TABLE IF EXISTS `%s`", row[0]))
		}
	}

	// Fix base tables
	for tbl, expected := range expectedTables {
		expectedIdx := make(map[string]bool, len(expected.Indexes))
		for _, idx := range expected.Indexes {
			expectedIdx[idx] = true
		}
		expectedCol := make(map[string]bool, len(expected.Columns))
		for _, col := range expected.Columns {
			expectedCol[col] = true
		}

		// Drop extra indexes
		indexes := LocalScaleVtgateQuery(t, localscaleURL, org, database, keyspace,
			fmt.Sprintf("SHOW INDEX FROM `%s`", tbl))
		seen := make(map[string]bool)
		for _, idx := range indexes {
			if len(idx) < 3 {
				continue
			}
			idxName := idx[2]
			if seen[idxName] {
				continue
			}
			seen[idxName] = true
			if !expectedIdx[idxName] {
				LocalScaleSeedDDL(t, localscaleURL, org, database, keyspace,
					fmt.Sprintf("ALTER TABLE `%s` DROP INDEX `%s`", tbl, idxName))
			}
		}

		// Drop extra columns
		cols := LocalScaleVtgateQuery(t, localscaleURL, org, database, keyspace,
			fmt.Sprintf("SHOW COLUMNS FROM `%s`", tbl))
		for _, col := range cols {
			if len(col) == 0 {
				continue
			}
			if !expectedCol[col[0]] {
				LocalScaleSeedDDL(t, localscaleURL, org, database, keyspace,
					fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN `%s`", tbl, col[0]))
			}
		}
	}
}

// TableSchema defines the expected schema for a single table.
type TableSchema struct {
	Indexes []string
	Columns []string
}
