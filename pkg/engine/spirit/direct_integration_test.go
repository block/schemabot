//go:build integration

package spirit

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// directPolicyMetadata is the engine metadata an enabled direct execution
// policy resolves to, as the config layer would deliver it.
func directPolicyMetadata(maxTableRows int64) map[string]string {
	return map[string]string{
		"direct_execution":                "true",
		"direct_execution_max_table_rows": fmt.Sprintf("%d", maxTableRows),
	}
}

// dropTablesOnCleanup drops the named tables when the test finishes, using a
// context that survives the test context's cancellation so the shared test
// database stays clean for later tests.
func dropTablesOnCleanup(t *testing.T, db *sql.DB, tables ...string) {
	t.Helper()
	cleanupCtx := context.WithoutCancel(t.Context())
	t.Cleanup(func() {
		for _, table := range tables {
			_, err := db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS `"+table+"`")
			assert.NoError(t, err, "drop table %s", table)
		}
	})
}

// With the direct execution policy enabled and the table within the size
// bound, the plan resolves a statement the engine refuses to the direct
// verdict, with the reason carrying the refusal and the row estimate — the
// operator sees exactly what will run natively and why before confirming.
func TestEngine_Plan_DirectVerdictWithinBound(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	dropTablesOnCleanup(t, db, "direct_plan")

	_, err := db.ExecContext(t.Context(), `CREATE TABLE direct_plan (
		id INT NOT NULL AUTO_INCREMENT,
		tenant_id INT NOT NULL,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create direct_plan table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"direct_plan.sql": `CREATE TABLE direct_plan (
				id INT NOT NULL AUTO_INCREMENT,
				tenant_id INT NOT NULL,
				PRIMARY KEY (id, tenant_id)
			)`,
		}),
		Credentials: &engine.Credentials{
			DSN:      dsn,
			Metadata: directPolicyMetadata(100000),
		},
	})
	require.NoError(t, err, "Plan()")
	require.False(t, result.NoChanges)

	changes := result.FlatTableChanges()
	require.Len(t, changes, 1)
	assert.Contains(t, changes[0].DDL, "DROP PRIMARY KEY")
	assert.Equal(t, "direct", changes[0].ExecutionMode, "the refused statement resolves to the direct verdict")
	assert.Contains(t, changes[0].ModeReason, "dropping primary key is not supported")
	assert.Contains(t, changes[0].ModeReason, "runs as native MySQL DDL on a table with ~")
}

// A table whose estimated row count exceeds max_table_rows keeps the blocked
// verdict even with the policy enabled: the bound is the fail-closed backstop
// against unbounded native rebuilds, and the reason names both the estimate
// and the configured limit so the operator sees why.
func TestEngine_Plan_DirectBlockedAboveBound(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	dropTablesOnCleanup(t, db, "direct_bound")

	_, err := db.ExecContext(t.Context(), `CREATE TABLE direct_bound (
		id INT NOT NULL AUTO_INCREMENT,
		tenant_id INT NOT NULL,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create direct_bound table")

	// Seed enough rows that the optimizer's estimate is safely above the tiny
	// bound, then refresh statistics so information_schema reflects them.
	var inserts strings.Builder
	inserts.WriteString("INSERT INTO direct_bound (tenant_id) VALUES (1)")
	for range 1999 {
		inserts.WriteString(",(1)")
	}
	_, err = db.ExecContext(t.Context(), inserts.String())
	require.NoError(t, err, "seed rows")
	_, err = db.ExecContext(t.Context(), "ANALYZE TABLE `direct_bound`")
	require.NoError(t, err, "analyze table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"direct_bound.sql": `CREATE TABLE direct_bound (
				id INT NOT NULL AUTO_INCREMENT,
				tenant_id INT NOT NULL,
				PRIMARY KEY (id, tenant_id)
			)`,
		}),
		Credentials: &engine.Credentials{
			DSN:      dsn,
			Metadata: directPolicyMetadata(10),
		},
	})
	require.NoError(t, err, "Plan()")

	changes := result.FlatTableChanges()
	require.Len(t, changes, 1)
	assert.Equal(t, "blocked", changes[0].ExecutionMode, "a table above the bound stays blocked")
	assert.Contains(t, changes[0].ModeReason, "dropping primary key is not supported")
	assert.Contains(t, changes[0].ModeReason, "above the configured limit of 10")
}

// A malformed direct execution policy fails the plan instead of silently
// disabling direct execution: enabling the policy without a size bound must
// surface as a config error, never as a mode downgrade.
func TestEngine_Plan_MalformedDirectPolicyFails(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	dropTablesOnCleanup(t, db, "direct_malformed")

	_, err := db.ExecContext(t.Context(), `CREATE TABLE direct_malformed (
		id INT NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create direct_malformed table")

	eng := New(Config{})

	_, err = eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"direct_malformed.sql": `CREATE TABLE direct_malformed (
				id INT NOT NULL AUTO_INCREMENT,
				body TEXT,
				PRIMARY KEY (id)
			)`,
		}),
		Credentials: &engine.Credentials{
			DSN:      dsn,
			Metadata: map[string]string{"direct_execution": "true"},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "direct_execution_max_table_rows is not set")
}
