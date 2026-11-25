//go:build integration

package postgres

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// A plan that alters tables in two namespaces, creates one table, and drops an
// undeclared partitioned table carries each existing table's row count and
// on-disk size, so the operator sees the same size clause on a PostgreSQL
// schema change as on any other engine. Same-named tables in two namespaces
// each carry their own size, a partitioned table reports its partitions'
// total, a table that has never been analyzed reports its size but no row
// count, and a table the plan creates reports neither.
func TestEnginePlanReportsTableSizes(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "plan_table_sizes_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY, email text NOT NULL);
		INSERT INTO public.users SELECT g, 'user' || g || '@example.com' FROM pg_catalog.generate_series(1, 1000) g;
		CREATE SCHEMA billing;
		CREATE TABLE billing.users (id bigint PRIMARY KEY, email text NOT NULL);
		INSERT INTO billing.users SELECT g, 'payer' || g || '@example.com' FROM pg_catalog.generate_series(1, 200) g;
		CREATE TABLE public.events (id bigint, day date) PARTITION BY RANGE (day);
		CREATE TABLE public.events_2025 PARTITION OF public.events FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
		CREATE TABLE public.events_2026 PARTITION OF public.events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
		INSERT INTO public.events SELECT g, DATE '2025-03-01' FROM pg_catalog.generate_series(1, 300) g;
		INSERT INTO public.events SELECT g, DATE '2026-03-01' FROM pg_catalog.generate_series(1, 400) g;
		ANALYZE public.users, billing.users, public.events;
		-- Fewer rows than autovacuum's analyze threshold, so the table stays
		-- unanalyzed for the life of the test.
		CREATE TABLE public.sessions (id bigint PRIMARY KEY);
		INSERT INTO public.sessions SELECT g FROM pg_catalog.generate_series(1, 10) g;`)
	require.NoError(t, err)
	req := &engine.PlanRequest{
		Database: "plan_table_sizes_test",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"users.sql":    "CREATE TABLE users (id bigint PRIMARY KEY, email text NOT NULL, nickname text)",
				"sessions.sql": "CREATE TABLE sessions (id bigint PRIMARY KEY, token text)",
				"audits.sql":   "CREATE TABLE audits (id bigint PRIMARY KEY)",
			}},
			"billing": {Files: map[string]string{
				"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY, email text NOT NULL, plan text)",
			}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	result, err := New().Plan(t.Context(), req)
	require.NoError(t, err)

	changes := tableChangesByName(t, result)
	tests := []struct {
		namespace, table string
		operation        ddl.StatementType
		wantRows         *int64
		wantBytes        *int64
	}{
		{"public", "users", ddl.StatementAlterTable, new(int64(1000)), new(relationSize(t, db, "public.users"))},
		{"billing", "users", ddl.StatementAlterTable, new(int64(200)), new(relationSize(t, db, "billing.users"))},
		{"public", "events", ddl.StatementDropTable, new(int64(700)),
			new(relationSize(t, db, "public.events") + relationSize(t, db, "public.events_2025") + relationSize(t, db, "public.events_2026"))},
		{"public", "sessions", ddl.StatementAlterTable, nil, new(relationSize(t, db, "public.sessions"))},
		{"public", "audits", ddl.StatementCreateTable, nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.namespace+"."+tt.table, func(t *testing.T) {
			change, ok := changes[tt.namespace+"."+tt.table]
			require.True(t, ok, "the plan changes %s.%s", tt.namespace, tt.table)
			assert.Equal(t, tt.operation, change.Operation)
			assert.Equal(t, tt.wantRows, change.EstimatedRows)
			assert.Equal(t, tt.wantBytes, change.EstimatedBytes)
			if tt.wantBytes != nil {
				assert.Positive(t, *tt.wantBytes, "a table holding rows occupies pages on disk")
			}
		})
	}
}

// tableChangesByName indexes the plan's table changes by namespace-qualified
// table name, failing when a table carries more than one change, since each
// case above expects exactly one.
func tableChangesByName(t *testing.T, result *engine.PlanResult) map[string]engine.TableChange {
	t.Helper()
	changes := make(map[string]engine.TableChange)
	for _, sc := range result.Changes {
		for _, tc := range sc.TableChanges {
			key := sc.Namespace + "." + tc.Table
			_, seen := changes[key]
			require.False(t, seen, "table %s carries more than one change", key)
			changes[key] = tc
		}
	}
	return changes
}

func relationSize(t *testing.T, db *sql.DB, table string) int64 {
	t.Helper()
	var size int64
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT pg_catalog.pg_total_relation_size($1::regclass)", table).Scan(&size))
	return size
}
