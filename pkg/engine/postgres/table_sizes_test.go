package postgres

import (
	"context"
	"testing"

	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
)

// Every change to the table carries its own copy of the table's estimate. A
// change to another table, or an enum type change that shares the table's
// name, carries none.
func TestAttachTableSize(t *testing.T) {
	changes := []engine.TableChange{
		{Table: "users", Operation: ddl.StatementAlterTable},
		{Table: "users", Operation: ddl.StatementCreateIndex},
		{Table: "users", Operation: ddl.StatementAlterType},
		{Table: "orders", Operation: ddl.StatementAlterTable},
	}

	attachTableSize(changes, "users", tableSizeEstimate{rows: new(int64(1000)), bytes: new(int64(81920))})

	for _, i := range []int{0, 1} {
		assert.Equal(t, new(int64(1000)), changes[i].EstimatedRows)
		assert.Equal(t, new(int64(81920)), changes[i].EstimatedBytes)
	}
	assert.NotSame(t, changes[0].EstimatedBytes, changes[1].EstimatedBytes, "each change owns its estimate")
	for _, i := range []int{2, 3} {
		assert.Nil(t, changes[i].EstimatedRows)
		assert.Nil(t, changes[i].EstimatedBytes)
	}
}

// A table over the native-safe ceiling is blocked, and its plan still shows
// the size the ceiling measured. The refusal carries no row estimate, so the
// row count is omitted rather than reported as zero.
func TestBlockOversizedTableKeepsMeasuredBytes(t *testing.T) {
	report := pgplan.Report{Schema: "public", Table: "users", Statements: []pgplan.Statement{{SQL: "ALTER TABLE public.users ALTER COLUMN id TYPE bigint"}}}
	changes := []engine.TableChange{{Table: "users", Operation: ddl.StatementAlterTable, DDL: "ALTER TABLE public.users ALTER COLUMN id TYPE bigint"}}
	checkTable := func(context.Context, *pgxpool.Pool, string, string, int64) (preflight.PreflightedTable, error) {
		return preflight.PreflightedTable{}, &preflight.SizeError{TotalBytes: 2048, LimitBytes: 1024}
	}

	changes, err := blockOversizedTableWithCheck(t.Context(), nil, "orders_db", report, changes, 1024, checkTable)
	require.NoError(t, err)

	require.Len(t, changes, 1)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[0].ExecutionMode)
	assert.Equal(t, new(int64(2048)), changes[0].EstimatedBytes)
	assert.Nil(t, changes[0].EstimatedRows)
}
