//go:build integration

package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// A schema file adds a NOT NULL column with no default to a table that holds
// rows. PostgreSQL would refuse the statement after locking the table, so the
// plan blocks it, and the plan comment says why and gives the two ways to
// make the same change: a default, or the column added nullable, filled, and
// made NOT NULL in a later change.
func TestEnginePlanRefusesNotNullColumnWithoutDefault(t *testing.T) {
	const database = "plan_not_null_without_default_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.accounts (id bigint PRIMARY KEY);
		INSERT INTO public.accounts VALUES (1), (2)`)
	require.NoError(t, err)
	req := &engine.PlanRequest{
		Database: database,
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"accounts.sql": `CREATE TABLE accounts (id bigint PRIMARY KEY, region text NOT NULL);`,
			}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	result, err := New().Plan(t.Context(), req)
	require.NoError(t, err)

	var changes []engine.TableChange
	for _, sc := range result.Changes {
		changes = append(changes, sc.TableChanges...)
	}
	require.Len(t, changes, 1, "the column add is the only change, got %v", changes)
	assert.Equal(t, "ALTER TABLE public.accounts ADD COLUMN region text NOT NULL", changes[0].DDL)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[0].ExecutionMode)
	assert.Equal(t, `statement for table "accounts" is refused: it adds a NOT NULL column with no default to a table that holds rows, which PostgreSQL refuses because every existing row would hold NULL; give the column a default, or add it without NOT NULL, fill every row, then declare NOT NULL in a later change, which SchemaBot runs online, then plan again`, changes[0].ModeReason)
}
