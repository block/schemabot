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

// A retype of a column that generated columns read is refused with the
// recipe for a type change, from what the real planner reports: on its own,
// the generated column is dropped and added back around the change in one
// transaction; beside a trigger that also reads the column, one transaction
// moves both out of the way, since PostgreSQL refuses the change while
// either stands.
func TestEnginePlanRefusesARetypeGeneratedColumnsRead(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "plan_dependent_generated_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.prices (
		    id bigint PRIMARY KEY,
		    amount int NOT NULL,
		    doubled int GENERATED ALWAYS AS (amount * 2) STORED
		);
		CREATE TABLE public.ledger (
		    id bigint PRIMARY KEY,
		    amount int NOT NULL,
		    doubled int GENERATED ALWAYS AS (amount * 2) STORED
		);
		CREATE FUNCTION public.ledger_touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
		CREATE TRIGGER ledger_audit BEFORE UPDATE OF amount ON public.ledger FOR EACH ROW EXECUTE FUNCTION public.ledger_touch()`)
	require.NoError(t, err)
	req := &engine.PlanRequest{
		Database: "plan_dependent_generated_test",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"prices.sql": `CREATE TABLE prices (
    id bigint PRIMARY KEY,
    amount bigint NOT NULL,
    doubled int GENERATED ALWAYS AS (amount * 2) STORED
);`,
				"ledger.sql": `CREATE TABLE ledger (
    id bigint PRIMARY KEY,
    amount bigint NOT NULL,
    doubled int GENERATED ALWAYS AS (amount * 2) STORED
);`,
			}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	result, err := New().Plan(t.Context(), req)
	require.NoError(t, err)

	byTable := map[string]engine.TableChange{}
	for _, sc := range result.Changes {
		for _, tc := range sc.TableChanges {
			byTable[tc.Table] = tc
		}
	}
	require.Len(t, byTable, 2, "one change per table, got %v", byTable)

	prices := byTable["prices"]
	assert.Equal(t, engine.ExecutionModeBlocked, prices.ExecutionMode)
	assert.Equal(t, `statement for table "prices" is refused: it changes the type of a column that generated column "public"."prices"."doubled" reads, which SchemaBot cannot run yet: PostgreSQL refuses the change while it stands; to keep it, outside SchemaBot, through a separately reviewed process, drop it, change the column, and add it back in one transaction, which blocks reads and writes until it commits, through a rewrite of the table when a generated column added back is stored, then plan again`, prices.ModeReason)

	ledger := byTable["ledger"]
	assert.Equal(t, engine.ExecutionModeBlocked, ledger.ExecutionMode)
	assert.Equal(t, `statement for table "ledger" is refused: it changes the type of a column that trigger "ledger_audit" ON "public"."ledger" and generated column "public"."ledger"."doubled" read, which SchemaBot cannot run yet: PostgreSQL refuses the change while any of them stands; to keep them, outside SchemaBot, through a separately reviewed process, drop the trigger and the generated column, change the column, and recreate them in one transaction, which blocks reads and writes while the table is rewritten, then plan again`, ledger.ModeReason)
}
