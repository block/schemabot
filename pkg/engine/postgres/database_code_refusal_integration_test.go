//go:build integration

package postgres

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// A DBA installs an event trigger that refuses every CREATE INDEX. Planning an
// index builds it in pg-sprite's scratch schema, so the trigger fires there and
// the plan is refused in SchemaBot's own words: the SQLSTATE and the schema
// prefix to exempt, never the trigger's raised text. Once the trigger skips
// that prefix, as the refusal says, the same files plan.
func TestEnginePlanRefusesWhenAnEventTriggerRaisesOnTheScratchSchema(t *testing.T) {
	const database = "plan_event_trigger_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.accounts (id bigint PRIMARY KEY, email text);
		CREATE FUNCTION public.refuse_create_index() RETURNS event_trigger LANGUAGE plpgsql AS $$
		DECLARE
			cmd record;
		BEGIN
			FOR cmd IN SELECT * FROM pg_event_trigger_ddl_commands() WHERE command_tag = 'CREATE INDEX' LOOP
				IF current_setting('test.exempt_scratch', true) = 'on' AND cmd.schema_name LIKE 'pgsprite\_scratch\_%' THEN
					CONTINUE;
				END IF;
				RAISE EXCEPTION 'CREATE INDEX must use CONCURRENTLY (dba policy on db-7.internal)';
			END LOOP;
		END $$;
		CREATE EVENT TRIGGER refuse_create_index ON ddl_command_end EXECUTE FUNCTION public.refuse_create_index()`)
	require.NoError(t, err)
	req := &engine.PlanRequest{
		Database: database,
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"accounts.sql": `CREATE TABLE accounts (id bigint PRIMARY KEY, email text);
CREATE INDEX accounts_email_idx ON accounts (email);`,
			}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	_, err = New().Plan(t.Context(), req)
	require.Error(t, err)
	refusal, ok := errors.AsType[*engine.PlanRefusedError](err)
	require.True(t, ok, "a plan the target's own code refuses is a refusal, not a server fault: %v", err)
	text, quotesRepository := refusal.AuthoredText()
	assert.False(t, quotesRepository)
	assert.Equal(t, `code on database "plan_event_trigger_test" raised SQLSTATE P0001 while SchemaBot planned. `+
		`Planning builds the declared tables in a schema named pgsprite_scratch_<suffix>, in a transaction it always rolls back, `+
		`so an event trigger on DDL fires there too. Have the trigger skip schemas whose name starts with pgsprite_scratch_, then plan again`, text)
	assert.NotContains(t, text, "db-7.internal", "the trigger's raised text stays in the cause")
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	require.True(t, ok, "the cause keeps the target's error for the server log")
	assert.Equal(t, "P0001", pgErr.Code)
	assert.Contains(t, err.Error(), "CREATE INDEX must use CONCURRENTLY", "the server log records the raised text")

	_, err = db.ExecContext(t.Context(), `ALTER DATABASE plan_event_trigger_test SET test.exempt_scratch = 'on'`)
	require.NoError(t, err)

	result, err := New().Plan(t.Context(), req)
	require.NoError(t, err, "a trigger that skips the scratch schema leaves planning alone")
	var ddls []string
	for _, sc := range result.Changes {
		for _, tc := range sc.TableChanges {
			ddls = append(ddls, tc.DDL)
		}
	}
	assert.Equal(t, []string{"CREATE INDEX CONCURRENTLY accounts_email_idx ON public.accounts USING btree (email)"}, ddls)
}
