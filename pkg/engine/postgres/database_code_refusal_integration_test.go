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

// A DBA installs an event trigger that refuses every CREATE INDEX, at either
// event a DDL guard can use. Planning an index builds it in pg-sprite's scratch
// schema, so the trigger fires there and the plan is refused in SchemaBot's own
// words: the SQLSTATE and the schema prefix to exempt, never the trigger's
// raised text. Once the trigger skips that prefix the way the refusal and the
// PostgreSQL guide say for its event, the same files plan.
func TestEnginePlanRefusesWhenAnEventTriggerRaisesOnTheScratchSchema(t *testing.T) {
	const database = "plan_event_trigger_test"
	triggers := map[string]string{
		// At ddl_command_start the command has no object yet, so the guard
		// reads the schema the session builds in.
		"ddl_command_start": `
			CREATE FUNCTION public.refuse_create_index() RETURNS event_trigger LANGUAGE plpgsql AS $$
			BEGIN
				IF current_setting('test.exempt_scratch', true) = 'on' AND current_schema() LIKE 'pgsprite\_scratch\_%' THEN
					RETURN;
				END IF;
				RAISE EXCEPTION 'CREATE INDEX must use CONCURRENTLY (dba policy on db-7.internal)';
			END $$;
			CREATE EVENT TRIGGER refuse_create_index ON ddl_command_start WHEN TAG IN ('CREATE INDEX')
				EXECUTE FUNCTION public.refuse_create_index()`,
		// A start guard that also covers CREATE SCHEMA cannot exempt the
		// scratch schema's own creation by current_schema(), which still
		// names the caller's schema then, so it lets CREATE SCHEMA through.
		"ddl_command_start on CREATE SCHEMA too": `
			CREATE FUNCTION public.refuse_create_index() RETURNS event_trigger LANGUAGE plpgsql AS $$
			BEGIN
				IF current_setting('test.exempt_scratch', true) = 'on' AND (tg_tag = 'CREATE SCHEMA' OR current_schema() LIKE 'pgsprite\_scratch\_%') THEN
					RETURN;
				END IF;
				RAISE EXCEPTION 'CREATE INDEX must use CONCURRENTLY (dba policy on db-7.internal)';
			END $$;
			CREATE EVENT TRIGGER refuse_create_index ON ddl_command_start WHEN TAG IN ('CREATE INDEX', 'CREATE SCHEMA')
				EXECUTE FUNCTION public.refuse_create_index()`,
		"ddl_command_end": `
			CREATE FUNCTION public.refuse_create_index() RETURNS event_trigger LANGUAGE plpgsql AS $$
			DECLARE
				cmd record;
			BEGIN
				FOR cmd IN SELECT * FROM pg_event_trigger_ddl_commands() WHERE command_tag = 'CREATE INDEX' LOOP
					CONTINUE WHEN current_setting('test.exempt_scratch', true) = 'on' AND cmd.schema_name LIKE 'pgsprite\_scratch\_%';
					RAISE EXCEPTION 'CREATE INDEX must use CONCURRENTLY (dba policy on db-7.internal)';
				END LOOP;
			END $$;
			CREATE EVENT TRIGGER refuse_create_index ON ddl_command_end EXECUTE FUNCTION public.refuse_create_index()`,
		// A guard with no tag filter also fires on the scratch schema's own
		// CREATE SCHEMA, whose row names the schema as its object, not as
		// its schema_name.
		"ddl_command_end on every command": `
			CREATE FUNCTION public.refuse_create_index() RETURNS event_trigger LANGUAGE plpgsql AS $$
			DECLARE
				cmd record;
			BEGIN
				FOR cmd IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
					CONTINUE WHEN current_setting('test.exempt_scratch', true) = 'on' AND (cmd.schema_name LIKE 'pgsprite\_scratch\_%'
						OR (cmd.command_tag = 'CREATE SCHEMA' AND cmd.object_identity LIKE 'pgsprite\_scratch\_%'));
					RAISE EXCEPTION 'CREATE INDEX must use CONCURRENTLY (dba policy on db-7.internal)';
				END LOOP;
			END $$;
			CREATE EVENT TRIGGER refuse_create_index ON ddl_command_end EXECUTE FUNCTION public.refuse_create_index()`,
	}
	for event, trigger := range triggers {
		t.Run(event, func(t *testing.T) {
			dsn, db := testutil.StartPostgres(t, database)
			_, err := db.ExecContext(t.Context(), `CREATE TABLE public.accounts (id bigint PRIMARY KEY, email text);`+trigger)
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
				`so an event trigger on DDL fires there too, on its CREATE SCHEMA and on the DDL inside it. `+
				`Have the trigger allow both for a schema whose name starts with pgsprite_scratch_ `+
				`(at ddl_command_start, check current_schema(), and let CREATE SCHEMA through, since its new name is not visible until ddl_command_end; `+
				`at ddl_command_end, check each command's schema_name, `+
				`and the object_identity of a CREATE SCHEMA), then plan again`, text)
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
		})
	}
}
