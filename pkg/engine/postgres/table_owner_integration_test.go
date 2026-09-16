//go:build integration

package postgres

import (
	"log/slog"
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// A configured table owner is used for every greenfield create step, and the
// table is born under that role rather than transferred to it afterwards, so
// the owner's default privileges land on it. An omitted owner preserves
// creation as the connected engine role, whose relations the owner's
// defaults do not reach.
func TestGreenfieldCreateTableOwner(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "table_owner_test")
	admin, err := pgxpool.New(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(admin.Close)

	_, err = admin.Exec(t.Context(), `
		CREATE ROLE app_owner;
		CREATE ROLE app_reader;
		CREATE ROLE engine LOGIN PASSWORD 'secret';
		GRANT app_owner TO engine;
		CREATE SCHEMA owned AUTHORIZATION app_owner;
		CREATE SCHEMA connected AUTHORIZATION engine;
		GRANT USAGE ON SCHEMA owned, connected TO app_reader;
		ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA owned GRANT SELECT ON TABLES TO app_reader;
		ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA owned GRANT USAGE ON SEQUENCES TO app_reader;
		ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA connected GRANT SELECT ON TABLES TO app_reader;
		ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA connected GRANT USAGE ON SEQUENCES TO app_reader;
	`)
	require.NoError(t, err)

	engineURL, err := url.Parse(dsn)
	require.NoError(t, err)
	engineURL.User = url.UserPassword("engine", "secret")
	engineDSN := engineURL.String()

	tests := []struct {
		name, schema, owner, wantOwner string
		// wantReaderAccess is whether app_owner's default privileges reached
		// the created relations, which they do only when the CREATE ran as
		// app_owner: defaults belong to the creating role, not the schema.
		wantReaderAccess bool
	}{
		{name: "configured owner", schema: "owned", owner: "app_owner", wantOwner: "app_owner", wantReaderAccess: true},
		{name: "connected role", schema: "connected", wantOwner: "engine", wantReaderAccess: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := executeOptimistic(t.Context(), targetConn{dsn: engineDSN}, nativeApply{
				namespace: tt.schema,
				table:     "widgets",
				sql:       "CREATE TABLE widgets (id serial PRIMARY KEY)",
			}, DefaultNativeSafeTableSizeLimitBytes, newTestTracker(t), slog.Default(), tt.owner)
			require.NoError(t, err)

			for _, relation := range []string{"widgets", "widgets_id_seq"} {
				var owner string
				err = admin.QueryRow(t.Context(), `
					SELECT pg_get_userbyid(c.relowner)
					FROM pg_class c
					JOIN pg_namespace n ON n.oid = c.relnamespace
					WHERE n.nspname = $1 AND c.relname = $2
				`, tt.schema, relation).Scan(&owner)
				require.NoError(t, err)
				assert.Equal(t, tt.wantOwner, owner)
			}

			var readerSelectsTable, readerUsesSequence bool
			err = admin.QueryRow(t.Context(), `
				SELECT has_table_privilege('app_reader', format('%I.%I', $1::text, 'widgets'), 'SELECT'),
				       has_sequence_privilege('app_reader', format('%I.%I', $1::text, 'widgets_id_seq'), 'USAGE')
			`, tt.schema).Scan(&readerSelectsTable, &readerUsesSequence)
			require.NoError(t, err)
			assert.Equal(t, tt.wantReaderAccess, readerSelectsTable, "the owner's default SELECT on tables")
			assert.Equal(t, tt.wantReaderAccess, readerUsesSequence, "the owner's default USAGE on sequences")
		})
	}
}

// A configured table owner is checked at plan time as the role the CREATE
// TABLE will run as, so a plan carries the owner's refusal before an apply
// can meet it: an owner the engine role cannot assume blocks the create step
// with the membership GRANT that would let it, an owner the target has no
// role for blocks it with no GRANT to print, and an owner the engine role is
// a member of leaves the step executable. Every case is decided by the
// owner alone — the schema grants are identical across them.
func TestEnginePlanTableOwner(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "plan_owner_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE ROLE held_owner;
		CREATE ROLE unheld_owner;
		CREATE ROLE plan_engine LOGIN PASSWORD 'plan_engine';
		GRANT held_owner TO plan_engine;
		GRANT CONNECT ON DATABASE plan_owner_test TO plan_engine;
		GRANT USAGE, CREATE ON SCHEMA public TO plan_engine, held_owner, unheld_owner`)
	require.NoError(t, err)

	engineDSN, err := url.Parse(dsn)
	require.NoError(t, err)
	engineDSN.User = url.UserPassword("plan_engine", "plan_engine")

	tests := []struct {
		name, owner   string
		wantBlocked   bool
		wantReason    []string
		wantNotReason []string
	}{
		{
			name:  "owner the engine role is a member of",
			owner: "held_owner",
		},
		{
			name:        "owner the engine role cannot assume",
			owner:       "unheld_owner",
			wantBlocked: true,
			wantReason:  []string{"provision with: GRANT", `GRANT "unheld_owner" TO "plan_engine"`},
		},
		{
			name:          "owner the target has no role for",
			owner:         "missing_owner",
			wantBlocked:   true,
			wantReason:    []string{"is not a role on the target", "correct the target's table_owner"},
			wantNotReason: []string{"GRANT"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &engine.PlanRequest{
				Database: "plan_owner_test",
				SchemaFiles: schema.SchemaFiles{
					"public": {Files: map[string]string{
						"widgets.sql": "CREATE TABLE widgets (id bigint PRIMARY KEY)",
					}},
				},
				Credentials: &engine.Credentials{DSN: engineDSN.String()},
			}

			result, err := New().WithTableOwner(tt.owner).Plan(t.Context(), req)
			require.NoError(t, err, "an owner refusal is a blocked step, never a failed plan")
			require.Len(t, result.Changes, 1)
			require.Len(t, result.Changes[0].TableChanges, 1)
			change := result.Changes[0].TableChanges[0]
			if !tt.wantBlocked {
				assert.Empty(t, change.ExecutionMode, "a create the engine role can run as the owner must plan executable")
				assert.Empty(t, change.ModeReason)
				return
			}
			assert.Equal(t, engine.ExecutionModeBlocked, change.ExecutionMode)
			for _, want := range tt.wantReason {
				assert.Contains(t, change.ModeReason, want)
			}
			for _, unwanted := range tt.wantNotReason {
				assert.NotContains(t, change.ModeReason, unwanted)
			}
		})
	}
}
