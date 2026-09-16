//go:build integration

package postgres

import (
	"log/slog"
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/testutil"
)

// A configured table owner is used for every greenfield create step, while an
// omitted owner preserves creation as the connected engine role.
func TestGreenfieldCreateTableOwner(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "table_owner_test")
	admin, err := pgxpool.New(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(admin.Close)

	_, err = admin.Exec(t.Context(), `
		CREATE ROLE app_owner;
		CREATE ROLE engine LOGIN PASSWORD 'secret';
		GRANT app_owner TO engine;
		CREATE SCHEMA owned AUTHORIZATION app_owner;
		CREATE SCHEMA connected AUTHORIZATION engine;
	`)
	require.NoError(t, err)

	engineURL, err := url.Parse(dsn)
	require.NoError(t, err)
	engineURL.User = url.UserPassword("engine", "secret")
	engineDSN := engineURL.String()

	tests := []struct {
		name, schema, owner, wantOwner string
	}{
		{name: "configured owner", schema: "owned", owner: "app_owner", wantOwner: "app_owner"},
		{name: "connected role", schema: "connected", wantOwner: "engine"},
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
		})
	}
}
