//go:build integration

package sqlstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/testutil"
)

func TestMySQLDriverPoolsIgnoreAndPruneStaleHeartbeats(t *testing.T) {
	clearTables(t)
	testDriverPoolsIgnoreAndPruneStaleHeartbeats(t, NewMySQL(testDB))
}

func TestPostgresDriverPoolsIgnoreAndPruneStaleHeartbeats(t *testing.T) {
	_, db := testutil.StartPostgres(t, "driver_pools_stale")
	applyPostgresTestSchema(t, db)
	clearPostgresTables(t, db)
	testDriverPoolsIgnoreAndPruneStaleHeartbeats(t, NewPostgres(db))
}

// A server whose heartbeat is older than the live window has stopped, so its
// drivers are not listed for deferred cutover admission to count, and a row older than the retention is removed by the next prune of any
// server.
func testDriverPoolsIgnoreAndPruneStaleHeartbeats(t *testing.T, store *Storage) {
	ctx := t.Context()
	servers := store.DriverPools()

	for _, server := range []*storage.DriverPool{
		{ServerID: "host-live/1/operator", Drivers: 2},
		{ServerID: "host-stale/1/operator", Drivers: 8},
		{ServerID: "host-gone/1/operator", Drivers: 8},
	} {
		require.NoError(t, servers.Heartbeat(ctx, server))
	}
	backdate := func(serverID string, age time.Duration) {
		t.Helper()
		_, err := store.db.ExecContext(ctx, `UPDATE driver_pools SET updated_at = `+driverPoolCutoff(store.driverPools.dialect, age.Microseconds())+` WHERE server_id = ?`, serverID)
		require.NoError(t, err)
	}
	backdate("host-stale/1/operator", 2*time.Minute)
	backdate("host-gone/1/operator", 2*time.Hour)

	live, err := servers.ListLive(ctx)
	require.NoError(t, err)
	require.Len(t, live, 1)
	assert.Equal(t, "host-live/1/operator", live[0].ServerID)

	require.NoError(t, servers.Heartbeat(ctx, &storage.DriverPool{ServerID: "host-live/1/operator", Drivers: 2}))
	assert.Equal(t, []string{"host-gone/1/operator", "host-live/1/operator", "host-stale/1/operator"}, driverPoolServerIDs(t, store), "a heartbeat alone removes nothing")
	require.NoError(t, servers.PruneStale(ctx))
	assert.Equal(t, []string{"host-live/1/operator", "host-stale/1/operator"}, driverPoolServerIDs(t, store))
}

// driverPoolServerIDs lists every driver pool row, live or not.
func driverPoolServerIDs(t *testing.T, store *Storage) []string {
	t.Helper()
	rows, err := store.db.QueryContext(t.Context(), `SELECT server_id FROM driver_pools ORDER BY server_id`)
	require.NoError(t, err)
	var ids []string
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
	return ids
}
