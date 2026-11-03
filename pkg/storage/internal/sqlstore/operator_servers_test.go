//go:build integration

package sqlstore

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// A server whose heartbeat is older than the live window has stopped, so its
// drivers are neither listed nor counted toward deferred cutover admission,
// and a row older than the retention is removed by the next heartbeat of any
// server.
func TestOperatorServersIgnoreAndPruneStaleHeartbeats(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)
	servers := store.OperatorServers()

	for _, server := range []*storage.OperatorServer{
		{ServerID: "host-live/1/operator", Drivers: 4, DeferredCutoverDrivers: 3},
		{ServerID: "host-stale/1/operator", Drivers: 8, DeferredCutoverDrivers: 7},
		{ServerID: "host-gone/1/operator", Drivers: 8, DeferredCutoverDrivers: 7},
	} {
		require.NoError(t, servers.Heartbeat(ctx, server))
	}
	_, err := testDB.ExecContext(ctx, `UPDATE operator_servers SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE server_id = 'host-stale/1/operator'`)
	require.NoError(t, err)
	_, err = testDB.ExecContext(ctx, `UPDATE operator_servers SET updated_at = NOW() - INTERVAL 2 HOUR WHERE server_id = 'host-gone/1/operator'`)
	require.NoError(t, err)

	live, err := servers.ListLive(ctx)
	require.NoError(t, err)
	require.Len(t, live, 1)
	assert.Equal(t, "host-live/1/operator", live[0].ServerID)

	apply := newGroupedCreateApply(time.Now(), "apply_deferred_stale_capacity")
	apply.Options = []byte(`{"defer_cutover":true}`)
	apply.DeferredCutoverOperations = 4
	groups := []*storage.ApplyOperationWithTasks{newGroupedCreateGroup(time.Now(), "payments-a", "", "users")}
	_, err = store.Applies().CreateWithGroupedOperations(ctx, apply, groups)
	refused, ok := errors.AsType[*storage.DeferredCutoverCapacityError](err)
	require.True(t, ok, "got %v", err)
	assert.Equal(t, 3, refused.Capacity, "only the live server's drivers count")
	assert.Equal(t, 1, refused.LiveServers)

	require.NoError(t, servers.Heartbeat(ctx, &storage.OperatorServer{ServerID: "host-live/1/operator", Drivers: 4, DeferredCutoverDrivers: 3}))
	var remaining []string
	rows, err := testDB.QueryContext(ctx, `SELECT server_id FROM operator_servers ORDER BY server_id`)
	require.NoError(t, err)
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		remaining = append(remaining, id)
	}
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{"host-live/1/operator", "host-stale/1/operator"}, remaining)
}
