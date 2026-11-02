package storagetest

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestOperatorServers runs the behavioral parity suite for
// storage.OperatorServerStore and the deferred cutover admission that reads it.
func TestOperatorServers(t *testing.T, h Harness) {
	// HeartbeatRegistersAndUpdates verifies a server's first heartbeat creates
	// its row and later ones replace its driver counts, and that Deregister
	// removes it, including a second time.
	t.Run("HeartbeatRegistersAndUpdates", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		servers := store.OperatorServers()

		require.NoError(t, servers.Heartbeat(ctx, &storage.OperatorServer{ServerID: "host-b/1/operator", Drivers: 4, DeferredCutoverDrivers: 3}))
		require.NoError(t, servers.Heartbeat(ctx, &storage.OperatorServer{ServerID: "host-a/1/operator", Drivers: 1, DeferredCutoverDrivers: 1}))
		require.NoError(t, servers.Heartbeat(ctx, &storage.OperatorServer{ServerID: "host-b/1/operator", Drivers: 8, DeferredCutoverDrivers: 7}))

		live, err := servers.ListLive(ctx)
		require.NoError(t, err)
		require.Len(t, live, 2)
		assert.Equal(t, "host-a/1/operator", live[0].ServerID)
		assert.Equal(t, 1, live[0].DeferredCutoverDrivers)
		assert.Equal(t, "host-b/1/operator", live[1].ServerID)
		assert.Equal(t, 8, live[1].Drivers)
		assert.Equal(t, 7, live[1].DeferredCutoverDrivers)
		assert.False(t, live[1].UpdatedAt.IsZero())

		require.NoError(t, servers.Deregister(ctx, "host-b/1/operator"))
		require.NoError(t, servers.Deregister(ctx, "host-b/1/operator"), "deregistering a missing row is not an error")
		live, err = servers.ListLive(ctx)
		require.NoError(t, err)
		require.Len(t, live, 1)
		assert.Equal(t, "host-a/1/operator", live[0].ServerID)
	})

	t.Run("HeartbeatRequiresServerID", func(t *testing.T) {
		store := h.NewStorage(t)
		require.Error(t, store.OperatorServers().Heartbeat(t.Context(), &storage.OperatorServer{Drivers: 4}))
	})

	// AdmitsDeferredCutoverWithinLiveDrivers verifies apply creation counts
	// a wide deferred apply against the deferred cutover drivers of the live
	// operator servers and the wide deferred applies already admitted: two
	// servers leave 6 drivers for deferred work, a 4-operation apply is
	// admitted, and a second 4-operation apply is refused with nothing
	// stored, naming the apply that holds the rest.
	t.Run("AdmitsDeferredCutoverWithinLiveDrivers", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		heartbeat(t, store, "host-a/1/operator", 4, 3)
		heartbeat(t, store, "host-b/1/operator", 4, 3)

		_, err := createDeferredApply(t, store, "apply_deferred_first", "orders", 4)
		require.NoError(t, err)

		_, err = createDeferredApply(t, store, "apply_deferred_second", "payments", 4)
		refused, ok := errors.AsType[*storage.DeferredCutoverCapacityError](err)
		require.True(t, ok, "got %v", err)
		assert.Equal(t, storage.DeferredCutoverCapacityError{
			Operations: 4, Reserved: 4, Capacity: 6, LiveServers: 2,
			HoldingApplyIDs: []string{"apply_deferred_first"},
		}, *refused)
		assert.False(t, refused.ExceedsCapacity())

		stored, err := store.Applies().GetByApplyIdentifier(ctx, "apply_deferred_second")
		require.NoError(t, err)
		assert.Nil(t, stored, "a refused apply is not stored")

		_, err = createDeferredApply(t, store, "apply_deferred_fits", "inventory", 2)
		require.NoError(t, err, "2 more operations fill the 6 drivers exactly")
	})

	// RefusesDeferredCutoverWiderThanLiveDrivers verifies an apply wider than
	// every live deferred cutover driver together is refused even with
	// nothing else running, and that with no live server at all every wide
	// deferred apply is refused.
	t.Run("RefusesDeferredCutoverWiderThanLiveDrivers", func(t *testing.T) {
		store := h.NewStorage(t)

		_, err := createDeferredApply(t, store, "apply_no_servers", "orders", 2)
		refused, ok := errors.AsType[*storage.DeferredCutoverCapacityError](err)
		require.True(t, ok, "got %v", err)
		assert.Equal(t, 0, refused.LiveServers)
		assert.True(t, refused.ExceedsCapacity())

		heartbeat(t, store, "host-a/1/operator", 4, 3)
		_, err = createDeferredApply(t, store, "apply_too_wide", "orders", 4)
		refused, ok = errors.AsType[*storage.DeferredCutoverCapacityError](err)
		require.True(t, ok, "got %v", err)
		assert.Equal(t, storage.DeferredCutoverCapacityError{Operations: 4, Capacity: 3, LiveServers: 1}, *refused)
		assert.True(t, refused.ExceedsCapacity())
	})

	// SingleOperationDeferredCutoverIsNotCounted verifies a deferred apply of
	// one operation is admitted with no live server and is not counted
	// against later wide applies: it is ready to cut over as soon as its own
	// operation waits, so it never holds a driver waiting on another.
	t.Run("SingleOperationDeferredCutoverIsNotCounted", func(t *testing.T) {
		store := h.NewStorage(t)

		_, err := createDeferredApply(t, store, "apply_single", "orders", 1)
		require.NoError(t, err)

		heartbeat(t, store, "host-a/1/operator", 3, 2)
		_, err = createDeferredApply(t, store, "apply_wide", "payments", 2)
		require.NoError(t, err)
	})

	// ConcurrentDeferredCutoverAdmissionsAdmitOne verifies admission is
	// serialized across creators: several wide deferred applies on different
	// databases, any one of which fills the live drivers, race to be created,
	// and exactly one is admitted.
	t.Run("ConcurrentDeferredCutoverAdmissionsAdmitOne", func(t *testing.T) {
		store := h.NewStorage(t)
		heartbeat(t, store, "host-a/1/operator", 4, 3)

		const creators = 4
		results := make([]error, creators)
		var wg sync.WaitGroup
		for i := range creators {
			wg.Go(func() {
				_, results[i] = createDeferredApply(t, store, fmt.Sprintf("apply_race_%d", i), fmt.Sprintf("race_%d", i), 3)
			})
		}
		wg.Wait()

		admitted := 0
		for _, err := range results {
			if err == nil {
				admitted++
				continue
			}
			_, ok := errors.AsType[*storage.DeferredCutoverCapacityError](err)
			assert.True(t, ok, "a losing creator is refused for capacity, got %v", err)
		}
		assert.Equal(t, 1, admitted)
	})
}

// heartbeat registers a live operator server.
func heartbeat(t *testing.T, store storage.Storage, serverID string, drivers, deferredDrivers int) {
	t.Helper()
	require.NoError(t, store.OperatorServers().Heartbeat(t.Context(), &storage.OperatorServer{
		ServerID: serverID, Drivers: drivers, DeferredCutoverDrivers: deferredDrivers,
	}))
}

// createDeferredApply creates a pending apply deferring its cutover on
// database, with one work operation per shard, asking admission for all of
// them.
func createDeferredApply(t *testing.T, store storage.Storage, applyID, database string, shards int) (int64, error) {
	t.Helper()
	apply := &storage.Apply{
		ApplyIdentifier:           applyID,
		PlanID:                    1,
		Database:                  database,
		DatabaseType:              storage.DatabaseTypeMySQL,
		Environment:               "production",
		Deployment:                "region-a",
		Engine:                    storage.EngineSpirit,
		State:                     state.Apply.Pending,
		Options:                   []byte(`{"defer_cutover":true}`),
		DeferredCutoverOperations: shards,
	}
	groups := make([]*storage.ApplyOperationWithTasks, 0, shards)
	for shard := range shards {
		key := fmt.Sprintf("%s/%d/users", database, shard)
		groups = append(groups, &storage.ApplyOperationWithTasks{
			Operation: &storage.ApplyOperation{
				Deployment: "region-a", OperationKey: key, OperationKind: storage.ApplyOperationKindWork,
				State: state.ApplyOperation.Pending,
			},
			Tasks: []*storage.Task{{
				TaskIdentifier: fmt.Sprintf("task_%s_%d", applyID, shard),
				PlanID:         1,
				Database:       database,
				DatabaseType:   storage.DatabaseTypeMySQL,
				Engine:         storage.EngineSpirit,
				Environment:    "production",
				State:          state.Task.Pending,
				TableName:      "users",
				DDL:            "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
				DDLAction:      "alter",
				Options:        []byte("{}"),
			}},
		})
	}
	return store.Applies().CreateWithGroupedOperations(t.Context(), apply, groups)
}
