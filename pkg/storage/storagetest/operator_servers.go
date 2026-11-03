package storagetest

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

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
	// operator servers and the deferred applies already admitted: two servers
	// leave 4 drivers for deferred work, two 2-operation applies fill them,
	// and a third is refused with nothing stored, naming the applies that
	// hold the rest.
	t.Run("AdmitsDeferredCutoverWithinLiveDrivers", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		heartbeat(t, store, "host-a/1/operator", 3, 2)
		heartbeat(t, store, "host-b/1/operator", 3, 2)

		_, err := createDeferredApply(t, store, "apply_deferred_first", "orders", 2)
		require.NoError(t, err)
		_, err = createDeferredApply(t, store, "apply_deferred_second", "inventory", 2)
		require.NoError(t, err, "2 more operations fill the 4 drivers exactly")

		_, err = createDeferredApply(t, store, "apply_deferred_third", "payments", 2)
		refused, ok := errors.AsType[*storage.DeferredCutoverCapacityError](err)
		require.True(t, ok, "got %v", err)
		assert.Equal(t, storage.DeferredCutoverCapacityError{
			Operations: 2, Reserved: 4, Capacity: 4, LiveServers: 2,
			HoldingApplyIDs: []string{"apply_deferred_first", "apply_deferred_second"},
		}, *refused)
		assert.False(t, refused.ExceedsCapacity())

		stored, err := store.Applies().GetByApplyIdentifier(ctx, "apply_deferred_third")
		require.NoError(t, err)
		assert.Nil(t, stored, "a refused apply is not stored")
	})

	// AdmitsAtMostMaxDriversPerApplyOfAnApply verifies an admitted apply wider
	// than max_drivers_per_apply counts only the cap against later applies:
	// its waiting operations keep counting against the cap, so it never holds
	// more drivers than that at its cutover.
	t.Run("AdmitsAtMostMaxDriversPerApplyOfAnApply", func(t *testing.T) {
		store := h.NewStorage(t)
		heartbeat(t, store, "host-a/1/operator", 4, 3)

		_, err := createDeferredApply(t, store, "apply_deferred_fanout", "orders", 8)
		require.NoError(t, err)

		_, err = createDeferredApply(t, store, "apply_deferred_after_fanout", "payments", 3)
		refused, ok := errors.AsType[*storage.DeferredCutoverCapacityError](err)
		require.True(t, ok, "got %v", err)
		assert.Equal(t, storage.DefaultMaxDriversPerApply, refused.Reserved, "the 8-operation apply holds the cap")
		assert.Equal(t, storage.DefaultMaxDriversPerApply, refused.Operations, "the refused apply asks for the cap")
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

		heartbeat(t, store, "host-a/1/operator", 2, 1)
		_, err = createDeferredApply(t, store, "apply_too_wide", "orders", 2)
		refused, ok = errors.AsType[*storage.DeferredCutoverCapacityError](err)
		require.True(t, ok, "got %v", err)
		assert.Equal(t, storage.DeferredCutoverCapacityError{Operations: 2, Capacity: 1, LiveServers: 1}, *refused)
		assert.True(t, refused.ExceedsCapacity())
	})

	// SingleOperationDeferredCutoverIsAdmittedAndCounted verifies a deferred
	// apply of one operation is admitted with no live server, since its one
	// cutover command reaches all of it, and that its waiting operation
	// still counts against later wide applies.
	t.Run("SingleOperationDeferredCutoverIsAdmittedAndCounted", func(t *testing.T) {
		store := h.NewStorage(t)

		_, err := createDeferredApply(t, store, "apply_single", "orders", 1)
		require.NoError(t, err)

		heartbeat(t, store, "host-a/1/operator", 3, 2)
		_, err = createDeferredApply(t, store, "apply_wide", "payments", 2)
		refused, ok := errors.AsType[*storage.DeferredCutoverCapacityError](err)
		require.True(t, ok, "got %v", err)
		assert.Equal(t, storage.DeferredCutoverCapacityError{
			Operations: 2, Reserved: 1, Capacity: 2, LiveServers: 1,
			HoldingApplyIDs: []string{"apply_single"},
		}, *refused)

		heartbeat(t, store, "host-b/1/operator", 2, 1)
		_, err = createDeferredApply(t, store, "apply_wide", "payments", 2)
		require.NoError(t, err, "a second server's driver makes room beside the single apply")
	})

	// StoppedDeferredApplyKeepsItsReservation verifies a stopped wide deferred
	// apply still counts against the live drivers: starting it again resumes
	// its operations without passing admission, so another wide deferred
	// apply admitted into its drivers would leave both short. Once an apply
	// takes over its work it can never be started again, so it stops counting,
	// including for the apply that takes it over.
	t.Run("StoppedDeferredApplyKeepsItsReservation", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		heartbeat(t, store, "host-a/1/operator", 4, 3)

		id, err := createDeferredApply(t, store, "apply_deferred_stopped", "orders", 2)
		require.NoError(t, err)
		stopped, err := store.Applies().Get(ctx, id)
		require.NoError(t, err)
		require.NotNil(t, stopped)
		stopped.State = state.Apply.Stopped
		require.NoError(t, store.Applies().Update(ctx, stopped))
		_, err = store.ApplyOperations().MarkPendingStoppedByApply(ctx, id)
		require.NoError(t, err)

		_, err = createDeferredApply(t, store, "apply_deferred_after_stop", "payments", 2)
		refused, ok := errors.AsType[*storage.DeferredCutoverCapacityError](err)
		require.True(t, ok, "got %v", err)
		assert.Equal(t, storage.DeferredCutoverCapacityError{
			Operations: 2, Reserved: 2, Capacity: 3, LiveServers: 1,
			HoldingApplyIDs: []string{"apply_deferred_stopped"},
		}, *refused)

		_, err = createDeferredApply(t, store, "apply_deferred_takeover", "orders", 2, id)
		require.NoError(t, err, "the apply taking over the stopped one does not count it")

		_, err = createDeferredApply(t, store, "apply_deferred_after_takeover", "payments", 2)
		refused, ok = errors.AsType[*storage.DeferredCutoverCapacityError](err)
		require.True(t, ok, "got %v", err)
		assert.Equal(t, []string{"apply_deferred_takeover"}, refused.HoldingApplyIDs, "the superseded apply holds nothing")
	})

	// ConcurrentDeferredCutoverAdmissionsAdmitOne verifies admission is
	// serialized across creators: several wide deferred applies on different
	// databases, any one of which leaves too few live drivers for another,
	// race to be created, and exactly one is admitted.
	t.Run("ConcurrentDeferredCutoverAdmissionsAdmitOne", func(t *testing.T) {
		store := h.NewStorage(t)
		heartbeat(t, store, "host-a/1/operator", 4, 3)

		const creators = 4
		results := make([]error, creators)
		var wg sync.WaitGroup
		for i := range creators {
			wg.Go(func() {
				_, results[i] = createDeferredApply(t, store, fmt.Sprintf("apply_race_%d", i), fmt.Sprintf("race_%d", i), 2)
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

	// RefusesDeferredCutoverPastADataPlanesDrivers verifies a deferred apply
	// whose operations run on a deployment's own data plane is counted against
	// the drivers that plane reported, beside the other deferred applies on
	// that deployment, even when this plane's own operators have room. Applies
	// on another deployment hold none of that plane's drivers, and a plane
	// with no live server refuses every wide deferred apply.
	t.Run("RefusesDeferredCutoverPastADataPlanesDrivers", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		heartbeat(t, store, "host-a/1/operator", 12, 11)
		east := storage.DeferredCutoverDataPlane{LiveServers: 1, Capacity: 3}

		_, err := createDataPlaneDeferredApply(t, store, "apply_east_first", "orders", "east", 2, east)
		require.NoError(t, err)

		_, err = createDataPlaneDeferredApply(t, store, "apply_east_second", "payments", "east", 2, east)
		refused, ok := errors.AsType[*storage.DeferredCutoverCapacityError](err)
		require.True(t, ok, "got %v", err)
		assert.Equal(t, storage.DeferredCutoverCapacityError{
			Deployment: "east", Operations: 2, Reserved: 2, Capacity: 3, LiveServers: 1,
			HoldingApplyIDs: []string{"apply_east_first"},
		}, *refused)
		stored, err := store.Applies().GetByApplyIdentifier(ctx, "apply_east_second")
		require.NoError(t, err)
		assert.Nil(t, stored, "a refused apply is not stored")

		_, err = createDataPlaneDeferredApply(t, store, "apply_west", "payments", "west", 3, storage.DeferredCutoverDataPlane{LiveServers: 1, Capacity: 3})
		require.NoError(t, err, "the east apply holds none of west's drivers")

		_, err = createDataPlaneDeferredApply(t, store, "apply_north", "inventory", "north", 2, storage.DeferredCutoverDataPlane{Capacity: 8})
		refused, ok = errors.AsType[*storage.DeferredCutoverCapacityError](err)
		require.True(t, ok, "got %v", err)
		assert.Equal(t, storage.DeferredCutoverCapacityError{Deployment: "north", Operations: 2, Capacity: 8}, *refused)
	})

	// CountsADataPlanePoolAcrossEveryRouteToIt verifies deferred applies sent
	// to one data-plane pool under different routes, another environment and
	// another deployment name, count against the pool's drivers together:
	// with three drivers shared, a staging apply holding two leaves one for
	// production, whichever route reaches the pool. A route to another pool
	// is not counted against it.
	t.Run("CountsADataPlanePoolAcrossEveryRouteToIt", func(t *testing.T) {
		store := h.NewStorage(t)
		heartbeat(t, store, "host-a/1/operator", 12, 11)
		shared := storage.DeferredCutoverDataPlane{Pool: "pool-shared", LiveServers: 1, Capacity: 3}

		_, err := createDataPlaneDeferredApplyIn(t, store, "apply_staging", "orders", "staging", "east", 2, shared)
		require.NoError(t, err)

		_, err = createDataPlaneDeferredApplyIn(t, store, "apply_production", "orders", "production", "east", 2, shared)
		refused, ok := errors.AsType[*storage.DeferredCutoverCapacityError](err)
		require.True(t, ok, "the production route reaches the staging apply's pool, got %v", err)
		assert.Equal(t, storage.DeferredCutoverCapacityError{
			Deployment: "east", Operations: 2, Reserved: 2, Capacity: 3, LiveServers: 1,
			HoldingApplyIDs: []string{"apply_staging"},
		}, *refused)

		_, err = createDataPlaneDeferredApplyIn(t, store, "apply_alias", "payments", "staging", "east-alias", 2, shared)
		refused, ok = errors.AsType[*storage.DeferredCutoverCapacityError](err)
		require.True(t, ok, "another deployment name for the pool counts it too, got %v", err)
		assert.Equal(t, []string{"apply_staging"}, refused.HoldingApplyIDs)

		_, err = createDataPlaneDeferredApplyIn(t, store, "apply_elsewhere", "payments", "staging", "east", 3,
			storage.DeferredCutoverDataPlane{Pool: "pool-other", LiveServers: 1, Capacity: 3})
		require.NoError(t, err, "the staging apply holds none of another pool's drivers")
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
// database, with one work operation per shard, asking admission for as many
// of them as max_drivers_per_apply lets it hold at once, as apply creation
// does. It takes over the stopped applies takesOver names.
func createDeferredApply(t *testing.T, store storage.Storage, applyID, database string, shards int, takesOver ...int64) (int64, error) {
	t.Helper()
	apply, groups := newDeferredApply(applyID, database, "region-a", shards)
	apply.TakesOverApplyIDs = takesOver
	return store.Applies().CreateWithGroupedOperations(t.Context(), apply, groups)
}

// createDataPlaneDeferredApply creates a pending production apply deferring
// its cutover on database whose shards run on deployment's own data plane,
// which reported the deferred cutover drivers in plane and the pool it names
// (the deployment's own name when plane names none), as apply creation does
// for a deployment that runs its operations on another plane's operators.
func createDataPlaneDeferredApply(t *testing.T, store storage.Storage, applyID, database, deployment string, shards int, plane storage.DeferredCutoverDataPlane) (int64, error) {
	t.Helper()
	return createDataPlaneDeferredApplyIn(t, store, applyID, database, "production", deployment, shards, plane)
}

// createDataPlaneDeferredApplyIn is createDataPlaneDeferredApply for an apply
// in environment.
func createDataPlaneDeferredApplyIn(t *testing.T, store storage.Storage, applyID, database, environment, deployment string, shards int, plane storage.DeferredCutoverDataPlane) (int64, error) {
	t.Helper()
	apply, groups := newDeferredApply(applyID, database, deployment, shards)
	apply.Environment = environment
	if plane.Pool == "" {
		plane.Pool = deployment
	}
	plane.Deployment = deployment
	plane.Operations = shards
	apply.DeferredCutoverDataPlanes = []storage.DeferredCutoverDataPlane{plane}
	apply.SetOptions(storage.ApplyOptions{DeferCutover: true, DeferredCutoverPools: map[string]string{deployment: plane.Pool}})
	return store.Applies().CreateWithGroupedOperations(t.Context(), apply, groups)
}

// newDeferredApply builds a pending apply deferring its cutover on database,
// with one work operation per shard on deployment.
func newDeferredApply(applyID, database, deployment string, shards int) (*storage.Apply, []*storage.ApplyOperationWithTasks) {
	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier:           applyID,
		PlanID:                    1,
		Database:                  database,
		DatabaseType:              storage.DatabaseTypeMySQL,
		Environment:               "production",
		Deployment:                deployment,
		Engine:                    storage.EngineSpirit,
		State:                     state.Apply.Pending,
		Options:                   []byte(`{"defer_cutover":true}`),
		DeferredCutoverOperations: min(shards, storage.DefaultMaxDriversPerApply),
		CreatedAt:                 now,
		UpdatedAt:                 now,
	}
	groups := make([]*storage.ApplyOperationWithTasks, 0, shards)
	for shard := range shards {
		key := fmt.Sprintf("%s/%d/users", database, shard)
		groups = append(groups, &storage.ApplyOperationWithTasks{
			Operation: &storage.ApplyOperation{
				Deployment: deployment, OperationKey: key, OperationKind: storage.ApplyOperationKindWork,
				State: state.ApplyOperation.Pending, CreatedAt: now, UpdatedAt: now,
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
				CreatedAt:      now,
				UpdatedAt:      now,
			}},
		})
	}
	return apply, groups
}
