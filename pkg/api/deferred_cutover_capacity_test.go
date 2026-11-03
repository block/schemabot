package api

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// An apply started with --defer-cutover asks storage to admit every operation
// it would hold at its cutover, and an apply that does not defer asks for
// nothing, so only deferred applies are counted against the drivers.
func TestDeferredCutoverApplyAsksStorageToAdmitItsOperations(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options map[string]string
		want    int
	}{
		{name: "deferred", options: map[string]string{"defer_cutover": "true"}, want: 1},
		{name: "not deferred", want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			applies := &capturingApplyStore{}
			svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{isRemote: true}, applies)

			_, _, err := svc.EnqueueAuthorizedApply(t.Context(), ApplyRequest{PlanID: "plan-1", Environment: "staging", Options: tc.options})
			require.NoError(t, err)
			require.NotNil(t, applies.apply)
			assert.Equal(t, tc.want, applies.apply.DeferredCutoverOperations)
		})
	}
}

// Storage refusing a deferred apply's admission reaches the caller as a
// DeferredCutoverCapacityError naming the database, environment and counts, not
// as a storage failure.
func TestDeferredCutoverRefusalFromStorageNamesTheCounts(t *testing.T) {
	applies := &capturingApplyStore{err: &storage.DeferredCutoverCapacityError{
		Operations: 128, Reserved: 128, Capacity: 192, LiveServers: 64, HoldingApplyIDs: []string{"apply-first"},
	}}
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{isRemote: true}, applies)

	_, _, err := svc.EnqueueAuthorizedApply(t.Context(), ApplyRequest{PlanID: "plan-1", Environment: "staging", Options: map[string]string{"defer_cutover": "true"}})

	refused, ok := errors.AsType[*DeferredCutoverCapacityError](err)
	require.True(t, ok, "got %v", err)
	assert.Equal(t, DeferredCutoverCapacityError{Database: "testdb", Environment: "staging", Operations: 128, Reserved: 128, Capacity: 192, LiveServers: 64}, *refused)
	_, isStorageErr := errors.AsType[*applyStorageError](err)
	assert.False(t, isStorageErr)
}

func TestDeferredCutoverCapacityErrorMessages(t *testing.T) {
	for _, tc := range []struct {
		name    string
		refused DeferredCutoverCapacityError
		want    string
	}{
		{
			name:    "alongside other deferred applies",
			refused: DeferredCutoverCapacityError{Database: "commerce", Environment: "production", Operations: 128, Reserved: 128, Capacity: 192, LiveServers: 64},
			want:    "commerce/production: --defer-cutover would hold 128 operations at their cutover at once, and other deferred applies hold 128 of the 192 deferred operations SchemaBot runs; apply again without --defer-cutover, or once those applies finish",
		},
		{
			name:    "wider than the whole capacity",
			refused: DeferredCutoverCapacityError{Database: "commerce", Environment: "production", Operations: 100, Capacity: 96, LiveServers: 32},
			want:    "commerce/production: --defer-cutover would hold 100 operations at their cutover at once, and SchemaBot runs at most 96 deferred operations; apply again without --defer-cutover",
		},
		{
			name:    "wider than max_drivers_per_apply",
			refused: DeferredCutoverCapacityError{Database: "commerce", Environment: "production", Operations: 16, PerApplyCap: 2},
			want:    "commerce/production: --defer-cutover would hold 16 operations at their cutover at once, and max_drivers_per_apply runs at most 2 operations of one apply at once; apply again without --defer-cutover, or raise max_drivers_per_apply to at least 16",
		},
		{
			name:    "no live server",
			refused: DeferredCutoverCapacityError{Database: "commerce", Environment: "production", Operations: 4},
			want:    "commerce/production: --defer-cutover would hold 4 operations at their cutover at once, and no SchemaBot server has reported its drivers in the last minute; apply again without --defer-cutover",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.refused.Error())
		})
	}
}

// Settled work and finalizers never wait at a cutover, so they do not widen
// the apply.
func TestDeferredCutoverWidthCountsOnlyWorkStillToDrive(t *testing.T) {
	groups := make([]*storage.ApplyOperationWithTasks, 0, 4)
	for _, opState := range []string{state.ApplyOperation.Completed, state.ApplyOperation.Pending, state.ApplyOperation.Pending} {
		groups = append(groups, &storage.ApplyOperationWithTasks{Operation: &storage.ApplyOperation{
			OperationKind: storage.ApplyOperationKindWork, State: opState,
		}})
	}
	groups = append(groups, &storage.ApplyOperationWithTasks{Operation: &storage.ApplyOperation{
		OperationKind: storage.ApplyOperationKindGroupFinalizer, State: state.ApplyOperation.Pending,
	}})
	assert.Equal(t, 2, deferredCutoverWidth(groups))
}

// A deferred apply holds at its cutover only the operations the claim lets run
// together (ST-10): one member at a time under rolling, every deployment at
// once under barrier and parallel, and one table step of each deployment at a
// time.
func TestDeferredCutoverWidthCountsOperationsHeldAtOnce(t *testing.T) {
	op := func(policy, deployment string, rolloutStep int) *storage.ApplyOperationWithTasks {
		return &storage.ApplyOperationWithTasks{Operation: &storage.ApplyOperation{
			OperationKind: storage.ApplyOperationKindWork, State: state.ApplyOperation.Pending,
			CutoverPolicy: policy, Deployment: deployment, Target: deployment, RolloutStep: rolloutStep,
		}}
	}
	shards := func(policy, deployment string, count int) []*storage.ApplyOperationWithTasks {
		groups := make([]*storage.ApplyOperationWithTasks, 0, count)
		for range count {
			groups = append(groups, op(policy, deployment, 0))
		}
		return groups
	}
	for _, tc := range []struct {
		name   string
		groups []*storage.ApplyOperationWithTasks
		want   int
	}{
		{
			name:   "rolling holds one deployment at a time",
			groups: append(append(shards(storage.CutoverPolicyRolling, "eu", 4), shards(storage.CutoverPolicyRolling, "us", 2)...), shards(storage.CutoverPolicyRolling, "ap", 3)...),
			want:   4,
		},
		{
			name:   "barrier holds every deployment together",
			groups: append(shards(storage.CutoverPolicyBarrier, "eu", 4), shards(storage.CutoverPolicyBarrier, "us", 2)...),
			want:   6,
		},
		{
			name:   "parallel holds every deployment together",
			groups: append(shards(storage.CutoverPolicyParallel, "eu", 4), shards(storage.CutoverPolicyParallel, "us", 2)...),
			want:   6,
		},
		{
			name: "rolling table steps hold one step of one deployment",
			groups: []*storage.ApplyOperationWithTasks{
				op(storage.CutoverPolicyRolling, "eu", 1), op(storage.CutoverPolicyRolling, "eu", 2),
				op(storage.CutoverPolicyRolling, "us", 1), op(storage.CutoverPolicyRolling, "us", 2),
			},
			want: 1,
		},
		{
			name: "barrier table steps hold one step of each deployment",
			groups: []*storage.ApplyOperationWithTasks{
				op(storage.CutoverPolicyBarrier, "eu", 1), op(storage.CutoverPolicyBarrier, "eu", 2),
				op(storage.CutoverPolicyBarrier, "us", 1), op(storage.CutoverPolicyBarrier, "us", 2),
			},
			want: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, deferredCutoverWidth(tc.groups))
		})
	}
}

// A running operator records its drivers in the registry before any driver
// claims, keeping one driver off deferred work, and removes its row when it
// stops, so deferred cutover admission counts exactly the servers running
// now. A restarted operator registers as a new run.
func TestOperatorRegistersItsDriversWhileRunning(t *testing.T) {
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, &capturingApplyStore{})
	svc.config.Drivers = 4
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))
	registry := &svc.storage.(*mockStorageWithApplyStores).operatorServers

	svc.StartOperator(t.Context())
	t.Cleanup(svc.StopOperator)

	live, err := registry.ListLive(t.Context())
	require.NoError(t, err)
	require.Len(t, live, 1)
	first := live[0].ServerID
	assert.Contains(t, first, storage.LeaseOwnerProcess()+"/operator-")
	assert.Equal(t, 4, live[0].Drivers)
	assert.Equal(t, 3, live[0].DeferredCutoverDrivers)

	svc.StopOperator()
	live, err = registry.ListLive(t.Context())
	require.NoError(t, err)
	assert.Empty(t, live)

	svc.StartOperator(t.Context())
	live, err = registry.ListLive(t.Context())
	require.NoError(t, err)
	require.Len(t, live, 1)
	assert.NotEqual(t, first, live[0].ServerID)
}

// The heartbeat keeps the row fresh on its own cadence while the operator runs.
func TestOperatorHeartbeatsOnItsInterval(t *testing.T) {
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, &capturingApplyStore{})
	svc.config.Drivers = 2
	svc.operatorHeartbeatEvery = 10 * time.Millisecond
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))
	registry := &svc.storage.(*mockStorageWithApplyStores).operatorServers

	svc.StartOperator(t.Context())
	t.Cleanup(svc.StopOperator)

	require.Eventually(t, func() bool {
		registry.mu.Lock()
		defer registry.mu.Unlock()
		return registry.beats >= 3
	}, drivePassesDeadline, 5*time.Millisecond)
}

// A registry that stops answering does not hold the operator's startup: the
// first heartbeat gives up at its own deadline, StartOperator returns, and
// the drivers start without the server's row.
func TestOperatorStartupHeartbeatGivesUpAtItsDeadline(t *testing.T) {
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, &capturingApplyStore{})
	svc.config.Drivers = 2
	svc.operatorHeartbeatDeadline = 10 * time.Millisecond
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))
	registry := &svc.storage.(*mockStorageWithApplyStores).operatorServers
	registry.hang = true

	started := make(chan struct{})
	go func() {
		svc.StartOperator(t.Context())
		close(started)
	}()
	select {
	case <-started:
	case <-time.After(drivePassesDeadline):
		require.FailNow(t, "StartOperator did not return while the registry stopped answering")
	}
	t.Cleanup(svc.StopOperator)

	live, err := registry.ListLive(t.Context())
	require.NoError(t, err)
	assert.Empty(t, live, "a heartbeat that timed out records no row")
}

// A deferred apply's waiting operations keep counting against
// max_drivers_per_apply, so an apply with more operations than the cap could
// never have them all waiting at once. Apply creation refuses it before
// storing anything, naming the cap, and admits the same apply once the cap
// covers every operation.
func TestDeferredCutoverWiderThanThePerApplyCapIsRefused(t *testing.T) {
	change := storage.TableChange{Namespace: "commerce", Table: "orders", DDL: "ALTER TABLE `orders` ADD INDEX (`created_at`)", Operation: "alter"}
	plan := trustedQueueApplyTestPlan()
	plan.Namespaces = map[string]*storage.NamespacePlanData{"commerce": {Tables: []storage.TableChange{change}}}
	for _, shard := range []string{"-40", "40-80", "80-"} {
		plan.Shards = append(plan.Shards, storage.ShardPlan{Namespace: "commerce", Shard: shard, Changes: []storage.TableChange{change}})
	}
	applies := &capturingApplyStore{}
	svc, _ := newQueueApplyTestService(plan, &mockTernClient{isRemote: true}, applies)
	deferred := ApplyRequest{PlanID: "plan-1", Environment: "staging", Options: map[string]string{"defer_cutover": "true"}}

	_, _, err := svc.EnqueueAuthorizedApply(t.Context(), deferred)

	refused, ok := errors.AsType[*DeferredCutoverCapacityError](err)
	require.True(t, ok, "got %v", err)
	assert.Equal(t, DeferredCutoverCapacityError{Database: "testdb", Environment: "staging", Operations: 3, PerApplyCap: storage.DefaultMaxDriversPerApply}, *refused)
	assert.Nil(t, applies.apply, "a refused apply stores nothing")

	svc.config.MaxDriversPerApply = 3
	_, _, err = svc.EnqueueAuthorizedApply(t.Context(), deferred)
	require.NoError(t, err)
	require.NotNil(t, applies.apply)
	assert.Equal(t, 3, applies.apply.DeferredCutoverOperations)
}
