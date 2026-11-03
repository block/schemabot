package api

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
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
	assert.Equal(t, DeferredCutoverCapacityError{
		Database: "testdb", Environment: "staging", Operations: 128, Reserved: 128, Capacity: 192, LiveServers: 64,
		HoldingApplyIDs: []string{"apply-first"},
	}, *refused)
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
			refused: DeferredCutoverCapacityError{Database: "commerce", Environment: "production", Operations: 128, Reserved: 128, Capacity: 192, LiveServers: 64, HoldingApplyIDs: []string{"apply-first"}},
			want:    "commerce/production: --defer-cutover would hold 128 operations at their cutover at once, and other deferred applies (apply-first) hold 128 of the 192 deferred operations SchemaBot runs; apply again without --defer-cutover, or once those applies are cut over or cancelled",
		},
		{
			name: "alongside many deferred applies",
			refused: DeferredCutoverCapacityError{Database: "commerce", Environment: "production", Operations: 2, Reserved: 14, Capacity: 15, LiveServers: 5, HoldingApplyIDs: []string{
				"apply-1", "apply-2", "apply-3", "apply-4", "apply-5", "apply-6", "apply-7",
			}},
			want: "commerce/production: --defer-cutover would hold 2 operations at their cutover at once, and other deferred applies (apply-1, apply-2, apply-3, apply-4, apply-5 and 2 more) hold 14 of the 15 deferred operations SchemaBot runs; apply again without --defer-cutover, or once those applies are cut over or cancelled",
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
// max_drivers_per_apply, and its cutover is accepted only once every
// operation waits, so an apply that would hold more operations at once than
// the cap could never be cut over. Apply creation refuses it before storing
// anything, naming the cap, and admits the same apply once the cap covers
// every operation.
func TestDeferredCutoverWiderThanThePerApplyCapIsRefused(t *testing.T) {
	plan := threeShardQueueApplyTestPlan()
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

// The same sharded apply without --defer-cutover is never checked against
// max_drivers_per_apply: its shards cut over on their own, so it runs in
// batches of the cap.
func TestApplyWiderThanThePerApplyCapRunsWithoutDeferCutover(t *testing.T) {
	applies := &capturingApplyStore{}
	svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), &mockTernClient{isRemote: true}, applies)

	_, _, err := svc.EnqueueAuthorizedApply(t.Context(), ApplyRequest{PlanID: "plan-1", Environment: "staging"})
	require.NoError(t, err)
	require.NotNil(t, applies.apply)
	assert.Zero(t, applies.apply.DeferredCutoverOperations)
}

// threeShardQueueApplyTestPlan is trustedQueueApplyTestPlan with one table
// change on each of three shards, so the apply is wider than the default
// max_drivers_per_apply.
func threeShardQueueApplyTestPlan() *storage.Plan {
	change := storage.TableChange{Namespace: "commerce", Table: "orders", DDL: "ALTER TABLE `orders` ADD INDEX (`created_at`)", Operation: "alter"}
	plan := trustedQueueApplyTestPlan()
	plan.Namespaces = map[string]*storage.NamespacePlanData{"commerce": {Tables: []storage.TableChange{change}}}
	for _, shard := range []string{"-40", "40-80", "80-"} {
		plan.Shards = append(plan.Shards, storage.ShardPlan{Namespace: "commerce", Shard: shard, Changes: []storage.TableChange{change}})
	}
	return plan
}

// A refusal that can clear on its own answers 409, one that fails on every
// retry answers 422, and both carry the deferred cutover error code, so a
// caller can tell a refusal of --defer-cutover from any other conflict. Either
// is a refusal of the request, never a server failure.
func TestDeferredCutoverRefusalStatusAndCode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		refused *storage.DeferredCutoverCapacityError
		status  int
	}{
		{name: "crowded by other deferred applies", refused: &storage.DeferredCutoverCapacityError{Operations: 2, Reserved: 5, Capacity: 6, LiveServers: 2, HoldingApplyIDs: []string{"apply-first"}}, status: http.StatusConflict},
		{name: "no live server", refused: &storage.DeferredCutoverCapacityError{Operations: 2}, status: http.StatusConflict},
		{name: "wider than every live driver", refused: &storage.DeferredCutoverCapacityError{Operations: 8, Capacity: 6, LiveServers: 2}, status: http.StatusUnprocessableEntity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			applies := &capturingApplyStore{err: tc.refused}
			svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{isRemote: true}, applies)

			status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-1","environment":"staging","options":{"defer_cutover":"true"}}`)

			assert.Equal(t, tc.status, status)
			assert.Equal(t, apitypes.ErrCodeDeferredCutoverCapacity, resp.ErrorCode)
			assert.True(t, strings.HasPrefix(resp.Error, "apply rejected: testdb/staging: --defer-cutover would hold"), resp.Error)
			assert.False(t, apitypes.IsRetryableErrorCode(resp.ErrorCode), "a retry without a change to the request or the plane is refused again")

			_, _, err := svc.EnqueueAuthorizedApply(t.Context(), ApplyRequest{PlanID: "plan-1", Environment: "staging", Options: map[string]string{"defer_cutover": "true"}})
			assert.Equal(t, applyMetricStatusRejected, applyMetricStatusForError(err))
		})
	}
}

// A deferred apply wider than max_drivers_per_apply is refused before
// storage with 422 and the deferred cutover error code: raising the cap or
// dropping the flag is the only way it runs, so a retry of the same request
// is refused again.
func TestDeferredCutoverWiderThanThePerApplyCapAnswers422(t *testing.T) {
	applies := &capturingApplyStore{}
	svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), &mockTernClient{isRemote: true}, applies)

	status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-1","environment":"staging","options":{"defer_cutover":"true"}}`)

	assert.Equal(t, http.StatusUnprocessableEntity, status)
	assert.Equal(t, apitypes.ErrCodeDeferredCutoverCapacity, resp.ErrorCode)
	assert.Equal(t, "apply rejected: testdb/staging: --defer-cutover would hold 3 operations at their cutover at once, and max_drivers_per_apply runs at most 2 operations of one apply at once; apply again without --defer-cutover, or raise max_drivers_per_apply to at least 3", resp.Error)
	assert.Nil(t, applies.apply, "a refused apply stores nothing")
}

// A heartbeat whose prune fails still records the server, so its drivers keep
// counting toward deferred cutover admission.
func TestOperatorHeartbeatSurvivesAFailedPrune(t *testing.T) {
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, &capturingApplyStore{})
	registry := &svc.storage.(*mockStorageWithApplyStores).operatorServers
	registry.pruneErr = errors.New("prune failed")

	svc.heartbeatOperatorServer(t.Context(), &storage.OperatorServer{ServerID: "host-a/1/operator", Drivers: 4, DeferredCutoverDrivers: 3})

	live, err := registry.ListLive(t.Context())
	require.NoError(t, err)
	require.Len(t, live, 1)
	assert.Equal(t, 3, live[0].DeferredCutoverDrivers)
	assert.Equal(t, 1, registry.prunes)
}
