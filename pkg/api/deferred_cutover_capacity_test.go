package api

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
)

// seedLiveServers records one live operator server per entry, each running
// that many drivers.
func seedLiveServers(t *testing.T, svc *Service, drivers ...int) {
	t.Helper()
	registry := &svc.storage.(*mockStorageWithApplyStores).operatorServers
	for i, n := range drivers {
		require.NoError(t, registry.Heartbeat(t.Context(), &storage.OperatorServer{ServerID: fmt.Sprintf("host-%d/1/operator", i), Drivers: n}))
	}
}

var deferredApplyRequest = ApplyRequest{PlanID: "plan-1", Environment: "staging", Options: map[string]string{"defer_cutover": "true"}}

// A deferred apply of three shards keeps three drivers waiting at its
// cutover. Two servers of two drivers each keep one driver apiece for other
// work, so they have two drivers that can wait and the apply is refused
// before anything is stored. A third server gives it the third driver, and
// the same apply is created.
func TestDeferredCutoverWiderThanTheFleetIsRefused(t *testing.T) {
	applies := &capturingApplyStore{}
	svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), &mockTernClient{isRemote: true}, applies)
	svc.config.MaxDriversPerApply = 3
	seedLiveServers(t, svc, 2, 2)

	_, _, err := svc.EnqueueAuthorizedApply(t.Context(), deferredApplyRequest)

	refused, ok := errors.AsType[*DeferredCutoverCapacityError](err)
	require.True(t, ok, "got %v", err)
	assert.Equal(t, DeferredCutoverCapacityError{Database: "testdb", Environment: "staging", Operations: 3, Drivers: 2, LiveServers: 2}, *refused)
	assert.Nil(t, applies.apply, "a refused apply stores nothing")

	seedLiveServers(t, svc, 2, 2, 2)
	_, _, err = svc.EnqueueAuthorizedApply(t.Context(), deferredApplyRequest)
	require.NoError(t, err)
	assert.NotNil(t, applies.apply)
}

// Only the apply's own width is checked: other applies use drivers the way
// any busy fleet does, so a deferred apply that fits the fleet is created
// whatever else is running.
func TestDeferredCutoverIsNotCheckedAgainstOtherApplies(t *testing.T) {
	applies := &capturingApplyStore{}
	svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), &mockTernClient{isRemote: true}, applies)
	svc.config.MaxDriversPerApply = 3
	seedLiveServers(t, svc, 4)

	for range 2 {
		_, _, err := svc.EnqueueAuthorizedApply(t.Context(), deferredApplyRequest)
		require.NoError(t, err)
	}
}

// A deferred apply's waiting operations keep counting against
// max_drivers_per_apply, and its cutover is accepted only once every
// operation waits, so an apply that would hold more operations at once than
// the cap could never be cut over. Apply creation refuses it before reading
// the fleet, naming the cap.
func TestDeferredCutoverWiderThanThePerApplyCapIsRefused(t *testing.T) {
	applies := &capturingApplyStore{}
	svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), &mockTernClient{isRemote: true}, applies)
	seedLiveServers(t, svc, 8)

	_, _, err := svc.EnqueueAuthorizedApply(t.Context(), deferredApplyRequest)

	refused, ok := errors.AsType[*DeferredCutoverCapacityError](err)
	require.True(t, ok, "got %v", err)
	assert.Equal(t, DeferredCutoverCapacityError{Database: "testdb", Environment: "staging", Operations: 3, PerApplyCap: storage.DefaultMaxDriversPerApply}, *refused)
	assert.Nil(t, applies.apply, "a refused apply stores nothing")
}

// The same sharded apply without --defer-cutover is never checked: its
// shards cut over on their own, so it runs in batches of the cap on whatever
// drivers are free, with no server registered at all.
func TestApplyWithoutDeferCutoverIsNotChecked(t *testing.T) {
	applies := &capturingApplyStore{}
	svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), &mockTernClient{isRemote: true}, applies)

	_, _, err := svc.EnqueueAuthorizedApply(t.Context(), ApplyRequest{PlanID: "plan-1", Environment: "staging"})
	require.NoError(t, err)
	assert.NotNil(t, applies.apply)
}

// A deferred apply of one operation is never checked: its one cutover
// command reaches all of it as soon as it waits.
func TestDeferredCutoverOfOneOperationIsNotChecked(t *testing.T) {
	applies := &capturingApplyStore{}
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{isRemote: true}, applies)

	_, _, err := svc.EnqueueAuthorizedApply(t.Context(), deferredApplyRequest)
	require.NoError(t, err)
	assert.NotNil(t, applies.apply)
}

// A registry that cannot be read refuses nothing and admits nothing: apply
// creation fails as a storage failure, never as a capacity refusal.
func TestDeferredCutoverRegistryFailureIsAStorageFailure(t *testing.T) {
	applies := &capturingApplyStore{}
	svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), &mockTernClient{isRemote: true}, applies)
	svc.config.MaxDriversPerApply = 3
	svc.storage.(*mockStorageWithApplyStores).operatorServers.listErr = errors.New("registry unavailable")

	_, _, err := svc.EnqueueAuthorizedApply(t.Context(), deferredApplyRequest)

	_, isStorageErr := errors.AsType[*applyStorageError](err)
	assert.True(t, isStorageErr, "got %v", err)
	_, isRefusal := errors.AsType[*DeferredCutoverCapacityError](err)
	assert.False(t, isRefusal)
	assert.Nil(t, applies.apply)
}

func TestDeferredCutoverCapacityErrorMessages(t *testing.T) {
	for _, tc := range []struct {
		name    string
		refused DeferredCutoverCapacityError
		want    string
	}{
		{
			name:    "wider than the fleet",
			refused: DeferredCutoverCapacityError{Database: "commerce", Environment: "production", Operations: 128, Drivers: 84, LiveServers: 12},
			want:    "commerce/production can't defer its cutover: it would keep 128 drivers waiting until you cut over, and SchemaBot's servers have 84. Apply without --defer-cutover",
		},
		{
			name:    "wider than max_drivers_per_apply",
			refused: DeferredCutoverCapacityError{Database: "commerce", Environment: "production", Operations: 128, PerApplyCap: 2},
			want:    "commerce/production can't defer its cutover: it would keep 128 drivers waiting until you cut over, and one apply can use at most 2 (max_drivers_per_apply). Apply without --defer-cutover, or raise max_drivers_per_apply to 128",
		},
		{
			name:    "no live server",
			refused: DeferredCutoverCapacityError{Database: "commerce", Environment: "production", Operations: 4},
			want:    "commerce/production can't defer its cutover yet: no SchemaBot server has reported its drivers in the last minute. Try again shortly, or apply without --defer-cutover",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.refused.Error())
		})
	}
}

// Only a refusal that can clear on its own, no server having reported its
// drivers yet, answers 409; one that is refused on every retry answers 422.
// Both carry the deferred cutover error code, so a caller can tell a refusal
// of --defer-cutover from any other conflict, and either is a refusal of the
// request, never a server failure.
func TestDeferredCutoverRefusalStatusAndCode(t *testing.T) {
	for _, tc := range []struct {
		name        string
		perApplyCap int
		servers     []int
		status      int
		message     string
	}{
		{name: "no live server", perApplyCap: 3, status: http.StatusConflict,
			message: "apply rejected: testdb/staging can't defer its cutover yet: no SchemaBot server has reported its drivers in the last minute. Try again shortly, or apply without --defer-cutover"},
		{name: "wider than the fleet", perApplyCap: 3, servers: []int{2, 2}, status: http.StatusUnprocessableEntity,
			message: "apply rejected: testdb/staging can't defer its cutover: it would keep 3 drivers waiting until you cut over, and SchemaBot's servers have 2. Apply without --defer-cutover"},
		{name: "wider than max_drivers_per_apply", servers: []int{8}, status: http.StatusUnprocessableEntity,
			message: "apply rejected: testdb/staging can't defer its cutover: it would keep 3 drivers waiting until you cut over, and one apply can use at most 2 (max_drivers_per_apply). Apply without --defer-cutover, or raise max_drivers_per_apply to 3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			applies := &capturingApplyStore{}
			svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), &mockTernClient{isRemote: true}, applies)
			svc.config.MaxDriversPerApply = tc.perApplyCap
			seedLiveServers(t, svc, tc.servers...)

			status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-1","environment":"staging","options":{"defer_cutover":"true"}}`)

			assert.Equal(t, tc.status, status)
			assert.Equal(t, apitypes.ErrCodeDeferredCutoverCapacity, resp.ErrorCode)
			assert.Equal(t, tc.message, resp.Error)
			assert.False(t, apitypes.IsRetryableErrorCode(resp.ErrorCode), "a retry without a change to the request or the plane is refused again")
			assert.Nil(t, applies.apply, "a refused apply stores nothing")

			_, _, err := svc.EnqueueAuthorizedApply(t.Context(), deferredApplyRequest)
			assert.Equal(t, applyMetricStatusRejected, applyMetricStatusForError(err))
		})
	}
}

// The check a caller runs before asking a person to confirm the apply gives
// the same answer apply creation would, without storing anything: a refusal
// for an apply wider than the fleet, and nothing once the fleet fits it or
// when the apply does not defer its cutover.
func TestDeferredCutoverCapacityRefusalAheadOfApplyCreation(t *testing.T) {
	applies := &capturingApplyStore{}
	svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), &mockTernClient{isRemote: true}, applies)
	svc.config.MaxDriversPerApply = 3
	seedLiveServers(t, svc, 2, 2)

	refused, err := svc.DeferredCutoverCapacityRefusal(t.Context(), deferredApplyRequest)
	require.NoError(t, err)
	require.NotNil(t, refused)
	assert.Equal(t, DeferredCutoverCapacityError{Database: "testdb", Environment: "staging", Operations: 3, Drivers: 2, LiveServers: 2}, *refused)

	refused, err = svc.DeferredCutoverCapacityRefusal(t.Context(), ApplyRequest{PlanID: "plan-1", Environment: "staging"})
	require.NoError(t, err)
	assert.Nil(t, refused, "an apply that does not defer is never checked")

	seedLiveServers(t, svc, 2, 2, 2)
	refused, err = svc.DeferredCutoverCapacityRefusal(t.Context(), deferredApplyRequest)
	require.NoError(t, err)
	assert.Nil(t, refused)
	assert.Nil(t, applies.apply, "the check stores nothing")
}

// The check ahead of apply creation returns an error only when it cannot read
// what it needs. A registry that cannot be read is that error; a plan that is
// not stored is left to apply creation, which says so in its own words.
func TestDeferredCutoverCapacityRefusalAheadOfApplyCreationErrors(t *testing.T) {
	svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), &mockTernClient{isRemote: true}, &capturingApplyStore{})
	svc.config.MaxDriversPerApply = 3
	svc.storage.(*mockStorageWithApplyStores).operatorServers.listErr = errors.New("registry unavailable")

	refused, err := svc.DeferredCutoverCapacityRefusal(t.Context(), deferredApplyRequest)
	require.ErrorContains(t, err, "registry unavailable")
	assert.Nil(t, refused)

	missing, _ := newQueueApplyTestService(nil, &mockTernClient{isRemote: true}, &capturingApplyStore{})
	refused, err = missing.DeferredCutoverCapacityRefusal(t.Context(), deferredApplyRequest)
	require.NoError(t, err)
	assert.Nil(t, refused)
}

// A running operator records its drivers in the registry before any driver
// claims, and removes its row when it stops, so deferred cutover admission counts exactly the servers running
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

// A heartbeat whose prune fails still records the server, so its drivers keep
// counting toward deferred cutover admission.
func TestOperatorHeartbeatSurvivesAFailedPrune(t *testing.T) {
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, &capturingApplyStore{})
	registry := &svc.storage.(*mockStorageWithApplyStores).operatorServers
	registry.pruneErr = errors.New("prune failed")

	svc.heartbeatOperatorServer(t.Context(), &storage.OperatorServer{ServerID: "host-a/1/operator", Drivers: 4})

	live, err := registry.ListLive(t.Context())
	require.NoError(t, err)
	require.Len(t, live, 1)
	assert.Equal(t, 4, live[0].Drivers)
	assert.Equal(t, 1, registry.prunes)
}
