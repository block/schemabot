package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// seedLiveServers records one live driver pool per entry, each running
// that many drivers.
func seedLiveServers(t *testing.T, svc *Service, drivers ...int) {
	t.Helper()
	pools := &svc.storage.(*mockStorageWithApplyStores).driverPools
	for i, n := range drivers {
		require.NoError(t, pools.Heartbeat(t.Context(), &storage.DriverPool{ServerID: fmt.Sprintf("host-%d/1/operator", i), Drivers: n}))
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
	svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), ampleDataPlane(), applies)
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
	svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), ampleDataPlane(), applies)
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
	svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), ampleDataPlane(), applies)
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
	svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), ampleDataPlane(), applies)

	_, _, err := svc.EnqueueAuthorizedApply(t.Context(), ApplyRequest{PlanID: "plan-1", Environment: "staging"})
	require.NoError(t, err)
	assert.NotNil(t, applies.apply)
}

// A deferred apply of one operation is never checked: its one cutover
// command reaches all of it as soon as it waits.
func TestDeferredCutoverOfOneOperationIsNotChecked(t *testing.T) {
	applies := &capturingApplyStore{}
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), ampleDataPlane(), applies)

	_, _, err := svc.EnqueueAuthorizedApply(t.Context(), deferredApplyRequest)
	require.NoError(t, err)
	assert.NotNil(t, applies.apply)
}

// Driver pools that cannot be read refuse nothing and admits nothing: apply
// creation fails as a storage failure, never as a capacity refusal.
func TestDeferredCutoverDriverPoolReadFailureIsAStorageFailure(t *testing.T) {
	applies := &capturingApplyStore{}
	svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), ampleDataPlane(), applies)
	svc.config.MaxDriversPerApply = 3
	svc.storage.(*mockStorageWithApplyStores).driverPools.listErr = errors.New("driver pools unavailable")

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
			want:    "commerce/production can't defer its cutover: it would keep 128 drivers waiting for the cutover, and SchemaBot's servers have 84 drivers that can wait. To defer it, give the servers 44 more by raising drivers or running more servers; otherwise apply without --defer-cutover",
		},
		{
			name:    "wider than max_drivers_per_apply",
			refused: DeferredCutoverCapacityError{Database: "commerce", Environment: "production", Operations: 128, PerApplyCap: 2},
			want:    "commerce/production can't defer its cutover: it would keep 128 drivers waiting for the cutover, and one apply can use at most 2 (max_drivers_per_apply). To defer it, raise max_drivers_per_apply to 128; otherwise apply without --defer-cutover",
		},
		{
			name:    "no live server",
			refused: DeferredCutoverCapacityError{Database: "commerce", Environment: "production", Operations: 4},
			want:    "commerce/production can't defer its cutover: no SchemaBot server running drivers has checked in during the last minute. Apply again once SchemaBot's drivers are running, or apply without --defer-cutover",
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
			message: "apply rejected: testdb/staging can't defer its cutover: no SchemaBot server running drivers has checked in during the last minute. Apply again once SchemaBot's drivers are running, or apply without --defer-cutover"},
		{name: "wider than the fleet", perApplyCap: 3, servers: []int{2, 2}, status: http.StatusUnprocessableEntity,
			message: "apply rejected: testdb/staging can't defer its cutover: it would keep 3 drivers waiting for the cutover, and SchemaBot's servers have 2 drivers that can wait. To defer it, give the servers 1 more by raising drivers or running more servers; otherwise apply without --defer-cutover"},
		{name: "wider than max_drivers_per_apply", servers: []int{8}, status: http.StatusUnprocessableEntity,
			message: "apply rejected: testdb/staging can't defer its cutover: it would keep 3 drivers waiting for the cutover, and one apply can use at most 2 (max_drivers_per_apply). To defer it, raise max_drivers_per_apply to 3; otherwise apply without --defer-cutover"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			applies := &capturingApplyStore{}
			svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), ampleDataPlane(), applies)
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
	svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), ampleDataPlane(), applies)
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
// what it needs. A failed read of the driver pools is that error; a plan that is
// not stored is left to apply creation, which says so in its own words.
func TestDeferredCutoverCapacityRefusalAheadOfApplyCreationErrors(t *testing.T) {
	svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), ampleDataPlane(), &capturingApplyStore{})
	svc.config.MaxDriversPerApply = 3
	svc.storage.(*mockStorageWithApplyStores).driverPools.listErr = errors.New("driver pools unavailable")

	refused, err := svc.DeferredCutoverCapacityRefusal(t.Context(), deferredApplyRequest)
	require.ErrorContains(t, err, "driver pools unavailable")
	assert.Nil(t, refused)

	missing, _ := newQueueApplyTestService(nil, ampleDataPlane(), &capturingApplyStore{})
	refused, err = missing.DeferredCutoverCapacityRefusal(t.Context(), deferredApplyRequest)
	require.NoError(t, err)
	assert.Nil(t, refused)
}

// A running operator records its drivers in driver_pools before any driver
// claims, and removes its row when it stops, so deferred cutover admission counts exactly the servers running
// now. A restarted operator registers as a new run.
func TestOperatorRegistersItsDriversWhileRunning(t *testing.T) {
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, &capturingApplyStore{})
	svc.config.Drivers = 4
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))
	pools := &svc.storage.(*mockStorageWithApplyStores).driverPools

	svc.StartOperator(t.Context())
	t.Cleanup(svc.StopOperator)

	live, err := pools.ListLive(t.Context())
	require.NoError(t, err)
	require.Len(t, live, 1)
	first := live[0].ServerID
	assert.Contains(t, first, storage.LeaseOwnerProcess()+"/operator-")
	assert.Equal(t, 4, live[0].Drivers)

	svc.StopOperator()
	live, err = pools.ListLive(t.Context())
	require.NoError(t, err)
	assert.Empty(t, live)

	svc.StartOperator(t.Context())
	live, err = pools.ListLive(t.Context())
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
	pools := &svc.storage.(*mockStorageWithApplyStores).driverPools

	svc.StartOperator(t.Context())
	t.Cleanup(svc.StopOperator)

	require.Eventually(t, func() bool {
		pools.mu.Lock()
		defer pools.mu.Unlock()
		return pools.beats >= 3
	}, drivePassesDeadline, 5*time.Millisecond)
}

// Storage that stops answering does not hold the operator's startup: the
// first heartbeat gives up at its own deadline, StartOperator returns, and
// the drivers start without the server's row.
func TestOperatorStartupHeartbeatGivesUpAtItsDeadline(t *testing.T) {
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, &capturingApplyStore{})
	svc.config.Drivers = 2
	svc.operatorHeartbeatDeadline = 10 * time.Millisecond
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))
	pools := &svc.storage.(*mockStorageWithApplyStores).driverPools
	pools.hang = true

	started := make(chan struct{})
	go func() {
		svc.StartOperator(t.Context())
		close(started)
	}()
	select {
	case <-started:
	case <-time.After(drivePassesDeadline):
		require.FailNow(t, "StartOperator did not return while storage stopped answering")
	}
	t.Cleanup(svc.StopOperator)

	live, err := pools.ListLive(t.Context())
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
	pools := &svc.storage.(*mockStorageWithApplyStores).driverPools
	pools.pruneErr = errors.New("prune failed")

	svc.heartbeatDriverPool(t.Context(), &storage.DriverPool{ServerID: "host-a/1/operator", Drivers: 4})

	live, err := pools.ListLive(t.Context())
	require.NoError(t, err)
	require.Len(t, live, 1)
	assert.Equal(t, 4, live[0].Drivers)
	assert.Equal(t, 1, pools.prunes)
}

// capacityReportingTernClient is a remote deployment that reports its
// drivers, or fails to read them with err.
type capacityReportingTernClient struct {
	*mockTernClient
	capacity *tern.DriverCapacity
	err      error
}

func (c *capacityReportingTernClient) DriverCapacity(context.Context) (*tern.DriverCapacity, error) {
	return c.capacity, c.err
}

// ampleDataPlane is a remote deployment with drivers for any test apply.
func ampleDataPlane() *capacityReportingTernClient {
	return &capacityReportingTernClient{
		mockTernClient: &mockTernClient{isRemote: true},
		capacity:       &tern.DriverCapacity{LiveServers: 4, Drivers: 40, DeferredCutoverDrivers: 36, MaxDriversPerApply: 16},
	}
}

// A deployment whose operations run on its own data plane holds one of that
// plane's drivers for each operation waiting at the cutover too, so a deferred
// apply that fits this plane is still refused when it would not fit that one.
// The refusal names the deployment, and stores nothing. Only a plane with no
// live server answers 409, since a server reporting in clears it; every other
// refusal answers 422. A deployment that does not report its drivers, as one
// running an older SchemaBot does not, is refused rather than admitted
// unchecked.
func TestDeferredCutoverIsRefusedByTheDataPlane(t *testing.T) {
	for _, tc := range []struct {
		name    string
		client  tern.Client
		refused DeferredCutoverCapacityError
		status  int
		message string
	}{
		{
			name: "wider than the data plane's max_drivers_per_apply",
			client: &capacityReportingTernClient{
				mockTernClient: &mockTernClient{isRemote: true},
				capacity:       &tern.DriverCapacity{LiveServers: 4, Drivers: 40, DeferredCutoverDrivers: 36, MaxDriversPerApply: 2},
			},
			refused: DeferredCutoverCapacityError{Database: "testdb", Environment: "staging", Deployment: "default", Operations: 3, PerApplyCap: 2},
			status:  http.StatusUnprocessableEntity,
			message: "apply rejected: testdb/staging can't defer its cutover: it would keep 3 drivers waiting for the cutover on the default deployment, and one apply can use at most 2 there (max_drivers_per_apply). To defer it, raise max_drivers_per_apply on that deployment to 3; otherwise apply without --defer-cutover",
		},
		{
			name: "wider than the data plane's drivers",
			client: &capacityReportingTernClient{
				mockTernClient: &mockTernClient{isRemote: true},
				capacity:       &tern.DriverCapacity{LiveServers: 2, Drivers: 4, DeferredCutoverDrivers: 2, MaxDriversPerApply: 16},
			},
			refused: DeferredCutoverCapacityError{Database: "testdb", Environment: "staging", Deployment: "default", Operations: 3, Drivers: 2, LiveServers: 2},
			status:  http.StatusUnprocessableEntity,
			message: "apply rejected: testdb/staging can't defer its cutover: it would keep 3 drivers waiting for the cutover on the default deployment, and its servers have 2 drivers that can wait. To defer it, give that deployment's servers 1 more by raising drivers or running more servers; otherwise apply without --defer-cutover",
		},
		{
			name: "no live server on the data plane",
			client: &capacityReportingTernClient{
				mockTernClient: &mockTernClient{isRemote: true},
				capacity:       &tern.DriverCapacity{MaxDriversPerApply: 16},
			},
			refused: DeferredCutoverCapacityError{Database: "testdb", Environment: "staging", Deployment: "default", Operations: 3},
			status:  http.StatusConflict,
			message: "apply rejected: testdb/staging can't defer its cutover: no SchemaBot server running drivers on the default deployment has checked in during the last minute. Apply again once its drivers are running, or apply without --defer-cutover",
		},
		{
			name:    "deployment does not report its drivers",
			client:  &mockTernClient{isRemote: true},
			refused: DeferredCutoverCapacityError{Database: "testdb", Environment: "staging", Deployment: "default", Unreported: true, Operations: 3},
			status:  http.StatusUnprocessableEntity,
			message: "apply rejected: testdb/staging can't defer its cutover: the default deployment doesn't report its drivers. To defer it, upgrade SchemaBot there; otherwise apply without --defer-cutover",
		},
		{
			name: "deployment reports its drivers without its max_drivers_per_apply",
			client: &capacityReportingTernClient{
				mockTernClient: &mockTernClient{isRemote: true},
				capacity:       &tern.DriverCapacity{LiveServers: 4, Drivers: 40, DeferredCutoverDrivers: 36},
			},
			refused: DeferredCutoverCapacityError{Database: "testdb", Environment: "staging", Deployment: "default", Unreported: true, Operations: 3},
			status:  http.StatusUnprocessableEntity,
			message: "apply rejected: testdb/staging can't defer its cutover: the default deployment doesn't report its drivers. To defer it, upgrade SchemaBot there; otherwise apply without --defer-cutover",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			applies := &capturingApplyStore{}
			svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), tc.client, applies)
			svc.config.MaxDriversPerApply = 3
			seedLiveServers(t, svc, 4)

			_, _, err := svc.EnqueueAuthorizedApply(t.Context(), deferredApplyRequest)
			refused, ok := errors.AsType[*DeferredCutoverCapacityError](err)
			require.True(t, ok, "got %v", err)
			assert.Equal(t, tc.refused, *refused)
			assert.Nil(t, applies.apply, "a refused apply stores nothing")

			ahead, err := svc.DeferredCutoverCapacityRefusal(t.Context(), deferredApplyRequest)
			require.NoError(t, err)
			require.NotNil(t, ahead, "the check before confirmation refuses it too")
			assert.Equal(t, tc.refused, *ahead)

			status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-1","environment":"staging","options":{"defer_cutover":"true"}}`)
			assert.Equal(t, tc.status, status)
			assert.Equal(t, apitypes.ErrCodeDeferredCutoverCapacity, resp.ErrorCode)
			assert.Equal(t, tc.message, resp.Error)
		})
	}
}

// A deployment whose operations run on this plane's own drivers is checked
// only against this plane: it is not asked for its drivers.
func TestDeferredCutoverOnThisPlaneDoesNotAskTheDeployment(t *testing.T) {
	applies := &capturingApplyStore{}
	client := &capacityReportingTernClient{mockTernClient: &mockTernClient{}, err: errors.New("asked")}
	svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), client, applies)
	svc.config.MaxDriversPerApply = 3
	seedLiveServers(t, svc, 4)

	_, _, err := svc.EnqueueAuthorizedApply(t.Context(), deferredApplyRequest)

	require.NoError(t, err)
	assert.NotNil(t, applies.apply)
}

// A data plane whose drivers cannot be read refuses the deferred apply with
// that error rather than admitting it unchecked, and stores nothing.
func TestDeferredCutoverIsRefusedWhenTheDataPlaneCannotBeRead(t *testing.T) {
	applies := &capturingApplyStore{}
	client := &capacityReportingTernClient{
		mockTernClient: &mockTernClient{isRemote: true},
		err:            errors.New("read driver capacity: rpc error: code = Unavailable"),
	}
	svc, _ := newQueueApplyTestService(threeShardQueueApplyTestPlan(), client, applies)
	svc.config.MaxDriversPerApply = 3
	seedLiveServers(t, svc, 4)

	_, _, err := svc.EnqueueAuthorizedApply(t.Context(), deferredApplyRequest)

	require.ErrorIs(t, err, client.err)
	assert.ErrorContains(t, err, "read driver capacity of deployment default/staging")
	_, isRefusal := errors.AsType[*DeferredCutoverCapacityError](err)
	assert.False(t, isRefusal, "an unread plane is an error, never a refusal")
	assert.Nil(t, applies.apply, "nothing is stored")
}
