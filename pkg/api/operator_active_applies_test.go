package api

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// installManualMeterReader points the global meter provider at an in-memory
// reader for the duration of a test so the test can assert metric deltas, then
// restores the previous provider.
func installManualMeterReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		require.NoError(t, mp.Shutdown(t.Context()))
	})
	return reader
}

// activeAppliesValue returns the schemabot.active_applies up/down counter value
// for a database/deployment/environment series, and whether such a series exists.
func activeAppliesValue(t *testing.T, reader *sdkmetric.ManualReader, database, deployment, environment string) (int64, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.active_applies" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "active_applies should be a Sum[int64] up/down counter")
			for _, dp := range sum.DataPoints {
				if attrEquals(dp.Attributes, "database", database) &&
					attrEquals(dp.Attributes, "deployment", deployment) &&
					attrEquals(dp.Attributes, "environment", environment) {
					return dp.Value, true
				}
			}
		}
	}
	return 0, false
}

func attrEquals(set attribute.Set, key, want string) bool {
	v, ok := set.Value(attribute.Key(key))
	return ok && v.AsString() == want
}

// The enqueue-time active-apply increment is keyed to a fan-out apply's primary
// deployment, and operation-lease-only drives (multi-operation rollouts, and
// deployment-keyed applies whose manifest still expects unattached operations)
// suppress the parent-level metric, so the operator projection that wins the
// parent state transition owns releasing it: it decrements once when such a
// rollout first reaches a terminal state, re-increments if a continuable
// failure reopens the parent, and leaves legacy single-operation applies
// (which still decrement in their direct parent-lease drive) untouched.
func TestUpdateApplyStateFromOperations_ActiveAppliesAccounting(t *testing.T) {
	const (
		database    = "testdb"
		deployment  = "eu"
		environment = "production"
	)

	newApply := func(parentState string) *storage.Apply {
		return &storage.Apply{
			ID:              42,
			ApplyIdentifier: "apply-active",
			State:           parentState,
			Database:        database,
			Deployment:      deployment,
			Environment:     environment,
		}
	}

	t.Run("multi-operation rollout decrements once on terminal", func(t *testing.T) {
		reader := installManualMeterReader(t)
		applyStore := &recordingApplyStore{swapped: true}
		svc := newOperatorStateTestService(&listingApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, State: state.ApplyOperation.Completed},
			{ID: 2, State: state.ApplyOperation.Completed},
		}}, applyStore)

		_, err := svc.updateApplyStateFromOperations(t.Context(), 1, newApply(state.Apply.Running), allowLeaseScopedFailedReopen)
		require.NoError(t, err)
		require.NotNil(t, applyStore.updated)
		assert.Equal(t, state.Apply.Completed, applyStore.updated.State)

		value, ok := activeAppliesValue(t, reader, database, deployment, environment)
		require.True(t, ok, "terminalizing a multi-op rollout must adjust the active-apply gauge")
		assert.Equal(t, int64(-1), value)
	})

	t.Run("single-operation rollout leaves the gauge to the direct drive", func(t *testing.T) {
		reader := installManualMeterReader(t)
		applyStore := &recordingApplyStore{swapped: true}
		svc := newOperatorStateTestService(&listingApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, State: state.ApplyOperation.Completed},
		}}, applyStore)

		_, err := svc.updateApplyStateFromOperations(t.Context(), 1, newApply(state.Apply.Running), allowLeaseScopedFailedReopen)
		require.NoError(t, err)

		_, ok := activeAppliesValue(t, reader, database, deployment, environment)
		assert.False(t, ok, "single-op projection must not touch the gauge; its drive owns the decrement")
	})

	t.Run("keyed apply with unattached manifest operations decrements on terminal failure", func(t *testing.T) {
		// A deployment-keyed apply drives under the operation lease even while
		// only one operation has attached, so the drive suppresses the
		// parent-level gauge; when the lone attached operation fails the
		// generation permanently, the projection that terminalizes the parent
		// must release the gauge or the apply counts as in flight forever.
		reader := installManualMeterReader(t)
		applyStore := &recordingApplyStore{swapped: true}
		svc := newOperatorStateTestService(&listingApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, OperationKey: "ns/-80/users", State: state.ApplyOperation.Failed},
		}}, applyStore)

		apply := newApply(state.Apply.Running)
		apply.ExpectedOperationKeys = []string{"ns/-80/users", "ns/80-/users"}

		_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
		require.NoError(t, err)
		require.NotNil(t, applyStore.updated)
		assert.Equal(t, state.Apply.Failed, applyStore.updated.State)

		value, ok := activeAppliesValue(t, reader, database, deployment, environment)
		require.True(t, ok, "terminalizing a keyed apply driven under the operation lease must adjust the gauge")
		assert.Equal(t, int64(-1), value)
	})

	t.Run("fully attached keyed single-operation apply leaves the gauge to the direct drive", func(t *testing.T) {
		// Once every manifest key has attached, a single-operation keyed apply
		// takes the legacy parent-lease drive, which releases the gauge itself.
		reader := installManualMeterReader(t)
		applyStore := &recordingApplyStore{swapped: true}
		svc := newOperatorStateTestService(&listingApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, OperationKey: "ns/-80/users", State: state.ApplyOperation.Completed},
		}}, applyStore)

		apply := newApply(state.Apply.Running)
		apply.ExpectedOperationKeys = []string{"ns/-80/users"}

		_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
		require.NoError(t, err)

		_, ok := activeAppliesValue(t, reader, database, deployment, environment)
		assert.False(t, ok, "a fully attached keyed single-op projection must not touch the gauge; its drive owns the decrement")
	})

	t.Run("lost projection race does not adjust the gauge", func(t *testing.T) {
		reader := installManualMeterReader(t)
		applyStore := &recordingApplyStore{swapped: false}
		svc := newOperatorStateTestService(&listingApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, State: state.ApplyOperation.Completed},
			{ID: 2, State: state.ApplyOperation.Completed},
		}}, applyStore)

		_, err := svc.updateApplyStateFromOperations(t.Context(), 1, newApply(state.Apply.Running), allowLeaseScopedFailedReopen)
		require.NoError(t, err)

		_, ok := activeAppliesValue(t, reader, database, deployment, environment)
		assert.False(t, ok, "a CAS that loses the race must not adjust the gauge")
	})

	t.Run("a stopped multi-operation rollout stays counted until it settles", func(t *testing.T) {
		// A stopped rollout is still in progress: it waits on an operator's
		// start or cancel. Stopping it leaves the gauge alone, resuming it
		// re-counts nothing, and the cancel that settles it releases it once.
		reader := installManualMeterReader(t)
		stopOps := &listingApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, State: state.ApplyOperation.Stopped},
			{ID: 2, State: state.ApplyOperation.Stopped},
		}}
		stopStore := &recordingApplyStore{swapped: true}
		_, err := newOperatorStateTestService(stopOps, stopStore).updateApplyStateFromOperations(t.Context(), 1, newApply(state.Apply.Running), allowLeaseScopedFailedReopen)
		require.NoError(t, err)
		require.NotNil(t, stopStore.updated)
		require.Equal(t, state.Apply.Stopped, stopStore.updated.State)
		_, ok := activeAppliesValue(t, reader, database, deployment, environment)
		assert.False(t, ok, "stopping a rollout must leave it counted")

		cancelOps := &listingApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, State: state.ApplyOperation.Cancelled},
			{ID: 2, State: state.ApplyOperation.Cancelled},
		}}
		cancelStore := &recordingApplyStore{swapped: true}
		_, err = newOperatorStateTestService(cancelOps, cancelStore).updateApplyStateFromOperations(t.Context(), 1, newApply(state.Apply.Stopped), allowLeaseScopedFailedReopen)
		require.NoError(t, err)
		require.NotNil(t, cancelStore.updated)
		require.Equal(t, state.Apply.Cancelled, cancelStore.updated.State)
		value, ok := activeAppliesValue(t, reader, database, deployment, environment)
		require.True(t, ok, "the cancel that settles a stopped rollout must release it")
		assert.Equal(t, int64(-1), value)
	})

	t.Run("a multi-operation rollout's retry pause is released and its retry counts it again", func(t *testing.T) {
		// The operation drives leave the gauge to the projection, so the
		// projection is the one owner of both halves of a retry: the pause
		// releases the rollout, and the projection that takes it out of the
		// pause counts it again.
		reader := installManualMeterReader(t)
		pauseStore := &recordingApplyStore{swapped: true}
		_, err := newOperatorStateTestService(&listingApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, State: state.ApplyOperation.FailedRetryable},
			{ID: 2, State: state.ApplyOperation.Completed},
		}}, pauseStore).updateApplyStateFromOperations(t.Context(), 1, newApply(state.Apply.Running), allowLeaseScopedFailedReopen)
		require.NoError(t, err)
		require.NotNil(t, pauseStore.updated)
		require.Equal(t, state.Apply.FailedRetryable, pauseStore.updated.State)
		value, ok := activeAppliesValue(t, reader, database, deployment, environment)
		require.True(t, ok, "pausing a rollout for a retry must release it")
		assert.Equal(t, int64(-1), value)

		retryStore := &recordingApplyStore{swapped: true}
		_, err = newOperatorStateTestService(&listingApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, State: state.ApplyOperation.Running},
			{ID: 2, State: state.ApplyOperation.Completed},
		}}, retryStore).updateApplyStateFromOperations(t.Context(), 1, newApply(state.Apply.FailedRetryable), allowLeaseScopedFailedReopen)
		require.NoError(t, err)
		require.NotNil(t, retryStore.updated)
		require.Equal(t, state.Apply.Running, retryStore.updated.State)
		value, _ = activeAppliesValue(t, reader, database, deployment, environment)
		assert.Equal(t, int64(0), value, "the retry that takes the rollout out of its pause must count it again")
	})

	t.Run("continuable reopen re-increments the gauge", func(t *testing.T) {
		reader := installManualMeterReader(t)
		applyStore := &recordingApplyStore{swapped: true}
		svc := newOperatorStateTestService(&listingApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureContinue},
			{ID: 2, State: state.ApplyOperation.Running, OnFailure: storage.OnFailureContinue},
		}}, applyStore)

		_, err := svc.updateApplyStateFromOperations(t.Context(), 1, newApply(state.Apply.Failed), allowLeaseScopedFailedReopen)
		require.NoError(t, err)
		require.NotNil(t, applyStore.updated)
		assert.Equal(t, state.Apply.RunningDegraded, applyStore.updated.State)

		value, ok := activeAppliesValue(t, reader, database, deployment, environment)
		require.True(t, ok, "reopening a failed rollout must re-acquire the active-apply gauge")
		assert.Equal(t, int64(1), value)
	})
}

// A drive that claims a retryable pause counts the apply again, except when the
// rollout projection owns the gauge: the operation drives of a paused rollout
// would otherwise each count it again, on top of the projection doing so when
// the parent leaves its pause.
func TestResumeClaimedApplyCountsARetryClaimOnlyWhenItOwnsTheGauge(t *testing.T) {
	for _, tc := range []struct {
		name                string
		projectionOwnsGauge bool
		wantSeries          bool
	}{
		{name: "a direct drive counts its retry claim", wantSeries: true},
		{name: "a drive under a projection-owned gauge leaves it alone", projectionOwnsGauge: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := installManualMeterReader(t)
			svc := New(&mockStorageWithApplyStores{plans: &staticPlanStore{}, applies: &staticApplyStore{}}, &ServerConfig{},
				map[string]tern.Client{"eu/production": &mockTernClient{}}, slog.New(slog.DiscardHandler))
			apply := &storage.Apply{
				ID: 42, ApplyIdentifier: "apply-retry-claim", Database: "testdb",
				Deployment: "eu", Environment: "production", State: state.Apply.FailedRetryable,
			}

			resumed, err := svc.resumeClaimedApplyWithOptions(t.Context(), 1, apply, 0, "",
				resumeClaimedApplyOptions{suppressRecoveredObserver: true, projectionOwnsGauge: tc.projectionOwnsGauge})

			require.NoError(t, err)
			require.True(t, resumed)
			value, ok := activeAppliesValue(t, reader, "testdb", "eu", "production")
			if !tc.wantSeries {
				assert.False(t, ok, "the claim must leave the gauge to the projection")
				return
			}
			require.True(t, ok)
			assert.Equal(t, int64(1), value)
		})
	}
}

// twoOperationStore serves the claimed operation through recoverOperationStore
// and lists a sibling beside it, so the apply reads as a multi-operation
// rollout.
type twoOperationStore struct {
	recoverOperationStore
	sibling storage.ApplyOperation
}

func (s *twoOperationStore) ListByApply(ctx context.Context, applyID int64) ([]*storage.ApplyOperation, error) {
	ops, err := s.recoverOperationStore.ListByApply(ctx, applyID)
	if err != nil {
		return nil, err
	}
	sibling := s.sibling
	return append(ops, &sibling), nil
}

// An operation drive that claims a multi-operation rollout paused for a retry
// counts the rollout once: the projection that takes the parent out of its
// pause counts it, and the drive's own retry claim leaves the gauge to that
// projection.
func TestRecoverMultiApplyOperation_PausedRolloutClaimCountsTheApplyOnce(t *testing.T) {
	reader := installManualMeterReader(t)
	applyStore := &casApplyStore{
		template: storage.Apply{
			ID: 7, ApplyIdentifier: "apply-paused-rollout", Database: "testdb",
			DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging", Deployment: "west",
		},
		state: state.Apply.FailedRetryable,
	}
	claimed := storage.ApplyOperation{ID: 42, ApplyID: 7, Deployment: "west", State: state.ApplyOperation.Running}
	opStore := &twoOperationStore{
		recoverOperationStore: recoverOperationStore{op: &claimed},
		sibling:               storage.ApplyOperation{ID: 43, ApplyID: 7, Deployment: "east", State: state.ApplyOperation.Pending},
	}
	svc := New(
		&recoverTestStorage{
			applies: applyStore, ops: opStore, control: &fakeControlRequestStore{},
			tasks: &stubTaskStore{tasks: []*storage.Task{{ID: 1, ApplyID: 7, TaskIdentifier: "task-1", State: state.Task.Running}}},
		},
		testServerConfig(),
		map[string]tern.Client{"west/staging": &mockTernClient{}},
		slog.New(slog.DiscardHandler),
	)

	op := claimed
	svc.recoverMultiApplyOperation(t.Context(), 1, &op, storage.OperationLease{})

	assert.Equal(t, state.Apply.Running, applyStore.currentState())
	value, ok := activeAppliesValue(t, reader, "testdb", "west", "staging")
	require.True(t, ok, "taking the rollout out of its pause must count it again")
	assert.Equal(t, int64(1), value, "the paused rollout is counted once when its retry claims it")
}

// projectionRepairApplyStore hands the operation-projection repair one stranded
// parent, with the lease the claim stamps on it.
type projectionRepairApplyStore struct {
	casApplyStore
}

func (s *projectionRepairApplyStore) FindNextApplyForOperationProjection(ctx context.Context, owner string) (*storage.Apply, error) {
	apply, err := s.Get(ctx, s.template.ID)
	if err != nil {
		return nil, err
	}
	apply.LeaseOwner = owner
	apply.LeaseToken = "repair-lease-token"
	return apply, nil
}

// A single-operation apply whose drive settled its operation but lost the
// apply lease before storing the parent outcome (a task-less cancel deferred to
// the projection, for one) is settled by the operation-projection repair. No
// drive released that apply, so the repair releases it when it settles the
// parent, and keeps a stopped parent counted.
func TestRecoverApplyOperationProjection_ReleasesASingleOperationApplyItSettles(t *testing.T) {
	tests := []struct {
		name        string
		opState     string
		wantState   string
		wantRelease bool
	}{
		{name: "a cancelled operation settles and releases the apply", opState: state.ApplyOperation.Cancelled, wantState: state.Apply.Cancelled, wantRelease: true},
		{name: "a completed operation settles and releases the apply", opState: state.ApplyOperation.Completed, wantState: state.Apply.Completed, wantRelease: true},
		{name: "a stopped operation keeps the apply counted", opState: state.ApplyOperation.Stopped, wantState: state.Apply.Stopped},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := installManualMeterReader(t)
			applyStore := &projectionRepairApplyStore{casApplyStore: casApplyStore{
				template: storage.Apply{
					ID: 7, ApplyIdentifier: "apply-stranded", Database: "testdb",
					DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging", Deployment: "west",
				},
				state: state.Apply.Running,
			}}
			opStore := &recoverOperationStore{op: &storage.ApplyOperation{ID: 42, ApplyID: 7, Deployment: "west", State: tt.opState}}
			svc := New(
				&recoverTestStorage{applies: applyStore, ops: opStore, control: &fakeControlRequestStore{}, tasks: &stubTaskStore{}},
				testServerConfig(), nil, slog.New(slog.DiscardHandler),
			)

			require.True(t, svc.recoverApplyOperationProjection(t.Context(), 1, "driver-1"))

			assert.Equal(t, tt.wantState, applyStore.currentState())
			value, ok := activeAppliesValue(t, reader, "testdb", "west", "staging")
			if !tt.wantRelease {
				assert.False(t, ok, "a stopped apply stays counted")
				return
			}
			require.True(t, ok, "settling a stranded apply must release it")
			assert.Equal(t, int64(-1), value)
		})
	}
}
