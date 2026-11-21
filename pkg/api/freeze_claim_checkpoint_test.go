package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/clock"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// newClaimCheckpointTestService is a service whose stop freezes and override
// requests are in memory, read by a database clock at freezeTestNow.
func newClaimCheckpointTestService(t *testing.T, freezes *memoryFreezeStore, overrides *memoryFreezeOverrideStore, controls ...*storage.ApplyControlRequest) *Service {
	t.Helper()
	controlStore := &memoryControlRequestStore{}
	for _, request := range controls {
		_, _, err := controlStore.RequestPending(t.Context(), request)
		require.NoError(t, err)
	}
	stores := &mockStorageWithApplyStores{freezes: freezes, controls: controlStore}
	if overrides != nil {
		stores.overrides = overrides
	}
	return New(stores, testServerConfig(), nil, slog.New(slog.DiscardHandler))
}

func claimCheckpointApply() *storage.Apply {
	return &storage.Apply{ID: 7, ApplyIdentifier: "apply-claim", Database: "testdb", Environment: "staging", Deployment: DefaultDeployment, State: state.Apply.Pending}
}

func stopFreeze(id string) *storage.EnvironmentFreeze {
	freeze := gateTestFreeze(id)
	freeze.Running = storage.FreezeRunningStop
	return freeze
}

// At claim only freezes with running "stop" act, and only one that covers
// the apply holds it. A continue freeze already refused the change at
// creation and start, so it does not act again here.
func TestStopFreezesHoldingApply(t *testing.T) {
	otherDatabase := stopFreeze("freeze-orders")
	otherDatabase.Databases = []string{"orders"}
	tests := []struct {
		name    string
		freezes []*storage.EnvironmentFreeze
		want    []string
	}{
		{"None", nil, nil},
		{"ContinueDoesNotAct", []*storage.EnvironmentFreeze{gateTestFreeze("freeze-continue")}, nil},
		{"StopHolds", []*storage.EnvironmentFreeze{gateTestFreeze("freeze-continue"), stopFreeze("freeze-stop")}, []string{"freeze-stop"}},
		{"StopOfAnotherDatabaseDoesNotHold", []*storage.EnvironmentFreeze{otherDatabase}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := newClaimCheckpointTestService(t, newGateFreezeStore(t, tc.freezes...), nil)

			holding, err := svc.stopFreezesHoldingApply(t.Context(), claimCheckpointApply())

			require.NoError(t, err)
			var ids []string
			for _, freeze := range holding {
				ids = append(ids, freeze.Identifier)
			}
			assert.Equal(t, tc.want, ids)
		})
	}
}

// An apply admitted on an approved override that names the stop freeze, at
// creation or at its start, runs. An override that names another freeze, one
// for another apply, or an approval its pending start was not admitted on,
// such as another requester's, does not let it through.
func TestStopFreezesHoldingApplyHonorsApprovedOverrides(t *testing.T) {
	approved := func(id, action, target string, blockers ...string) *storage.FreezeOverrideRequest {
		at := freezeTestNow.Add(-time.Minute)
		return &storage.FreezeOverrideRequest{
			Identifier: id, Environment: "staging", Action: action, TargetID: target, Database: "testdb",
			Blockers: blockers, Reason: "SEV-1234 fix forward", RequestedBy: "alice", ApprovedBy: "bob", ApprovedAt: &at,
			ExpiresAt: freezeTestNow.Add(time.Hour),
		}
	}
	tests := []struct {
		name          string
		request       *storage.FreezeOverrideRequest
		creationNamed string
		startNamed    string
		wantHeld      bool
	}{
		{"CreationOverride", approved("ovr-create", storage.FreezeOverrideActionApply, "testdb", "freeze freeze-stop"), "ovr-create", "", false},
		{"StartOverride", approved("ovr-start", storage.FreezeOverrideActionStart, "apply-claim", "freeze freeze-stop"), "", "ovr-start", false},
		{"StartNotAdmittedOnTheApproval", approved("ovr-start", storage.FreezeOverrideActionStart, "apply-claim", "freeze freeze-stop"), "", "", true},
		{"OverrideOfAnotherFreeze", approved("ovr-other", storage.FreezeOverrideActionStart, "apply-claim", "freeze freeze-old"), "", "ovr-other", true},
		{"OverrideOfAnotherApply", approved("ovr-elsewhere", storage.FreezeOverrideActionStart, "apply-other", "freeze freeze-stop"), "", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			overrides := &memoryFreezeOverrideStore{clock: clock.NewFake(freezeTestNow), requests: []*storage.FreezeOverrideRequest{tc.request}}
			apply := claimCheckpointApply()
			apply.SetOptions(storage.ApplyOptions{FreezeOverride: tc.creationNamed})
			metadata, err := json.Marshal(startControlRequestMetadata{FreezeOverride: tc.startNamed})
			require.NoError(t, err)
			start := &storage.ApplyControlRequest{
				ApplyID: apply.ID, Operation: storage.ControlOperationStart, Status: storage.ControlRequestPending,
				RequestedBy: "cli:alice", Metadata: metadata,
			}
			svc := newClaimCheckpointTestService(t, newGateFreezeStore(t, stopFreeze("freeze-stop")), overrides, start)

			holding, err := svc.stopFreezesHoldingApply(t.Context(), apply)

			require.NoError(t, err)
			assert.Equal(t, tc.wantHeld, len(holding) == 1)
		})
	}
}

// When the freezes or the override requests cannot be read, the claim
// checkpoint cannot decide, and the operation is not started.
func TestStopFreezesHoldingApplyFailsClosed(t *testing.T) {
	t.Run("Freezes", func(t *testing.T) {
		freezes := newGateFreezeStore(t)
		freezes.listInEffectErr = errors.New("connection refused")
		svc := newClaimCheckpointTestService(t, freezes, nil)

		_, err := svc.stopFreezesHoldingApply(t.Context(), claimCheckpointApply())

		require.ErrorContains(t, err, "connection refused")
	})
	t.Run("Overrides", func(t *testing.T) {
		overrides := &memoryFreezeOverrideStore{clock: clock.NewFake(freezeTestNow), getErr: errors.New("connection refused")}
		svc := newClaimCheckpointTestService(t, newGateFreezeStore(t, stopFreeze("freeze-stop")), overrides)
		apply := claimCheckpointApply()
		apply.SetOptions(storage.ApplyOptions{FreezeOverride: "ovr-create"})

		_, err := svc.stopFreezesHoldingApply(t.Context(), apply)

		require.ErrorContains(t, err, "connection refused")
	})
}

// A pending cancel is delivered whatever the freezes: the claim checkpoint
// drives a stopped operation claimed to deliver it, even under a covering stop
// freeze and even when the freezes cannot be read (AZ-10, CO-2).
func TestClaimCheckpointLetsAPendingCancelThrough(t *testing.T) {
	for _, tc := range []struct {
		name       string
		unreadable bool
	}{{"UnderAStopFreeze", false}, {"WhenTheFreezesCannotBeRead", true}} {
		t.Run(tc.name, func(t *testing.T) {
			freezes := newGateFreezeStore(t, stopFreeze("freeze-stop"))
			if tc.unreadable {
				freezes.listInEffectErr = errors.New("connection refused")
			}
			apply := claimCheckpointApply()
			apply.State = state.Apply.Stopped
			cancel := &storage.ApplyControlRequest{
				ApplyID: apply.ID, Operation: storage.ControlOperationCancel, Status: storage.ControlRequestPending, RequestedBy: "cli:alice",
			}
			svc := newClaimCheckpointTestService(t, freezes, nil, cancel)
			op := &storage.ApplyOperation{ID: 70, ApplyID: apply.ID, State: state.ApplyOperation.Stopped}

			held := svc.holdClaimForStopFreeze(t.Context(), 1, apply, op, op.Lease())

			assert.False(t, held, "the cancel is delivered")
		})
	}
}

// An operation held back unstarted is left out of this pod's claims until
// heldClaimRetryAfter passes, so the oldest-first claim order reaches other
// work instead of the same held operation on every pass.
func TestHeldOperationsAreSkippedForARetryWindow(t *testing.T) {
	svc := newClaimCheckpointTestService(t, newGateFreezeStore(t), nil)
	fake := clock.NewFake(freezeTestNow)
	svc.clock = fake

	svc.skipHeldOperation(70)
	svc.skipHeldOperation(42)

	assert.Equal(t, []int64{42, 70}, svc.heldOperationIDs())
	assert.Equal(t, []int64{42, 70}, storage.BuildOperationClaimFilter(svc.operationClaimOptions(1)...).SkipOperationIDs)
	fake.Advance(heldClaimRetryAfter)
	assert.Empty(t, svc.heldOperationIDs(), "the held operations are claimable again")
	assert.Empty(t, svc.operationClaimOptions(1))
}
