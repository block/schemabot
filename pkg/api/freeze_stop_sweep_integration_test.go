//go:build integration

package api

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

// Setting a freeze with running "stop" records the ordinary durable stop on
// each admitted apply it covers, pending or running, with the freeze as the
// stop's requester, so the driver delivers it as it would an operator's stop
// and the apply can say which freeze stopped it. An apply in the same
// environment that the freeze does not cover keeps running, as does every
// apply under a freeze that lets running applies continue.
func TestFreezeStopSweepStopsTheAdmittedAppliesInScope(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(stor, &ServerConfig{}, nil, logger)
	// A multi-deployment apply's stop is durable only, so these clients are
	// never called; a stop resolves one before it records anything.
	svc.ternClients["eu/staging"] = &mockTernClient{}
	now := time.Now().UTC()

	freeze := func(running string, deployments, databases []string) *storage.EnvironmentFreeze {
		return &storage.EnvironmentFreeze{
			Identifier: "freeze-sweep", Environment: "staging", Deployments: deployments, Databases: databases,
			Kind: storage.FreezeKindOnDemand, Running: running, StartsAt: now.Add(-time.Minute),
			Reason: "SEV-1234", CreatedBy: "alice", Source: storage.FreezeSourceManual,
		}
	}
	seed := func(t *testing.T, identifier, parentState, opState string) int64 {
		t.Helper()
		resetMatrixTables(t, ctx, db)
		return seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: identifier,
			parentState:     parentState,
			cutoverPolicy:   storage.CutoverPolicyBarrier,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"eu", "us"},
			opState:         opState,
			taskState:       state.Task.Running,
		}).applyID
	}
	pendingStop := func(t *testing.T, applyID int64) *storage.ApplyControlRequest {
		t.Helper()
		stop, err := stor.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStop)
		require.NoError(t, err)
		return stop
	}

	for _, tc := range []struct {
		name, parentState, opState string
	}{
		{"Running", state.Apply.Running, state.ApplyOperation.Running},
		{"Pending", state.Apply.Pending, state.ApplyOperation.Pending},
		// A rollout paused after a failure can still have members running.
		{"PausedRollout", state.Apply.Paused, state.ApplyOperation.Running},
	} {
		t.Run("Stops"+tc.name, func(t *testing.T) {
			applyID := seed(t, "sweep-"+tc.name, tc.parentState, tc.opState)

			sweep := svc.stopAppliesForFreeze(ctx, freeze(storage.FreezeRunningStop, nil, nil), now)

			require.NotNil(t, sweep)
			assert.Equal(t, []string{"sweep-" + tc.name}, sweep.Stopped)
			assert.Empty(t, sweep.NotStopped)
			stop := pendingStop(t, applyID)
			require.NotNil(t, stop, "the freeze recorded a durable stop")
			assert.Equal(t, storage.FreezeStopCaller("freeze-sweep"), stop.RequestedBy)
			assert.True(t, applyLogContains(t, stor, applyID, "Stop requested by freeze freeze-sweep"),
				"the apply log says the freeze asked for the stop")
		})
	}

	t.Run("StopsWhenOneDeploymentIsCovered", func(t *testing.T) {
		applyID := seed(t, "sweep-one-deployment", state.Apply.Running, state.ApplyOperation.Running)

		sweep := svc.stopAppliesForFreeze(ctx, freeze(storage.FreezeRunningStop, []string{"us"}, nil), now)

		assert.Equal(t, []string{"sweep-one-deployment"}, sweep.Stopped, "one covered deployment holds the whole change")
		assert.NotNil(t, pendingStop(t, applyID))
	})

	for _, tc := range []struct {
		name   string
		freeze *storage.EnvironmentFreeze
	}{
		{"OtherDeployment", freeze(storage.FreezeRunningStop, []string{"ap"}, nil)},
		{"OtherDatabase", freeze(storage.FreezeRunningStop, nil, []string{"orders"})},
	} {
		t.Run("LeavesAlone"+tc.name, func(t *testing.T) {
			applyID := seed(t, "sweep-"+tc.name, state.Apply.Running, state.ApplyOperation.Running)

			sweep := svc.stopAppliesForFreeze(ctx, tc.freeze, now)

			require.NotNil(t, sweep)
			assert.Empty(t, sweep.Stopped)
			assert.Empty(t, sweep.NotStopped)
			assert.Nil(t, pendingStop(t, applyID), "an apply the freeze does not cover keeps running")
		})
	}

	t.Run("ContinueStopsNothing", func(t *testing.T) {
		applyID := seed(t, "sweep-continue", state.Apply.Running, state.ApplyOperation.Running)

		assert.Nil(t, svc.stopAppliesForFreeze(ctx, freeze(storage.FreezeRunningContinue, nil, nil), now))
		assert.Nil(t, pendingStop(t, applyID))
	})

	t.Run("StartsLaterStopsNothing", func(t *testing.T) {
		applyID := seed(t, "sweep-later", state.Apply.Running, state.ApplyOperation.Running)
		later := freeze(storage.FreezeRunningStop, nil, nil)
		later.StartsAt = now.Add(time.Hour)

		assert.Nil(t, svc.stopAppliesForFreeze(ctx, later, now))
		assert.Nil(t, pendingStop(t, applyID))
	})

	// A start queued on a stopped apply before the freeze would resume it
	// once a driver claims it. The freeze's stop withdraws that start (CO-4)
	// and reports the apply as stopped. Once a claim has moved an operation
	// to resuming, the stop stays pending for that drive to deliver. A
	// stopped apply with no start queued has nothing to stop and is in
	// neither list.
	seedStopped := func(t *testing.T, identifier string, perOpState map[string]string, queueStart bool) int64 {
		t.Helper()
		resetMatrixTables(t, ctx, db)
		applyID := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: identifier,
			parentState:     state.Apply.Stopped,
			cutoverPolicy:   storage.CutoverPolicyBarrier,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"eu", "us"},
			opState:         state.ApplyOperation.Stopped,
			taskState:       state.Task.Stopped,
			perOpState:      perOpState,
		}).applyID
		if queueStart {
			_, _, err := stor.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
				ApplyID: applyID, Operation: storage.ControlOperationStart, Status: storage.ControlRequestPending, RequestedBy: "cli:alice",
			})
			require.NoError(t, err)
		}
		return applyID
	}
	startStatus := func(t *testing.T, applyID int64) storage.ControlRequestStatus {
		t.Helper()
		start, err := stor.ControlRequests().GetByOperation(ctx, applyID, storage.ControlOperationStart)
		require.NoError(t, err)
		require.NotNil(t, start)
		return start.Status
	}

	t.Run("WithdrawsAStartQueuedOnAStoppedApply", func(t *testing.T) {
		applyID := seedStopped(t, "sweep-queued-start", nil, true)

		sweep := svc.stopAppliesForFreeze(ctx, freeze(storage.FreezeRunningStop, nil, nil), now)

		require.NotNil(t, sweep)
		assert.Equal(t, []string{"sweep-queued-start"}, sweep.Stopped)
		assert.Empty(t, sweep.NotStopped)
		assert.Equal(t, storage.ControlRequestCompleted, startStatus(t, applyID), "the queued start is withdrawn")
		assert.Equal(t, state.Apply.Stopped, getApply(t, ctx, stor, applyID).State)
	})

	t.Run("LeavesTheStopForTheDriveThatClaimedTheStart", func(t *testing.T) {
		applyID := seedStopped(t, "sweep-claimed-start", map[string]string{"eu": state.ApplyOperation.Resuming}, true)

		sweep := svc.stopAppliesForFreeze(ctx, freeze(storage.FreezeRunningStop, nil, nil), now)

		require.NotNil(t, sweep)
		assert.Equal(t, []string{"sweep-claimed-start"}, sweep.Stopped)
		stop := pendingStop(t, applyID)
		require.NotNil(t, stop, "the stop is pending for the drive that resumed an operation")
		assert.Equal(t, storage.FreezeStopCaller("freeze-sweep"), stop.RequestedBy)
	})

	t.Run("SkipsAStoppedApplyWithNoStartQueued", func(t *testing.T) {
		applyID := seedStopped(t, "sweep-stopped", nil, false)

		sweep := svc.stopAppliesForFreeze(ctx, freeze(storage.FreezeRunningStop, nil, nil), now)

		require.NotNil(t, sweep)
		assert.Empty(t, sweep.Stopped)
		assert.Empty(t, sweep.NotStopped)
		assert.Nil(t, pendingStop(t, applyID))
	})

	// PlanetScale has no stop, so the sweep cannot stop such an apply. It
	// says so for that apply rather than skipping it.
	t.Run("ReportsAnApplyItCannotStop", func(t *testing.T) {
		resetMatrixTables(t, ctx, db)
		applyID := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: "sweep-planetscale",
			parentState:     state.Apply.Running,
			cutoverPolicy:   storage.CutoverPolicyBarrier,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"eu"},
			databaseType:    storage.DatabaseTypeVitess,
			opState:         state.ApplyOperation.Running,
			taskState:       state.Task.Running,
		}).applyID

		sweep := svc.stopAppliesForFreeze(ctx, freeze(storage.FreezeRunningStop, nil, nil), now)

		require.NotNil(t, sweep)
		assert.Empty(t, sweep.Stopped)
		require.Len(t, sweep.NotStopped, 1)
		assert.Equal(t, "sweep-planetscale", sweep.NotStopped[0].ApplyID)
		assert.NotEmpty(t, sweep.NotStopped[0].Reason)
		assert.Nil(t, pendingStop(t, applyID))
	})
}
