//go:build integration

package tern

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// deferredCutoverApply stores a running apply that asked to hold its cutover.
// Each one gets its own database so several can be live at once — storage
// admits a single active apply per database.
func deferredCutoverApply(t *testing.T, stor storage.Storage, name string) *storage.Apply {
	t.Helper()
	now := time.Now()
	database := "deferral_" + name
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-%s-%d", name, now.UnixNano()),
		Database:       database,
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     database,
		Environment:    localClientTestEnvironment,
		CreatedAt:      now,
	}
	planID, err := stor.Plans().Create(t.Context(), plan)
	require.NoError(t, err)

	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-%s-%d", name, now.UnixNano()),
		PlanID:          planID,
		Database:        database,
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      database,
		Environment:     localClientTestEnvironment,
		State:           state.Apply.Running,
		Options:         []byte(`{"defer_cutover":true}`),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := stor.Applies().CreateWithTasks(t.Context(), apply, nil)
	require.NoError(t, err)
	apply.ID = applyID
	return apply
}

// deferralViolationEntry returns the apply's recorded deferred-cutover
// violation, or nil when it recorded none.
func deferralViolationEntry(t *testing.T, stor storage.Storage, applyID int64) *storage.ApplyLog {
	t.Helper()
	logs, err := stor.ApplyLogs().GetByApply(t.Context(), applyID)
	require.NoError(t, err)
	for _, entry := range logs {
		if entry.Level == storage.LogLevelError && strings.Contains(entry.Message, "the engine cut over on its own") {
			return entry
		}
	}
	return nil
}

// An operator who defers a cutover is promised the schema will not swap without
// them. The drive keeps that promise by declining to trigger the cutover, which
// holds nothing if the engine backend swaps on its own — the apply simply never
// reports the gate. Reaching the swap with no SchemaBot-triggered cutover behind
// it is that failure, and it is recorded on the operator's timeline rather than
// completing quietly.
func TestDetectDeferredCutoverViolation_ReportsABackendThatCutOverItself(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)

	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client, _ := newTasklessControlClient(t, dsn, stor)

	apply := deferredCutoverApply(t, stor, "deferred-violated")
	ps := &atomicPollState{}

	client.detectDeferredCutoverViolation(t.Context(), apply, true, engine.StateCuttingOver, ps)

	entry := deferralViolationEntry(t, stor, apply.ID)
	require.NotNil(t, entry, "a deferred apply the backend cut over must say so on the timeline")
	assert.Contains(t, entry.Message, string(engine.StateCuttingOver))
	assert.True(t, ps.deferredCutoverViolationLogged, "the violation latches so it is stated once per drive")
}

// The same state reached through SchemaBot's own cutover is the deferral working
// as asked: the operator issued the cutover, the drive recorded the trigger, and
// the swap that follows is theirs.
func TestDetectDeferredCutoverViolation_SilentWhenSchemaBotTriggeredTheCutover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)

	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client, _ := newTasklessControlClient(t, dsn, stor)

	apply := deferredCutoverApply(t, stor, "deferred-honored")
	client.logApplyEvent(t.Context(), apply.ID, nil, storage.LogLevelInfo, storage.LogEventCutoverTriggered,
		storage.LogSourceSchemaBot, "Cutover triggered by operator", "", "")

	ps := &atomicPollState{}
	client.detectDeferredCutoverViolation(t.Context(), apply, true, engine.StateCompleted, ps)

	assert.Nil(t, deferralViolationEntry(t, stor, apply.ID), "an operator's own cutover is not a violation")
	assert.False(t, ps.deferredCutoverViolationLogged)
}

// An apply that never asked to hold, and one still short of the swap, are both
// ordinary: neither is a deferral the backend broke.
func TestDetectDeferredCutoverViolation_QuietOnApplesThatDidNotDefer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)

	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client, _ := newTasklessControlClient(t, dsn, stor)

	notDeferred := deferredCutoverApply(t, stor, "not-deferred")
	client.detectDeferredCutoverViolation(t.Context(), notDeferred, false, engine.StateCompleted, &atomicPollState{})
	assert.Nil(t, deferralViolationEntry(t, stor, notDeferred.ID))

	stillCopying := deferredCutoverApply(t, stor, "still-copying")
	client.detectDeferredCutoverViolation(t.Context(), stillCopying, true, engine.StateRunning, &atomicPollState{})
	assert.Nil(t, deferralViolationEntry(t, stor, stillCopying.ID))

	parked := deferredCutoverApply(t, stor, "parked")
	client.detectDeferredCutoverViolation(t.Context(), parked, true, engine.StateWaitingForCutover, &atomicPollState{})
	assert.Nil(t, deferralViolationEntry(t, stor, parked.ID), "parking at the gate is the deferral working")
}

// A failed apply did not swap the schema, so it is not a broken hold — it is a
// change that did not land.
func TestCutoverGatePassedExcludesFailureStates(t *testing.T) {
	assert.True(t, cutoverGatePassed(engine.StateCuttingOver))
	assert.True(t, cutoverGatePassed(engine.StateRevertWindow))
	assert.True(t, cutoverGatePassed(engine.StateCompleted))
	assert.True(t, cutoverGatePassed(engine.StateReverted))
	assert.False(t, cutoverGatePassed(engine.StateFailed))
	assert.False(t, cutoverGatePassed(engine.StateCancelled))
	assert.False(t, cutoverGatePassed(engine.StateStopped))
	assert.False(t, cutoverGatePassed(engine.StateWaitingForCutover))
}
