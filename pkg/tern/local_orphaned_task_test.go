package tern

import (
	"testing"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
)

// A terminal apply with a fresh lease is a driver still settling it: its own
// task writes may be in flight, and an unleased settlement racing them would
// leave the row to whichever write lands last. The sweep must defer to the
// lease and settle only once it has aged out.
func TestConflictCheckDefersStrandedSettlementToAFreshLease(t *testing.T) {
	stranded, holdingApply := strandedRetryableTask()
	holdingApply.LeaseOwner = "host/1/driver-0"
	holdingApply.UpdatedAt = time.Now()
	client := restingTaskClient(stranded, holdingApply, nil)

	client.settleOrphanedTask(t.Context(), stranded, holdingApply)
	assert.Equal(t, state.Task.FailedRetryable, stranded.State,
		"a fresh lease means a driver may be mid-settlement, so the sweep must not race it")

	holdingApply.UpdatedAt = time.Now().Add(-2 * storage.ApplyLeaseStaleAfter)
	client.settleOrphanedTask(t.Context(), stranded, holdingApply)
	assert.Equal(t, state.Task.Stopped, stranded.State,
		"once the lease has aged out no driver is coming, so the sweep settles the task")
}

// A settled task is at rest, so the row must not keep a frozen ETA or read as
// throttled with no copy in flight. The sweep repairs rows written by paths
// that never cleared these, so it cannot assume they arrive clean — while the
// engine failure that paused the copy is preserved, because a settlement to
// stopped keeps the change resumable and that message is why it stopped.
func TestConflictCheckClearsRestingFieldsOnStrandedSettlement(t *testing.T) {
	stranded, holdingApply := strandedRetryableTask()
	stranded.ETASeconds = 240
	stranded.Throttled = true
	stranded.ThrottleReason = "replica lag"
	client := restingTaskClient(stranded, holdingApply, nil)

	client.settleOrphanedTask(t.Context(), stranded, holdingApply)

	assert.Equal(t, state.Task.Stopped, stranded.State)
	assert.Zero(t, stranded.ETASeconds, "a resting row must not carry a frozen estimate")
	assert.False(t, stranded.Throttled, "a resting row must not render as paused with no copy in flight")
	assert.Empty(t, stranded.ThrottleReason)
	assert.Equal(t, "error writing checkpoint: too many connections", stranded.ErrorMessage,
		"the engine failure that paused the copy is preserved")
}
