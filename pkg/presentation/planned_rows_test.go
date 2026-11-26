package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
)

func TestCopyStillAhead(t *testing.T) {
	ahead := []string{state.Task.Pending, state.Task.WaitingForDeploy, state.Task.Running, state.Task.Recovering}
	for _, s := range ahead {
		assert.True(t, CopyStillAhead(s), "%s has not reported its copy yet", s)
	}
	past := []string{
		state.Task.CatchingUp, state.Task.Checksumming, state.Task.PostChecksum,
		state.Task.WaitingForCutover, state.Task.CuttingOver, state.Task.Reverting,
		state.Task.RevertWindow, state.Task.Completed,
		state.Task.Failed, state.Task.FailedRetryable, state.Task.Stopped, state.Task.Cancelled,
	}
	for _, s := range past {
		assert.False(t, CopyStillAhead(s), "%s has no copy ahead for an estimate to stand in for", s)
	}
}

// The planned rows line carries the row estimate, the targets it spans, and
// the table's size. A table the plan sized but could not count shows its size
// alone, and the line is empty when the plan had neither.
func TestPlannedRowsText(t *testing.T) {
	rows := int64(1_200_000)
	bytes := int64(3_000_000_000)
	assert.Equal(t, "Rows: ~1.2M", PlannedRowsText(&rows, nil, ""))
	assert.Equal(t, "Rows: ~1.2M across 3 targets · ~3.0 GB", PlannedRowsText(&rows, &bytes, PlannedRowsSpan(3)))
	assert.Equal(t, "Size: ~3.0 GB", PlannedRowsText(nil, &bytes, ""))
	assert.Equal(t, "Size: ~3.0 GB across 3 targets", PlannedRowsText(nil, &bytes, PlannedRowsSpan(3)))
	assert.Empty(t, PlannedRowsText(nil, nil, ""))
}

// A planned row count names how many targets it is summed across without
// claiming they are all of the rollout's, and reads naturally for one.
func TestPlannedRowsSpan(t *testing.T) {
	assert.Equal(t, " across 3 targets", PlannedRowsSpan(3))
	assert.Equal(t, " on 1 target", PlannedRowsSpan(1))
}
