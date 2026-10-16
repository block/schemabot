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
