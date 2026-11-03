package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
)

// A start moves a stopped apply to resuming before its operations leave
// stopped. Through that window the rollout reads as resuming, with no resume
// offered, and a member that already finished keeps its outcome. Once the
// apply is in any other state its operations read as they are stored.
func TestResumeClaimed(t *testing.T) {
	so := state.ApplyOperation
	stored := []Operation{
		{Deployment: "us", State: so.Completed},
		{Deployment: "eu", State: so.Stopped},
		{Deployment: "ap", State: so.Stopped},
	}

	resuming := Derive(ResumeClaimed(state.Apply.Resuming, stored))
	assert.Equal(t, state.Apply.Resuming, resuming.State)
	assert.Equal(t, "resuming", resuming.Label)
	assert.Equal(t, NextActionNone, resuming.NextAction.Kind, "the start is already under way, so none is offered")
	assert.Equal(t, []string{so.Completed, so.Resuming, so.Resuming},
		[]string{resuming.Deployments[0].State, resuming.Deployments[1].State, resuming.Deployments[2].State})
	assert.Equal(t, so.Stopped, stored[1].State, "the caller's operations are not modified")

	stopped := Derive(ResumeClaimed(state.Apply.Stopped, stored))
	assert.Equal(t, state.Apply.Stopped, stopped.State)
	assert.Equal(t, NextActionResume, stopped.NextAction.Kind)
}
