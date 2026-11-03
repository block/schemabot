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

// A table of a resuming apply reads as resuming only while it stands where the
// stop left it on every part: stopped on some, finished or not yet begun on
// the rest, which its line keeps as counts. Once a part moves it on, its own
// progress speaks for it.
func TestAwaitingResume(t *testing.T) {
	ts := state.Task
	for _, tc := range []struct {
		name     string
		statuses []string
		coverage string
		ok       bool
	}{
		{"every part stopped", []string{ts.Stopped, ts.Stopped}, "", true},
		{"finished on one", []string{ts.Completed, ts.Stopped}, " · 1 complete", true},
		{"queued on one", []string{ts.Stopped, ts.Pending}, " · 1 queued", true},
		{"copying again on one", []string{ts.Running, ts.Stopped}, "", false},
		{"failed on one", []string{ts.Failed, ts.Stopped}, "", false},
		{"none stopped", []string{ts.Completed, ts.Pending}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coverage, ok := AwaitingResume(tc.statuses)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.coverage, coverage)
		})
	}
}
