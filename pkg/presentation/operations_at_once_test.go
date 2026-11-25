package presentation

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
)

func TestWithLimit(t *testing.T) {
	cases := []struct {
		name                     string
		units, operations, limit int
		want                     string
	}{
		{
			name:  "a queue behind the limit states it",
			units: 128, operations: 128, limit: 32,
			want: "32 running, 96 queued (32 at a time)",
		},
		{
			name:  "shards with several tables state it in table changes",
			units: 64, operations: 128, limit: 32,
			want: "32 running, 96 queued (32 table changes at a time)",
		},
		{
			name:  "a rollout the limit cannot hold back reads as its counts",
			units: 32, operations: 32, limit: 32,
			want: "32 running, 96 queued",
		},
		{
			name:  "an unknown limit reads as the counts",
			units: 128, operations: 128, limit: 0,
			want: "32 running, 96 queued",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, WithLimit("32 running, 96 queued", tc.units, tc.operations, tc.limit))
		})
	}
}

// rolloutOps is a parallel rollout of n targets: the first running members
// run, the next converged ones already held the change, and the rest wait.
func rolloutOps(n, running, converged int) []Operation {
	ops := make([]Operation, n)
	for i := range ops {
		op := Operation{Deployment: "prod", Target: fmt.Sprintf("payments-%03d", i+1), State: state.ApplyOperation.Pending, Parallel: true, ContinueOnFailure: true}
		switch {
		case i < running:
			op.State = state.ApplyOperation.Running
		case i < running+converged:
			op.State = state.ApplyOperation.Completed
			op.AlreadyConverged = true
		}
		ops[i] = op
	}
	return ops
}

// A rollout states its limit while members wait on it. Targets that already
// held the change ran nothing, so they never count against the limit, and a
// finished apply has nothing left to wait.
func TestApplyLimitPhrase(t *testing.T) {
	cases := []struct {
		name string
		ops  []Operation
		want string
	}{
		{name: "more members than the limit", ops: rolloutOps(6, 2, 0), want: "2 running, 4 queued (2 at a time)"},
		{name: "converged targets leave too few to wait", ops: rolloutOps(4, 2, 2), want: "2 running, 4 queued"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			model := Derive(tc.ops)
			model.OperationsAtOnce = 2
			assert.Equal(t, tc.want, model.CountsWithLimit("2 running, 4 queued"))
		})
	}

	t.Run("a terminal apply", func(t *testing.T) {
		ops := rolloutOps(6, 0, 0)
		for i := range ops {
			ops[i].State = state.ApplyOperation.Cancelled
		}
		model := Derive(ops)
		model.OperationsAtOnce = 2
		require.True(t, state.IsTerminalApplyState(model.State), "every member cancelled ends the apply, got %q", model.State)
		assert.Empty(t, model.LimitPhrase())
	})
}
