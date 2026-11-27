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
		name                                string
		units, operations, competing, limit int
		want                                string
	}{
		{
			name:  "a queue behind the limit states it",
			units: 128, operations: 128, competing: 128, limit: 32,
			want: "32 running, 96 queued (32 at a time)",
		},
		{
			name:  "shards with several tables state it in table changes",
			units: 64, operations: 128, competing: 128, limit: 32,
			want: "32 running, 96 queued (32 table changes at a time)",
		},
		{
			name:  "a rollout the limit cannot hold back reads as its counts",
			units: 32, operations: 32, competing: 32, limit: 32,
			want: "32 running, 96 queued",
		},
		{
			name:  "an unknown limit reads as the counts",
			units: 128, operations: 128, competing: 128, limit: 0,
			want: "32 running, 96 queued",
		},
		{
			name:  "a rollout whose remaining work fits the limit reads as its counts",
			units: 8, operations: 8, competing: 2, limit: 2,
			want: "32 running, 96 queued",
		},
		{
			name:  "a rollout with more work competing for a slot than the limit states it",
			units: 8, operations: 8, competing: 3, limit: 2,
			want: "32 running, 96 queued (2 at a time)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, WithLimit("32 running, 96 queued", tc.units, tc.operations, tc.competing, tc.limit))
		})
	}
}

// rolloutOps is a parallel rollout of n targets: the first running members
// run, the next converged ones already held the change, and the rest wait.
func rolloutOps(n, running, converged int) []Operation {
	return rolloutOpsWithCompleted(n, 0, running, converged)
}

// rolloutOpsWithCompleted is rolloutOps whose first completed members have
// already landed the change.
func rolloutOpsWithCompleted(n, completed, running, converged int) []Operation {
	ops := make([]Operation, n)
	for i := range ops {
		op := Operation{Deployment: "prod", Target: fmt.Sprintf("payments-%03d", i+1), State: state.ApplyOperation.Pending, Parallel: true, ContinueOnFailure: true}
		switch {
		case i < completed:
			op.State = state.ApplyOperation.Completed
		case i < completed+running:
			op.State = state.ApplyOperation.Running
		case i < completed+running+converged:
			op.State = state.ApplyOperation.Completed
			op.AlreadyConverged = true
		}
		ops[i] = op
	}
	return ops
}

// A rollout states its limit while members wait on it. Targets that already
// held the change ran nothing, so they never count against the limit. Targets
// that landed the change hold no slot, so once the remaining work fits the
// limit nothing waits on it, and a finished apply has nothing left to wait.
func TestApplyLimitPhrase(t *testing.T) {
	cases := []struct {
		name string
		ops  []Operation
		want string
	}{
		{name: "more members than the limit", ops: rolloutOps(6, 2, 0), want: "2 running, 4 queued (2 at a time)"},
		{name: "converged targets leave too few to wait", ops: rolloutOps(4, 2, 2), want: "2 running, 4 queued"},
		{name: "landed targets leave nothing queued", ops: rolloutOpsWithCompleted(8, 6, 2, 0), want: "2 running, 4 queued"},
		{name: "landed targets with work still queued", ops: rolloutOpsWithCompleted(8, 5, 2, 0), want: "2 running, 4 queued (2 at a time)"},
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

// orderedOps is a rollout of n targets that runs in order, rolling or under
// the barrier policy, whose first member is running and whose later members
// wait for it.
func orderedOps(n int, barrier bool) []Operation {
	ops := make([]Operation, n)
	for i := range ops {
		ops[i] = Operation{Deployment: "prod", Target: fmt.Sprintf("payments-%03d", i+1), State: state.ApplyOperation.Pending, Barrier: barrier}
	}
	ops[0].State = state.ApplyOperation.Running
	return ops
}

// Members that wait for an earlier member, or are held behind a failure, wait
// on the rollout's order or on an operator, not on the limit. A four-target
// rollout with a limit of two runs one target while three wait for it, and
// neither the rolling nor the barrier policy lets the limit hold anything
// back, so neither states it. A paused rollout whose first target failed
// holds the other three for an operator, so it does not state the limit
// either. The same holds once every copy is done under the barrier: while an
// earlier deployment runs its cutover, or after the first target failed under
// pause, the other three wait parked at their cutover, and the limit is not
// what holds them.
func TestApplyLimitPhraseCountsOnlyMembersWaitingForASlot(t *testing.T) {
	paused := rolloutOps(4, 0, 0)
	for i := range paused {
		paused[i].ContinueOnFailure = false
		paused[i].PauseOnFailure = true
	}
	paused[0].State = state.ApplyOperation.Failed

	parked := func(first Operation, pause bool) []Operation {
		ops := orderedOps(4, true)
		for i := range ops {
			ops[i].State = state.ApplyOperation.WaitingForCutover
			ops[i].PauseOnFailure = pause
		}
		ops[0].Deployment = first.Deployment
		ops[0].State = first.State
		return ops
	}
	earlierDeploymentCuttingOver := Operation{Deployment: "eu", State: state.ApplyOperation.CuttingOver}
	firstTargetFailed := Operation{Deployment: "prod", State: state.ApplyOperation.Failed}

	cases := []struct {
		name    string
		ops     []Operation
		pending PresentationState
	}{
		{name: "rolling", ops: orderedOps(4, false), pending: StateWaiting},
		{name: "barrier", ops: orderedOps(4, true), pending: StateWaiting},
		{name: "paused behind a failure", ops: paused, pending: StatePaused},
		{name: "parked behind an earlier deployment", ops: parked(earlierDeploymentCuttingOver, false), pending: StateReadyForCutoverWaiting},
		{name: "parked behind a failure", ops: parked(firstTargetFailed, true), pending: StateReadyForCutoverWaiting},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			model := Derive(tc.ops)
			model.OperationsAtOnce = 2
			require.False(t, state.IsTerminalApplyState(model.State), "the rollout is still under way, got %q", model.State)
			require.Len(t, model.Deployments, 4)
			for _, d := range model.Deployments[1:] {
				require.Equal(t, tc.pending, d.Presentation, "later members wait on the rollout, not on the limit")
			}
			assert.Empty(t, model.LimitPhrase())
		})
	}
}

// A sharded apply alters mutes on four shards of one target and runs two
// operations at a time. The first shard failed and the other three are held
// behind it, so nothing competes for a slot and the limit is not stated. With
// one shard running and three queued instead, all four compete and the limit
// holds two of them back.
func TestCompetingShardOperations(t *testing.T) {
	shardWork := func(states ...string) []ShardWork {
		work := make([]ShardWork, len(states))
		for i, s := range states {
			op := Operation{Deployment: "prod", Target: "commerce-001", State: s, Work: true}
			work[i] = ShardWork{Keyspace: "commerce", Shard: fmt.Sprintf("%02x-", i*0x40), Operations: []Operation{op}}
		}
		return work
	}
	so := state.ApplyOperation

	held := shardWork(so.Failed, so.Pending, so.Pending, so.Pending)
	heldStatuses := DeriveShards(state.Apply.Running, held)
	require.Len(t, heldStatuses, 4)
	for _, s := range heldStatuses[1:] {
		require.False(t, s.Presentation.CompetesForSlot(), "shard %s reads %q behind the failed shard", s.Shard, s.Label)
	}
	assert.Equal(t, 0, CompetingShardOperations(held, heldStatuses))
	assert.Empty(t, QueueLimitPhrase(4, 4, CompetingShardOperations(held, heldStatuses), 2))

	queued := shardWork(so.Running, so.Pending, so.Pending, so.Pending)
	queuedStatuses := DeriveShards(state.Apply.Running, queued)
	assert.Equal(t, 4, CompetingShardOperations(queued, queuedStatuses))
	assert.Equal(t, "2 at a time", QueueLimitPhrase(4, 4, CompetingShardOperations(queued, queuedStatuses), 2))
}
