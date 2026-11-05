package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
)

// endedFor builds a barrier, halt-on-failure row SchemaBot failed at the
// cutover for the row of deployment ended.
func endedFor(dep, ended string) Operation {
	return Operation{Deployment: dep, State: so.Failed, Barrier: true, EndedBy: &OperationRef{Deployment: ended}}
}

func plainName(name string) string { return name }

// A rollout defers its cutover across three deployments. The middle one fails
// under halt, so SchemaBot stops the other two at the cutover and fails them
// for it. They read as stopped at cutover naming the failure, the counts tell
// them apart from the failure, the first failure is the deployment that
// really failed although an earlier one also reads failed, and the note says
// their copies are saved.
func TestDerive_StoppedAtCutoverNamesTheFailure(t *testing.T) {
	got := Derive([]Operation{
		endedFor("eu", "us"),
		{Deployment: "us", State: so.Failed, Barrier: true, Error: "duplicate key"},
		endedFor("au", "us"),
	})

	require.Len(t, got.Deployments, 3)
	for _, i := range []int{0, 2} {
		d := got.Deployments[i]
		assert.Equal(t, StateStoppedAtCutover, d.Presentation, d.Name)
		assert.Equal(t, "stopped at cutover — us failed; copy saved", d.Label, d.Name)
		assert.Equal(t, "⏹️", d.Emoji, d.Name)
		assert.Equal(t, "us", d.EndedBy, d.Name)
	}
	assert.Equal(t, StateFailed, got.Deployments[1].Presentation)
	assert.Equal(t, state.Apply.Failed, got.State)
	assert.Equal(t, []StateCount{{Label: "failed", Count: 1}, {Label: StoppedAtCutoverLabel, Count: 2}}, got.Counts)

	require.NotNil(t, got.FirstFailure)
	assert.Equal(t, "us", got.FirstFailure.Name)
	assert.Equal(t, "duplicate key", got.FirstFailure.Error)

	assert.Equal(t, "2 deployments stopped at the cutover because us failed. Their copies are saved, so applying again resumes them.",
		StoppedAtCutoverOf(got.Deployments).Note(DeploymentNoun, plainName))
}

// One stopped member reads in the singular, and a stopped member whose
// ended row the apply does not list still reads stopped at cutover, without
// naming a member it cannot place.
func TestStoppedAtCutoverNote_SingularAndUnplacedFailure(t *testing.T) {
	got := Derive([]Operation{
		{Deployment: "us", State: so.Failed, Barrier: true},
		endedFor("eu", "gone"),
	})

	eu := got.Deployments[1]
	assert.Equal(t, StateStoppedAtCutover, eu.Presentation)
	assert.Equal(t, "stopped at cutover — copy saved", eu.Label)
	assert.Empty(t, eu.EndedBy)
	assert.Equal(t, "1 target stopped at the cutover because another target failed. Its copy is saved, so applying again resumes it.",
		StoppedAtCutoverOf(got.Deployments).Note(TargetNoun, plainName))
	assert.Empty(t, StoppedAtCutoverOf(got.Deployments[:1]).Note(TargetNoun, plainName), "no note without a stopped member")
}

// Stopped members that name different failures are counted together, and
// the note names no single failure.
func TestStoppedAtCutoverOf_MixedFailures(t *testing.T) {
	var s StoppedAtCutover
	s.Add("us")
	s.Add("eu")
	s.Add("us")
	assert.Equal(t, StoppedAtCutover{Count: 3}, s)
}

// A target run table by table folds its rows into one member. It reads
// stopped at cutover only when every failed row was failed for another
// target; a row that failed on its own makes the member read failed.
func TestDerive_FoldedMemberStoppedAtCutoverOnlyWhenNoRowFailedOnItsOwn(t *testing.T) {
	ended := &OperationRef{Deployment: "prod", OperationKey: "a/users"}
	stopped := Derive([]Operation{
		{Deployment: "prod", Target: "a", OperationKey: "a/users", RolloutStep: 1, State: so.Failed, Barrier: true},
		{Deployment: "prod", Target: "b", OperationKey: "b/users", RolloutStep: 1, State: so.Failed, Barrier: true, EndedBy: ended},
		{Deployment: "prod", Target: "b", OperationKey: "b/orders", RolloutStep: 2, State: so.Cancelled, Barrier: true},
	})
	require.Len(t, stopped.Deployments, 2)
	assert.Equal(t, StateFailed, stopped.Deployments[0].Presentation)
	assert.Equal(t, StateStoppedAtCutover, stopped.Deployments[1].Presentation)
	assert.Equal(t, "prod/a", stopped.Deployments[1].EndedBy)

	failed := Derive([]Operation{
		{Deployment: "prod", Target: "a", OperationKey: "a/users", RolloutStep: 1, State: so.Failed, Barrier: true},
		{Deployment: "prod", Target: "b", OperationKey: "b/users", RolloutStep: 1, State: so.Failed, Barrier: true, EndedBy: ended},
		{Deployment: "prod", Target: "b", OperationKey: "b/orders", RolloutStep: 2, State: so.Failed, Barrier: true},
	})
	require.Len(t, failed.Deployments, 2)
	assert.Equal(t, StateFailed, failed.Deployments[1].Presentation)
}

// A sharded apply defers its cutover. Shard -40 fails, so SchemaBot stops
// 40-80 and 80- at the cutover. Each shard's reference to the failed row is
// read as the shard that runs it, so the labels and the Shards count name
// shard -40.
func TestDeriveShards_StoppedAtCutoverNamesTheFailedShard(t *testing.T) {
	ended := &OperationRef{Deployment: "prod", OperationKey: "shop/-40/users"}
	shards := DeriveShards(state.Apply.Failed, []ShardWork{
		{Keyspace: "shop", Shard: "-40", Operations: []Operation{
			{Deployment: "prod", OperationKey: "shop/-40/users", State: so.Failed, Barrier: true, Error: "duplicate key"},
		}},
		{Keyspace: "shop", Shard: "40-80", Operations: []Operation{
			{Deployment: "prod", OperationKey: "shop/40-80/users", State: so.Failed, Barrier: true, EndedBy: ended},
		}},
		{Keyspace: "shop", Shard: "80-", Operations: []Operation{
			{Deployment: "prod", OperationKey: "shop/80-/users", State: so.Failed, Barrier: true, EndedBy: ended},
		}},
	})

	require.Len(t, shards, 3)
	assert.Equal(t, StateFailed, shards[0].Presentation)
	labels := make([]string, 0, len(shards))
	for _, s := range shards {
		labels = append(labels, s.Label)
	}
	assert.Equal(t, "stopped at cutover — -40 failed; copy saved", shards[1].Label)
	assert.Equal(t, "-40", shards[2].EndedBy)
	assert.Equal(t, "1 failed, 2 stopped at cutover", ShardCounts(labels))
}
