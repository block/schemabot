package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyOperationRemoteApplyID(t *testing.T) {
	t.Run("nil operation", func(t *testing.T) {
		var op *ApplyOperation
		assert.Empty(t, op.RemoteApplyID())
	})
	t.Run("external id is canonical", func(t *testing.T) {
		op := &ApplyOperation{ExternalID: "apply-remote-1", EngineResumeContext: "legacy-ctx"}
		assert.Equal(t, "apply-remote-1", op.RemoteApplyID())
	})
	t.Run("legacy resume context carrier", func(t *testing.T) {
		op := &ApplyOperation{EngineResumeContext: "apply-legacy-1"}
		assert.Equal(t, "apply-legacy-1", op.RemoteApplyID())
	})
	t.Run("nothing recorded", func(t *testing.T) {
		assert.Empty(t, (&ApplyOperation{}).RemoteApplyID())
	})
}

func TestMemberRemoteApplyID(t *testing.T) {
	t.Run("no operations", func(t *testing.T) {
		id, err := MemberRemoteApplyID(nil, &ApplyOperation{Deployment: "west"})
		require.NoError(t, err)
		assert.Empty(t, id)
	})

	t.Run("nothing recorded yet", func(t *testing.T) {
		ops := []*ApplyOperation{
			{ID: 1, Deployment: "west", OperationKey: "ns/-80/users"},
			{ID: 2, Deployment: "west", OperationKey: "ns/80-/users"},
		}
		id, err := MemberRemoteApplyID(ops, &ApplyOperation{Deployment: "west"})
		require.NoError(t, err)
		assert.Empty(t, id)
	})

	t.Run("all siblings agree", func(t *testing.T) {
		ops := []*ApplyOperation{
			{ID: 1, Deployment: "west", ExternalID: "apply-remote-1"},
			{ID: 2, Deployment: "west", ExternalID: "apply-remote-1"},
			{ID: 3, Deployment: "west"},
		}
		id, err := MemberRemoteApplyID(ops, &ApplyOperation{Deployment: "west"})
		require.NoError(t, err)
		assert.Equal(t, "apply-remote-1", id)
	})

	t.Run("legacy carrier counts as the recorded id", func(t *testing.T) {
		ops := []*ApplyOperation{
			{ID: 1, Deployment: "west", EngineResumeContext: "apply-remote-1"},
			{ID: 2, Deployment: "west", ExternalID: "apply-remote-1"},
		}
		id, err := MemberRemoteApplyID(ops, &ApplyOperation{Deployment: "west"})
		require.NoError(t, err)
		assert.Equal(t, "apply-remote-1", id)
	})

	t.Run("sibling deployments keep their own remote applies", func(t *testing.T) {
		ops := []*ApplyOperation{
			{ID: 1, Deployment: "west", ExternalID: "apply-remote-west"},
			{ID: 2, Deployment: "east", ExternalID: "apply-remote-east"},
			{ID: 3, Deployment: "south", ExternalID: "apply-remote-south"},
		}
		id, err := MemberRemoteApplyID(ops, &ApplyOperation{Deployment: "east"})
		require.NoError(t, err)
		assert.Equal(t, "apply-remote-east", id)
	})

	// A deployment addressing several targets is one member per target: each
	// target keeps its own remote apply, and a disagreement inside one target
	// still fails closed.
	t.Run("sibling targets of one deployment keep their own remote applies", func(t *testing.T) {
		first := &ApplyOperation{ID: 1, Deployment: "default", Target: "payments-001", OperationKey: "payments-001", ExternalID: "apply-remote-001"}
		second := &ApplyOperation{ID: 2, Deployment: "default", Target: "payments-002", OperationKey: "payments-002", ExternalID: "apply-remote-002"}
		ops := []*ApplyOperation{first, second}
		id, err := MemberRemoteApplyID(ops, second)
		require.NoError(t, err)
		assert.Equal(t, "apply-remote-002", id)

		ops = append(ops, &ApplyOperation{ID: 3, Deployment: "default", Target: "payments-002", OperationKey: "payments-002/extra", ExternalID: "apply-remote-001"})
		_, err = MemberRemoteApplyID(ops, second)
		require.Error(t, err, "two remote applies for one target must fail closed")
		assert.Contains(t, err.Error(), "apply_operation 3")
	})

	// A deployment addressing one target is one member whatever its rows name,
	// so a row that records no target stays in the deployment's one member.
	t.Run("a single-target deployment is one member", func(t *testing.T) {
		ops := []*ApplyOperation{
			{ID: 1, Deployment: "west", Target: "west-db", ExternalID: "apply-remote-1"},
			{ID: 2, Deployment: "west", ExternalID: "apply-remote-2"},
		}
		_, err := MemberRemoteApplyID(ops, ops[0])
		require.Error(t, err, "a target-less row of a single-target deployment is still the same member")
	})

	t.Run("disagreeing siblings fail closed", func(t *testing.T) {
		ops := []*ApplyOperation{
			{ID: 1, Deployment: "west", ExternalID: "apply-remote-1"},
			{ID: 2, Deployment: "west", ExternalID: "apply-remote-2"},
		}
		id, err := MemberRemoteApplyID(ops, &ApplyOperation{Deployment: "west"})
		require.Error(t, err)
		assert.Empty(t, id)
		assert.Contains(t, err.Error(), "apply-remote-1")
		assert.Contains(t, err.Error(), "apply-remote-2")
		assert.Contains(t, err.Error(), `deployment "west"`)
		assert.Contains(t, err.Error(), "apply_operation 2",
			"the error must pin the offending row even when its operation key is empty")
	})
}

func TestDeploymentExternalID(t *testing.T) {
	t.Run("all siblings agree", func(t *testing.T) {
		ops := []*ApplyOperation{
			{ID: 1, Deployment: "west", ExternalID: "apply-remote-1"},
			{ID: 2, Deployment: "west", ExternalID: "apply-remote-1"},
			{ID: 3, Deployment: "west"},
		}
		id, err := DeploymentExternalID(ops, "west")
		require.NoError(t, err)
		assert.Equal(t, "apply-remote-1", id)
	})

	t.Run("legacy resume context carrier is not an external id", func(t *testing.T) {
		ops := []*ApplyOperation{
			{ID: 1, Deployment: "west", EngineResumeContext: "engine-owned-resume-state"},
			{ID: 2, Deployment: "west", EngineResumeContext: "other-engine-state"},
		}
		id, err := DeploymentExternalID(ops, "west")
		require.NoError(t, err)
		assert.Empty(t, id, "engine resume state must never surface as an apply id")
	})

	t.Run("disagreeing siblings fail closed", func(t *testing.T) {
		ops := []*ApplyOperation{
			{ID: 1, Deployment: "west", ExternalID: "apply-remote-1"},
			{ID: 2, Deployment: "west", ExternalID: "apply-remote-2"},
		}
		id, err := DeploymentExternalID(ops, "west")
		require.Error(t, err)
		assert.Empty(t, id)
		assert.Contains(t, err.Error(), "apply-remote-1")
		assert.Contains(t, err.Error(), "apply-remote-2")
		assert.Contains(t, err.Error(), "apply_operation 2",
			"the error must pin the offending row even when its operation key is empty")
	})

	t.Run("sibling deployments keep their own ids", func(t *testing.T) {
		ops := []*ApplyOperation{
			{ID: 1, Deployment: "west", ExternalID: "apply-remote-west"},
			{ID: 2, Deployment: "east", ExternalID: "apply-remote-east"},
		}
		id, err := DeploymentExternalID(ops, "east")
		require.NoError(t, err)
		assert.Equal(t, "apply-remote-east", id)
	})
}
