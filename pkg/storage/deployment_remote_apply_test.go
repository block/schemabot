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

func TestDeploymentRemoteApplyID(t *testing.T) {
	t.Run("no operations", func(t *testing.T) {
		id, err := DeploymentRemoteApplyID(nil, "west")
		require.NoError(t, err)
		assert.Empty(t, id)
	})

	t.Run("nothing recorded yet", func(t *testing.T) {
		ops := []*ApplyOperation{
			{ID: 1, Deployment: "west", OperationKey: "ns/-80/users"},
			{ID: 2, Deployment: "west", OperationKey: "ns/80-/users"},
		}
		id, err := DeploymentRemoteApplyID(ops, "west")
		require.NoError(t, err)
		assert.Empty(t, id)
	})

	t.Run("all siblings agree", func(t *testing.T) {
		ops := []*ApplyOperation{
			{ID: 1, Deployment: "west", ExternalID: "apply-remote-1"},
			{ID: 2, Deployment: "west", ExternalID: "apply-remote-1"},
			{ID: 3, Deployment: "west"},
		}
		id, err := DeploymentRemoteApplyID(ops, "west")
		require.NoError(t, err)
		assert.Equal(t, "apply-remote-1", id)
	})

	t.Run("legacy carrier counts as the recorded id", func(t *testing.T) {
		ops := []*ApplyOperation{
			{ID: 1, Deployment: "west", EngineResumeContext: "apply-remote-1"},
			{ID: 2, Deployment: "west", ExternalID: "apply-remote-1"},
		}
		id, err := DeploymentRemoteApplyID(ops, "west")
		require.NoError(t, err)
		assert.Equal(t, "apply-remote-1", id)
	})

	t.Run("sibling deployments keep their own remote applies", func(t *testing.T) {
		ops := []*ApplyOperation{
			{ID: 1, Deployment: "west", ExternalID: "apply-remote-west"},
			{ID: 2, Deployment: "east", ExternalID: "apply-remote-east"},
			{ID: 3, Deployment: "south", ExternalID: "apply-remote-south"},
		}
		id, err := DeploymentRemoteApplyID(ops, "east")
		require.NoError(t, err)
		assert.Equal(t, "apply-remote-east", id)
	})

	t.Run("disagreeing siblings fail closed", func(t *testing.T) {
		ops := []*ApplyOperation{
			{ID: 1, Deployment: "west", ExternalID: "apply-remote-1"},
			{ID: 2, Deployment: "west", ExternalID: "apply-remote-2"},
		}
		id, err := DeploymentRemoteApplyID(ops, "west")
		require.Error(t, err)
		assert.Empty(t, id)
		assert.Contains(t, err.Error(), "apply-remote-1")
		assert.Contains(t, err.Error(), "apply-remote-2")
		assert.Contains(t, err.Error(), `deployment "west"`)
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
