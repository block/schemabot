package tern

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// persistDeploymentIDHarness builds a bare GRPCClient over mock storage with a
// multi-operation apply whose claimed operation and sibling rows are supplied
// by the test, so persistRemoteApplyID can be exercised directly against the
// deployment-shared remote apply id invariant.
func persistDeploymentIDHarness(ops map[int64]*storage.ApplyOperation) (*GRPCClient, *storage.Apply, *mockApplyOperationStore) {
	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-deployment-id",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	operationStore := &mockApplyOperationStore{ops: ops}
	client := &GRPCClient{storage: &mockStorage{
		applies:    &mockApplyStore{apply: apply},
		operations: operationStore,
	}}
	return client, apply, operationStore
}

// A deployment's sibling operations all attach into the deployment's one
// data-plane apply, so a dispatch result carrying the id the siblings already
// recorded persists cleanly on the claimed operation.
func TestPersistRemoteApplyIDAcceptsDeploymentSharedID(t *testing.T) {
	client, apply, operationStore := persistDeploymentIDHarness(map[int64]*storage.ApplyOperation{
		41: {ID: 41, ApplyID: 7, Deployment: "west", OperationKey: "ns/-80/users", ExternalID: "apply-remote-west"},
		42: {ID: 42, ApplyID: 7, Deployment: "west", OperationKey: "ns/80-/users"},
	})
	scope := applyTaskScope{multiOperation: true, operation: operationStore.ops[42]}

	require.NoError(t, client.persistRemoteApplyID(t.Context(), apply, scope, "apply-remote-west", "902"))

	assert.Equal(t, "apply-remote-west", operationStore.ops[42].ExternalID)
	assert.Equal(t, "902", operationStore.ops[42].ExternalOperationID)
	assert.Empty(t, apply.ExternalID, "multi-operation dispatch must not write the parent apply external_id")
}

// A dispatch result whose remote apply id disagrees with the id the
// deployment's siblings recorded is refused fail-closed: recording it would
// correlate one deployment to two data-plane applies.
func TestPersistRemoteApplyIDRefusesSecondRemoteApplyForDeployment(t *testing.T) {
	client, apply, operationStore := persistDeploymentIDHarness(map[int64]*storage.ApplyOperation{
		41: {ID: 41, ApplyID: 7, Deployment: "west", OperationKey: "ns/-80/users", ExternalID: "apply-remote-west"},
		42: {ID: 42, ApplyID: 7, Deployment: "west", OperationKey: "ns/80-/users"},
	})
	scope := applyTaskScope{multiOperation: true, operation: operationStore.ops[42]}

	err := client.persistRemoteApplyID(t.Context(), apply, scope, "apply-remote-other", "903")

	require.Error(t, err)
	assert.Contains(t, err.Error(), `"apply-remote-west"`)
	assert.Contains(t, err.Error(), `"apply-remote-other"`)
	assert.Empty(t, operationStore.ops[42].ExternalID, "a refused remote apply id must not be persisted")
	assert.Empty(t, operationStore.ops[42].ExternalOperationID)
}

// Sibling deployments of one apply own their own remote applies, so a
// different remote apply id on another deployment's operation never blocks
// this deployment's persist.
func TestPersistRemoteApplyIDIgnoresOtherDeploymentsRemoteIDs(t *testing.T) {
	client, apply, operationStore := persistDeploymentIDHarness(map[int64]*storage.ApplyOperation{
		41: {ID: 41, ApplyID: 7, Deployment: "east", OperationKey: "", ExternalID: "apply-remote-east"},
		42: {ID: 42, ApplyID: 7, Deployment: "west", OperationKey: ""},
	})
	scope := applyTaskScope{multiOperation: true, operation: operationStore.ops[42]}

	require.NoError(t, client.persistRemoteApplyID(t.Context(), apply, scope, "apply-remote-west", ""))

	assert.Equal(t, "apply-remote-west", operationStore.ops[42].ExternalID)
	assert.Equal(t, "apply-remote-east", operationStore.ops[41].ExternalID,
		"the sibling deployment's remote id must be untouched")
}

// When the deployment's siblings already disagree with each other, the planes
// have diverged before this dispatch; recording a third id is refused until an
// operator reconciles them.
func TestPersistRemoteApplyIDRefusesWhenSiblingsAlreadyDiverged(t *testing.T) {
	client, apply, operationStore := persistDeploymentIDHarness(map[int64]*storage.ApplyOperation{
		40: {ID: 40, ApplyID: 7, Deployment: "west", OperationKey: "ns/-80/users", ExternalID: "apply-remote-a"},
		41: {ID: 41, ApplyID: 7, Deployment: "west", OperationKey: "ns/80-/users", ExternalID: "apply-remote-b"},
		42: {ID: 42, ApplyID: 7, Deployment: "west", OperationKey: "ns/group_finalizer"},
	})
	scope := applyTaskScope{multiOperation: true, operation: operationStore.ops[42]}

	err := client.persistRemoteApplyID(t.Context(), apply, scope, "apply-remote-a", "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "more than one remote apply")
	assert.Empty(t, operationStore.ops[42].ExternalID)
}

// An idempotent replay whose id matches the claimed operation's own recorded
// remote apply id is still refused when a sibling of the same deployment pins
// a different one: the own-row match proves nothing about the deployment, and
// re-recording the id would leave the deployment correlated to two remote
// applies.
func TestPersistRemoteApplyIDRefusesIdempotentReplayWhenSiblingDiverged(t *testing.T) {
	client, apply, operationStore := persistDeploymentIDHarness(map[int64]*storage.ApplyOperation{
		41: {ID: 41, ApplyID: 7, Deployment: "west", OperationKey: "ns/-80/users", ExternalID: "apply-remote-west"},
		42: {ID: 42, ApplyID: 7, Deployment: "west", OperationKey: "ns/80-/users", ExternalID: "apply-remote-other"},
	})
	scope := applyTaskScope{multiOperation: true, operation: operationStore.ops[42]}

	err := client.persistRemoteApplyID(t.Context(), apply, scope, "apply-remote-other", "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), `"apply-remote-west"`)
	assert.Contains(t, err.Error(), `"apply-remote-other"`)
	assert.Equal(t, "apply-remote-other", operationStore.ops[42].ExternalID,
		"a refused replay must not clear the operation's recorded id")
}

// A sibling dispatch can persist between this dispatch's unlocked guard read
// and its write, so the store re-verifies the deployment invariant inside the
// writing transaction; its refusal surfaces as the same fail-closed error.
func TestPersistRemoteApplyIDSurfacesStorageDeploymentConflict(t *testing.T) {
	client, apply, operationStore := persistDeploymentIDHarness(map[int64]*storage.ApplyOperation{
		42: {ID: 42, ApplyID: 7, Deployment: "west", OperationKey: "ns/80-/users"},
	})
	operationStore.saveErr = fmt.Errorf("deployment %q of apply 7 already correlates to remote apply %q; refusing to store %q for apply_operation 42: %w",
		"west", "apply-remote-west", "apply-remote-other", storage.ErrRemoteApplyDeploymentIDConflict)
	scope := applyTaskScope{multiOperation: true, operation: operationStore.ops[42]}

	err := client.persistRemoteApplyID(t.Context(), apply, scope, "apply-remote-other", "")

	require.ErrorIs(t, err, storage.ErrRemoteApplyDeploymentIDConflict)
	assert.Empty(t, operationStore.ops[42].ExternalID, "a refused remote apply id must not be persisted")
}

// A sibling that recorded its remote apply id in the legacy engine resume
// context carrier still pins the deployment's remote apply, so a disagreeing
// dispatch result is refused the same way.
func TestPersistRemoteApplyIDHonorsLegacyResumeContextCarrier(t *testing.T) {
	client, apply, operationStore := persistDeploymentIDHarness(map[int64]*storage.ApplyOperation{
		41: {ID: 41, ApplyID: 7, Deployment: "west", OperationKey: "ns/-80/users", EngineResumeContext: "apply-remote-west"},
		42: {ID: 42, ApplyID: 7, Deployment: "west", OperationKey: "ns/80-/users"},
	})
	scope := applyTaskScope{multiOperation: true, operation: operationStore.ops[42]}

	err := client.persistRemoteApplyID(t.Context(), apply, scope, "apply-remote-other", "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), `"apply-remote-west"`)
	assert.Empty(t, operationStore.ops[42].ExternalID)
}
