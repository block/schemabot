package tern

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// An operation's progress and cutover calls address its own remote operation
// whenever its deployment runs sibling operations in the same remote apply, and
// the whole remote apply only when the operation is the deployment's one.
func TestApplyTaskScope_RemoteOperationScope(t *testing.T) {
	shard := func(externalOperationID string) *storage.ApplyOperation {
		return &storage.ApplyOperation{
			ID: 41, ApplyID: 7, Deployment: "commerce", OperationKey: "commerce/-80/users",
			ExternalID: "remote-commerce", ExternalOperationID: externalOperationID,
		}
	}
	siblingKeys := []string{"commerce/-80/users", "commerce/80-/users"}

	tests := []struct {
		name          string
		scope         applyTaskScope
		wantScope     string
		wantErr       string
		wantUnscoped  bool
		wantsSiblings bool
	}{
		{
			name:          "a shard whose deployment runs sibling shards is scoped to its own remote operation",
			scope:         applyTaskScope{multiOperation: true, operation: shard("901"), deploymentOperationKeys: siblingKeys},
			wantScope:     "901",
			wantsSiblings: true,
		},
		{
			name:          "a shard dispatched without a recorded remote operation id addresses the whole remote apply",
			scope:         applyTaskScope{multiOperation: true, operation: shard(""), deploymentOperationKeys: siblingKeys},
			wantScope:     "",
			wantUnscoped:  true,
			wantsSiblings: true,
		},
		{
			name:      "the deployment's only operation addresses the whole remote apply",
			scope:     applyTaskScope{multiOperation: true, operation: shard("901"), deploymentOperationKeys: []string{"commerce/-80/users"}},
			wantScope: "",
		},
		{
			name:      "a whole-apply drive addresses the whole remote apply",
			scope:     wholeApplyTaskScope(),
			wantScope: "",
		},
		{
			name: "a rollout member target is scoped to its own remote operation",
			scope: applyTaskScope{multiOperation: true, memberTarget: "payments-002", deploymentOperationKeys: []string{"payments-001", "payments-002"},
				operation: &storage.ApplyOperation{ID: 42, ApplyID: 7, Deployment: "default", Target: "payments-002", OperationKey: "payments-002", ExternalID: "remote-default", ExternalOperationID: "902"}},
			wantScope:     "902",
			wantsSiblings: true,
		},
		{
			name: "a rollout member target with no recorded remote operation id is refused",
			scope: applyTaskScope{multiOperation: true, memberTarget: "payments-002", deploymentOperationKeys: []string{"payments-001", "payments-002"},
				operation: &storage.ApplyOperation{ID: 42, ApplyID: 7, Deployment: "default", Target: "payments-002", OperationKey: "payments-002", ExternalID: "remote-default"}},
			wantErr:       "rollout member target payments-002 (apply_operation 42) records remote apply remote-default but no remote operation id",
			wantsSiblings: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantsSiblings, tt.scope.sharesRemoteApplyWithSiblings())
			got, err := tt.scope.remoteOperationScope()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantScope, got)
			if tt.scope.operation != nil {
				assert.Equal(t, tt.wantUnscoped, tt.scope.dispatchedWithoutRemoteOperationID())
			}
		})
	}
}

// Two shards of one deployment run as sibling operations of one remote apply,
// each with its own remote operation there. Each shard's drive records the
// remote operation its dispatch was answered with and polls that operation
// alone, so a shard's progress and outcome come from its own tasks rather than
// from whichever sibling the whole remote apply reports.
func TestGRPCClient_SiblingShardOperationsPollTheirOwnRemoteOperation(t *testing.T) {
	server := &capturingTernServer{
		remoteApplyID:     "remote-shared-1",
		remoteOperationID: "901",
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "commerce",
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-sharded",
		PlanID:          99,
		Database:        "commerce",
		DatabaseType:    storage.DatabaseTypeStrata,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "commerce-target"})
	opA, opB := int64(41), int64(42)
	taskA := &storage.Task{
		ID: 11, TaskIdentifier: "task-users-a", ApplyID: apply.ID, ApplyOperationID: &opA,
		TableName: "users", Shard: "-80", Namespace: "commerce",
		DDL: "ALTER TABLE users ADD COLUMN email varchar(255)", DDLAction: "alter", State: state.Task.Pending,
	}
	taskB := &storage.Task{
		ID: 12, TaskIdentifier: "task-users-b", ApplyID: apply.ID, ApplyOperationID: &opB,
		TableName: "users", Shard: "80-", Namespace: "commerce",
		DDL: "ALTER TABLE users ADD COLUMN email varchar(255)", DDLAction: "alter", State: state.Task.Pending,
	}
	operationStore := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		opA: {ID: opA, ApplyID: apply.ID, Deployment: "commerce-deployment", OperationKey: "commerce/-80/users", OperationKind: storage.ApplyOperationKindWork, State: state.ApplyOperation.Pending},
		opB: {ID: opB, ApplyID: apply.ID, Deployment: "commerce-deployment", OperationKey: "commerce/80-/users", OperationKind: storage.ApplyOperationKindWork, State: state.ApplyOperation.Pending},
	}}
	client.storage = &mockStorage{
		applies:    &mockApplyStore{apply: apply},
		tasks:      &mockTaskStore{tasks: []*storage.Task{taskA, taskB}},
		plans:      &mockPlanStore{plan: &storage.Plan{ID: apply.PlanID, PlanIdentifier: "plan-sharded"}},
		operations: operationStore,
	}
	drive := func(operationID int64) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		require.NoError(t, client.ResumeApplyOperation(ctx, apply, operationID))
	}

	drive(opA)
	assert.Equal(t, "901", operationStore.ops[opA].ExternalOperationID, "the shard records the remote operation its dispatch was answered with")
	progressA := server.getProgressRequest()
	require.NotNil(t, progressA, "the first shard's drive polls the remote apply")
	assert.Equal(t, "remote-shared-1", progressA.ApplyId)
	assert.Equal(t, "901", progressA.ApplyOperationId, "the first shard polls its own remote operation, not the apply its sibling shares")

	server.mu.Lock()
	server.remoteOperationID = "902"
	server.mu.Unlock()
	drive(opB)
	assert.Equal(t, "902", operationStore.ops[opB].ExternalOperationID)
	progressB := server.getProgressRequest()
	require.NotNil(t, progressB)
	assert.Equal(t, "remote-shared-1", progressB.ApplyId, "both shards share the deployment's one remote apply")
	assert.Equal(t, "902", progressB.ApplyOperationId, "the sibling shard polls its own remote operation")
}

// A shard's progress comes from its own remote operation, so a data plane that
// answers for the whole remote apply instead is refused rather than read as the
// shard's state: the whole apply carries the sibling shards' tasks, which match
// this shard's by table and DDL.
func TestGRPCClient_ShardProgressAnsweredForTheWholeApplyIsRefused(t *testing.T) {
	server := &capturingTernServer{omitProgressScope: true}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-sharded", Database: "commerce", DatabaseType: storage.DatabaseTypeStrata, Environment: "staging", State: state.Apply.Running}
	scope := applyTaskScope{
		applyOperationID:        41,
		multiOperation:          true,
		deploymentOperationKeys: []string{"commerce/-80/users", "commerce/80-/users"},
		operation:               &storage.ApplyOperation{ID: 41, ApplyID: 7, Deployment: "commerce-deployment", OperationKey: "commerce/-80/users", ExternalID: "remote-shared-1", ExternalOperationID: "901"},
	}

	_, err := client.remoteProgress(t.Context(), apply, scope, "remote-shared-1")

	require.ErrorContains(t, err, `remote progress for apply remote-shared-1 operation 901 came back scoped to ""`)
	progressReq := server.getProgressRequest()
	require.NotNil(t, progressReq)
	assert.Equal(t, "901", progressReq.ApplyOperationId)
}
