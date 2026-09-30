package tern

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// memberTargetFixture is one apply whose deployment "default" addresses two
// targets, payments-001 and payments-002, each owning one whole-target
// operation keyed by its target and one task.
type memberTargetFixture struct {
	apply      *storage.Apply
	operations *mockApplyOperationStore
	first      int64
	second     int64
}

func newMemberTargetFixture(t *testing.T, client *GRPCClient) memberTargetFixture {
	t.Helper()
	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-two-targets",
		PlanID:          99,
		Database:        "payments",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "payments-001"})
	first, second := int64(41), int64(42)
	task := func(id int64, identifier string, operationID *int64) *storage.Task {
		return &storage.Task{
			ID: id, TaskIdentifier: identifier, ApplyID: apply.ID, ApplyOperationID: operationID,
			TableName: "orders", Namespace: "payments",
			DDL: "ALTER TABLE orders ADD COLUMN note varchar(255)", DDLAction: "alter", State: state.Task.Pending,
		}
	}
	operations := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		first:  {ID: first, ApplyID: apply.ID, Deployment: "default", Target: "payments-001", OperationKey: "payments-001", OperationKind: storage.ApplyOperationKindWork, State: state.ApplyOperation.Pending},
		second: {ID: second, ApplyID: apply.ID, Deployment: "default", Target: "payments-002", OperationKey: "payments-002", OperationKind: storage.ApplyOperationKindWork, State: state.ApplyOperation.Pending},
	}}
	client.storage = &mockStorage{
		applies:    &mockApplyStore{apply: apply},
		tasks:      &mockTaskStore{tasks: []*storage.Task{task(11, "task-orders-001", &first), task(12, "task-orders-002", &second)}},
		plans:      &mockPlanStore{plan: &storage.Plan{ID: apply.PlanID, PlanIdentifier: "plan-two-targets"}},
		operations: operations,
	}
	return memberTargetFixture{apply: apply, operations: operations, first: first, second: second}
}

// Two targets of one deployment are two rollout members, and a data-plane
// apply drives one target, so each dispatches to its own remote apply: the
// dispatches carry distinct idempotency keys, each declares only its own
// target's operation key as the generation manifest, and each names its
// target so the data plane derives the operation key the planner stored. Each
// operation then records its own remote apply id, which the member-scoped
// guard accepts instead of refusing the second as a deployment's second apply.
func TestGRPCClient_SiblingTargetsOfOneDeploymentDispatchTheirOwnRemoteApplies(t *testing.T) {
	server := &capturingTernServer{remoteApplyID: "remote-001"}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	fx := newMemberTargetFixture(t, client)

	drive := func(operationID int64) *ternv1.ApplyRequest {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		require.NoError(t, client.ResumeApplyOperation(ctx, fx.apply, operationID))
		req := server.getApplyRequest()
		require.NotNil(t, req, "operation %d must dispatch to the data plane", operationID)
		return req
	}

	firstReq := drive(fx.first)
	server.mu.Lock()
	server.remoteApplyID = "remote-002"
	server.mu.Unlock()
	secondReq := drive(fx.second)

	assert.NotEqual(t, firstReq.IdempotencyKey, secondReq.IdempotencyKey,
		"sibling targets must not share a key, or the data plane replays the first target's apply as the second's")
	assert.Equal(t, []string{"payments-001"}, firstReq.GenerationOperationKeys)
	assert.Equal(t, []string{"payments-002"}, secondReq.GenerationOperationKeys,
		"a target's manifest must not declare its sibling target, whose work never arrives at this remote apply")
	assert.Equal(t, "payments-001", firstReq.Options[dispatchMemberTargetOption])
	assert.Equal(t, "payments-002", secondReq.Options[dispatchMemberTargetOption])

	assert.Equal(t, "remote-001", fx.operations.ops[fx.first].ExternalID)
	assert.Equal(t, "remote-002", fx.operations.ops[fx.second].ExternalID,
		"each target records its own remote apply id")
	assert.Empty(t, fx.apply.ExternalID, "a multi-operation dispatch must not write the parent apply external_id")
}

// A data plane that predates member targets derives an empty key for a
// whole-target dispatch and echoes it. The control plane derives the target
// from the request it sent, so the empty echo is refused: the response's
// remote ids are never persisted, and the dispatch fails for an operator
// instead of being tracked as this target's apply.
func TestGRPCClient_MemberTargetDispatchFailsClosedAgainstDataPlaneWithoutTargetKeys(t *testing.T) {
	server := &capturingTernServer{remoteApplyID: "remote-old", omitOperationKey: true}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	fx := newMemberTargetFixture(t, client)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, fx.apply, fx.second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `echoed operation key "", expected "payments-002"`)

	require.NotNil(t, server.getApplyRequest(), "the dispatch itself must have been sent")
	assert.Empty(t, fx.operations.ops[fx.second].ExternalID, "the unverified response's remote apply id must not be persisted")
	assert.Empty(t, fx.apply.ExternalID)
}

// A single-target apply and a deployments-map apply key their dispatches
// exactly as they always have, so an apply in flight across an upgrade
// re-dispatches under the key it first used and resolves to its existing
// remote apply. Only a member target adds to the key.
func TestRemoteApplyIdempotencyKey_MemberTarget(t *testing.T) {
	apply := &storage.Apply{ApplyIdentifier: "apply-abc123"}
	legacyKey := func(parts ...string) string {
		sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
		return "schemabot:v1:" + hex.EncodeToString(sum[:])
	}
	scope := func(target, memberTarget string) applyTaskScope {
		return applyTaskScope{
			applyOperationID: 1,
			operation:        &storage.ApplyOperation{Deployment: "default", Target: target, OperationKey: "orders/-80/orders"},
			multiOperation:   true,
			memberTarget:     memberTarget,
		}
	}

	assert.Equal(t,
		legacyKey("schemabot-remote-apply-v1", "apply-abc123", "deployment", "default", "0"),
		remoteApplyIdempotencyKey(apply, scope("payments-001", "")),
		"a deployment addressing one target keeps its deployment key, whatever target its rows name")
	assert.Equal(t,
		legacyKey("schemabot-remote-apply-v1", "apply-abc123", "whole", "0"),
		remoteApplyIdempotencyKey(apply, wholeApplyTaskScope()),
		"a whole-apply drive keeps its whole-apply key")

	first := remoteApplyIdempotencyKey(apply, scope("payments-001", "payments-001"))
	second := remoteApplyIdempotencyKey(apply, scope("payments-002", "payments-002"))
	assert.NotEqual(t, first, second, "sibling targets of one deployment key separately")
	assert.NotEqual(t, remoteApplyIdempotencyKey(apply, scope("payments-001", "")), first,
		"a member target is part of the key")
	assert.Equal(t, first, remoteApplyIdempotencyKey(apply, scope("payments-001", "payments-001")),
		"a member's key is stable across re-dispatch")
}

// The claim-time scope decides the member from the apply's operation rows: a
// deployment addressing several targets makes each target its own member, with
// its own manifest, while a deployment addressing one target stays one member
// and names no member target. A row of a multi-target deployment that names no
// target has no member and is refused rather than dispatched under a guess.
func TestLoadOperationApplyTaskScope_MemberTarget(t *testing.T) {
	ops := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		1: {ID: 1, ApplyID: 100, Deployment: "default", Target: "payments-001", OperationKey: "payments-001"},
		2: {ID: 2, ApplyID: 100, Deployment: "default", Target: "payments-002", OperationKey: "payments-002"},
		3: {ID: 3, ApplyID: 100, Deployment: "east", Target: "payments-east", OperationKey: "payments/-80/orders"},
		4: {ID: 4, ApplyID: 100, Deployment: "east", Target: "payments-east", OperationKey: "payments/80-/orders"},
	}}
	client := &GRPCClient{storage: &mockStorage{operations: ops}}
	apply := &storage.Apply{ID: 100, ApplyIdentifier: "apply-members"}

	scope, err := client.loadOperationApplyTaskScope(t.Context(), apply, 2)
	require.NoError(t, err)
	assert.Equal(t, "payments-002", scope.memberTarget)
	assert.Equal(t, []string{"payments-002"}, scope.generationOperationKeys())

	east, err := client.loadOperationApplyTaskScope(t.Context(), apply, 3)
	require.NoError(t, err)
	assert.Empty(t, east.memberTarget, "a single-target deployment names no member target")
	assert.Equal(t, []string{"payments/-80/orders", "payments/80-/orders"}, east.generationOperationKeys())

	ops.ops[5] = &storage.ApplyOperation{ID: 5, ApplyID: 100, Deployment: "default", OperationKey: "orphan"}
	_, err = client.loadOperationApplyTaskScope(t.Context(), apply, 5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "names no target")
}

// Both planes derive a dispatch's operation key from the request with the same
// helpers. A whole-target dispatch naming a member target derives the target
// itself, which is the key the planner stored; one naming none derives the
// empty key it always has. A shard or finalizer dispatch naming a member target
// is refused, and so is a target that could not be split back out of a key.
func TestOperationIdentityForDispatch_MemberTarget(t *testing.T) {
	plan := &storage.Plan{PlanIdentifier: "plan-members"}
	changes := []*ternv1.TableChange{{
		Namespace:  "payments",
		TableName:  "orders",
		Ddl:        "ALTER TABLE `orders` ADD COLUMN `note` varchar(255)",
		ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
	}}
	derive := func(req *ternv1.ApplyRequest) (string, error) {
		scope, err := deriveDispatchScope(plan, req)
		if err != nil {
			return "", err
		}
		key, _, err := operationIdentityForDispatch(scope)
		return key, err
	}

	key, err := derive(&ternv1.ApplyRequest{DdlChanges: changes, Options: map[string]string{dispatchMemberTargetOption: "payments-001"}})
	require.NoError(t, err)
	assert.Equal(t, "payments-001", key)

	key, err = derive(&ternv1.ApplyRequest{DdlChanges: changes})
	require.NoError(t, err)
	assert.Empty(t, key, "a dispatch naming no member target keeps the empty whole-deployment key")

	_, err = derive(&ternv1.ApplyRequest{DdlChanges: changes, TargetShards: []string{"-80"}, Options: map[string]string{dispatchMemberTargetOption: "payments-001"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only whole-target work is keyed by target")

	_, err = derive(&ternv1.ApplyRequest{DdlChanges: changes, Options: map[string]string{dispatchMemberTargetOption: "payments/001"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "operation key delimiter")
}
