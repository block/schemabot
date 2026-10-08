package tern

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/inventory"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// memberTargetFixture is one apply whose deployment "default" addresses two
// targets, payments-001 and payments-002, each owning one whole-target
// operation keyed by its target and one task. The apply is created from
// payments-001's reviewed plan; payments-002 was planned against its own live
// schema, so its operation runs its own member plan, whose DDL differs.
type memberTargetFixture struct {
	apply      *storage.Apply
	operations *mockApplyOperationStore
	plans      *mockPlanStore
	first      int64
	second     int64
}

const (
	memberFixtureFirstDDL  = "ALTER TABLE `orders` ADD COLUMN `note` varchar(255)"
	memberFixtureSecondDDL = "ALTER TABLE `orders` ADD COLUMN `note` varchar(255), ADD INDEX `idx_note` (`note`)"
)

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
	const secondPlanID = int64(100)
	task := func(id int64, identifier, ddl string, planID int64, operationID *int64) *storage.Task {
		return &storage.Task{
			ID: id, TaskIdentifier: identifier, ApplyID: apply.ID, ApplyOperationID: operationID, PlanID: planID,
			TableName: "orders", Namespace: "payments",
			DDL: ddl, DDLAction: "alter", State: state.Task.Pending,
		}
	}
	operations := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		first:  {ID: first, ApplyID: apply.ID, Deployment: "default", Target: "payments-001", OperationKey: "payments-001", OperationKind: storage.ApplyOperationKindWork, State: state.ApplyOperation.Pending},
		second: {ID: second, ApplyID: apply.ID, PlanID: secondPlanID, Deployment: "default", Target: "payments-002", OperationKey: "payments-002", OperationKind: storage.ApplyOperationKindWork, State: state.ApplyOperation.Pending},
	}}
	plans := &mockPlanStore{byID: map[int64]*storage.Plan{
		apply.PlanID: {ID: apply.PlanID, PlanIdentifier: "plan-two-targets", Database: "payments", DatabaseType: storage.DatabaseTypeMySQL, Deployment: "default", Target: "payments-001", Environment: "staging"},
		secondPlanID: {ID: secondPlanID, PlanIdentifier: "plan-member-002", Database: "payments", DatabaseType: storage.DatabaseTypeMySQL, Deployment: "default", Target: "payments-002", Environment: "staging"},
	}}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks: &mockTaskStore{tasks: []*storage.Task{
			task(11, "task-orders-001", memberFixtureFirstDDL, apply.PlanID, &first),
			task(12, "task-orders-002", memberFixtureSecondDDL, secondPlanID, &second),
		}},
		plans:      plans,
		operations: operations,
	}
	return memberTargetFixture{apply: apply, operations: operations, plans: plans, first: first, second: second}
}

// The targets of one deployment share its one remote apply, exactly as the
// shards of a Vitess deployment do. Both dispatches carry the deployment's
// idempotency key and declare both targets' operation keys as the generation
// manifest, so the first creates the remote apply and the second attaches to
// it. Each names its target, so the data plane derives a distinct,
// target-qualified operation key for each and the two never resolve to the
// same remote operation. Both operations record the one remote apply id, and
// each records the remote operation id the data plane gave its own operation.
func TestGRPCClient_SiblingTargetsOfOneDeploymentShareItsRemoteApply(t *testing.T) {
	server := &capturingTernServer{remoteApplyID: "remote-payments", remoteOperationID: "remote-op-001"}
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
	server.remoteOperationID = "remote-op-002"
	server.mu.Unlock()
	secondReq := drive(fx.second)

	assert.Equal(t, firstReq.IdempotencyKey, secondReq.IdempotencyKey,
		"sibling targets share the deployment's key, and so its one remote apply")
	assert.Equal(t, []string{"payments-001", "payments-002"}, firstReq.GenerationOperationKeys)
	assert.Equal(t, []string{"payments-001", "payments-002"}, secondReq.GenerationOperationKeys,
		"the manifest declares every target's operation, so the shared remote apply waits for both")
	assert.Equal(t, "payments-001", firstReq.Options[dispatchMemberTargetOption])
	assert.Equal(t, "payments-002", secondReq.Options[dispatchMemberTargetOption])

	firstKey, err := dispatchOperationKey(fx.plans.byID[fx.apply.PlanID], firstReq)
	require.NoError(t, err)
	secondKey, err := dispatchOperationKey(fx.plans.byID[fx.operations.ops[fx.second].PlanID], secondReq)
	require.NoError(t, err)
	assert.Equal(t, "payments-001", firstKey)
	assert.Equal(t, "payments-002", secondKey, "each target derives its own operation key, so it attaches its own operation")

	first, second := fx.operations.ops[fx.first], fx.operations.ops[fx.second]
	assert.Equal(t, "remote-payments", first.ExternalID)
	assert.Equal(t, "remote-payments", second.ExternalID, "both targets record the deployment's one remote apply")
	assert.Equal(t, "remote-op-001", first.ExternalOperationID)
	assert.Equal(t, "remote-op-002", second.ExternalOperationID, "each target records its own remote operation")
	assert.Empty(t, fx.apply.ExternalID, "a multi-operation dispatch must not write the parent apply external_id")
}

// dispatchOperationKey derives a dispatch's operation key the way both planes
// do: from the dispatch's plan and request shape.
func dispatchOperationKey(plan *storage.Plan, req *ternv1.ApplyRequest) (string, error) {
	scope, err := deriveDispatchScope(plan, req)
	if err != nil {
		return "", err
	}
	key, _, err := operationIdentityForDispatch(scope)
	return key, err
}

// Each target of a deployment that addresses several is dispatched to the one
// data plane serving all of them, whose router picks the client, and so the
// database, that runs the dispatch's DDL. payments-002 was planned against its
// own live schema, so its member plan was minted on the control plane and never
// stored on the data plane: the router finds no plan to route by and routes on
// the request's target. The apply row names the reviewed plan's target,
// payments-001, so the drive must reach the dispatch through the routing
// client, which scopes the apply to the operation's own target. Driven that
// way — the only way the operator drives an operation — payments-002's
// dispatch carries its own plan and DDL and lands on payments-002, and the
// primary's lands on payments-001 by its stored plan.
func TestMemberTargetDispatchRoutesToItsOwnTargetOnTheDataPlane(t *testing.T) {
	server := &capturingTernServer{remoteApplyID: "remote-member", remoteOperationID: "remote-op-member"}
	grpcClient, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	fx := newMemberTargetFixture(t, grpcClient)

	routingClient, err := NewRoutingClient(RoutingClientConfig{
		Resolver: routingResolverFunc(func(context.Context, routing.Request) ([]routing.ExecutionTarget, error) {
			return nil, fmt.Errorf("an operation drive routes by its stored operation, never by config")
		}),
		PlanLookup:           routingPlanLookup{},
		ApplyLookup:          routingApplyLookup{},
		ApplyOperationLookup: fx.operations,
		ClientForDeployment: func(_ context.Context, deployment, _ string) (Client, error) {
			if deployment != "default" {
				return nil, fmt.Errorf("unexpected deployment %q", deployment)
			}
			return grpcClient, nil
		},
	})
	require.NoError(t, err)

	resolver, err := inventory.NewStaticResolver(inventory.StaticConfig{Targets: map[string]inventory.StaticTarget{
		"payments-001": {DatabaseType: storage.DatabaseTypeMySQL, DSN: "root@tcp(10.0.0.1:3306)/"},
		"payments-002": {DatabaseType: storage.DatabaseTypeMySQL, DSN: "root@tcp(10.0.0.2:3306)/"},
	}})
	require.NoError(t, err)
	dataPlanePlans := targetRouterPlanStore{byIdentifier: map[string]*storage.Plan{
		"plan-two-targets": {PlanIdentifier: "plan-two-targets", Database: "payments", DatabaseType: storage.DatabaseTypeMySQL, Target: "payments-001", Environment: "staging"},
	}}

	for _, tc := range []struct {
		name        string
		operationID int64
		target      string
		dsn         string
		planID      string
		ddl         string
	}{
		{name: "primary", operationID: fx.first, target: "payments-001", dsn: "root@tcp(10.0.0.1:3306)/", planID: "plan-two-targets", ddl: memberFixtureFirstDDL},
		{name: "independently planned member", operationID: fx.second, target: "payments-002", dsn: "root@tcp(10.0.0.2:3306)/", planID: "plan-member-002", ddl: memberFixtureSecondDDL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			require.NoError(t, routingClient.ResumeApplyOperation(ctx, fx.apply, tc.operationID))
			req := server.getApplyRequest()
			require.NotNil(t, req, "operation %d must dispatch to the data plane", tc.operationID)

			assert.Equal(t, tc.planID, req.PlanId, "the dispatch must carry its own member's plan")
			assert.Equal(t, tc.target, req.Target, "the dispatch must name its own target, not the reviewed plan's")
			assert.Equal(t, tc.target, req.Options[dispatchMemberTargetOption])
			require.Len(t, req.DdlChanges, 1)
			assert.Equal(t, tc.ddl, req.DdlChanges[0].Ddl)

			created := make(map[string]*targetRouterRecordingClient)
			router := newTargetRouterForTest(t, resolver, nil, dataPlanePlans, created)
			_, err := router.Apply(ctx, req)
			require.NoError(t, err)
			require.Len(t, created, 1)
			for _, routed := range created {
				assert.Equal(t, tc.dsn, routed.targetDSN, "the dispatch must run on its own target's database")
				require.NotNil(t, routed.applyReq)
				assert.Equal(t, tc.target, routed.applyReq.Target)
				assert.Equal(t, tc.planID, routed.applyReq.PlanId)
				require.Len(t, routed.applyReq.DdlChanges, 1)
				assert.Equal(t, tc.ddl, routed.applyReq.DdlChanges[0].Ddl)
			}
		})
	}
	assert.Equal(t, "payments-001", fx.apply.GetOptions().Target, "the drive scopes a copy of the apply, never the stored row")
}

// A data plane that predates member targets ignores the target the dispatch
// names and derives the empty whole-deployment key, which for the second
// target resolves to the first target's operation of the shared remote apply.
// The control plane derives the target from the request it sent, so the echo
// is refused: the response's remote ids are never persisted, and the dispatch
// fails for an operator instead of tracking one target's operation as
// another's.
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
	assert.Empty(t, fx.operations.ops[fx.second].ExternalOperationID)
	assert.Empty(t, fx.apply.ExternalID)
}

// A response that answers payments-002's dispatch with payments-001's
// operation of the shared remote apply is refused: the echoed key names the
// sibling target, not the one this dispatch drives, so neither the remote
// apply id nor the sibling's remote operation id is recorded on payments-002.
func TestGRPCClient_MemberTargetDispatchRefusesASiblingTargetsEcho(t *testing.T) {
	server := &capturingTernServer{remoteApplyID: "remote-payments", remoteOperationID: "remote-op-001", echoOperationKey: "payments-001"}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	fx := newMemberTargetFixture(t, client)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, fx.apply, fx.second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `echoed operation key "payments-001", expected "payments-002"`)

	second := fx.operations.ops[fx.second]
	assert.Empty(t, second.ExternalID, "a response addressing the sibling target's operation must not be persisted")
	assert.Empty(t, second.ExternalOperationID, "the sibling target's remote operation id must never be recorded as this target's")
}

// The targets of one deployment key their generation-zero dispatches on the
// deployment alone, exactly as its shards do, so they land on one remote
// apply; that key is the one a single-target deployment has always used, and a
// whole-apply drive keeps its own. A deliberate retry (the operation's attempt
// above zero) keys on the operation, so each target's retry gets a remote
// apply of its own and never lands on the other target's.
func TestRemoteApplyIdempotencyKey_MemberTarget(t *testing.T) {
	apply := &storage.Apply{ApplyIdentifier: "apply-abc123"}
	legacyKey := func(parts ...string) string {
		sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
		return "schemabot:v1:" + hex.EncodeToString(sum[:])
	}
	scope := func(target, operationKey, memberTarget string, attempt int) applyTaskScope {
		return applyTaskScope{
			applyOperationID: 1,
			operation:        &storage.ApplyOperation{Deployment: "default", Target: target, OperationKey: operationKey, Attempt: attempt},
			multiOperation:   true,
			memberTarget:     memberTarget,
		}
	}
	deploymentKey := legacyKey("schemabot-remote-apply-v1", "apply-abc123", "deployment", "default", "0")

	assert.Equal(t, deploymentKey, remoteApplyIdempotencyKey(apply, scope("payments-001", "payments-001", "payments-001", 0)))
	assert.Equal(t, deploymentKey, remoteApplyIdempotencyKey(apply, scope("payments-002", "payments-002", "payments-002", 0)),
		"sibling targets share the deployment's key, and so its one remote apply")
	assert.Equal(t, deploymentKey, remoteApplyIdempotencyKey(apply, scope("payments-001", "orders/-80/orders", "", 0)),
		"a shard of a single-target deployment keys on the deployment as it always has")
	assert.Equal(t,
		legacyKey("schemabot-remote-apply-v1", "apply-abc123", "whole", "0"),
		remoteApplyIdempotencyKey(apply, wholeApplyTaskScope()),
		"a whole-apply drive keeps its whole-apply key")

	firstRetry := remoteApplyIdempotencyKey(apply, scope("payments-001", "payments-001", "payments-001", 1))
	secondRetry := remoteApplyIdempotencyKey(apply, scope("payments-002", "payments-002", "payments-002", 1))
	assert.Equal(t, legacyKey("schemabot-remote-apply-v1", "apply-abc123", "deployment", "default", "1", "operation", "payments-001"), firstRetry)
	assert.NotEqual(t, firstRetry, secondRetry, "a retried target keys on its own operation")
}

// The claim-time scope decides the member target from the apply's operation
// rows: a deployment addressing several targets names the claimed operation's
// target, and its manifest declares every target's operation that will be
// dispatched, since they all attach to the deployment's one remote apply. A
// target that already held the change is settled at creation and never
// dispatched, so the manifest leaves it out. A deployment addressing one
// target names no member target. A row of a multi-target deployment that names
// no target is refused rather than dispatched under a guessed key.
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
	assert.Equal(t, []string{"payments-001", "payments-002"}, scope.generationOperationKeys())

	east, err := client.loadOperationApplyTaskScope(t.Context(), apply, 3)
	require.NoError(t, err)
	assert.Empty(t, east.memberTarget, "a single-target deployment names no member target")
	assert.Equal(t, []string{"payments/-80/orders", "payments/80-/orders"}, east.generationOperationKeys())

	ops.ops[6] = &storage.ApplyOperation{ID: 6, ApplyID: 100, Deployment: "default", Target: "payments-003", OperationKey: "payments-003",
		State: state.ApplyOperation.Completed, AlreadyConverged: true}
	withConverged, err := client.loadOperationApplyTaskScope(t.Context(), apply, 2)
	require.NoError(t, err)
	assert.Equal(t, "payments-002", withConverged.memberTarget)
	assert.Equal(t, []string{"payments-001", "payments-002"}, withConverged.generationOperationKeys(),
		"a target that already held the change is never dispatched, so the manifest does not wait for it")
	delete(ops.ops, 6)

	ops.ops[5] = &storage.ApplyOperation{ID: 5, ApplyID: 100, Deployment: "default", OperationKey: "orphan"}
	_, err = client.loadOperationApplyTaskScope(t.Context(), apply, 5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "names no target")
}

// Both planes derive a dispatch's operation key from the request with the same
// helpers. A whole-target dispatch naming a member target derives the target
// itself, which is the key the planner stored; one naming none derives the
// empty key it always has. A shard dispatch naming a member target is refused
// (see TestOperationIdentityForDispatch_MemberTargetFinalizer for finalizers),
// and so is a target that could not be split back out of a key,
// and so is a member target the dispatch's plan was not produced for.
func TestOperationIdentityForDispatch_MemberTarget(t *testing.T) {
	plan := &storage.Plan{PlanIdentifier: "plan-members", Target: "payments-001"}
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
	assert.Contains(t, err.Error(), `dispatch for member target "payments-001" is shard-scoped`)

	_, err = derive(&ternv1.ApplyRequest{DdlChanges: changes, Options: map[string]string{dispatchMemberTargetOption: "payments/001"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "operation key delimiter")

	_, err = derive(&ternv1.ApplyRequest{DdlChanges: changes, Options: map[string]string{dispatchMemberTargetOption: "payments-002"}})
	require.Error(t, err, "a target must never run a plan produced for its sibling")
	assert.Contains(t, err.Error(), `runs plan plan-members, which was produced for target "payments-001"`)
}

// The data plane serves the targets of one deployment from one shared apply,
// but each target is its own route with its own connection. payments-001's
// operation is already driving on its route's client when payments-002's
// operation is claimed. The operator scopes that drive to payments-002 (see
// operationScopedApply), and the router must hand it payments-002's client:
// the client that already owns the apply for payments-001 connects to a
// different database. Each target keeps its own owner for the rest of the
// apply, so a later drive of payments-001 returns to its own client.
func TestTargetRouterDrivesEachTargetOfASharedApplyOnItsOwnConnection(t *testing.T) {
	resolver, err := inventory.NewStaticResolver(inventory.StaticConfig{Targets: map[string]inventory.StaticTarget{
		"payments-001": {DatabaseType: storage.DatabaseTypeMySQL, DSN: "root@tcp(10.0.0.1:3306)/"},
		"payments-002": {DatabaseType: storage.DatabaseTypeMySQL, DSN: "root@tcp(10.0.0.2:3306)/"},
	}})
	require.NoError(t, err)
	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-shared-targets",
		Database:        "payments",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "payments-001"})
	store := targetRouterApplyStore{
		byID:         map[int64]*storage.Apply{apply.ID: apply},
		byIdentifier: map[string]*storage.Apply{apply.ApplyIdentifier: apply},
	}
	created := make(map[string]*targetRouterRecordingClient)
	router := newTargetRouterForTest(t, resolver, store, nil, created)

	clientFor := func(dsn string) *targetRouterRecordingClient {
		for _, client := range created {
			if client.targetDSN == dsn {
				return client
			}
		}
		return nil
	}
	drive := func(target string, operationID int64) {
		for _, client := range created {
			client.resumeApply = nil
		}
		scoped := operationScopedApply(apply, &storage.ApplyOperation{ID: operationID, Deployment: "payments", Target: target})
		require.NoError(t, router.ResumeApplyOperation(t.Context(), scoped, operationID))
	}

	drive("payments-001", 41)
	first := clientFor("root@tcp(10.0.0.1:3306)/")
	require.NotNil(t, first)
	require.NotNil(t, first.resumeApply, "payments-001's operation drives on payments-001's connection")

	drive("payments-002", 42)
	second := clientFor("root@tcp(10.0.0.2:3306)/")
	require.NotNil(t, second, "payments-002's operation must get a client for its own route")
	require.NotNil(t, second.resumeApply, "payments-002's operation drives on payments-002's connection")
	assert.Equal(t, "payments-002", second.resumeApply.GetOptions().Target)
	assert.Nil(t, first.resumeApply, "payments-002's operation must never drive on the connection that owns payments-001's share")

	drive("payments-001", 41)
	require.NotNil(t, first.resumeApply, "payments-001 keeps its own owner")
	assert.Nil(t, second.resumeApply)

	router.mu.Lock()
	defer router.mu.Unlock()
	owners := router.applyOwners[apply.ApplyIdentifier]
	require.Len(t, owners, 2, "each target of the shared apply has an owner of its own")
	assert.Equal(t, "payments-001", owners["payments-001"].key.target)
	assert.Equal(t, "payments-002", owners["payments-002"].key.target)
}

// A member target's dispatch is only tracked once the data plane names the
// remote operation it attached, since that id is what scopes the member's
// progress and cutover away from its sibling targets'. A response that names
// none is refused and nothing it carried is persisted, rather than leaving a
// member that could only ever be polled as the whole shared apply.
func TestGRPCClient_MemberTargetDispatchRefusesAResponseWithoutItsRemoteOperation(t *testing.T) {
	server := &capturingTernServer{remoteApplyID: "remote-payments"}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	fx := newMemberTargetFixture(t, client)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, fx.apply, fx.second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `remote apply "remote-payments" accepted rollout member target "payments-002" without naming its remote operation`)

	second := fx.operations.ops[fx.second]
	assert.Empty(t, second.ExternalID, "an untrackable response's remote apply id must not be persisted")
	assert.Empty(t, second.ExternalOperationID)
	assert.Nil(t, server.getProgressRequest(), "a refused dispatch is never polled")
}

// Two member targets share their deployment's remote apply, so each polls
// progress scoped to its own remote operation: payments-002's answer must not
// carry payments-001's verdict or tables. The poll names the operation.
func TestGRPCClient_MemberTargetPollsProgressScopedToItsOwnRemoteOperation(t *testing.T) {
	server := &capturingTernServer{remoteApplyID: "remote-payments", remoteOperationID: "remote-op-002"}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	fx := newMemberTargetFixture(t, client)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApplyOperation(ctx, fx.apply, fx.second))

	progressReq := server.getProgressRequest()
	require.NotNil(t, progressReq, "the member's drive must poll its remote apply")
	assert.Equal(t, "remote-payments", progressReq.ApplyId)
	assert.Equal(t, "remote-op-002", progressReq.ApplyOperationId, "the poll must be scoped to this member's own remote operation")
}

// A data plane that answers a scoped poll for the whole shared apply, which
// one rolled back to a build without per-operation progress does, is refused:
// the poll is an error, so the member's drive counts it toward its error
// streak and parks the member retryable rather than recording its sibling
// targets' state as its own.
func TestGRPCClient_MemberTargetRefusesProgressAnsweredForTheWholeApply(t *testing.T) {
	server := &capturingTernServer{omitProgressScope: true, progressState: ternv1.State_STATE_COMPLETED, progressStateSet: true}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	fx := newMemberTargetFixture(t, client)
	op := fx.operations.ops[fx.second]
	op.ExternalOperationID = "remote-op-002"
	scope := applyTaskScope{applyOperationID: fx.second, operation: op, multiOperation: true, memberTarget: "payments-002"}

	resp, err := client.remoteProgress(t.Context(), fx.apply, scope, "remote-payments")
	require.Error(t, err)
	assert.Nil(t, resp, "a whole-apply answer must never reach the member's state machine")
	assert.Contains(t, err.Error(), `remote progress for apply remote-payments operation remote-op-002 came back scoped to ""`)
	assert.Equal(t, "remote-op-002", server.getProgressRequest().ApplyOperationId, "the poll named the member's operation")
}

// A member target parked at its cutover swaps only its own table: the preflight
// and the Cutover RPC both name its remote operation, so the data plane cuts
// over this member and leaves its sibling targets parked. A deployment with a
// single target keeps sending the unscoped calls it always has.
func TestGRPCClient_MemberTargetCutoverNamesItsOwnRemoteOperation(t *testing.T) {
	driveCutover := func(t *testing.T, memberTarget string) *capturingTernServer {
		t.Helper()
		server := &capturingTernServer{
			cutoverAccepted:  true,
			progressState:    ternv1.State_STATE_WAITING_FOR_CUTOVER,
			progressStateSet: true,
		}
		client, cleanup := testCapturingGRPCClient(t, server)
		t.Cleanup(cleanup)

		apply := newCutoverDriveApply()
		operationID, siblingID := int64(42), int64(43)
		st, _, operationStore, _ := buildCutoverDriveStorage(apply, operationID, siblingID, state.ApplyOperation.WaitingForCutover, "remote-payments")
		client.storage = st
		op := operationStore.ops[operationID]
		if memberTarget != "" {
			op.Target = memberTarget
			op.ExternalOperationID = "remote-op-002"
		}

		scope := applyTaskScope{applyOperationID: operationID, operation: op, multiOperation: true, memberTarget: memberTarget}
		poll, err := client.triggerRemoteOperationCutover(t.Context(), apply, scope, "remote-payments")
		require.NoError(t, err)
		assert.True(t, poll)
		return server
	}

	t.Run("member target", func(t *testing.T) {
		server := driveCutover(t, "payments-002")
		assert.Equal(t, "remote-op-002", server.getProgressRequest().ApplyOperationId, "the preflight reads this member's own state")
		cutoverReq := server.getCutoverRequest()
		require.NotNil(t, cutoverReq)
		assert.Equal(t, "remote-payments", cutoverReq.ApplyId)
		assert.Equal(t, "remote-op-002", cutoverReq.ApplyOperationId, "the swap is bound to this member's own remote operation")
	})

	t.Run("single-target deployment", func(t *testing.T) {
		server := driveCutover(t, "")
		assert.Empty(t, server.getProgressRequest().ApplyOperationId)
		cutoverReq := server.getCutoverRequest()
		require.NotNil(t, cutoverReq)
		assert.Empty(t, cutoverReq.ApplyOperationId, "a deployment with one target keeps the unscoped cutover every data plane accepts")
	})
}
