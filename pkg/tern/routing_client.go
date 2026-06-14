package tern

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/storage"
)

// DeploymentClientFunc returns the Tern client that owns a deployment in an environment.
type DeploymentClientFunc func(ctx context.Context, deployment, environment string) (Client, error)

// PlanLookup loads stored plans by their external plan identifier.
type PlanLookup interface {
	Get(ctx context.Context, planIdentifier string) (*storage.Plan, error)
}

// ApplyLookup loads stored applies by their external apply identifier.
type ApplyLookup interface {
	GetByApplyIdentifier(ctx context.Context, applyIdentifier string) (*storage.Apply, error)
}

// ApplyOperationLookup loads operation-scoped routing metadata for a stored apply.
type ApplyOperationLookup interface {
	Get(ctx context.Context, id int64) (*storage.ApplyOperation, error)
}

// RoutingClientConfig wires a RoutingClient to routing, storage, and deployment clients.
type RoutingClientConfig struct {
	Resolver             routing.Resolver
	PlanLookup           PlanLookup
	ApplyLookup          ApplyLookup
	ApplyOperationLookup ApplyOperationLookup
	ClientForDeployment  DeploymentClientFunc
}

// RoutingClient resolves logical requests to deployment-scoped Tern clients.
// Planning resolves the current logical target. Apply and apply-scoped controls
// route from stored plan/apply metadata so in-flight work keeps using the
// plan-time execution target even if routing configuration later changes.
type RoutingClient struct {
	resolver             routing.Resolver
	planLookup           PlanLookup
	applyLookup          ApplyLookup
	applyOperationLookup ApplyOperationLookup
	clientForDeployment  DeploymentClientFunc

	observerMu      sync.Mutex
	pendingObserver ProgressObserver
	activeObservers map[int64]ProgressObserver
}

var _ Client = (*RoutingClient)(nil)

// NewRoutingClient creates a routing client.
func NewRoutingClient(config RoutingClientConfig) (*RoutingClient, error) {
	if config.Resolver == nil {
		return nil, fmt.Errorf("routing resolver is required")
	}
	if config.PlanLookup == nil {
		return nil, fmt.Errorf("plan lookup is required")
	}
	if config.ApplyLookup == nil {
		return nil, fmt.Errorf("apply lookup is required")
	}
	if config.ClientForDeployment == nil {
		return nil, fmt.Errorf("deployment client lookup is required")
	}
	return &RoutingClient{
		resolver:             config.Resolver,
		planLookup:           config.PlanLookup,
		applyLookup:          config.ApplyLookup,
		applyOperationLookup: config.ApplyOperationLookup,
		clientForDeployment:  config.ClientForDeployment,
		activeObservers:      make(map[int64]ProgressObserver),
	}, nil
}

// PullSchema fetches live schema from the resolved execution target.
func (c *RoutingClient) PullSchema(ctx context.Context, req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("pull schema request is required: %w", ErrPullSchemaInvalidRequest)
	}
	target, err := c.resolveSingleExecutionTarget(ctx, req.Database, req.Environment)
	if err != nil {
		return nil, err
	}
	if req.Type != "" && req.Type != target.DatabaseType {
		return nil, fmt.Errorf("pull schema request type %q does not match resolved database type %q: %w", req.Type, target.DatabaseType, ErrPullSchemaInvalidRequest)
	}
	client, err := c.clientForDeployment(ctx, target.Deployment, req.Environment)
	if err != nil {
		return nil, fmt.Errorf("get client for deployment %q environment %q: %w", target.Deployment, req.Environment, err)
	}
	routedReq := proto.Clone(req).(*ternv1.PullSchemaRequest)
	routedReq.Type = target.DatabaseType
	routedReq.Target = target.Target
	return client.PullSchema(ctx, routedReq)
}

// Plan generates a schema change plan on the resolved execution target.
func (c *RoutingClient) Plan(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("plan request is required")
	}
	target, err := c.resolveSingleExecutionTarget(ctx, req.Database, req.Environment)
	if err != nil {
		return nil, err
	}
	client, err := c.clientForDeployment(ctx, target.Deployment, req.Environment)
	if err != nil {
		return nil, fmt.Errorf("get client for deployment %q environment %q: %w", target.Deployment, req.Environment, err)
	}
	routedReq := proto.Clone(req).(*ternv1.PlanRequest)
	routedReq.Type = target.DatabaseType
	routedReq.Target = target.Target
	return client.Plan(ctx, routedReq)
}

// Apply executes a stored plan on its plan-time execution target.
func (c *RoutingClient) Apply(ctx context.Context, req *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("apply request is required")
	}
	plan, err := c.planLookup.Get(ctx, req.PlanId)
	if err != nil {
		return nil, fmt.Errorf("load plan %q for apply routing: %w", req.PlanId, err)
	}
	if plan == nil {
		return nil, fmt.Errorf("plan %q not found for apply routing", req.PlanId)
	}
	if req.Environment != "" && req.Environment != plan.Environment {
		return nil, fmt.Errorf("plan %q was created for environment %q, not %q", req.PlanId, plan.Environment, req.Environment)
	}
	if plan.Deployment == "" {
		return nil, fmt.Errorf("plan %q is missing deployment for apply routing", req.PlanId)
	}
	if plan.Target == "" {
		return nil, fmt.Errorf("plan %q is missing target for apply routing", req.PlanId)
	}
	client, err := c.clientForDeployment(ctx, plan.Deployment, plan.Environment)
	if err != nil {
		return nil, fmt.Errorf("get client for plan %q deployment %q environment %q: %w", req.PlanId, plan.Deployment, plan.Environment, err)
	}
	if observer := c.takePendingObserver(); observer != nil {
		client.SetPendingObserver(observer)
		defer func() {
			if err != nil {
				client.SetPendingObserver(nil)
			}
		}()
	}
	routedReq := proto.Clone(req).(*ternv1.ApplyRequest)
	routedReq.Database = plan.Database
	routedReq.Type = plan.DatabaseType
	routedReq.Environment = plan.Environment
	routedReq.Target = plan.Target
	if routedReq.Options == nil {
		routedReq.Options = make(map[string]string)
	}
	routedReq.Options["target"] = plan.Target
	var resp *ternv1.ApplyResponse
	resp, err = client.Apply(ctx, routedReq)
	return resp, err
}

// Progress returns detailed progress for an active schema change.
func (c *RoutingClient) Progress(ctx context.Context, req *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("progress request is required")
	}
	client, apply, ternApplyID, err := c.clientForApply(ctx, req.ApplyId, req.Environment, "progress")
	if err != nil {
		return nil, err
	}
	routedReq := proto.Clone(req).(*ternv1.ProgressRequest)
	routedReq.ApplyId = ternApplyID
	routedReq.Environment = apply.Environment
	return client.Progress(ctx, routedReq)
}

// Cutover triggers the cutover phase when defer_cutover was used.
func (c *RoutingClient) Cutover(ctx context.Context, req *ternv1.CutoverRequest) (*ternv1.CutoverResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("cutover request is required")
	}
	client, apply, ternApplyID, err := c.clientForApply(ctx, req.ApplyId, req.Environment, "cutover")
	if err != nil {
		return nil, err
	}
	routedReq := proto.Clone(req).(*ternv1.CutoverRequest)
	routedReq.ApplyId = ternApplyID
	routedReq.Environment = apply.Environment
	return client.Cutover(ctx, routedReq)
}

// Stop pauses an in-progress schema change.
func (c *RoutingClient) Stop(ctx context.Context, req *ternv1.StopRequest) (*ternv1.StopResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("stop request is required")
	}
	client, apply, ternApplyID, err := c.clientForApply(ctx, req.ApplyId, req.Environment, "stop")
	if err != nil {
		return nil, err
	}
	routedReq := proto.Clone(req).(*ternv1.StopRequest)
	routedReq.ApplyId = ternApplyID
	routedReq.Environment = apply.Environment
	return client.Stop(ctx, routedReq)
}

// Start resumes a stopped schema change.
func (c *RoutingClient) Start(ctx context.Context, req *ternv1.StartRequest) (*ternv1.StartResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("start request is required")
	}
	client, apply, ternApplyID, err := c.clientForApply(ctx, req.ApplyId, req.Environment, "start")
	if err != nil {
		return nil, err
	}
	routedReq := proto.Clone(req).(*ternv1.StartRequest)
	routedReq.ApplyId = ternApplyID
	routedReq.Environment = apply.Environment
	return client.Start(ctx, routedReq)
}

// Volume modifies the schema change speed/concurrency in-flight.
func (c *RoutingClient) Volume(ctx context.Context, req *ternv1.VolumeRequest) (*ternv1.VolumeResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("volume request is required")
	}
	client, apply, ternApplyID, err := c.clientForApply(ctx, req.ApplyId, req.Environment, "volume")
	if err != nil {
		return nil, err
	}
	routedReq := proto.Clone(req).(*ternv1.VolumeRequest)
	routedReq.ApplyId = ternApplyID
	routedReq.Environment = apply.Environment
	return client.Volume(ctx, routedReq)
}

// Revert reverts a completed schema change during the revert window.
func (c *RoutingClient) Revert(ctx context.Context, req *ternv1.RevertRequest) (*ternv1.RevertResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("revert request is required")
	}
	client, apply, ternApplyID, err := c.clientForApply(ctx, req.ApplyId, req.Environment, "revert")
	if err != nil {
		return nil, err
	}
	routedReq := proto.Clone(req).(*ternv1.RevertRequest)
	routedReq.ApplyId = ternApplyID
	routedReq.Environment = apply.Environment
	return client.Revert(ctx, routedReq)
}

// SkipRevert skips the revert window and finalizes the schema change.
func (c *RoutingClient) SkipRevert(ctx context.Context, req *ternv1.SkipRevertRequest) (*ternv1.SkipRevertResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("skip revert request is required")
	}
	client, apply, ternApplyID, err := c.clientForApply(ctx, req.ApplyId, req.Environment, "skip revert")
	if err != nil {
		return nil, err
	}
	routedReq := proto.Clone(req).(*ternv1.SkipRevertRequest)
	routedReq.ApplyId = ternApplyID
	routedReq.Environment = apply.Environment
	return client.SkipRevert(ctx, routedReq)
}

// RollbackPlan is not routable without an environment-scoped request.
func (c *RoutingClient) RollbackPlan(ctx context.Context, database string) (*ternv1.PlanResponse, error) {
	return nil, fmt.Errorf("rollback plan for database %q requires an environment-scoped client", database)
}

// Health is not routable without a specific deployment and environment.
func (c *RoutingClient) Health(ctx context.Context) error {
	return fmt.Errorf("routing client health requires a deployment-scoped client")
}

// ResumeApply starts or resumes work claimed by an operator worker.
func (c *RoutingClient) ResumeApply(ctx context.Context, apply *storage.Apply) error {
	client, err := c.clientForStoredApply(ctx, apply)
	if err != nil {
		return err
	}
	c.attachObserver(client, apply.ID)
	return client.ResumeApply(ctx, apply)
}

// ResumeApplyOperation starts or resumes one operation for a stored apply.
func (c *RoutingClient) ResumeApplyOperation(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	if apply == nil {
		return fmt.Errorf("stored apply is required for routing")
	}
	operation, err := c.loadApplyOperation(ctx, apply, applyOperationID)
	if err != nil {
		return err
	}
	client, err := c.clientForDeployment(ctx, operation.Deployment, apply.Environment)
	if err != nil {
		return fmt.Errorf("get client for apply %q operation %d deployment %q environment %q: %w", apply.ApplyIdentifier, applyOperationID, operation.Deployment, apply.Environment, err)
	}
	c.attachObserver(client, apply.ID)
	return client.ResumeApplyOperation(ctx, operationScopedApply(apply, operation), applyOperationID)
}

// Endpoint returns a descriptive endpoint for the routing wrapper.
func (c *RoutingClient) Endpoint() string { return "routing" }

// IsRemote reports false only as a temporary Client-interface shim. Routing is
// request-scoped and may delegate to local or remote deployment clients, so API
// paths that branch on transport mode must keep using concrete deployment
// clients until transport metadata becomes request-scoped.
func (c *RoutingClient) IsRemote() bool { return false }

// SetPendingObserver sets an observer that will be consumed by the next Apply call.
func (c *RoutingClient) SetPendingObserver(observer ProgressObserver) {
	c.observerMu.Lock()
	defer c.observerMu.Unlock()
	c.pendingObserver = observer
}

// SetObserver registers a progress observer for an active apply.
func (c *RoutingClient) SetObserver(applyID int64, observer ProgressObserver) {
	c.observerMu.Lock()
	defer c.observerMu.Unlock()
	if observer == nil {
		delete(c.activeObservers, applyID)
		return
	}
	c.activeObservers[applyID] = observer
}

// Close releases resources owned by the routing wrapper.
func (c *RoutingClient) Close() error { return nil }

func (c *RoutingClient) resolveSingleExecutionTarget(ctx context.Context, database, environment string) (routing.ExecutionTarget, error) {
	targets, err := c.resolver.ResolveTargets(ctx, routing.Request{Database: database, Environment: environment})
	if err != nil {
		return routing.ExecutionTarget{}, fmt.Errorf("resolve execution targets for database %q environment %q: %w", database, environment, err)
	}
	if len(targets) == 0 {
		return routing.ExecutionTarget{}, fmt.Errorf("database %q environment %q resolved to no execution targets", database, environment)
	}
	if len(targets) > 1 {
		return routing.ExecutionTarget{}, fmt.Errorf("database %q environment %q resolved to %d execution targets; routing client only supports single-target requests", database, environment, len(targets))
	}
	if targets[0].DatabaseType == "" {
		return routing.ExecutionTarget{}, fmt.Errorf("database %q environment %q resolved target is missing database type", database, environment)
	}
	if targets[0].Deployment == "" {
		return routing.ExecutionTarget{}, fmt.Errorf("database %q environment %q resolved target is missing deployment", database, environment)
	}
	if targets[0].Target == "" {
		return routing.ExecutionTarget{}, fmt.Errorf("database %q environment %q resolved target is missing target", database, environment)
	}
	return targets[0], nil
}

func (c *RoutingClient) clientForApply(ctx context.Context, applyIdentifier, requestEnvironment, operation string) (Client, *storage.Apply, string, error) {
	apply, err := c.applyLookup.GetByApplyIdentifier(ctx, applyIdentifier)
	if err != nil {
		return nil, nil, "", fmt.Errorf("load apply %q for routing: %w", applyIdentifier, err)
	}
	if apply == nil {
		return nil, nil, "", fmt.Errorf("apply %q not found for routing", applyIdentifier)
	}
	if err := validateStoredApplyEnvironment(operation, apply, requestEnvironment); err != nil {
		return nil, nil, "", err
	}
	client, err := c.clientForStoredApply(ctx, apply)
	if err != nil {
		return nil, nil, "", err
	}
	c.attachObserver(client, apply.ID)
	ternApplyID, err := ternApplyIdentifierForClient(apply, client)
	if err != nil {
		return nil, nil, "", err
	}
	return client, apply, ternApplyID, nil
}

func (c *RoutingClient) clientForStoredApply(ctx context.Context, apply *storage.Apply) (Client, error) {
	if apply == nil {
		return nil, fmt.Errorf("stored apply is required for routing")
	}
	deployment := apply.Deployment
	if deployment == "" {
		return nil, fmt.Errorf("apply %q is missing deployment for routing", apply.ApplyIdentifier)
	}
	client, err := c.clientForDeployment(ctx, deployment, apply.Environment)
	if err != nil {
		return nil, fmt.Errorf("get client for apply %q deployment %q environment %q: %w", apply.ApplyIdentifier, deployment, apply.Environment, err)
	}
	return client, nil
}

func (c *RoutingClient) loadApplyOperation(ctx context.Context, apply *storage.Apply, applyOperationID int64) (*storage.ApplyOperation, error) {
	if c.applyOperationLookup == nil {
		return nil, fmt.Errorf("apply operation lookup is required to route operation %d for apply %q", applyOperationID, apply.ApplyIdentifier)
	}
	operation, err := c.applyOperationLookup.Get(ctx, applyOperationID)
	if err != nil {
		return nil, fmt.Errorf("load apply operation %d for apply %q routing: %w", applyOperationID, apply.ApplyIdentifier, err)
	}
	if operation == nil {
		return nil, fmt.Errorf("apply operation %d not found for apply %q routing", applyOperationID, apply.ApplyIdentifier)
	}
	if operation.ApplyID != apply.ID {
		return nil, fmt.Errorf("apply operation %d belongs to apply %d, not apply %d", applyOperationID, operation.ApplyID, apply.ID)
	}
	if operation.Deployment == "" {
		return nil, fmt.Errorf("apply operation %d for apply %q is missing deployment for routing", applyOperationID, apply.ApplyIdentifier)
	}
	return operation, nil
}

func validateStoredApplyEnvironment(operation string, apply *storage.Apply, requestEnvironment string) error {
	if requestEnvironment == "" || requestEnvironment == apply.Environment {
		return nil
	}
	return fmt.Errorf("apply %q is stored for environment %q, not %q; cannot route %s", apply.ApplyIdentifier, apply.Environment, requestEnvironment, operation)
}

func ternApplyIdentifierForClient(apply *storage.Apply, client Client) (string, error) {
	if apply.ExternalID != "" {
		return apply.ExternalID, nil
	}
	if client.IsRemote() {
		return "", fmt.Errorf("apply %q has not been accepted by remote deployment %q yet", apply.ApplyIdentifier, apply.Deployment)
	}
	return apply.ApplyIdentifier, nil
}

func operationScopedApply(apply *storage.Apply, operation *storage.ApplyOperation) *storage.Apply {
	scopedApply := *apply
	scopedApply.Deployment = operation.Deployment
	if operation.Target != "" {
		options := scopedApply.GetOptions()
		options.Target = operation.Target
		scopedApply.SetOptions(options)
	}
	return &scopedApply
}

func (c *RoutingClient) takePendingObserver() ProgressObserver {
	c.observerMu.Lock()
	defer c.observerMu.Unlock()
	observer := c.pendingObserver
	c.pendingObserver = nil
	return observer
}

func (c *RoutingClient) attachObserver(client Client, applyID int64) {
	c.observerMu.Lock()
	observer := c.activeObservers[applyID]
	c.observerMu.Unlock()
	if observer != nil {
		client.SetObserver(applyID, observer)
	}
}
