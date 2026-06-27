package tern

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/block/schemabot/pkg/inventory"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// LocalClientFactory builds a deployment-local client from a resolved target.
type LocalClientFactory func(LocalConfig, storage.Storage, *slog.Logger) (Client, error)

// TargetRouterConfig wires a target router to a resolver and storage.
type TargetRouterConfig struct {
	Resolver           inventory.Resolver
	Storage            storage.Storage
	Logger             *slog.Logger
	LocalClientFactory LocalClientFactory
}

// TargetRouter routes data-plane requests to LocalClients resolved and cached
// per resolved target route and namespace — keyed by target, database type,
// environment, and database, because LocalClient is still namespace-bound via
// LocalConfig.Database. It is the data-plane complement to RoutingClient: the
// control plane decides which target to use, while this router decides how the
// data plane connects to that target.
type TargetRouter struct {
	resolver inventory.Resolver
	storage  storage.Storage
	logger   *slog.Logger
	factory  LocalClientFactory

	mu               sync.Mutex
	clientsByTarget  map[targetClientKey]Client
	activeObservers  map[int64]ProgressObserver
	pendingObservers map[targetClientKey]ProgressObserver
}

type targetClientKey struct {
	target       string
	databaseType string
	environment  string
	database     string
}

var _ Client = (*TargetRouter)(nil)

// NewTargetRouter creates a data-plane target router.
func NewTargetRouter(config TargetRouterConfig) (*TargetRouter, error) {
	if config.Resolver == nil {
		return nil, fmt.Errorf("target resolver is required")
	}
	if config.Storage == nil {
		return nil, fmt.Errorf("storage is required")
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	factory := config.LocalClientFactory
	if factory == nil {
		factory = func(cfg LocalConfig, st storage.Storage, logger *slog.Logger) (Client, error) {
			return NewLocalClient(cfg, st, logger)
		}
	}
	return &TargetRouter{
		resolver:         config.Resolver,
		storage:          config.Storage,
		logger:           logger,
		factory:          factory,
		clientsByTarget:  make(map[targetClientKey]Client),
		activeObservers:  make(map[int64]ProgressObserver),
		pendingObservers: make(map[targetClientKey]ProgressObserver),
	}, nil
}

// PullSchema fetches live schema from the resolved target.
func (r *TargetRouter) PullSchema(ctx context.Context, req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("pull schema request is required: %w", ErrPullSchemaInvalidRequest)
	}
	localDatabase := req.Database
	if req.GetNamespace() != "" {
		localDatabase = req.GetNamespace()
	}
	client, resolved, err := r.clientForTarget(ctx, targetOrDatabase(req.Target, req.Database), req.Type, req.Environment, localDatabase)
	if err != nil {
		return nil, err
	}
	routedReq := proto.Clone(req).(*ternv1.PullSchemaRequest)
	routedReq.Database = req.Database
	routedReq.Type = resolved.DatabaseType
	routedReq.Target = resolved.Target
	return client.PullSchema(ctx, routedReq)
}

// Plan generates a plan on the resolved target.
func (r *TargetRouter) Plan(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("plan request is required")
	}
	client, resolved, err := r.clientForTarget(ctx, targetOrDatabase(req.Target, req.Database), req.Type, req.Environment, req.Database)
	if err != nil {
		return nil, err
	}
	routedReq := proto.Clone(req).(*ternv1.PlanRequest)
	routedReq.Database = req.Database
	routedReq.Type = resolved.DatabaseType
	routedReq.Target = resolved.Target
	return client.Plan(ctx, routedReq)
}

// Apply starts a stored plan on the resolved target.
func (r *TargetRouter) Apply(ctx context.Context, req *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("apply request is required")
	}
	// An apply executes a previously created plan, so the plan is authoritative
	// for routing: it carries the execution target, namespace, type, and
	// environment chosen at plan time. Derive the route from the plan and reject
	// a request that contradicts it. Never treat req.Database as the target — the
	// opaque execution target and the schema namespace are distinct, and that is
	// the whole point of this router.
	target := req.Target
	if target == "" && req.Options != nil {
		target = req.Options["target"]
	}
	namespace := req.Database
	databaseType := req.Type
	environment := req.Environment

	if req.PlanId != "" && r.storage.Plans() != nil {
		plan, err := r.storage.Plans().Get(ctx, req.PlanId)
		if err != nil {
			return nil, fmt.Errorf("load plan %q for apply target routing: %w", req.PlanId, err)
		}
		if plan == nil {
			return nil, fmt.Errorf("plan %q not found for apply target routing", req.PlanId)
		}
		if target, err = planAuthoritativeField("target", req.PlanId, target, plan.Target); err != nil {
			return nil, err
		}
		if namespace, err = planAuthoritativeField("database", req.PlanId, namespace, plan.Database); err != nil {
			return nil, err
		}
		if databaseType, err = planAuthoritativeField("type", req.PlanId, databaseType, plan.DatabaseType); err != nil {
			return nil, err
		}
		if environment, err = planAuthoritativeField("environment", req.PlanId, environment, plan.Environment); err != nil {
			return nil, err
		}
	}
	if target == "" {
		return nil, fmt.Errorf("apply for plan %q has no execution target; the request supplied none and the plan has no stored target", req.PlanId)
	}

	client, resolved, err := r.clientForTarget(ctx, target, databaseType, environment, namespace)
	if err != nil {
		return nil, err
	}
	if observer := r.takePendingObserver(cacheKeyForResolvedTarget(resolved, environment, namespace)); observer != nil {
		client.SetPendingObserver(observer)
	}
	routedReq := proto.Clone(req).(*ternv1.ApplyRequest)
	routedReq.Database = namespace
	routedReq.Type = resolved.DatabaseType
	routedReq.Target = resolved.Target
	routedReq.Environment = environment
	if routedReq.Options == nil {
		routedReq.Options = make(map[string]string)
	}
	routedReq.Options["target"] = resolved.Target
	return client.Apply(ctx, routedReq)
}

// planAuthoritativeField resolves one routing field where the stored plan is
// authoritative: the plan value wins, and a non-empty request value that
// disagrees with the plan fails closed rather than silently overriding the
// plan-time route. An empty plan value leaves the request value unchanged.
func planAuthoritativeField(name, planID, requested, planValue string) (string, error) {
	if planValue == "" {
		return requested, nil
	}
	if requested != "" && requested != planValue {
		return "", fmt.Errorf("apply request %s %q does not match plan %q %s %q", name, requested, planID, name, planValue)
	}
	return planValue, nil
}

// applyScopedRequest is implemented by the apply-scoped tern request protos,
// which all carry an apply id and environment used to route to a deployment.
type applyScopedRequest[T any] interface {
	*T
	proto.Message
	GetApplyId() string
	GetEnvironment() string
}

// routeStoredApply resolves the target client for an apply-scoped request and
// dispatches the request to it, attaching any registered progress observer. The
// operation name is used for routing context and the missing-request error.
func routeStoredApply[T any, PT applyScopedRequest[T], Resp any](
	ctx context.Context,
	r *TargetRouter,
	req PT,
	operation string,
	dispatch func(client Client, ctx context.Context, req PT) (*Resp, error),
) (*Resp, error) {
	if req == nil {
		return nil, fmt.Errorf("%s request is required", operation)
	}
	client, apply, err := r.clientForApplyIdentifier(ctx, req.GetApplyId(), req.GetEnvironment(), operation)
	if err != nil {
		return nil, err
	}
	r.attachObserver(client, apply.ID)
	return dispatch(client, ctx, req)
}

// Progress returns progress for a stored apply by routing through its target.
func (r *TargetRouter) Progress(ctx context.Context, req *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	return routeStoredApply(ctx, r, req, "progress", Client.Progress)
}

// Cutover triggers cutover for a stored apply by routing through its target.
func (r *TargetRouter) Cutover(ctx context.Context, req *ternv1.CutoverRequest) (*ternv1.CutoverResponse, error) {
	return routeStoredApply(ctx, r, req, "cutover", Client.Cutover)
}

// Stop pauses a stored apply by routing through its target.
func (r *TargetRouter) Stop(ctx context.Context, req *ternv1.StopRequest) (*ternv1.StopResponse, error) {
	return routeStoredApply(ctx, r, req, "stop", Client.Stop)
}

// Cancel terminates a stored apply by routing through its target.
func (r *TargetRouter) Cancel(ctx context.Context, req *ternv1.CancelRequest) (*ternv1.CancelResponse, error) {
	return routeStoredApply(ctx, r, req, "cancel", Client.Cancel)
}

// Start resumes a stored apply by routing through its target.
func (r *TargetRouter) Start(ctx context.Context, req *ternv1.StartRequest) (*ternv1.StartResponse, error) {
	return routeStoredApply(ctx, r, req, "start", Client.Start)
}

// Volume modifies a stored apply by routing through its target.
func (r *TargetRouter) Volume(ctx context.Context, req *ternv1.VolumeRequest) (*ternv1.VolumeResponse, error) {
	return routeStoredApply(ctx, r, req, "volume", Client.Volume)
}

// Revert reverts a stored apply by routing through its target.
func (r *TargetRouter) Revert(ctx context.Context, req *ternv1.RevertRequest) (*ternv1.RevertResponse, error) {
	return routeStoredApply(ctx, r, req, "revert", Client.Revert)
}

// SkipRevert skips the revert window for a stored apply by routing through its target.
func (r *TargetRouter) SkipRevert(ctx context.Context, req *ternv1.SkipRevertRequest) (*ternv1.SkipRevertResponse, error) {
	return routeStoredApply(ctx, r, req, "skip revert", Client.SkipRevert)
}

// Health checks storage connectivity for the data-plane router.
func (r *TargetRouter) Health(ctx context.Context) error {
	return r.storage.Ping(ctx)
}

// ResumeApply resumes a claimed apply by routing through its stored target.
func (r *TargetRouter) ResumeApply(ctx context.Context, apply *storage.Apply) error {
	client, err := r.clientForStoredApply(ctx, apply)
	if err != nil {
		return err
	}
	r.attachObserver(client, apply.ID)
	return client.ResumeApply(ctx, apply)
}

// ResumeApplyOperation resumes one claimed apply operation by routing through its stored target.
func (r *TargetRouter) ResumeApplyOperation(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	client, err := r.clientForStoredApply(ctx, apply)
	if err != nil {
		return err
	}
	r.attachObserver(client, apply.ID)
	return client.ResumeApplyOperation(ctx, apply, applyOperationID)
}

// ResumeApplyOperationCutover drives one parked apply operation through its
// cutover phase by routing through its stored target.
func (r *TargetRouter) ResumeApplyOperationCutover(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	client, err := r.clientForStoredApply(ctx, apply)
	if err != nil {
		return err
	}
	r.attachObserver(client, apply.ID)
	return client.ResumeApplyOperationCutover(ctx, apply, applyOperationID)
}

// Endpoint returns a descriptive endpoint for the router.
func (r *TargetRouter) Endpoint() string { return "target-router" }

// IsRemote reports false because the router delegates to in-process LocalClients.
func (r *TargetRouter) IsRemote() bool { return false }

// SetPendingObserver stores an observer for the next apply only when callers use
// the target-aware SetPendingObserverForTarget helper. The Client interface lacks
// a target parameter, so this method cannot safely attach observers in a
// multi-target data plane.
func (r *TargetRouter) SetPendingObserver(ProgressObserver) {
	r.logger.Warn("target router: targetless pending observer ignored; use SetPendingObserverForTarget")
}

// SetPendingObserverForTarget stores an observer for the next Apply on a target
// when the target key is globally unique. Use SetPendingObserverForRequest when
// environment or database type can affect target resolution.
func (r *TargetRouter) SetPendingObserverForTarget(target string, observer ProgressObserver) error {
	if target == "" {
		return fmt.Errorf("target is required for target-scoped pending observer")
	}
	return r.SetPendingObserverForRequest(inventory.Request{Target: target}, observer)
}

// SetPendingObserverForRequest stores an observer for the next Apply matching
// the same target resolution inputs.
func (r *TargetRouter) SetPendingObserverForRequest(req inventory.Request, observer ProgressObserver) error {
	if req.Target == "" {
		return fmt.Errorf("target is required for target-scoped pending observer")
	}
	key := cacheKeyForTargetRequest(req)
	r.mu.Lock()
	defer r.mu.Unlock()
	if observer == nil {
		delete(r.pendingObservers, key)
		return nil
	}
	r.pendingObservers[key] = observer
	return nil
}

// SetObserver registers an observer for an active apply and attaches it to the
// concrete target client when the apply is already stored.
func (r *TargetRouter) SetObserver(applyID int64, observer ProgressObserver) {
	r.mu.Lock()
	if observer == nil {
		delete(r.activeObservers, applyID)
	} else {
		r.activeObservers[applyID] = observer
	}
	r.mu.Unlock()

	apply, err := r.storage.Applies().Get(context.Background(), applyID)
	if err != nil {
		r.logger.Warn("target router: failed to load apply for observer attachment", "apply_id", applyID, "error", err)
		return
	}
	if apply == nil {
		r.logger.Debug("target router: apply not found for observer attachment", "apply_id", applyID)
		return
	}
	client, err := r.clientForStoredApply(context.Background(), apply)
	if err != nil {
		r.logger.Warn("target router: failed to resolve apply target for observer attachment", "apply_id", applyID, "apply_identifier", apply.ApplyIdentifier, "error", err)
		return
	}
	client.SetObserver(applyID, observer)
}

// Close closes cached clients.
func (r *TargetRouter) Close() error {
	r.mu.Lock()
	clients := make([]Client, 0, len(r.clientsByTarget))
	for _, client := range r.clientsByTarget {
		clients = append(clients, client)
	}
	r.clientsByTarget = make(map[targetClientKey]Client)
	r.mu.Unlock()

	var closeErr error
	for _, client := range clients {
		if err := client.Close(); err != nil {
			closeErr = err
		}
	}
	if closeErr != nil {
		return fmt.Errorf("close target router clients: %w", closeErr)
	}
	return nil
}

func (r *TargetRouter) clientForApplyIdentifier(ctx context.Context, applyIdentifier, requestEnvironment, operation string) (Client, *storage.Apply, error) {
	if applyIdentifier == "" {
		return nil, nil, fmt.Errorf("apply id is required for %s", operation)
	}
	apply, err := r.storage.Applies().GetByApplyIdentifier(ctx, applyIdentifier)
	if err != nil {
		return nil, nil, fmt.Errorf("load apply %q for %s routing: %w", applyIdentifier, operation, err)
	}
	if apply == nil {
		return nil, nil, fmt.Errorf("apply %q not found for %s routing", applyIdentifier, operation)
	}
	if requestEnvironment != "" && requestEnvironment != apply.Environment {
		return nil, nil, fmt.Errorf("apply %q is stored for environment %q, not %q; cannot route %s", applyIdentifier, apply.Environment, requestEnvironment, operation)
	}
	client, err := r.clientForStoredApply(ctx, apply)
	if err != nil {
		return nil, nil, err
	}
	return client, apply, nil
}

func (r *TargetRouter) clientForStoredApply(ctx context.Context, apply *storage.Apply) (Client, error) {
	if apply == nil {
		return nil, fmt.Errorf("stored apply is required for target routing")
	}
	target := apply.GetOptions().Target
	if target == "" {
		planTarget, err := r.planTargetForStoredApply(ctx, apply)
		if err != nil {
			return nil, err
		}
		target = planTarget
	}
	return r.clientOnlyForTarget(ctx, target, apply.DatabaseType, apply.Environment, apply.Database)
}

func (r *TargetRouter) clientOnlyForTarget(ctx context.Context, target, databaseType, environment, namespace string) (Client, error) {
	client, _, err := r.clientForTarget(ctx, target, databaseType, environment, namespace)
	return client, err
}

func (r *TargetRouter) clientForTarget(ctx context.Context, target, databaseType, environment, namespace string) (Client, *inventory.Target, error) {
	if target == "" {
		return nil, nil, fmt.Errorf("target is required")
	}
	if namespace == "" {
		return nil, nil, fmt.Errorf("database is required for target %q", target)
	}

	resolved, err := r.resolver.ResolveTarget(ctx, inventory.Request{Target: target, DatabaseType: databaseType, Environment: environment})
	if err != nil {
		return nil, nil, err
	}
	// Fail closed on a nil or incomplete resolver result rather than building a
	// client that surfaces a confusing connection error later. Keep the causes
	// separate so the log names the missing field, and never log the DSN or
	// secret values — only field names.
	if resolved == nil {
		return nil, nil, fmt.Errorf("resolver returned no target for %q", target)
	}
	if resolved.Target == "" {
		return nil, nil, fmt.Errorf("resolver returned a target with no identifier for %q", target)
	}
	if resolved.DatabaseType == "" {
		return nil, nil, fmt.Errorf("resolver returned a target with no database type for %q", target)
	}
	if err := resolvedTargetConnectable(resolved); err != nil {
		return nil, nil, fmt.Errorf("resolver returned an incomplete target for %q (type=%q): %w", target, resolved.DatabaseType, err)
	}
	key := cacheKeyForResolvedTarget(resolved, environment, namespace)
	r.mu.Lock()
	cached := r.clientsByTarget[key]
	r.mu.Unlock()
	if cached != nil {
		return cached, resolved, nil
	}

	client, err := r.factory(LocalConfig{
		Database:  namespace,
		Type:      resolved.DatabaseType,
		TargetDSN: resolved.DSN,
		Metadata:  maps.Clone(resolved.Metadata),
	}, r.storage, r.logger)
	if err != nil {
		return nil, nil, fmt.Errorf("create local client for target %q database %q: %w", target, namespace, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if existing := r.clientsByTarget[key]; existing != nil {
		if err := client.Close(); err != nil {
			r.logger.Warn("target router: failed to close duplicate local client", "target", target, "error", err)
		}
		return existing, resolved, nil
	}
	r.clientsByTarget[key] = client
	return client, resolved, nil
}

func (r *TargetRouter) planTargetForStoredApply(ctx context.Context, apply *storage.Apply) (string, error) {
	if apply.PlanID > 0 && r.storage.Plans() != nil {
		plan, err := r.storage.Plans().GetByID(ctx, apply.PlanID)
		if err != nil {
			return "", fmt.Errorf("load plan %d for apply %q target routing: %w", apply.PlanID, apply.ApplyIdentifier, err)
		}
		if plan != nil && plan.Target != "" {
			return plan.Target, nil
		}
	}
	r.logger.Debug("target router: stored apply missing plan-time target; falling back to apply database",
		"apply_id", apply.ApplyIdentifier,
		"database", apply.Database,
		"environment", apply.Environment)
	return apply.Database, nil
}

func (r *TargetRouter) attachObserver(client Client, applyID int64) {
	r.mu.Lock()
	observer := r.activeObservers[applyID]
	r.mu.Unlock()
	if observer != nil {
		client.SetObserver(applyID, observer)
	}
}

func (r *TargetRouter) takePendingObserver(key targetClientKey) ProgressObserver {
	r.mu.Lock()
	defer r.mu.Unlock()
	observer := r.pendingObservers[key]
	delete(r.pendingObservers, key)
	if observer != nil {
		return observer
	}
	targetAndRouteKey := targetClientKey{target: key.target, databaseType: key.databaseType, environment: key.environment}
	observer = r.pendingObservers[targetAndRouteKey]
	delete(r.pendingObservers, targetAndRouteKey)
	if observer != nil {
		return observer
	}
	targetOnlyKey := targetClientKey{target: key.target}
	observer = r.pendingObservers[targetOnlyKey]
	delete(r.pendingObservers, targetOnlyKey)
	return observer
}

func cacheKeyForTargetRequest(req inventory.Request) targetClientKey {
	return targetClientKey{target: req.Target, databaseType: req.DatabaseType, environment: req.Environment}
}

// resolvedTargetConnectable verifies a resolved target carries enough to open a
// connection for its engine. Vitess reaches the database through the PlanetScale
// API using metadata (organization, service token, API URL) and carries no DSN;
// every other engine connects via a DSN.
func resolvedTargetConnectable(resolved *inventory.Target) error {
	if resolved.DatabaseType == storage.DatabaseTypeVitess {
		for _, key := range []string{
			inventory.MetadataOrganization,
			inventory.MetadataTokenName,
			inventory.MetadataTokenValue,
			inventory.MetadataAPIURL,
		} {
			if resolved.Metadata[key] == "" {
				return fmt.Errorf("missing %q metadata", key)
			}
		}
		return nil
	}
	if resolved.DSN == "" {
		return fmt.Errorf("missing DSN")
	}
	return nil
}

func cacheKeyForResolvedTarget(target *inventory.Target, environment, namespace string) targetClientKey {
	return targetClientKey{target: target.Target, databaseType: target.DatabaseType, environment: environment, database: namespace}
}

func targetOrDatabase(target, database string) string {
	if target != "" {
		return target
	}
	return database
}
