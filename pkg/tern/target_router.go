package tern

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/metrics"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
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
//
// Each route publishes one current client generation, identified by a hash of
// the connection identity it was built from. A resolver that assembles the DSN
// per request (dsn_from) returns a different DSN after a credential rotation;
// the next request misses on the hash and publishes a fresh generation instead
// of waiting for the stale client to fail authentication. The replaced
// generation is not closed: a LocalClient owns in-process state for the
// applies it drives, so it keeps every apply it started until that apply
// settles (AV-2), stays visible to Close and HaltForShutdown (OW-3), and is
// closed once it owns nothing and no request is in flight on it.
type TargetRouter struct {
	resolver inventory.Resolver
	storage  storage.Storage
	logger   *slog.Logger
	factory  LocalClientFactory

	mu sync.Mutex
	// current is the generation new work on a route goes to.
	current map[targetClientKey]*targetClientGeneration
	// creating marks routes with a client build in progress; it is closed when
	// the build publishes or fails. One build per route at a time means two
	// requests that miss together cannot each publish, with the slower one
	// replacing the faster one's generation on the strength of an older
	// resolution.
	creating map[targetClientKey]chan struct{}
	// retiring holds replaced generations that still own an apply or serve
	// an in-flight request.
	retiring map[*targetClientGeneration]struct{}
	// applyOwners maps an apply identifier to the generation driving it.
	applyOwners      map[string]*targetClientGeneration
	activeObservers  map[int64]ProgressObserver
	pendingObservers map[targetClientKey]ProgressObserver
}

type targetClientKey struct {
	target       string
	databaseType string
	environment  string
	database     string
}

// targetClientGeneration is one published client for a route and namespace,
// pinned to the connection identity it was opened with. A generation is
// acquired for the duration of each request dispatched on it and owns the
// applies it started or resumed; it may be closed only when neither holds.
type targetClientGeneration struct {
	key      targetClientKey
	dsnHash  string
	client   Client
	inflight int
	owned    map[string]struct{}
}

func (g *targetClientGeneration) idle() bool {
	return g.inflight == 0 && len(g.owned) == 0
}

// connectionIdentityHash is a short one-way digest of what a resolved target
// connects with, safe to compare and to log. It covers the DSN and the
// engine's connection metadata — for Vitess the API fields that stand in for
// a DSN, for PostgreSQL the CA the connection verifies against — so a rotated
// password, service token, or certificate changes the hash and nothing else
// about the route does.
func connectionIdentityHash(resolved *inventory.Target) string {
	parts := []string{resolved.DSN}
	for _, key := range inventory.ConnectionMetadataKeys(resolved.DatabaseType) {
		parts = append(parts, resolved.Metadata[key])
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:6])
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
		current:          make(map[targetClientKey]*targetClientGeneration),
		creating:         make(map[targetClientKey]chan struct{}),
		retiring:         make(map[*targetClientGeneration]struct{}),
		applyOwners:      make(map[string]*targetClientGeneration),
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
	gen, resolved, err := r.clientForTarget(ctx, targetOrDatabase(req.Target, req.Database), req.Type, req.Environment, localDatabase)
	if err != nil {
		return nil, err
	}
	defer r.release(gen)
	routedReq := proto.Clone(req).(*ternv1.PullSchemaRequest)
	routedReq.Database = req.Database
	routedReq.Type = resolved.DatabaseType
	routedReq.Target = resolved.Target
	return gen.client.PullSchema(ctx, routedReq)
}

// Plan generates a plan on the resolved target.
func (r *TargetRouter) Plan(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("plan request is required")
	}
	gen, resolved, err := r.clientForTarget(ctx, targetOrDatabase(req.Target, req.Database), req.Type, req.Environment, req.Database)
	if err != nil {
		return nil, err
	}
	defer r.release(gen)
	routedReq := proto.Clone(req).(*ternv1.PlanRequest)
	routedReq.Database = req.Database
	routedReq.Type = resolved.DatabaseType
	routedReq.Target = resolved.Target
	return gen.client.Plan(ctx, routedReq)
}

// PlanDiff routes a non-persisting desired-vs-live diff to the resolved target,
// mirroring Plan's routing.
func (r *TargetRouter) PlanDiff(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanDiffResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("plan diff request is required")
	}
	gen, resolved, err := r.clientForTarget(ctx, targetOrDatabase(req.Target, req.Database), req.Type, req.Environment, req.Database)
	if err != nil {
		return nil, err
	}
	defer r.release(gen)
	routedReq := proto.Clone(req).(*ternv1.PlanRequest)
	routedReq.Database = req.Database
	routedReq.Type = resolved.DatabaseType
	routedReq.Target = resolved.Target
	return gen.client.PlanDiff(ctx, routedReq)
}

// Apply starts a stored plan on the resolved target.
func (r *TargetRouter) Apply(ctx context.Context, req *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("apply request is required")
	}
	// An apply executes a previously created plan, so a locally stored plan is
	// authoritative for routing: it carries the execution target, namespace,
	// type, and environment chosen at plan time. Derive the route from the plan
	// and reject a request that contradicts it. Never treat req.Database as the
	// target — the opaque execution target and the schema namespace are
	// distinct, and that is the whole point of this router.
	//
	// A non-primary deployment of a multi-deployment environment never planned
	// locally — the plan row lives on the primary deployment's Tern — so no
	// local plan exists here. The dispatch request carries the plan's
	// authoritative DDL changes and schema files, so routing proceeds on the
	// request's own route fields and the deployment-local client materializes
	// (and re-verifies against the live schema) the plan before applying.
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
		switch {
		case plan != nil:
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
		case applyRequestCarriesPlanPayload(req):
			r.logger.Info("apply target routing: no local plan; routing dispatch by request fields so the deployment-local client can materialize the plan",
				"plan_id", req.PlanId,
				"database", namespace,
				"database_type", databaseType,
				"environment", environment,
				"target", target,
			)
		default:
			return nil, fmt.Errorf("plan %q not found for apply target routing and the request carries no DDL changes or schema files to materialize it", req.PlanId)
		}
	}
	if target == "" {
		return nil, fmt.Errorf("apply for plan %q has no execution target; the request supplied none and the plan has no stored target", req.PlanId)
	}

	gen, resolved, err := r.clientForTarget(ctx, target, databaseType, environment, namespace)
	if err != nil {
		return nil, err
	}
	defer r.release(gen)
	if observer := r.takePendingObserver(cacheKeyForResolvedTarget(resolved, environment, namespace)); observer != nil {
		gen.client.SetPendingObserver(observer)
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
	resp, err := gen.client.Apply(ctx, routedReq)
	if err != nil {
		return nil, err
	}
	// The generation that queued an apply holds its pending observer and is
	// where the operator's claim drives it, so it owns the apply until that
	// apply is terminal (AV-2): a later credential rotation publishes a new
	// generation for new work without pulling this one out from under a
	// running schema change. An accepted response also comes back for a
	// re-dispatch that adopted an apply already queued or driving, so
	// acceptance never moves ownership off a generation that already has it.
	if resp.GetAccepted() && resp.GetApplyId() != "" {
		r.recordOwner(gen, resp.GetApplyId())
	}
	return resp, nil
}

// applyRequestCarriesPlanPayload reports whether a dispatched apply request
// carries the plan's authoritative content — DDL changes or schema files —
// which lets a deployment that never planned locally materialize the plan
// before applying.
func applyRequestCarriesPlanPayload(req *ternv1.ApplyRequest) bool {
	return len(req.DdlChanges) > 0 || len(req.SchemaFiles) > 0
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
	gen, apply, err := r.clientForApplyIdentifier(ctx, req.GetApplyId(), req.GetEnvironment(), operation)
	if err != nil {
		return nil, err
	}
	defer r.release(gen)
	r.attachObserver(gen.client, apply.ID)
	return dispatch(gen.client, ctx, req)
}

// Progress returns progress for a stored apply by routing through its target.
func (r *TargetRouter) Progress(ctx context.Context, req *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	return routeStoredApply(ctx, r, req, "progress", Client.Progress)
}

func (r *TargetRouter) Logs(ctx context.Context, req *ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("logs request is required")
	}
	gen, _, err := r.clientForTarget(ctx, req.Target, req.Type, req.Environment, req.Database)
	if err != nil {
		return nil, fmt.Errorf("route logs for target %q: %w", req.Target, err)
	}
	defer r.release(gen)
	return gen.client.Logs(ctx, req)
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
	return r.resumeOnOwner(ctx, apply, Client.ResumeApply)
}

// ResumeApplyOperation resumes one claimed apply operation by routing through its stored target.
func (r *TargetRouter) ResumeApplyOperation(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	return r.resumeOnOwner(ctx, apply, func(client Client, ctx context.Context, apply *storage.Apply) error {
		return client.ResumeApplyOperation(ctx, apply, applyOperationID)
	})
}

// ResumeApplyOperationCutover drives one parked apply operation through its
// cutover phase by routing through its stored target.
func (r *TargetRouter) ResumeApplyOperationCutover(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	return r.resumeOnOwner(ctx, apply, func(client Client, ctx context.Context, apply *storage.Apply) error {
		return client.ResumeApplyOperationCutover(ctx, apply, applyOperationID)
	})
}

// resumeOnOwner routes a resume to the generation that already drives the
// apply, or to the current generation when none does, and records that
// generation as the apply's owner before handing it the drive. A local drive
// runs inside the resume call for as long as the schema change takes, and
// the requests that arrive meanwhile — progress, stop, observer attachment —
// must find the owner already recorded so they reach the client whose
// in-process state they act on. The owner stays recorded when the resume
// returns an error: a drive the engine detached may still be running on it,
// and the apply's terminal state is what releases it.
func (r *TargetRouter) resumeOnOwner(ctx context.Context, apply *storage.Apply, resume func(client Client, ctx context.Context, apply *storage.Apply) error) error {
	gen, err := r.clientForStoredApply(ctx, apply)
	if err != nil {
		return err
	}
	defer r.release(gen)
	r.recordOwner(gen, apply.ApplyIdentifier)
	r.attachObserver(gen.client, apply.ID)
	return resume(gen.client, ctx, apply)
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
	gen, err := r.clientForStoredApply(context.Background(), apply)
	if err != nil {
		r.logger.Warn("target router: failed to resolve apply target for observer attachment", append(apply.LogAttrs(), "error", err)...)
		return
	}
	defer r.release(gen)
	gen.client.SetObserver(applyID, observer)
}

// Close closes every generation the router holds, current and retiring.
func (r *TargetRouter) Close() error {
	r.mu.Lock()
	gens := r.allGenerationsLocked()
	r.current = make(map[targetClientKey]*targetClientGeneration)
	r.retiring = make(map[*targetClientGeneration]struct{})
	r.applyOwners = make(map[string]*targetClientGeneration)
	r.mu.Unlock()

	var closeErr error
	for _, gen := range gens {
		if err := gen.client.Close(); err != nil {
			closeErr = err
		}
	}
	if closeErr != nil {
		return fmt.Errorf("close target router clients: %w", closeErr)
	}
	return nil
}

// HaltForShutdown halts every resolved client that drives its schema changes in
// this process. A router serves targets resolved per request, so its held
// clients are the only handle a shutting-down process has on the engines it
// started; halting them here is what keeps a dynamically-routed target from
// staying held after this process stops renewing its applies' leases. Retiring
// generations are walked too: a generation replaced by a credential rotation
// still drives the applies it owns (OW-3).
//
// Every client is attempted even after one fails, so one target that will not
// come down does not leave the rest held, and the failures are reported
// together.
func (r *TargetRouter) HaltForShutdown(ctx context.Context) error {
	r.mu.Lock()
	gens := r.allGenerationsLocked()
	r.mu.Unlock()

	var errs []error
	for _, gen := range gens {
		halter, ok := gen.client.(ShutdownHalter)
		if !ok {
			r.logger.Debug("routed client drives its schema changes outside this process; nothing to halt for shutdown",
				"endpoint", gen.client.Endpoint())
			continue
		}
		if err := halter.HaltForShutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("halt routed clients for shutdown: %w", errors.Join(errs...))
	}
	return nil
}

// allGenerationsLocked lists every generation the router holds, current and
// retiring. The caller holds r.mu.
func (r *TargetRouter) allGenerationsLocked() []*targetClientGeneration {
	gens := make([]*targetClientGeneration, 0, len(r.current)+len(r.retiring))
	for _, gen := range r.current {
		gens = append(gens, gen)
	}
	for gen := range r.retiring {
		gens = append(gens, gen)
	}
	return gens
}

func (r *TargetRouter) clientForApplyIdentifier(ctx context.Context, applyIdentifier, requestEnvironment, operation string) (*targetClientGeneration, *storage.Apply, error) {
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
	gen, err := r.clientForStoredApply(ctx, apply)
	if err != nil {
		return nil, nil, err
	}
	return gen, apply, nil
}

// clientForStoredApply returns the generation that drives a stored apply —
// the one that started or resumed it while the apply is not yet terminal —
// or the route's current generation otherwise. The returned generation is
// acquired; the caller must release it.
func (r *TargetRouter) clientForStoredApply(ctx context.Context, apply *storage.Apply) (*targetClientGeneration, error) {
	if apply == nil {
		return nil, fmt.Errorf("stored apply is required for target routing")
	}
	if owner := r.ownerOf(apply); owner != nil {
		return owner, nil
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

func (r *TargetRouter) clientOnlyForTarget(ctx context.Context, target, databaseType, environment, namespace string) (*targetClientGeneration, error) {
	gen, _, err := r.clientForTarget(ctx, target, databaseType, environment, namespace)
	return gen, err
}

// clientForTarget resolves a route and returns its current generation,
// acquired for one request; the caller must release it once the dispatch
// returns. A resolved connection identity that differs from the current
// generation's publishes a fresh generation and retires the prior one.
func (r *TargetRouter) clientForTarget(ctx context.Context, target, databaseType, environment, namespace string) (*targetClientGeneration, *inventory.Target, error) {
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
	dsnHash := connectionIdentityHash(resolved)
	gen, publish, err := r.acquireCurrentOrCreationSlot(ctx, key, dsnHash)
	if err != nil {
		return nil, nil, fmt.Errorf("route target %q database %q: %w", target, namespace, err)
	}
	if gen != nil {
		return gen, resolved, nil
	}

	client, err := r.factory(LocalConfig{
		Database:        namespace,
		Type:            resolved.DatabaseType,
		TargetDSN:       resolved.DSN,
		Metadata:        maps.Clone(resolved.Metadata),
		SchemaOverrides: maps.Clone(resolved.SchemaOverrides),
	}, r.storage, r.logger)
	if err != nil {
		publish.done()
		return nil, nil, fmt.Errorf("create local client for target %q database %q: %w", target, namespace, err)
	}

	gen = &targetClientGeneration{key: key, dsnHash: dsnHash, client: client, owned: make(map[string]struct{})}

	r.mu.Lock()
	replaced := r.current[key]
	r.current[key] = gen
	gen.inflight++
	var replacedOwned int
	if replaced != nil {
		replacedOwned = len(replaced.owned)
		r.retiring[replaced] = struct{}{}
	}
	r.mu.Unlock()
	publish.done()

	if replaced == nil {
		return gen, resolved, nil
	}
	r.logger.Info("target router: connection identity changed; new work goes to a fresh client and the prior one retires once its applies settle",
		"target", resolved.Target,
		"database_type", resolved.DatabaseType,
		"environment", environment,
		"database", namespace,
		"old_dsn_hash", replaced.dsnHash,
		"new_dsn_hash", dsnHash,
		"retiring_owned_applies", replacedOwned,
	)
	metrics.RecordTargetClientEviction(ctx, resolved.DatabaseType, environment, "dsn_changed")
	// The replaced generation joined the retiring set above; the sweep closes
	// it now if it is idle, and with it any older generation whose applies
	// have finished since the rotation that retired it.
	r.sweepOwnership(ctx)
	return gen, resolved, nil
}

// creationSlot is the exclusive right to build and publish a client for one
// route. Requests that miss on the same route wait for it to finish, then
// re-check the published generation rather than building one of their own.
type creationSlot struct {
	router *TargetRouter
	key    targetClientKey
	built  chan struct{}
}

func (s creationSlot) done() {
	s.router.mu.Lock()
	delete(s.router.creating, s.key)
	s.router.mu.Unlock()
	close(s.built)
}

// acquireCurrentOrCreationSlot returns the route's current generation,
// acquired, when it was opened with the resolved connection identity.
// Otherwise it returns the slot the caller must build and publish under,
// after waiting out any build already in progress on the route. Only one
// build runs per route at a time, so a slow build cannot publish over a
// newer generation another request published while it ran.
func (r *TargetRouter) acquireCurrentOrCreationSlot(ctx context.Context, key targetClientKey, dsnHash string) (*targetClientGeneration, creationSlot, error) {
	for {
		r.mu.Lock()
		if gen := r.current[key]; gen != nil && gen.dsnHash == dsnHash {
			gen.inflight++
			r.mu.Unlock()
			return gen, creationSlot{}, nil
		}
		inProgress := r.creating[key]
		if inProgress == nil {
			slot := creationSlot{router: r, key: key, built: make(chan struct{})}
			r.creating[key] = slot.built
			r.mu.Unlock()
			return nil, slot, nil
		}
		r.mu.Unlock()
		select {
		case <-inProgress:
		case <-ctx.Done():
			return nil, creationSlot{}, fmt.Errorf("wait for a concurrent client build: %w", ctx.Err())
		}
	}
}

// release ends one request's hold on a generation. A retiring generation
// that no longer serves a request and owns no apply is closed here.
func (r *TargetRouter) release(gen *targetClientGeneration) {
	r.mu.Lock()
	gen.inflight--
	closeNow := r.retireIfIdleLocked(gen)
	r.mu.Unlock()
	if closeNow {
		r.closeGeneration(gen)
	}
}

// retireIfIdleLocked removes a retiring, idle generation from the retiring
// set and reports whether the caller must close it. Membership in the set is
// what makes a generation closable, so a generation is handed out for closing
// at most once. The caller holds r.mu.
func (r *TargetRouter) retireIfIdleLocked(gen *targetClientGeneration) bool {
	if _, retiring := r.retiring[gen]; !retiring || !gen.idle() {
		return false
	}
	delete(r.retiring, gen)
	return true
}

func (r *TargetRouter) closeGeneration(gen *targetClientGeneration) {
	if err := gen.client.Close(); err != nil {
		r.logger.Warn("target router: failed to close retired client generation",
			"target", gen.key.target,
			"database_type", gen.key.databaseType,
			"environment", gen.key.environment,
			"database", gen.key.database,
			"dsn_hash", gen.dsnHash,
			"error", err)
	}
}

// recordOwner marks gen as the generation driving an apply unless another
// generation already does. An apply has one owner for as long as it is not
// terminal, and that owner is the client holding the apply's in-process
// state — a drive, its cancel handle, its observer — so a later request that
// lands elsewhere, such as a re-dispatch adopting the queued apply on the
// current generation, cannot take the apply away from the one driving it.
func (r *TargetRouter) recordOwner(gen *targetClientGeneration, applyIdentifier string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if prior := r.applyOwners[applyIdentifier]; prior != nil && prior != gen {
		r.logger.Debug("target router: apply already owned by another client generation; leaving ownership with it",
			"apply_id", applyIdentifier, "owner_dsn_hash", prior.dsnHash, "requested_dsn_hash", gen.dsnHash)
		return
	}
	gen.owned[applyIdentifier] = struct{}{}
	r.applyOwners[applyIdentifier] = gen
}

// ownerOf returns the generation driving a stored apply, acquired, or nil
// when no generation owns it. A terminal apply releases its owner: nothing is
// driving it any more, so a later resume of a stopped apply goes to the
// current generation and its connection identity.
func (r *TargetRouter) ownerOf(apply *storage.Apply) *targetClientGeneration {
	r.mu.Lock()
	owner := r.applyOwners[apply.ApplyIdentifier]
	if owner == nil {
		r.mu.Unlock()
		return nil
	}
	if !state.IsTerminalApplyState(apply.State) {
		owner.inflight++
		r.mu.Unlock()
		return owner
	}
	closeOwner := r.releaseOwnershipLocked(owner, apply.ApplyIdentifier)
	r.mu.Unlock()
	r.logger.Debug("target router: apply is terminal; its client generation no longer owns it",
		append(apply.LogAttrs(), "dsn_hash", owner.dsnHash)...)
	if closeOwner {
		r.closeGeneration(owner)
	}
	return nil
}

// releaseOwnershipLocked drops an apply from its owner and reports whether
// the owner must now be closed. The caller holds r.mu.
func (r *TargetRouter) releaseOwnershipLocked(owner *targetClientGeneration, applyIdentifier string) bool {
	delete(owner.owned, applyIdentifier)
	delete(r.applyOwners, applyIdentifier)
	return r.retireIfIdleLocked(owner)
}

// sweepOwnership closes retiring generations that are idle and releases, from
// every generation, the applies that have since reached a terminal state.
// Ownership is otherwise released only when a request for the apply arrives,
// so without the sweep a rotation would leave an old generation open for
// long-finished applies until one did, and a current generation would keep
// an entry for every apply it ever drove. One storage read lists every apply
// still in progress, so the cost does not grow with the number owned. A
// storage error keeps the ownership: closing a generation that may still be
// driving an apply is the failure this bookkeeping exists to prevent.
func (r *TargetRouter) sweepOwnership(ctx context.Context) {
	r.mu.Lock()
	var toClose []*targetClientGeneration
	for gen := range r.retiring {
		if r.retireIfIdleLocked(gen) {
			toClose = append(toClose, gen)
		}
	}
	owned := len(r.applyOwners)
	r.mu.Unlock()
	for _, gen := range toClose {
		r.closeGeneration(gen)
	}
	if owned == 0 {
		return
	}

	applies := r.storage.Applies()
	if applies == nil {
		r.logger.Warn("target router: apply store unavailable; client generations keep their applies until a request for one arrives")
		return
	}
	inProgress, err := applies.GetInProgress(ctx)
	if err != nil {
		r.logger.Warn("target router: failed to list in-progress applies while sweeping client generations; keeping every owner open", "error", err)
		return
	}
	active := make(map[string]struct{}, len(inProgress))
	for _, apply := range inProgress {
		active[apply.ApplyIdentifier] = struct{}{}
	}

	r.mu.Lock()
	var settled []*targetClientGeneration
	released := 0
	for applyIdentifier, owner := range r.applyOwners {
		if _, driving := active[applyIdentifier]; driving {
			continue
		}
		released++
		if r.releaseOwnershipLocked(owner, applyIdentifier) {
			settled = append(settled, owner)
		}
	}
	r.mu.Unlock()
	if released > 0 {
		r.logger.Debug("target router: released client generation ownership of applies no longer in progress", "released", released)
	}
	for _, gen := range settled {
		r.closeGeneration(gen)
	}
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
