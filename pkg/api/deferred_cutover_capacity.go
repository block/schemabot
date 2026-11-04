package api

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// DeferredCutoverCapacityError is apply creation refusing an apply started
// with --defer-cutover that would keep more drivers waiting at its cutover at
// once than it can have. Each operation waiting at a deferred cutover keeps its
// driver until an operator cuts it over, and the cutover is accepted only once
// every one of them waits, so an apply that cannot have them all waiting holds
// its drivers with nothing cut over until the waiting-for-cutover timeout. The
// message names only the database, environment and counts, so a caller may
// show it as is.
type DeferredCutoverCapacityError struct {
	Database    string
	Environment string
	// Deployment is set when the refusal is about a deployment whose
	// operators run its operations on another plane: the counts are then that
	// deployment's operations and that plane's drivers and cap.
	Deployment string
	// Unreported is set when Deployment does not report its drivers, which a
	// deployment running an older SchemaBot does not.
	Unreported bool
	// Operations is how many operations the refused apply would hold at its
	// cutover at once.
	Operations int
	// Drivers is how many drivers across every live server can run the apply's
	// operations: each server's drivers but the one it keeps for other work
	// (reservedDriverID).
	Drivers int
	// LiveServers is how many servers' operators reported their drivers
	// within the live window.
	LiveServers int
	// PerApplyCap is the server's max_drivers_per_apply, set only when the
	// apply would hold more operations at once than it. A waiting operation
	// keeps counting against the cap, so the operations past it could never
	// start while the first ones wait.
	PerApplyCap int
}

// ExceedsPerApplyCap reports whether the apply would hold more operations at
// once than max_drivers_per_apply lets one apply run.
func (e *DeferredCutoverCapacityError) ExceedsPerApplyCap() bool {
	return e.PerApplyCap > 0 && e.Operations > e.PerApplyCap
}

// NoLiveServers reports whether no server has reported its drivers within the
// live window, so the fleet's drivers are not known yet.
func (e *DeferredCutoverCapacityError) NoLiveServers() bool {
	return !e.Unreported && !e.ExceedsPerApplyCap() && e.LiveServers == 0
}

// Transient reports whether the refusal can clear without changing the
// request: only a server reporting its drivers can. An apply wider than
// max_drivers_per_apply, or than the fleet's drivers, is refused again on
// every retry.
func (e *DeferredCutoverCapacityError) Transient() bool {
	return e.NoLiveServers()
}

func (e *DeferredCutoverCapacityError) Error() string {
	if e.Deployment != "" {
		return e.deploymentError()
	}
	switch {
	case e.ExceedsPerApplyCap():
		return fmt.Sprintf("%s/%s can't defer its cutover: it would keep %d drivers waiting for the cutover, and one apply can use at most %d (max_drivers_per_apply). To defer it, raise max_drivers_per_apply to %d; otherwise apply without --defer-cutover",
			e.Database, e.Environment, e.Operations, e.PerApplyCap, e.Operations)
	case e.NoLiveServers():
		return fmt.Sprintf("%s/%s can't defer its cutover: no SchemaBot server running drivers has checked in during the last %s. Apply again once SchemaBot's drivers are running, or apply without --defer-cutover",
			e.Database, e.Environment, LiveWindowText())
	default:
		return fmt.Sprintf("%s/%s can't defer its cutover: it would keep %d drivers waiting for the cutover, and SchemaBot's servers have %d drivers that can wait. To defer it, give the servers %d more by raising drivers or running more servers; otherwise apply without --defer-cutover",
			e.Database, e.Environment, e.Operations, e.Drivers, e.Operations-e.Drivers)
	}
}

func (e *DeferredCutoverCapacityError) deploymentError() string {
	switch {
	case e.Unreported:
		return fmt.Sprintf("%s/%s can't defer its cutover: the %s deployment doesn't report its drivers. To defer it, upgrade SchemaBot there; otherwise apply without --defer-cutover",
			e.Database, e.Environment, e.Deployment)
	case e.ExceedsPerApplyCap():
		return fmt.Sprintf("%s/%s can't defer its cutover: it would keep %d drivers waiting for the cutover on the %s deployment, and one apply can use at most %d there (max_drivers_per_apply). To defer it, raise max_drivers_per_apply on that deployment to %d; otherwise apply without --defer-cutover",
			e.Database, e.Environment, e.Operations, e.Deployment, e.PerApplyCap, e.Operations)
	case e.NoLiveServers():
		return fmt.Sprintf("%s/%s can't defer its cutover: no SchemaBot server running drivers on the %s deployment has checked in during the last %s. Apply again once its drivers are running, or apply without --defer-cutover",
			e.Database, e.Environment, e.Deployment, LiveWindowText())
	default:
		return fmt.Sprintf("%s/%s can't defer its cutover: it would keep %d drivers waiting for the cutover on the %s deployment, and its servers have %d drivers that can wait. To defer it, give that deployment's servers %d more by raising drivers or running more servers; otherwise apply without --defer-cutover",
			e.Database, e.Environment, e.Operations, e.Deployment, e.Drivers, e.Operations-e.Drivers)
	}
}

// deferredCutoverRefusalStatus is the HTTP status of a deferred cutover
// refusal: a conflict with the plane's current state when it can clear on its
// own, and an unprocessable request otherwise.
func deferredCutoverRefusalStatus(refused *DeferredCutoverCapacityError) int {
	if refused.Transient() {
		return http.StatusConflict
	}
	return http.StatusUnprocessableEntity
}

// LiveWindowText names storage.DriverPoolLiveWindow for a sentence ending
// "during the last ...".
func LiveWindowText() string {
	if storage.DriverPoolLiveWindow == time.Minute {
		return "minute"
	}
	return storage.DriverPoolLiveWindow.String()
}

// resolvedDrivers is this server's driver count: the configured Drivers, or
// DefaultDrivers when unset.
func (c *ServerConfig) resolvedDrivers() int {
	if c.Drivers > 0 {
		return c.Drivers
	}
	return DefaultDrivers
}

// deferredCutoverWidth counts the most operations an apply's groups would
// hold at a deferred cutover at once (storage.DeferredCutoverHeldAtOnce).
func deferredCutoverWidth(groups []*storage.ApplyOperationWithTasks) int {
	ops := make([]*storage.ApplyOperation, 0, len(groups))
	for _, group := range groups {
		ops = append(ops, group.Operation)
	}
	return storage.DeferredCutoverHeldAtOnce(ops)
}

// deferredCutoverCapacityRefusal refuses a deferred apply that would hold two
// or more operations at once when it would hold more of them than
// max_drivers_per_apply, or than the drivers of every live server can run. It
// returns nil when the apply fits. A deferred apply of one operation is never
// refused: its one cutover command reaches all of it as soon as it waits.
//
// A deployment whose operators run its operations on another plane holds one
// of that plane's drivers for each of them too, so each such deployment is
// checked the same way against its own plane (deferredCutoverDataPlaneRefusal).
//
// Only the apply's own width is checked. Other applies, deferred or not, use
// drivers the way any busy fleet does, and an operation waits for a free one.
// The one driver each server keeps for other work (reservedDriverID) is why
// deferred applies together can never leave every driver waiting on a person.
func (s *Service) deferredCutoverCapacityRefusal(ctx context.Context, plan *storage.Plan, environment, applyIdentifier, caller string, groups []*storage.ApplyOperationWithTasks) (*DeferredCutoverCapacityError, error) {
	operations := deferredCutoverWidth(groups)
	if operations <= 1 {
		return nil, nil
	}
	refusal := func(refused *DeferredCutoverCapacityError, reason string) *DeferredCutoverCapacityError {
		s.logger.Warn("apply rejected: "+reason,
			"plan_id", plan.PlanIdentifier, "apply_id", applyIdentifier, "database", plan.Database,
			"database_type", plan.DatabaseType, "environment", environment, "caller", caller,
			"operations", operations, "max_drivers_per_apply", refused.PerApplyCap,
			"drivers", refused.Drivers, "live_servers", refused.LiveServers)
		refused.Database = plan.Database
		refused.Environment = environment
		refused.Operations = operations
		return refused
	}
	perApplyCap := storage.BuildOptions(storage.WithMaxDriversPerApply(s.config.MaxDriversPerApply)).MaxDriversPerApply
	if operations > perApplyCap {
		return refusal(&DeferredCutoverCapacityError{PerApplyCap: perApplyCap},
			"deferred cutover would keep more drivers waiting at once than max_drivers_per_apply gives one apply"), nil
	}
	servers, err := s.storage.DriverPools().ListLive(ctx)
	if err != nil {
		return nil, fmt.Errorf("read live driver pools to admit deferred cutover of %s/%s: %w", plan.Database, environment, err)
	}
	drivers := 0
	for _, server := range servers {
		drivers += storage.DeferredCutoverDrivers(server.Drivers)
	}
	if len(servers) == 0 {
		return refusal(&DeferredCutoverCapacityError{},
			"deferred cutover cannot be admitted until a server reports its drivers"), nil
	}
	if operations > drivers {
		return refusal(&DeferredCutoverCapacityError{Drivers: drivers, LiveServers: len(servers)},
			"deferred cutover would keep more drivers waiting at once than the live servers' drivers can hold"), nil
	}
	return s.deferredCutoverDataPlaneRefusal(ctx, plan, environment, applyIdentifier, caller, groups)
}

// deferredCutoverDataPlaneRefusal checks each deployment of a deferred apply
// whose operators run its operations on another plane against that plane: the
// operations the apply would hold there at once must fit its
// max_drivers_per_apply and the drivers its live servers can keep waiting. A
// deployment that does not report its drivers is refused, since answering is
// also what says it runs the cutover of every operation a cutover command
// names. A
// report that cannot be read is returned as an error, so the apply is refused
// rather than admitted unchecked.
func (s *Service) deferredCutoverDataPlaneRefusal(ctx context.Context, plan *storage.Plan, environment, applyIdentifier, caller string, groups []*storage.ApplyOperationWithTasks) (*DeferredCutoverCapacityError, error) {
	byDeployment := make(map[string][]*storage.ApplyOperation)
	for _, group := range groups {
		byDeployment[group.Operation.Deployment] = append(byDeployment[group.Operation.Deployment], group.Operation)
	}
	for _, deployment := range slices.Sorted(maps.Keys(byDeployment)) {
		operations := storage.DeferredCutoverHeldAtOnce(byDeployment[deployment])
		if operations == 0 {
			continue
		}
		client, err := s.TernClient(deployment, environment)
		if err != nil {
			return nil, fmt.Errorf("resolve deployment %s/%s to check the deferred cutover of %s: %w", deployment, environment, plan.Database, err)
		}
		if !client.IsRemote() {
			s.logger.Debug("deferred cutover capacity: deployment runs on this plane's drivers, which are already checked",
				"plan_id", plan.PlanIdentifier, "apply_id", applyIdentifier, "database", plan.Database,
				"environment", environment, "deployment", deployment)
			continue
		}
		capacity, err := s.readDriverCapacity(ctx, client)
		if err != nil {
			return nil, fmt.Errorf("read driver capacity of deployment %s/%s to check the deferred cutover of %s: %w", deployment, environment, plan.Database, err)
		}
		refusal := func(refused *DeferredCutoverCapacityError, reason string) *DeferredCutoverCapacityError {
			s.logger.Warn("apply rejected: "+reason,
				"plan_id", plan.PlanIdentifier, "apply_id", applyIdentifier, "database", plan.Database,
				"database_type", plan.DatabaseType, "environment", environment, "caller", caller,
				"deployment", deployment, "operations", operations, "max_drivers_per_apply", refused.PerApplyCap,
				"drivers", refused.Drivers, "live_servers", refused.LiveServers)
			refused.Database = plan.Database
			refused.Environment = environment
			refused.Deployment = deployment
			refused.Operations = operations
			return refused
		}
		switch {
		case capacity == nil:
			return refusal(&DeferredCutoverCapacityError{Unreported: true},
				"deferred cutover needs a deployment that reports its drivers, and this one does not"), nil
		case capacity.MaxDriversPerApply <= 0:
			// A SchemaBot data plane always reports its resolved cap, so a
			// missing one is a deployment this check cannot reason about.
			return refusal(&DeferredCutoverCapacityError{Unreported: true},
				"deployment reported its drivers without its max_drivers_per_apply, so it is treated as not reporting them"), nil
		case operations > capacity.MaxDriversPerApply:
			return refusal(&DeferredCutoverCapacityError{PerApplyCap: capacity.MaxDriversPerApply},
				"deferred cutover would keep more drivers waiting on a deployment at once than its max_drivers_per_apply gives one apply"), nil
		case capacity.LiveServers == 0:
			return refusal(&DeferredCutoverCapacityError{},
				"deferred cutover cannot be checked until a server of the deployment reports its drivers"), nil
		case operations > capacity.DeferredCutoverDrivers:
			return refusal(&DeferredCutoverCapacityError{Drivers: capacity.DeferredCutoverDrivers, LiveServers: capacity.LiveServers},
				"deferred cutover would keep more drivers waiting on a deployment at once than its live servers run"), nil
		}
	}
	return nil, nil
}

// readDriverCapacity asks a remote deployment for the drivers its operators
// run, bounded like a health check. A client that cannot ask reports nothing,
// as an older deployment does.
func (s *Service) readDriverCapacity(ctx context.Context, client tern.Client) (*tern.DriverCapacity, error) {
	reader, ok := client.(tern.DriverCapacityReader)
	if !ok {
		s.logger.Warn("deferred cutover capacity: the remote deployment's client cannot read its drivers, so it is treated as not reporting them",
			"client_type", fmt.Sprintf("%T", client))
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, RemoteDeploymentHealthCheckTimeout)
	defer cancel()
	return reader.DriverCapacity(ctx)
}

// OperatorHeartbeatInterval is how often a running operator records its
// drivers in driver_pools. It is a quarter of
// storage.DriverPoolLiveWindow, so three heartbeats in a row can fail
// before the server's drivers stop counting toward deferred cutover admission.
const OperatorHeartbeatInterval = storage.DriverPoolLiveWindow / 4

// operatorHeartbeatTimeout bounds one driver pool heartbeat. The first one runs
// on the startup path, so a storage call that stops answering must not hold
// startup short of its shutdown handling; a heartbeat that times out is
// logged and retried on the next tick like any other failure.
const operatorHeartbeatTimeout = 5 * time.Second

// operatorDeregisterTimeout bounds the driver pool removal at operator shutdown.
// A removal that does not finish leaves the row to drop out of the live
// window on its own.
const operatorDeregisterTimeout = 5 * time.Second

// newDriverPoolServerID identifies one run of this process's operator. The
// process identity makes the row readable; the suffix keeps two operators in
// one process, and a restarted one, from sharing a row.
func newDriverPoolServerID() string {
	return storage.LeaseOwnerProcess() + "/operator-" + uuid.NewString()[:8]
}

// heartbeatDriverPool records this server's drivers in driver_pools. A
// failure is logged and left to the next heartbeat: until one lands, the
// server's drivers may drop out of the live count, which refuses wide
// deferred applies sooner rather than admitting more than can run.
func (s *Service) heartbeatDriverPool(ctx context.Context, server *storage.DriverPool) {
	timeout := s.operatorHeartbeatDeadline
	if timeout <= 0 {
		timeout = operatorHeartbeatTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := s.storage.DriverPools().Heartbeat(ctx, server); err != nil {
		s.logger.Warn("operator heartbeat failed; deferred cutover admission will not count this server's drivers once its last heartbeat leaves the live window",
			"server_id", server.ServerID, "drivers", server.Drivers,
			"live_window", storage.DriverPoolLiveWindow, "timeout", timeout, "error", err)
		return
	}
	if err := s.storage.DriverPools().PruneStale(ctx); err != nil {
		s.logger.Warn("driver pool prune failed; rows of stopped servers stay in the table until a later heartbeat prunes them, and are not counted toward deferred cutover admission",
			"server_id", server.ServerID, "retention", storage.DriverPoolRetention, "timeout", timeout, "error", err)
	}
}

// operatorHeartbeatLoop heartbeats server every interval until the claim gate
// closes or ctx ends.
func (s *Service) operatorHeartbeatLoop(ctx context.Context, stop <-chan struct{}, interval time.Duration, server *storage.DriverPool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			s.logger.Debug("operator: heartbeat stopping", "server_id", server.ServerID)
			return
		case <-ctx.Done():
			s.logger.Debug("operator: heartbeat stopping", "server_id", server.ServerID, "error", ctx.Err())
			return
		case <-ticker.C:
			s.heartbeatDriverPool(ctx, server)
		}
	}
}

// deregisterDriverPool removes this server's driver pool row at shutdown, so
// its drivers stop counting at once rather than when the live window lapses.
func (s *Service) deregisterDriverPool(serverID string) {
	ctx, cancel := context.WithTimeout(context.Background(), operatorDeregisterTimeout)
	defer cancel()
	if err := s.storage.DriverPools().Deregister(ctx, serverID); err != nil {
		s.logger.Warn("operator deregistration failed; this server's drivers keep counting toward deferred cutover admission until its last heartbeat leaves the live window",
			"server_id", serverID, "live_window", storage.DriverPoolLiveWindow, "error", err)
	}
}

// DeferredCutoverCapacityRefusal reports whether an apply of req's plan would
// be refused for keeping more drivers waiting at its deferred cutover than it
// can have, so a caller that asks a person to confirm the apply can refuse
// before asking. It returns nil when req does not defer the cutover or the
// apply fits. Apply creation checks again, so a fleet that shrinks between
// the two is still refused there.
func (s *Service) DeferredCutoverCapacityRefusal(ctx context.Context, req ApplyRequest) (*DeferredCutoverCapacityError, error) {
	plan, err := s.storage.Plans().Get(ctx, req.PlanID)
	if errors.Is(err, storage.ErrPlanNotFound) {
		s.logger.Info("deferred cutover capacity not checked ahead of apply creation; the plan is not stored, which apply creation reports",
			"plan_id", req.PlanID, "environment", req.Environment, "error", err)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load plan %s to check deferred cutover capacity: %w", req.PlanID, err)
	}
	if plan == nil {
		s.logger.Info("deferred cutover capacity not checked ahead of apply creation; the plan is not stored, which apply creation reports",
			"plan_id", req.PlanID, "environment", req.Environment)
		return nil, nil
	}
	options := maps.Clone(req.Options)
	if options == nil {
		options = make(map[string]string)
	}
	options["target"] = plan.Target
	applyOpts := storage.ApplyOptionsFromMap(options)
	if !applyOpts.DeferCutover {
		return nil, nil
	}
	groups, err := s.preflightApplyOperationGroups(ctx, plan, req, applyOpts)
	if err != nil {
		if _, ok := errors.AsType[*applyStorageError](err); ok {
			return nil, fmt.Errorf("build the operations of plan %s to check deferred cutover capacity: %w", req.PlanID, err)
		}
		// Apply creation refuses the same request with the refusal's own
		// message, which says more than a capacity check could.
		s.logger.Info("deferred cutover capacity not checked ahead of apply creation; apply creation refuses the request",
			"plan_id", plan.PlanIdentifier, "database", plan.Database, "database_type", plan.DatabaseType,
			"environment", req.Environment, "error", err)
		return nil, nil
	}
	return s.deferredCutoverCapacityRefusal(ctx, plan, req.Environment, "", resolveCaller(ctx, req.Caller), groups)
}

// preflightApplyOperationGroups builds the operations an apply of plan would
// create, the way createStoredApply builds them, without creating anything.
func (s *Service) preflightApplyOperationGroups(ctx context.Context, plan *storage.Plan, req ApplyRequest, applyOpts storage.ApplyOptions) ([]*storage.ApplyOperationWithTasks, error) {
	directExecution, err := s.applyDirectExecution(plan, req.Environment)
	if err != nil {
		return nil, err
	}
	applyOpts.DirectExecution = directExecution
	targets, _, err := s.applyTargets(plan, req)
	if err != nil {
		return nil, err
	}
	members, err := s.resolveApplyMembers(ctx, plan, req.Environment, targets)
	if err != nil {
		return nil, err
	}
	groups, _, err := buildApplyOperationGroups(plan, applyTaskChanges(plan), members, req.Environment, applyOpts,
		s.config.CutoverPolicyFor(plan.Database, req.Environment), s.config.OnFailure(plan.Database, req.Environment), time.Now())
	return groups, err
}
