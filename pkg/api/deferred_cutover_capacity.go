package api

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// DeferredCutoverCapacityError is apply creation refusing an apply started
// with --defer-cutover that would keep more drivers waiting at its cutover
// than the running operators have left for deferred work. Each operation
// waiting at a deferred cutover keeps its driver until an operator cuts it
// over, so deferred applies admitted past that capacity would leave every
// deferred driver waiting on a person: another apply's operations could start
// only as someone cut over one already waiting, and a cutover command would
// reach only the operations that had started. The message names only the
// database, environment, counts and SchemaBot's own apply identifiers, so a
// caller may show it as is.
type DeferredCutoverCapacityError struct {
	Database    string
	Environment string
	// Deployment names the deployment whose operators the refusal counted
	// when they run on another plane, and is empty when it counted this
	// plane's own.
	Deployment string
	// Unreported is a refusal because Deployment does not report how many
	// operations it can hold at a deferred cutover, which a deployment
	// running an older SchemaBot does not. Only Operations is counted then.
	Unreported bool
	// Operations is how many operations the refused apply would hold at its
	// cutover at once.
	Operations int
	// Reserved is how many operations other active deferred applies hold or
	// will hold at theirs.
	Reserved int
	// Capacity is how many deferred operations the running operators run at
	// once, across every server.
	Capacity int
	// LiveServers is how many servers' operators reported their drivers
	// within the live window.
	LiveServers int
	// HoldingApplyIDs names the deferred applies counted in Reserved, so an
	// operator knows which to cut over or cancel to make room.
	HoldingApplyIDs []string
	// PerApplyCap is the server's max_drivers_per_apply, set only when the
	// apply would hold more operations at once than it. A waiting operation
	// keeps counting against the cap, so the operations past it could never
	// start while the first ones wait, and the apply would never have every
	// operation waiting, which its cutover needs.
	PerApplyCap int
}

// ExceedsPerApplyCap reports whether the apply would hold more operations at
// once than max_drivers_per_apply lets one apply run.
func (e *DeferredCutoverCapacityError) ExceedsPerApplyCap() bool {
	return e.PerApplyCap > 0 && e.Operations > e.PerApplyCap
}

// ExceedsCapacity reports whether the apply is wider than the running
// operators' whole deferred capacity, so waiting for other deferred applies
// would not admit it.
func (e *DeferredCutoverCapacityError) ExceedsCapacity() bool {
	return e.Operations > e.Capacity
}

// Transient reports whether the refusal can clear without changing the
// request: other deferred applies finishing, or a server reporting its
// drivers. An apply wider than max_drivers_per_apply, or than every live
// deferred driver together, is refused again on every retry.
func (e *DeferredCutoverCapacityError) Transient() bool {
	if e.Unreported || e.ExceedsPerApplyCap() {
		return false
	}
	return e.LiveServers == 0 || !e.ExceedsCapacity()
}

// operatorsText names whose operators a refusal counted.
func (e *DeferredCutoverCapacityError) operatorsText() string {
	if e.Deployment == "" {
		return "SchemaBot"
	}
	return "the " + e.Deployment + " deployment"
}

func (e *DeferredCutoverCapacityError) Error() string {
	switch {
	case e.Unreported:
		return fmt.Sprintf("%s/%s: --defer-cutover would hold %d operations on the %s deployment at their cutover at once, and that deployment does not report how many it can hold; upgrade SchemaBot there, or apply again without --defer-cutover",
			e.Database, e.Environment, e.Operations, e.Deployment)
	case e.ExceedsPerApplyCap() && e.Deployment != "":
		return fmt.Sprintf("%s/%s: --defer-cutover would hold %d operations on the %s deployment at their cutover at once, and max_drivers_per_apply there runs at most %d operations of one apply at once; apply again without --defer-cutover, or raise max_drivers_per_apply on that deployment to at least %d",
			e.Database, e.Environment, e.Operations, e.Deployment, e.PerApplyCap, e.Operations)
	case e.ExceedsPerApplyCap():
		return fmt.Sprintf("%s/%s: --defer-cutover would hold %d operations at their cutover at once, and max_drivers_per_apply runs at most %d operations of one apply at once; apply again without --defer-cutover, or raise max_drivers_per_apply to at least %d",
			e.Database, e.Environment, e.Operations, e.PerApplyCap, e.Operations)
	case e.LiveServers == 0 && e.Deployment != "":
		return fmt.Sprintf("%s/%s: --defer-cutover would hold %d operations on the %s deployment at their cutover at once, and no server of that deployment has reported its drivers in the last %s; apply again without --defer-cutover",
			e.Database, e.Environment, e.Operations, e.Deployment, LiveWindowText())
	case e.LiveServers == 0:
		return fmt.Sprintf("%s/%s: --defer-cutover would hold %d operations at their cutover at once, and no SchemaBot server has reported its drivers in the last %s; apply again without --defer-cutover",
			e.Database, e.Environment, e.Operations, LiveWindowText())
	case e.ExceedsCapacity():
		return fmt.Sprintf("%s/%s: --defer-cutover would hold %d operations at their cutover at once, and %s runs at most %d deferred operations; apply again without --defer-cutover",
			e.Database, e.Environment, e.Operations, e.operatorsText(), e.Capacity)
	default:
		return fmt.Sprintf("%s/%s: --defer-cutover would hold %d operations at their cutover at once, and other deferred applies (%s) hold %d of the %d deferred operations %s runs; apply again without --defer-cutover, or once those applies are cut over or cancelled",
			e.Database, e.Environment, e.Operations, HoldingAppliesText(e.HoldingApplyIDs), e.Reserved, e.Capacity, e.operatorsText())
	}
}

// deferredCutoverRefusalStatus is the HTTP status of a deferred cutover
// refusal: a conflict with the plane's current state when it can clear on its
// own, and an unprocessable request when the apply is wider than every live
// deferred driver together.
func deferredCutoverRefusalStatus(refused *DeferredCutoverCapacityError) int {
	if refused.Transient() {
		return http.StatusConflict
	}
	return http.StatusUnprocessableEntity
}

// maxHoldingAppliesNamed bounds how many holding applies a refusal names, so
// a crowded plane does not turn one refusal into a wall of identifiers.
const maxHoldingAppliesNamed = 5

// HoldingAppliesText lists the deferred applies a refusal counted, naming at
// most maxHoldingAppliesNamed and the number left over.
func HoldingAppliesText(ids []string) string {
	if len(ids) <= maxHoldingAppliesNamed {
		return strings.Join(ids, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(ids[:maxHoldingAppliesNamed], ", "), len(ids)-maxHoldingAppliesNamed)
}

// LiveWindowText names storage.OperatorServerLiveWindow for a sentence ending
// "in the last ...".
func LiveWindowText() string {
	if storage.OperatorServerLiveWindow == time.Minute {
		return "minute"
	}
	return storage.OperatorServerLiveWindow.String()
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
// hold at a deferred cutover at once, before any max_drivers_per_apply cap
// (storage.DeferredCutoverHeldAtOnce).
func deferredCutoverWidth(groups []*storage.ApplyOperationWithTasks) int {
	ops := make([]*storage.ApplyOperation, 0, len(groups))
	for _, group := range groups {
		ops = append(ops, group.Operation)
	}
	return storage.DeferredCutoverHeldAtOnce(ops, 0)
}

// deferredCutoverPerApplyCapRefusal refuses a deferred apply that would hold
// two or more operations at once, and more of them than max_drivers_per_apply.
// Each waiting operation keeps counting against the cap, so once the first
// operations wait at their cutover the rest can never start, and a cutover
// command, which needs every operation waiting, is refused until the apply
// times out. It returns nil when the apply fits. The cap is this server's
// configuration, so the check needs no storage read.
func (s *Service) deferredCutoverPerApplyCapRefusal(plan *storage.Plan, environment, applyIdentifier, caller string, operations int) *DeferredCutoverCapacityError {
	perApplyCap := storage.BuildOptions(storage.WithMaxDriversPerApply(s.config.MaxDriversPerApply)).MaxDriversPerApply
	if operations <= 1 || operations <= perApplyCap {
		return nil
	}
	s.logger.Warn("apply rejected: deferred cutover would hold more operations at once than max_drivers_per_apply runs of one apply",
		"plan_id", plan.PlanIdentifier, "apply_id", applyIdentifier, "database", plan.Database,
		"database_type", plan.DatabaseType, "environment", environment, "caller", caller,
		"operations", operations, "max_drivers_per_apply", perApplyCap)
	return &DeferredCutoverCapacityError{
		Database:    plan.Database,
		Environment: environment,
		Operations:  operations,
		PerApplyCap: perApplyCap,
	}
}

// deferredCutoverDataPlanes reads, for each deployment that runs the apply's
// operations on another plane's operators, how many of them the apply would
// hold at a deferred cutover at once and how many that plane reports it can
// hold, for apply creation to admit against (storage.Apply's
// DeferredCutoverDataPlanes). A deployment that does not report its capacity
// cannot hold a deferred apply's operations, so the apply is refused here, and
// a deployment whose report cannot be read refuses it with that error: either
// way nothing is stored.
func (s *Service) deferredCutoverDataPlanes(ctx context.Context, plan *storage.Plan, environment, applyIdentifier, caller string, groups []*storage.ApplyOperationWithTasks) ([]storage.DeferredCutoverDataPlane, error) {
	ops := make([]*storage.ApplyOperation, 0, len(groups))
	for _, group := range groups {
		ops = append(ops, group.Operation)
	}
	widths := storage.DeferredCutoverHeldAtOnceByDeployment(ops, storage.BuildOptions(storage.WithMaxDriversPerApply(s.config.MaxDriversPerApply)).MaxDriversPerApply)
	var planes []storage.DeferredCutoverDataPlane
	for _, deployment := range slices.Sorted(maps.Keys(widths)) {
		client, err := s.TernClient(deployment, environment)
		if err != nil {
			return nil, fmt.Errorf("resolve deployment %s/%s to admit the deferred cutover of apply %s: %w", deployment, environment, applyIdentifier, err)
		}
		if !client.IsRemote() {
			s.logger.Debug("deferred cutover admission: deployment runs on this plane's operators, which admission counts already",
				"apply_id", applyIdentifier, "database", plan.Database, "environment", environment, "deployment", deployment)
			continue
		}
		capacity, err := s.readDeferredCutoverCapacity(ctx, client)
		if err != nil {
			return nil, fmt.Errorf("read deferred cutover capacity of deployment %s/%s for apply %s: %w", deployment, environment, applyIdentifier, err)
		}
		if capacity == nil {
			s.logger.Warn("apply rejected: deferred cutover needs a deployment that reports how many operations it can hold at a deferred cutover, and this one does not",
				"plan_id", plan.PlanIdentifier, "apply_id", applyIdentifier, "database", plan.Database,
				"database_type", plan.DatabaseType, "environment", environment, "caller", caller,
				"deployment", deployment, "operations", widths[deployment])
			return nil, &DeferredCutoverCapacityError{
				Database:    plan.Database,
				Environment: environment,
				Deployment:  deployment,
				Unreported:  true,
				Operations:  widths[deployment],
			}
		}
		planes = append(planes, storage.DeferredCutoverDataPlane{
			Deployment:  deployment,
			Operations:  widths[deployment],
			LiveServers: capacity.LiveServers,
			Capacity:    capacity.Drivers,
		})
	}
	return planes, nil
}

// readDeferredCutoverCapacity asks a remote deployment how many operations it
// can hold at a deferred cutover, bounded like a health check. A client that
// cannot ask reports nothing, as an older deployment does.
func (s *Service) readDeferredCutoverCapacity(ctx context.Context, client tern.Client) (*tern.DeferredCutoverCapacity, error) {
	reader, ok := client.(tern.DeferredCutoverCapacityReader)
	if !ok {
		s.logger.Warn("deferred cutover admission: the remote deployment's client cannot read its deferred cutover capacity, so it is treated as not reporting one",
			"client_type", fmt.Sprintf("%T", client))
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, RemoteDeploymentHealthCheckTimeout)
	defer cancel()
	return reader.DeferredCutoverCapacity(ctx)
}

// deferredCutoverRefusal turns storage refusing a deferred apply's admission
// into the error apply creation returns, logging what the refusal counted.
func (s *Service) deferredCutoverRefusal(plan *storage.Plan, environment, applyIdentifier, caller string, refused *storage.DeferredCutoverCapacityError) *DeferredCutoverCapacityError {
	s.logger.Warn("apply rejected: deferred cutover does not fit in the deferred cutover drivers of the live operator servers",
		"plan_id", plan.PlanIdentifier, "apply_id", applyIdentifier, "database", plan.Database,
		"database_type", plan.DatabaseType, "environment", environment, "caller", caller,
		"operations", refused.Operations, "reserved_operations", refused.Reserved,
		"deferred_cutover_capacity", refused.Capacity, "live_operator_servers", refused.LiveServers,
		"holding_apply_ids", refused.HoldingApplyIDs)
	return &DeferredCutoverCapacityError{
		Database:        plan.Database,
		Environment:     environment,
		Deployment:      refused.Deployment,
		Operations:      refused.Operations,
		Reserved:        refused.Reserved,
		Capacity:        refused.Capacity,
		LiveServers:     refused.LiveServers,
		HoldingApplyIDs: refused.HoldingApplyIDs,
	}
}

// OperatorHeartbeatInterval is how often a running operator records its
// drivers in the operator server registry. It is a quarter of
// storage.OperatorServerLiveWindow, so three heartbeats in a row can fail
// before the server's drivers stop counting toward deferred cutover admission.
const OperatorHeartbeatInterval = storage.OperatorServerLiveWindow / 4

// operatorHeartbeatTimeout bounds one registry heartbeat. The first one runs
// on the startup path, so a storage call that stops answering must not hold
// startup short of its shutdown handling; a heartbeat that times out is
// logged and retried on the next tick like any other failure.
const operatorHeartbeatTimeout = 5 * time.Second

// operatorDeregisterTimeout bounds the registry removal at operator shutdown.
// A removal that does not finish leaves the row to drop out of the live
// window on its own.
const operatorDeregisterTimeout = 5 * time.Second

// newOperatorServerID identifies one run of this process's operator. The
// process identity makes the row readable; the suffix keeps two operators in
// one process, and a restarted one, from sharing a row.
func newOperatorServerID() string {
	return storage.LeaseOwnerProcess() + "/operator-" + uuid.NewString()[:8]
}

// heartbeatOperatorServer records this server's drivers in the registry. A
// failure is logged and left to the next heartbeat: until one lands, the
// server's drivers may drop out of the live count, which refuses wide
// deferred applies sooner rather than admitting more than run.
func (s *Service) heartbeatOperatorServer(ctx context.Context, server *storage.OperatorServer) {
	timeout := s.operatorHeartbeatDeadline
	if timeout <= 0 {
		timeout = operatorHeartbeatTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := s.storage.OperatorServers().Heartbeat(ctx, server); err != nil {
		s.logger.Warn("operator heartbeat failed; deferred cutover admission will not count this server's drivers once its last heartbeat leaves the live window",
			"server_id", server.ServerID, "drivers", server.Drivers,
			"deferred_cutover_drivers", server.DeferredCutoverDrivers,
			"live_window", storage.OperatorServerLiveWindow, "timeout", timeout, "error", err)
		return
	}
	if err := s.storage.OperatorServers().PruneStale(ctx); err != nil {
		s.logger.Warn("operator server registry prune failed; rows of stopped servers stay in the table until a later heartbeat prunes them, and are not counted toward deferred cutover admission",
			"server_id", server.ServerID, "retention", storage.OperatorServerRetention, "timeout", timeout, "error", err)
	}
}

// operatorHeartbeatLoop heartbeats server every interval until the claim gate
// closes or ctx ends.
func (s *Service) operatorHeartbeatLoop(ctx context.Context, stop <-chan struct{}, interval time.Duration, server *storage.OperatorServer) {
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
			s.heartbeatOperatorServer(ctx, server)
		}
	}
}

// deregisterOperatorServer removes this server's registry row at shutdown, so
// its drivers stop counting at once rather than when the live window lapses.
func (s *Service) deregisterOperatorServer(serverID string) {
	ctx, cancel := context.WithTimeout(context.Background(), operatorDeregisterTimeout)
	defer cancel()
	if err := s.storage.OperatorServers().Deregister(ctx, serverID); err != nil {
		s.logger.Warn("operator deregistration failed; this server's drivers keep counting toward deferred cutover admission until its last heartbeat leaves the live window",
			"server_id", serverID, "live_window", storage.OperatorServerLiveWindow, "error", err)
	}
}
