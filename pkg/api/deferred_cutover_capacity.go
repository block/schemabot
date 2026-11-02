package api

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// DeferredCutoverCapacityError is apply creation refusing an apply started
// with --defer-cutover whose operations could not all wait at their cutover at
// once. The apply is ready to cut over only when every operation waits, and
// each waiting operation keeps its driver, so an apply wider than the drivers
// left for it would hold part of them until it timed out. The message names
// only the database, environment and counts, so a caller may show it as is.
type DeferredCutoverCapacityError struct {
	Database    string
	Environment string
	// Operations is how many operations the refused apply would hold at its
	// cutover.
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
}

// ExceedsCapacity reports whether the apply is wider than the running
// operators' whole deferred capacity, so waiting for other deferred applies
// would not admit it.
func (e *DeferredCutoverCapacityError) ExceedsCapacity() bool {
	return e.Operations > e.Capacity
}

func (e *DeferredCutoverCapacityError) Error() string {
	switch {
	case e.LiveServers == 0:
		return fmt.Sprintf("%s/%s: --defer-cutover would hold %d operations at their cutover at once, and no SchemaBot server has reported its drivers in the last %s; apply again without --defer-cutover",
			e.Database, e.Environment, e.Operations, LiveWindowText())
	case e.ExceedsCapacity():
		return fmt.Sprintf("%s/%s: --defer-cutover would hold %d operations at their cutover at once, and SchemaBot runs at most %d deferred operations; apply again without --defer-cutover",
			e.Database, e.Environment, e.Operations, e.Capacity)
	default:
		return fmt.Sprintf("%s/%s: --defer-cutover would hold %d operations at their cutover at once, and other deferred applies hold %d of the %d deferred operations SchemaBot runs; apply again without --defer-cutover, or once those applies finish",
			e.Database, e.Environment, e.Operations, e.Reserved, e.Capacity)
	}
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

// deferredCutoverWidth counts the operations an apply's groups would hold at a
// deferred cutover: its work operations that are not already settled. A
// finalizer runs after the work it finalizes and never waits at a cutover.
func deferredCutoverWidth(groups []*storage.ApplyOperationWithTasks) int {
	width := 0
	for _, group := range groups {
		op := group.Operation
		if op.OperationKind != storage.ApplyOperationKindWork || state.IsApplyOperationTerminal(op.State) {
			continue
		}
		width++
	}
	return width
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
		Database:    plan.Database,
		Environment: environment,
		Operations:  refused.Operations,
		Reserved:    refused.Reserved,
		Capacity:    refused.Capacity,
		LiveServers: refused.LiveServers,
	}
}

// OperatorHeartbeatInterval is how often a running operator records its
// drivers in the operator server registry. It is a quarter of
// storage.OperatorServerLiveWindow, so three heartbeats in a row can fail
// before the server's drivers stop counting toward deferred cutover admission.
const OperatorHeartbeatInterval = storage.OperatorServerLiveWindow / 4

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
	if err := s.storage.OperatorServers().Heartbeat(ctx, server); err != nil {
		s.logger.Warn("operator heartbeat failed; deferred cutover admission will not count this server's drivers once its last heartbeat leaves the live window",
			"server_id", server.ServerID, "drivers", server.Drivers,
			"deferred_cutover_drivers", server.DeferredCutoverDrivers,
			"live_window", storage.OperatorServerLiveWindow, "error", err)
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
