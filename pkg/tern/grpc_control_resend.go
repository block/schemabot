package tern

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

const (
	// remoteControlResendInterval bounds how often a driver retransmits a
	// pending stop/cancel control request to the data plane. The data plane
	// records the request durably on first receipt and its own driver consumes
	// it, so retransmission is redelivery insurance, not the mechanism of
	// record — re-sending on every progress tick only floods the data plane
	// with duplicate RPCs and the apply log with duplicate accept events.
	remoteControlResendInterval = 30 * time.Second

	// remoteControlStaleThreshold is how long an accepted stop/cancel may
	// remain pending (measured from the durable request's creation) before the
	// driver escalates each retransmission from info to warn and counts it in
	// the stale-control metric. A healthy data plane consumes a stop/cancel
	// within seconds; past this threshold the remote is not converging and an
	// operator should look at the data-plane logs for the failing consume.
	remoteControlStaleThreshold = 2 * time.Minute
)

// remoteControlSendGate throttles retransmission of durable control requests
// to the data plane. Entries are keyed by control-request row ID, so a new
// request (a fresh operator command) always transmits immediately. State is
// in-memory by design: it is a per-process optimization over the durable
// request row — after a restart the driver re-sends once and re-enters the
// throttled cadence.
type remoteControlSendGate struct {
	mu       sync.Mutex
	lastSent map[int64]time.Time
}

// shouldSend reports whether the control request should be (re-)transmitted
// now: always for a request this process has not sent yet, and at most once
// per remoteControlResendInterval afterwards.
func (g *remoteControlSendGate) shouldSend(controlReqID int64, now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	last, ok := g.lastSent[controlReqID]
	return !ok || now.Sub(last) >= remoteControlResendInterval
}

// recordSend records a successful transmission and reports whether it was this
// process's first for the request.
func (g *remoteControlSendGate) recordSend(controlReqID int64, now time.Time) (first bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.lastSent == nil {
		g.lastSent = make(map[int64]time.Time)
	}
	_, seen := g.lastSent[controlReqID]
	g.lastSent[controlReqID] = now
	return !seen
}

// clear forgets a control request once it is completed or failed. This is not
// only map hygiene: there is one request row per apply and operation, and
// re-requesting an operation reuses that row, so a resolved request's entry
// would still be keyed to the id a re-issued command arrives under. Without
// this the operator's next stop or cancel for the same apply would be throttled
// behind the resolved one instead of transmitting immediately.
func (g *remoteControlSendGate) clear(controlReqID int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.lastSent, controlReqID)
}

// logRemoteControlResend records a retransmission of a still-pending control
// request. Only the first transmission writes an operator-facing apply-log
// event (the caller does that); retransmissions log server-side, escalating to
// warn with a metric once the request has been pending past
// remoteControlStaleThreshold — at that point the data plane is not consuming
// an accepted command and an operator must check its logs for the failing
// consume.
// The logger is expected to carry the apply's identity attributes already
// bound, so each line appends only the mutable snapshot.
func logRemoteControlResend(ctx context.Context, logger *slog.Logger, apply *storage.Apply, controlReq *storage.ApplyControlRequest, now time.Time) {
	pendingFor := now.Sub(controlReq.CreatedAt)
	attrs := append(apply.MutableLogAttrs(),
		"operation", string(controlReq.Operation),
		"requested_by", controlReq.RequestedBy,
		"pending_for", pendingFor.Round(time.Second),
	)
	if pendingFor >= remoteControlStaleThreshold {
		metrics.RecordRemoteControlRequestStale(ctx, string(controlReq.Operation), apply.Database, apply.Deployment, apply.Environment)
		logger.Warn("remote control request accepted but still unconsumed by the data plane; driver keeps re-sending and polling — check data-plane logs for the failing consume", attrs...)
		return
	}
	logger.Info("re-sent pending remote control request to the data plane", attrs...)
}
