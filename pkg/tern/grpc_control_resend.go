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
	// remain pending (measured from when the operator's command was recorded)
	// before the driver escalates each retransmission from info to warn and
	// counts it in the stale-control metric. A healthy data plane consumes a
	// stop/cancel within seconds; past this threshold the remote is not
	// converging and an operator should look at the data-plane logs for the
	// failing consume.
	remoteControlStaleThreshold = 2 * time.Minute
)

// remoteControlSendGate throttles retransmission of durable control requests
// to the data plane. Entries are keyed by control-request row ID and remember
// which arming of that row they sent, by its revision and arming stamp, so a
// new request (a fresh operator command) always transmits immediately,
// including one that re-arms a row this process already sent for. State is in-memory by design: it is a per-process
// optimization over the durable request row — after a restart the driver
// re-sends once and re-enters the throttled cadence.
type remoteControlSendGate struct {
	mu       sync.Mutex
	lastSent map[int64]remoteControlSend
}

// remoteControlSend is one transmission of a control request: when it was
// sent, and which arming of the request row it carried.
type remoteControlSend struct {
	sentAt   time.Time
	armedAt  time.Time
	revision int64
}

// carries reports whether this transmission carried the command the request
// row holds now. The revision tells apart two commands recorded within the
// same second; the arming stamp tells apart a re-arm written by a server that
// does not advance the revision.
func (s remoteControlSend) carries(controlReq *storage.ApplyControlRequest) bool {
	return s.revision == controlReq.Revision && s.armedAt.Equal(controlRequestArmedAt(controlReq))
}

// controlRequestArmedAt is when the operator's command behind a pending request
// was recorded. There is one request row per apply and operation, and
// re-requesting an operation re-arms that row in place: it keeps the row's id
// and creation time and stamps its last write. Nothing else writes a pending
// row, so the later of the two stamps names the command the row now carries.
func controlRequestArmedAt(controlReq *storage.ApplyControlRequest) time.Time {
	if controlReq.UpdatedAt.After(controlReq.CreatedAt) {
		return controlReq.UpdatedAt
	}
	return controlReq.CreatedAt
}

// shouldSend reports whether the control request should be (re-)transmitted
// now: always for a command this process has not sent yet, and at most once
// per remoteControlResendInterval afterwards.
func (g *remoteControlSendGate) shouldSend(controlReq *storage.ApplyControlRequest, now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	last, ok := g.lastSent[controlReq.ID]
	if !ok || !last.carries(controlReq) {
		return true
	}
	return now.Sub(last.sentAt) >= remoteControlResendInterval
}

// recordSend records a successful transmission and reports whether it was this
// process's first for the command the request carries.
func (g *remoteControlSendGate) recordSend(controlReq *storage.ApplyControlRequest, now time.Time) (first bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.lastSent == nil {
		g.lastSent = make(map[int64]remoteControlSend)
	}
	last, seen := g.lastSent[controlReq.ID]
	g.lastSent[controlReq.ID] = remoteControlSend{sentAt: now, armedAt: controlRequestArmedAt(controlReq), revision: controlReq.Revision}
	return !seen || !last.carries(controlReq)
}

// clear forgets a control request once its drive completes or fails it. A
// request resolved elsewhere, such as by the rollout projection on another
// drive, is never cleared here, so a re-issued command is told apart from the
// resolved one by its arming rather than by this.
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
	pendingFor := now.Sub(controlRequestArmedAt(controlReq))
	attrs := append(apply.MutableLogAttrs(),
		"operation", string(controlReq.Operation),
		"requested_by", controlReq.RequestedBy,
		"pending_for", pendingFor.Round(time.Second),
	)
	if pendingFor >= remoteControlStaleThreshold {
		metrics.RecordRemoteControlRequestStale(ctx, string(controlReq.Operation), apply.Database, apply.Deployment, apply.Environment)
		logger.WarnContext(ctx, "remote control request accepted but still unconsumed by the data plane; driver keeps re-sending and polling — check data-plane logs for the failing consume", attrs...)
		return
	}
	logger.InfoContext(ctx, "re-sent pending remote control request to the data plane", attrs...)
}
