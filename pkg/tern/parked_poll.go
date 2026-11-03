package tern

import (
	"context"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/storage"
)

// parkedFullPollInterval is how often a drive held at a deferred cutover runs
// its full poll. An operator's cutover, stop, or cancel wakes the drive at once,
// so the full poll refreshes progress, keeps the task rows' liveness writes
// landing, and bounds how late the drive sees a change that no request
// announces: an engine failure, or a remote that leaves the hold on its own, is
// seen within this interval. It must stay well inside
// storage.ApplyDriveStallAfter, or the operator would read the quiet task rows
// as a wedged drive and cancel it.
const parkedFullPollInterval = 5 * time.Second

// parkedPoll paces a drive's poll loop while it holds at a deferred cutover.
// Until the drive reports that it holds there, every tick runs the full poll.
// After that, a tick runs the full poll only when parkedFullPollInterval has
// passed since the last one or the apply has a pending cutover, stop, or
// cancel; every other tick makes one storage read and skips the rest. Those
// three are the requests that end a hold, and each is a pending control request
// from the moment it is recorded, so it reaches the drive on the next tick
// exactly as it would have at full cadence, from whichever process recorded it.
// Any other request, such as a skip-revert queued for later, waits for the
// next refresh, which resolves it as a full-cadence poll would.
//
// Each loop decides that it holds from its own source of truth: the stored
// task row for a sequential drive, the engine's answer for a grouped one, and
// the remote apply's projected state for a control-plane drive.
//
// A drive woken by a pending request runs every tick until it ends. The full
// poll that consumes a cutover can still find the engine holding, since the
// engine takes a poll or more to start its swap, and the request it consumed
// is no longer pending to wake the drive again. Slowing down there would leave
// the cutover's outcome unobserved until the next refresh. So every control
// read the full poll makes reports what it found through observe, which wakes
// the drive just as a pending request found between full polls does: a request
// the full poll consumes on a refresh, or one recorded while it runs, never
// leaves the drive slowed behind it.
type parkedPoll struct {
	interval time.Duration
	held     bool
	woken    bool
	lastFull time.Time
}

func newParkedPoll() parkedPoll {
	return parkedPoll{interval: parkedFullPollInterval}
}

// hold records that the full poll just run found the drive holding at a
// deferred cutover. A drive a pending request has woken stays at full cadence.
func (p *parkedPoll) hold() {
	if p.woken {
		return
	}
	p.held = true
}

// runFullPoll reports whether this tick runs the drive's full poll. A tick
// that runs it clears the hold, so the drive stays slowed only while each full
// poll finds it still holding. A failed read runs the full poll, whose own
// reads then surface whatever is wrong with storage.
func (p *parkedPoll) runFullPoll(ctx context.Context, store storage.Storage, apply *storage.Apply, now time.Time, logger *slog.Logger) bool {
	if p.held && ctx.Err() != nil {
		logger.DebugContext(ctx, "drive context ended while held at a deferred cutover; skipping the poll so the loop can exit",
			apply.MutableLogAttrs()...)
		return false
	}
	if !p.held || now.Sub(p.lastFull) >= p.interval {
		p.beginFullPoll(now)
		return true
	}
	pending, err := loadPendingControlOperations(ctx, store, apply)
	if err != nil {
		logger.WarnContext(ctx, "could not check for a pending control request while held at a deferred cutover; running the full poll",
			append(apply.MutableLogAttrs(), "error", err)...)
		p.beginFullPoll(now)
		return true
	}
	if wakesHeldDrive(pending) {
		logger.DebugContext(ctx, "cutover, stop, or cancel pending for an apply held at a deferred cutover; running every poll until the drive ends",
			apply.MutableLogAttrs()...)
		p.woken = true
		p.beginFullPoll(now)
		return true
	}
	return false
}

// observe records what a control read made during the full poll found. A
// pending cutover, stop, or cancel wakes the drive, so the hold that follows
// the poll leaves it at full cadence. A nil pacer, as in a call that paces
// nothing, ignores it.
func (p *parkedPoll) observe(pending pendingControlOperations) {
	if p == nil || !wakesHeldDrive(pending) {
		return
	}
	p.woken = true
}

// wakesHeldDrive reports whether a pending request is one that ends a hold at
// a deferred cutover, so the drive must see its effect at full cadence.
func wakesHeldDrive(pending pendingControlOperations) bool {
	return pending.has(storage.ControlOperationCutover, storage.ControlOperationStop, storage.ControlOperationCancel)
}

func (p *parkedPoll) beginFullPoll(now time.Time) {
	p.held = false
	p.lastFull = now
}
