package tern

import (
	"context"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/storage"
)

// parkedFullPollInterval is how often a drive held at a deferred cutover runs
// its full poll. Nothing changes for such a drive until an operator acts, and
// parkedPoll wakes it at once when one does, so the full poll only refreshes
// progress and keeps the task rows' liveness writes landing. It must stay well
// inside storage.ApplyDriveStallAfter, or the operator would read the quiet
// task rows as a wedged drive and cancel it.
const parkedFullPollInterval = 30 * time.Second

// parkedPoll paces a drive's poll loop while it holds at a deferred cutover.
// Until the drive reports that it holds there, every tick runs the full poll.
// After that, a tick runs the full poll only when parkedFullPollInterval has
// passed since the last one or the apply has a pending control request; every
// other tick makes one storage read and skips the rest. A cutover, stop, or
// cancel is a pending control request from the moment it is recorded, so it
// reaches the drive on the next tick exactly as it would have at full cadence,
// from whichever process recorded it.
//
// A drive woken by a pending request runs every tick until it ends. The full
// poll that consumes a cutover can still find the engine holding, since the
// engine takes a poll or more to start its swap, and the request it consumed
// is no longer pending to wake the drive again. Slowing down there would leave
// the cutover's outcome unobserved until the next refresh.
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
	if len(pending) > 0 {
		logger.DebugContext(ctx, "control request pending for an apply held at a deferred cutover; running every poll until the drive ends",
			apply.MutableLogAttrs()...)
		p.woken = true
		p.beginFullPoll(now)
		return true
	}
	return false
}

func (p *parkedPoll) beginFullPoll(now time.Time) {
	p.held = false
	p.lastFull = now
}
