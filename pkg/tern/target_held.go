package tern

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

// targetHeldEscalationAfter is how long an apply's drives can be refused the
// target by another run before the wait is escalated to an Error log and a
// timeline event. The holder is normally a run an earlier driver is bringing
// down, which lets go within seconds; a hold this long means something else
// has the table — a run outside SchemaBot, or work a halt could not bring
// down — and an operator needs to find it. The apply keeps waiting, and stop
// and cancel stay available.
const targetHeldEscalationAfter = 2 * time.Minute

// targetHeldRefusalGap is how long after an apply's last refusal its hold is
// taken to have ended. It spans the grouped drive's cycle — hand back, let the
// lease go stale, be claimed again and refused again — so the refusals of one
// hold read as one hold rather than each restarting the measure.
const targetHeldRefusalGap = 2 * storage.ApplyLeaseStaleAfter

// targetHeldWaits measures, per apply, how long its drives in this process
// have been refused the target. The measure survives a hand-back, so the
// grouped drive that hands the apply back on every refusal is measured across
// the drives that claim it here; a drive in another process measures its own.
type targetHeldWaits struct {
	mu    sync.Mutex
	waits map[int64]*targetHeldWait
}

type targetHeldWait struct {
	since     time.Time
	lastSeen  time.Time
	escalated bool
}

// observe records a refusal of the apply's start at now. It returns how long
// the apply has been refused, and whether this refusal is the one that crosses
// targetHeldEscalationAfter, which is reported once per hold. A refusal more
// than targetHeldRefusalGap after the last one starts a new hold.
func (w *targetHeldWaits) observe(applyID int64, now time.Time) (heldFor time.Duration, escalate bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.waits == nil {
		w.waits = make(map[int64]*targetHeldWait)
	}
	for id, wait := range w.waits {
		if now.Sub(wait.lastSeen) > targetHeldRefusalGap {
			delete(w.waits, id)
		}
	}
	wait, ok := w.waits[applyID]
	if !ok {
		wait = &targetHeldWait{since: now}
		w.waits[applyID] = wait
	}
	wait.lastSeen = now
	heldFor = now.Sub(wait.since)
	if heldFor >= targetHeldEscalationAfter && !wait.escalated {
		wait.escalated = true
		return heldFor, true
	}
	return heldFor, false
}

// clear ends the apply's hold: the refused work was started again and was not
// refused.
func (w *targetHeldWaits) clear(applyID int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.waits, applyID)
}

// observeTargetHeld records a refusal of the apply's start and escalates the
// hold once it has persisted past targetHeldEscalationAfter. table names the
// refused table when the drive starts one table at a time, and is empty when
// the refused work spans the apply.
func (c *LocalClient) observeTargetHeld(ctx context.Context, logger *slog.Logger, apply *storage.Apply, table string) {
	heldFor, escalate := c.targetHeld.observe(apply.ID, time.Now())
	if !escalate {
		return
	}
	logger.Error("the target has been held by another run of a schema change past the escalation bound; the apply keeps waiting, and an operator needs to find the run holding it",
		append(apply.MutableLogAttrs(), "table", table, "held_for", heldFor.Round(time.Second), "escalation_after", targetHeldEscalationAfter)...)
	subject := "The target"
	if table != "" {
		subject = "Table " + table
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelError, storage.LogEventInfo, storage.LogSourceSchemaBot,
		fmt.Sprintf("%s has been held by another run of a schema change for %s; the apply keeps waiting for it. Find the run holding it, or stop the apply.", subject, heldFor.Round(time.Second)), "", "")
}

// engineRefusedTheTarget reports a poll whose run failed because another run
// of a schema change holds the target.
func engineRefusedTheTarget(result *engine.ProgressResult) bool {
	return result.State == engine.StateFailed && result.TargetHeld
}
