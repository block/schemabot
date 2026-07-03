package tern

import (
	"context"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// ProgressObserver receives notifications from the apply progress poller.
// Implementations can post PR comments, update dashboards, send Slack
// messages, etc. The observer is optional — if nil, the poller runs
// execution only. Errors from observer methods are logged but never
// block or fail the schema change.
//
// Lifecycle:
//   - OnProgress is called on each poller tick with the current state
//   - OnTerminal is called once when the apply reaches a terminal state
//
// The observer is per-apply, not per-client. It's set when the apply
// starts (webhook handler creates it) or when recovery resumes an apply
// (reconstructed from the apply record's stored GitHub context).
type ProgressObserver interface {
	// OnProgress is called on each progress poller tick.
	OnProgress(apply *storage.Apply, tasks []*storage.Task)

	// OnTerminal is called when the apply reaches a terminal state
	// (completed, failed, reverted, cancelled).
	OnTerminal(apply *storage.Apply, tasks []*storage.Task)
}

// SetObserver registers a progress observer for an apply.
// Called by the operator before resuming an apply. Safe to call concurrently.
func (c *LocalClient) SetObserver(applyID int64, observer ProgressObserver) {
	c.observerMu.Lock()
	defer c.observerMu.Unlock()

	if observer == nil {
		delete(c.observers, applyID)
		if c.logger != nil {
			c.logger.Debug("cleared progress observer for apply", "apply_id", applyID)
		}
		return
	}

	if c.observers == nil {
		c.observers = make(map[int64]ProgressObserver)
	}
	if _, exists := c.observers[applyID]; exists {
		if c.logger != nil {
			c.logger.Debug("progress observer already registered for apply", "apply_id", applyID)
		}
		return
	}
	c.observers[applyID] = observer
}

// SetPendingObserver sets an observer that will be consumed by the next direct
// client Apply() call. The API service uses its own pending-observer registry
// because operator drivers dispatch API-created applies asynchronously.
func (c *LocalClient) SetPendingObserver(observer ProgressObserver) {
	c.observerMu.Lock()
	defer c.observerMu.Unlock()
	c.pendingObserver = observer
}

// consumePendingObserver returns and clears the pending observer.
// Called inside Apply() to register it on the new apply.
func (c *LocalClient) consumePendingObserver() ProgressObserver {
	c.observerMu.Lock()
	defer c.observerMu.Unlock()
	obs := c.pendingObserver
	c.pendingObserver = nil
	return obs
}

// getObserver returns the observer for an apply, or nil if none is set.
func (c *LocalClient) getObserver(applyID int64) ProgressObserver {
	c.observerMu.RLock()
	defer c.observerMu.RUnlock()
	return c.observers[applyID]
}

// clearObserver removes the observer for an apply (called on terminal state).
func (c *LocalClient) clearObserver(applyID int64) {
	c.observerMu.Lock()
	defer c.observerMu.Unlock()
	delete(c.observers, applyID)
}

// takeObserver atomically removes and returns the observer for an apply, or
// nil if none is set. Terminal delivery goes through it so that when the
// drive's own terminal path races a late registration's re-check, exactly one
// of them holds the observer and notifies.
func (c *LocalClient) takeObserver(applyID int64) ProgressObserver {
	c.observerMu.Lock()
	defer c.observerMu.Unlock()
	obs := c.observers[applyID]
	delete(c.observers, applyID)
	return obs
}

// deliverTerminalIfSettled re-checks a freshly registered observer's apply and
// delivers the terminal notification if the drive has already finished. An
// apply becomes claimable the moment its create transaction commits — before
// Apply registers the dispatch's observer — so a poll-tick claim can start the
// drive inside that window. Progress lookups re-read the registry per event,
// so a still-running drive picks the late observer up on its own; a drive fast
// enough to have already settled (e.g. a task-less no-op) delivered its
// terminal notification to nobody, which without this re-check would strand
// the observer unnotified and registered forever.
func (c *LocalClient) deliverTerminalIfSettled(ctx context.Context, applyID int64) {
	if c.getObserver(applyID) == nil {
		return
	}
	apply, err := c.storage.Applies().Get(ctx, applyID)
	if err != nil {
		c.logger.Warn("could not re-check apply state after observer registration; a drive that already settled will not have notified the observer",
			"apply_id", applyID, "error", err)
		return
	}
	if apply == nil {
		// The apply was created moments earlier on this same path, so a missing
		// row is anomalous rather than an error condition. Leave the observer for
		// the drive rather than notifying against a phantom terminal.
		c.logger.Warn("apply not found when re-checking state after observer registration; leaving the observer for the drive",
			"apply_id", applyID)
		return
	}
	if !state.IsTerminalApplyState(apply.State) {
		return
	}
	// Claim the observer before loading tasks. If the drive's own terminal path
	// won the race and already took it, there is nothing to deliver — return
	// early and skip the tasks query entirely.
	obs := c.takeObserver(applyID)
	if obs == nil {
		return
	}
	tasks, err := c.storage.Tasks().GetByApplyID(ctx, applyID)
	if err != nil {
		c.logger.Warn("could not load tasks for late terminal observer notification; notifying without them",
			"apply_id", applyID, "error", err)
		tasks = nil
	}
	obs.OnTerminal(apply, tasks)
}
