package tern

import "github.com/block/schemabot/pkg/storage"

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
// Called by the scheduler before resuming an apply. Safe to call concurrently.
func (c *LocalClient) SetObserver(applyID int64, observer ProgressObserver) {
	c.observerMu.Lock()
	defer c.observerMu.Unlock()
	if c.observers == nil {
		c.observers = make(map[int64]ProgressObserver)
	}
	c.observers[applyID] = observer
}

// SetPendingObserver sets an observer that will be consumed by the next Apply()
// call. The observer is registered on the apply record before the engine starts,
// preventing the race where the apply completes before the observer is set.
// Called by the webhook handler before triggering the apply API call.
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
