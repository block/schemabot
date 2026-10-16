package tern

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/panicsafe"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// defaultObserverPollInterval is how often a multi-operation observer poller
// reads the apply back from storage.
const defaultObserverPollInterval = 5 * time.Second

func (c *LocalClient) observerPollInterval() time.Duration {
	if c.observerPollIntervalOverride > 0 {
		return c.observerPollIntervalOverride
	}
	return defaultObserverPollInterval
}

// pollMultiOperationObserver notifies an apply's observer from storage while
// the apply's work runs as more than one operation. Each operation's drive
// holds only its operation lease and leaves the parent apply to the operator's
// projection, so no drive reports apply-level progress or the terminal state
// to the observer; this poller does, the way GRPCClient's poller does for
// applies driven in another process. A single-operation apply's drive notifies
// the observer itself, so the poller stays silent for it.
//
// The poll runs behind a panic boundary: a panic in an observer callback stops
// this apply's notifications, not the process that drives every apply.
func (c *LocalClient) pollMultiOperationObserver(ctx context.Context, applyID int64) {
	err := panicsafe.Call(func() error {
		c.notifyMultiOperationObserverUntilSettled(ctx, applyID)
		return nil
	})
	if err == nil {
		return
	}
	var pollPanic *panicsafe.Error
	if !errors.As(err, &pollPanic) {
		c.logger.Error("multi-operation observer poll failed; progress and terminal notifications for this apply are stopped", "error", err)
		c.clearObserver(applyID)
		return
	}
	attrs := []any{
		"panic", fmt.Sprint(pollPanic.Value),
		"stack", string(pollPanic.Stack),
	}
	// Best-effort identifier lookup for triage: the poller may have panicked
	// before its first successful apply load.
	if apply, lookupErr := c.storage.Applies().Get(context.WithoutCancel(ctx), applyID); lookupErr == nil && apply != nil {
		attrs = append(apply.LogAttrs(), attrs...)
	}
	c.logger.Error("multi-operation observer poll panicked; progress and terminal notifications for this apply are stopped", attrs...)
	metrics.RecordRecoveredPanic(context.WithoutCancel(ctx), "observer_poll")
	c.clearObserver(applyID)
}

// notifyMultiOperationObserverUntilSettled is pollMultiOperationObserver's
// poll loop. It ticks until the observer is cleared, the apply settles, the
// apply row disappears, or the client closes.
func (c *LocalClient) notifyMultiOperationObserverUntilSettled(ctx context.Context, applyID int64) {
	ticker := time.NewTicker(c.observerPollInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Debug("observer poll: client closed; stopping poll")
			return
		case <-ticker.C:
		}
		if c.getObserver(applyID) == nil {
			c.logger.Debug("observer poll: observer cleared; stopping poll")
			return
		}
		if done := c.notifyMultiOperationObserverOnce(ctx, applyID); done {
			return
		}
	}
}

// notifyMultiOperationObserverOnce runs one poll tick and reports whether the
// poller is done. Load failures are transient: the next tick retries them.
func (c *LocalClient) notifyMultiOperationObserverOnce(ctx context.Context, applyID int64) bool {
	apply, err := c.storage.Applies().Get(ctx, applyID)
	if err != nil {
		c.logger.Warn("observer poll: failed to load apply; will retry on next tick", "error", err)
		return false
	}
	if apply == nil {
		c.logger.Warn("observer poll: apply not found; stopping poll")
		c.clearObserver(applyID)
		return true
	}
	ops, err := c.storage.ApplyOperations().ListByApply(ctx, applyID)
	if err != nil {
		c.logger.Warn("observer poll: failed to list operations; will retry on next tick",
			append(apply.MutableLogAttrs(), "error", err)...)
		return false
	}
	terminal := state.IsTerminalApplyState(apply.State)
	if !runsAsMultipleOperations(apply, ops) {
		if terminal {
			c.logger.Debug("observer poll: single-operation apply settled; its drive notifies the observer, stopping poll",
				apply.MutableLogAttrs()...)
			return true
		}
		c.logger.Debug("observer poll: single-operation apply; its drive notifies the observer",
			apply.MutableLogAttrs()...)
		return false
	}
	tasks, err := c.storage.Tasks().GetByApplyID(ctx, applyID)
	if err != nil {
		c.logger.Warn("observer poll: failed to load tasks; will retry on next tick",
			append(apply.MutableLogAttrs(), "error", err)...)
		return false
	}
	if terminal {
		// takeObserver, not get-then-clear: OnTerminal fires at most once even
		// when another path settles the same observer concurrently.
		if obs := c.takeObserver(applyID); obs != nil {
			obs.OnTerminal(apply, tasks)
		}
		return true
	}
	if obs := c.getObserver(applyID); obs != nil {
		obs.OnProgress(apply, tasks)
	}
	return false
}

// runsAsMultipleOperations reports whether an apply's work runs as more than
// one operation: more than one is attached, or its generation manifest
// declares more than one, of which some are still to attach.
func runsAsMultipleOperations(apply *storage.Apply, ops []*storage.ApplyOperation) bool {
	return len(ops) > 1 || len(apply.ExpectedOperationKeys) > 1
}
