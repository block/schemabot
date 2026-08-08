package tern

import (
	"context"
	"fmt"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// A deferred cutover is a hold SchemaBot owes the operator, not merely an action
// it declines to take.
//
// The drive honors `--defer-cutover` by not calling Cutover when the engine
// parks at the cutover gate. That is the whole of the mechanism, and it is
// enough only while the engine actually parks. An engine backend configured to
// cut over on its own never reports the gate: it goes straight from copying to
// swapping, the drive's defer branch is never evaluated, and the schema moves
// without the operator who asked to hold it. Nothing about that outcome is
// distinguishable, from the outside, from a deferral that worked and an operator
// who cut over promptly.
//
//	requested:  running ──▶ waiting_for_cutover ──[operator]──▶ cutting_over
//	violated:   running ─────────────────────────────────────▶ cutting_over
//	                     the gate is not bypassed; it never appears
//
// So the drive checks the other side: if a deferred apply is at or past the
// swap and SchemaBot never triggered the cutover, the backend did, and that is
// reported as the safety-gate failure it is rather than completing quietly.
//
// The check is deliberately not generalized to `--defer-deploy`. There, the
// engine performs the deploy as part of its own normal flow rather than through
// a SchemaBot call, so "advanced without a trigger event" is the ordinary case
// and the same shape would fire on every healthy apply.

// cutoverGatePassed reports whether an engine state means the schema swap has
// begun. Failure states are excluded: they say the change did not land, not that
// it landed without the operator.
func cutoverGatePassed(state engine.State) bool {
	switch state {
	case engine.StateCuttingOver, engine.StateRevertWindow, engine.StateReverting,
		engine.StateCompleted, engine.StateReverted:
		return true
	}
	return false
}

// detectDeferredCutoverViolation reports a deferred apply that the engine
// backend cut over on its own.
//
// It runs on the progress path, so it reads storage only once the state has
// already passed the gate on an apply that asked to hold — rare enough that the
// read costs nothing on a healthy fleet.
func (c *LocalClient) detectDeferredCutoverViolation(ctx context.Context, apply *storage.Apply, deferCutover bool, state engine.State, ps *atomicPollState) {
	if !deferCutover || ps.deferredCutoverViolationLogged || !cutoverGatePassed(state) {
		return
	}
	logger := c.logger.With(apply.IdentityLogAttrs()...)

	triggered, err := c.cutoverTriggeredBySchemaBot(ctx, apply)
	if err != nil {
		// An unreadable history is not evidence of a clean deferral, and must
		// not be latched as one: the check stays open and retries on the next
		// progress tick.
		logger.Warn("cannot tell whether a deferred cutover was honored; the apply's cutover history is unreadable and the check retries on the next progress tick",
			append(apply.MutableLogAttrs(), "engine_state", string(state), "error", err)...)
		return
	}
	if triggered {
		return
	}

	ps.deferredCutoverViolationLogged = true
	logger.Error("deferred cutover was not honored: the engine backend swapped the schema without SchemaBot triggering the cutover",
		append(apply.MutableLogAttrs(), "engine_state", string(state), "requested_cutover", "deferred")...)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelError, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
		fmt.Sprintf("Cutover was deferred, but the engine cut over on its own (engine state: %s). The schema has already been swapped without an operator cutover.", state),
		"", "")
	metrics.RecordDeferredCutoverViolation(ctx, apply.Database, apply.Deployment, apply.Environment, string(state))
}

// cutoverTriggeredBySchemaBot reports whether this apply's cutover came from
// SchemaBot — the drive's automatic trigger or an operator's cutover command,
// both of which record the same timeline event. Reading the durable timeline
// rather than the drive's own memory keeps the answer right across a lease
// handover, which is exactly when a long-running deferred apply changes hands.
func (c *LocalClient) cutoverTriggeredBySchemaBot(ctx context.Context, apply *storage.Apply) (bool, error) {
	logs, err := c.storage.ApplyLogs().List(ctx, storage.ApplyLogFilter{
		ApplyID:   apply.ID,
		EventType: storage.LogEventCutoverTriggered,
		Limit:     1,
	})
	if err != nil {
		return false, fmt.Errorf("list cutover-triggered events for apply %s: %w", apply.ApplyIdentifier, err)
	}
	return len(logs) > 0, nil
}
