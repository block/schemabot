package tern

import (
	"context"
	"fmt"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// closedApplyTerminalization says how an operation settles when the data plane
// refused its dispatch because the remote apply had already ended, and whether
// it settles at all.
//
// The refusal proves the operation's work never started: the data plane
// answers an operation already attached under this idempotency key with its
// remote apply, and refuses only work it has not taken. So the operation ends
// the way its siblings did rather than as a failure of its own:
//
//   - cancelled: the operator cancelled the apply while this shard was still
//     being dispatched, so it is cancelled with them.
//   - stopped: it is stopped with them, and a later start dispatches it again.
//   - failed: a sibling failed and closed the apply. This shard did not run,
//     so it settles cancelled, and the sibling's failure stays the apply's
//     verdict (a failed operation outranks a cancelled one when the parent
//     state is derived).
//
// Any other closed state, such as completed, is not one a never-started shard
// can join: the change did not land on it. That refusal keeps failing the
// operation closed so an operator looks at the target.
func closedApplyTerminalization(closedState, databaseType string) (undispatchedTerminalization, bool) {
	switch {
	case state.IsState(closedState, state.Apply.Cancelled):
		return cancelUndispatchedTerminalization(), true
	case state.IsState(closedState, state.Apply.Stopped):
		return stopUndispatchedTerminalization(databaseType), true
	case state.IsState(closedState, state.Apply.Failed):
		return cancelUndispatchedTerminalization(), true
	default:
		return undispatchedTerminalization{}, false
	}
}

// settleDispatchRefusedByClosedApply settles an operation whose dispatch the
// data plane refused because the remote apply had already ended, and reports
// whether it did. When it returns false the caller records the refusal as a
// failure, as for any other refusal.
//
// It settles only an operation-scoped drive: a whole-apply drive refused this
// way has no siblings whose outcome it could take, so its refusal stays a
// failure. The operation's tasks carry no error message, because the shard
// did not fail; the data plane's refusal text is logged here, never stored
// where a pull request renders it. The apply log entry carries no state pair:
// it is scoped to the apply, and the operation's own transition, which the
// server log carries, would read there as the apply's. The parent apply is
// not written, and an apply-level stop or cancel request stays pending for the
// projection to settle once every operation is terminal.
func (c *GRPCClient) settleDispatchRefusedByClosedApply(ctx context.Context, apply *storage.Apply, scope applyTaskScope, resp *ternv1.ApplyResponse, refusal string) (bool, error) {
	closedState := resp.GetClosedApplyState()
	if closedState == "" {
		return false, nil
	}
	logger := c.applyLogger(apply)
	if !scope.usesOperationRemoteResume() {
		logger.WarnContext(ctx, "data plane refused a whole-apply dispatch because its remote apply already ended; failing the apply closed",
			append(apply.MutableLogAttrs(),
				"closed_apply_state", closedState,
				"refusal", refusal)...)
		return false, nil
	}
	op := scope.operation
	terminalization, ok := closedApplyTerminalization(closedState, apply.DatabaseType)
	if !ok {
		logger.WarnContext(ctx, "data plane refused an operation because its remote apply already ended in a state a never-started operation cannot join; failing the operation closed",
			append(apply.MutableLogAttrs(),
				"apply_operation_id", op.ID,
				"operation_deployment", op.Deployment,
				"closed_apply_state", closedState,
				"refusal", refusal)...)
		return false, nil
	}
	oldState, err := c.settleUndispatchedOperationRows(ctx, apply, scope, terminalization.taskState, terminalization.operationState,
		"closed_apply_state", closedState)
	if err != nil {
		return false, fmt.Errorf("settle apply_operation %d of apply %s after the remote apply ended %s: %w", op.ID, apply.ApplyIdentifier, closedState, err)
	}
	logger.InfoContext(ctx, "data plane refused an operation because its remote apply already ended; settled the operation with its siblings instead of failing it",
		append(apply.MutableLogAttrs(),
			"apply_operation_id", op.ID,
			"operation_deployment", op.Deployment,
			"closed_apply_state", closedState,
			"old_operation_state", oldState,
			"new_operation_state", terminalization.operationState,
			"refusal", refusal)...)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo,
		fmt.Sprintf("Remote apply operation %d (deployment %s) %s before it started: the apply had already ended (%s)", op.ID, op.Deployment, terminalization.verb, closedState),
		"", "")
	return true, nil
}
