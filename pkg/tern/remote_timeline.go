package tern

import (
	"context"
	"fmt"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// remoteTransitionMirrorLimit bounds the data-plane records read when the drive
// reconciles a finished schema change. A single apply's timeline is far smaller
// than this; the bound exists so a data plane with a runaway log cannot make the
// reconcile expensive.
const remoteTransitionMirrorLimit = 500

// mirrorMissingRemoteTransitions records the state transitions the data plane
// passed through that the control plane's polling never sampled.
//
// The control plane learns remote state by asking for it on an interval, so it
// records the states a schema change happened to be in when it asked. A state
// the data plane occupies only briefly between two polls leaves no trace at all,
// and the control-plane timeline reads as a transition that never happened:
// revert_window straight to completed, with the skipping_revert in between
// simply gone. Reconstructing the run then means reading a second surface and
// knowing to distrust the first.
//
// The data plane keeps its own record of every transition, so once the schema
// change is finished that record is complete and can be read once and merged.
// The mirrored entries are marked as observed on the data plane, so the
// timeline distinguishes what this plane saw from what it was told.
//
// Best-effort by design: this runs after the terminal state is already
// persisted, and a timeline that stays incomplete must not turn a finished
// schema change into a failed reconcile. Every skipped path says why.
func (c *GRPCClient) mirrorMissingRemoteTransitions(ctx context.Context, apply *storage.Apply, remoteID string) {
	logger := c.applyLogger(apply)
	if remoteID == "" {
		logger.Debug("skipping data-plane transition mirror: the drive has no remote apply id",
			apply.MutableLogAttrs()...)
		return
	}
	if c.client == nil {
		logger.Debug("skipping data-plane transition mirror: the drive holds no data-plane connection to read from",
			apply.MutableLogAttrs()...)
		return
	}
	resp, err := c.Logs(ctx, &ternv1.LogsRequest{
		ApplyId:     remoteID,
		Database:    apply.Database,
		Type:        apply.DatabaseType,
		Environment: apply.Environment,
		Limit:       remoteTransitionMirrorLimit,
	})
	if err != nil {
		logger.Warn("the control-plane timeline will omit any state this schema change passed through between polls: its data-plane records could not be read",
			append(apply.MutableLogAttrs(), "remote_apply_id", remoteID, "error", err)...)
		return
	}
	recorded, err := c.recordedTransitionStates(ctx, apply)
	if err != nil {
		logger.Warn("the control-plane timeline will omit any state this schema change passed through between polls: its own records could not be read",
			append(apply.MutableLogAttrs(), "remote_apply_id", remoteID, "error", err)...)
		return
	}
	mirrored := 0
	for _, remote := range resp.Logs {
		if remote.EventType != storage.LogEventStateTransition || remote.NewState == "" {
			continue
		}
		if recorded[remote.NewState] {
			continue
		}
		recorded[remote.NewState] = true
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition,
			fmt.Sprintf("State changed to %s (observed on the data plane between control-plane polls)", remote.NewState),
			remote.OldState, remote.NewState)
		mirrored++
	}
	if mirrored > 0 {
		logger.Info("recorded states this schema change passed through between control-plane polls",
			append(apply.MutableLogAttrs(), "remote_apply_id", remoteID, "mirrored_transitions", mirrored)...)
	}
}

// recordedTransitionStates returns the states the control-plane timeline already
// names, so a mirror adds only what its polling missed.
func (c *GRPCClient) recordedTransitionStates(ctx context.Context, apply *storage.Apply) (map[string]bool, error) {
	logs, err := c.storage.ApplyLogs().List(ctx, storage.ApplyLogFilter{
		ApplyID:   apply.ID,
		EventType: storage.LogEventStateTransition,
		Limit:     remoteTransitionMirrorLimit,
	})
	if err != nil {
		return nil, fmt.Errorf("list recorded state transitions for apply %s: %w", apply.ApplyIdentifier, err)
	}
	recorded := make(map[string]bool, len(logs))
	for _, entry := range logs {
		if entry.NewState != "" {
			recorded[entry.NewState] = true
		}
	}
	return recorded, nil
}
