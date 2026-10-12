// local_cancelled_resume_state.go reclaims the work a cancelled apply's stored
// engine resume state names, so a cancel that lands between drives does not
// strand work the engine set up in the service it drives.
//
// A running drive ends on the operator's cancel cause and reclaims its own work
// on the way out. A cancel consumed by the drive that claims the apply next
// stands that drive down before it resumes, so no drive sees the work: the
// stored resume state is its only record, and this file hands that record to
// the engine. It never decides the cancel: the destructive direction is leaving
// the work running, so a reclaim that fails is logged and the cancel proceeds.
package tern

import (
	"context"
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

// reclaimCancelledResumeState hands each stored engine resume state of the
// cancelled apply to the engine for reclaiming, and records what the engine
// removed and what it left in place on the apply log. The caller holds the
// claim on the apply, so no drive is resuming from the same state meanwhile.
//
// Every apply operation with a stored state is visited: an apply with several
// deployments has one per deployment, and each can name work of its own. A
// reclaim that fails in one state is reported and the others are still
// visited, so one unreachable service does not leave every state's work
// behind. The error returned joins what failed; the caller logs it and goes
// on with the cancel.
func (c *LocalClient) reclaimCancelledResumeState(ctx context.Context, eng engine.Engine, apply *storage.Apply, tasks []*storage.Task) error {
	if eng == nil {
		return fmt.Errorf("no engine configured for type: %s", c.config.Type)
	}
	if apply == nil {
		return fmt.Errorf("apply is required to reclaim a cancelled schema change's stored engine state")
	}
	if _, ok := eng.(engine.CancelledResumeStateReclaimer); !ok {
		c.logger.Debug("engine keeps no work between drives that a cancel must reclaim",
			append(apply.LogAttrs(), "engine", eng.Name())...)
		return nil
	}

	var failures []string
	for _, task := range c.operationTasks(apply, tasks) {
		stored, err := c.loadStoredEngineResumeState(ctx, task, "cancel reclaim")
		if err != nil {
			failures = append(failures, err.Error())
			continue
		}
		if stored == nil {
			continue
		}
		creds, err := c.credentialsForTask(task)
		if err != nil {
			failures = append(failures, fmt.Sprintf("resolve credentials to reclaim stored engine state of task %s: %v", task.TaskIdentifier, err))
			continue
		}
		_, result, err := engine.ReclaimCancelledResumeState(ctx, eng, &engine.ReclaimResumeStateRequest{
			Database:    apply.Database,
			Credentials: creds,
			Options:     apply.GetOptions().Map(),
			ResumeState: stored,
			Logger:      c.logger.With(apply.IdentityLogAttrs()...),
		})
		c.recordReclaimResult(ctx, apply, result)
		if err != nil {
			failures = append(failures, err.Error())
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("reclaim the cancelled schema change's stored engine state: %s", strings.Join(failures, "; "))
	}
	return nil
}

// operationTasks returns one task of the apply per apply operation, so each
// operation's stored engine resume state is loaded once. A task with no apply
// operation has no stored state to load and is left out.
func (c *LocalClient) operationTasks(apply *storage.Apply, tasks []*storage.Task) []*storage.Task {
	seen := map[int64]bool{}
	var representatives []*storage.Task
	for _, task := range tasks {
		if task == nil || task.ApplyID != apply.ID {
			continue
		}
		operationID, err := applyOperationIDForTask(task)
		if err != nil {
			c.logger.Debug("task has no apply operation, so it holds no stored engine state for the cancel to reclaim",
				append(task.LogAttrs(), "reason", err.Error())...)
			continue
		}
		if seen[operationID] {
			continue
		}
		seen[operationID] = true
		representatives = append(representatives, task)
	}
	return representatives
}

// recordReclaimResult writes what the engine removed and what it left in place
// to the server log and the apply log, so an operator reading the cancelled
// schema change learns what is still in the service and why.
func (c *LocalClient) recordReclaimResult(ctx context.Context, apply *storage.Apply, result *engine.ReclaimResumeStateResult) {
	if result == nil {
		return
	}
	if len(result.Reclaimed) > 0 {
		c.logger.Info("reclaimed the work the cancelled schema change had set up",
			append(apply.LogAttrs(), "reclaimed", result.Reclaimed)...)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
			fmt.Sprintf("Removed what the cancelled schema change had set up: %s", strings.Join(result.Reclaimed, ", ")), "", "")
	}
	if len(result.Kept) > 0 {
		c.logger.Warn("the cancelled schema change's work was left in place",
			append(apply.LogAttrs(), "kept", result.Kept, "reason", result.KeptReason)...)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventInfo, storage.LogSourceSchemaBot,
			fmt.Sprintf("Left in place because %s: %s", result.KeptReason, strings.Join(result.Kept, ", ")), "", "")
	}
}
