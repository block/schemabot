package api

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine/spirit"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// The reaper is the operator's cleanup component: it settles rows that nothing
// will ever act on again, so dead rows cannot masquerade as live work. Each
// kind of dead row is its own independent sweep within a shared per-tick pass;
// new cleanup responsibilities belong here as additional sweeps.
//
// The stranded-operation sweep settles apply_operations rows that nothing will
// ever claim.
//
// An operation row is claimed and driven while its parent apply is active. When
// the parent reaches a final verdict, that verdict is recorded on the parent
// alone, so a row still pending underneath it becomes unreachable: no driver
// claims an operation whose parent has settled. The row stays pending forever,
// and pending comes to mean two things at once — queued work, and dead history.
//
//   applies                     apply_operations (apply_id = 7)
//   ┌──────────────────┐        ┌────────────────────────────────┐
//   │ id=7  completed  │        │ id=8  completed                │
//   └──────────────────┘        │ id=9  pending   ← stranded     │
//     the verdict, and          └────────────────────────────────┘
//     only the verdict            nothing will claim id=9 again
//
// A pass mirrors the parent's verdict onto those rows. Which rows qualify, how
// each write is guarded, and why the parent apply row is never touched are the
// storage layer's contract — see storage.ApplyOperationStore.ReapStranded. What
// lives here is the shape of a pass:
//
//   every StrandedReaperInterval
//        │
//        ▼
//   elect one reaper (advisory lock) ─── held elsewhere ──▶ skip the pass
//        │ elected
//        ▼
//   select ≤ strandedReaperBatch rows, oldest first: pending, unleased,
//   and under a parent that settled at least a quiescence window ago
//        │
//        ▼
//   guarded write per row ─── row moved meanwhile ──▶ skip the row
//        │ landed
//        ▼
//   log the settlement + count it by parent_state
//
// Two properties of that shape are worth keeping in mind when changing it.
//
// The cadence is deliberately slow and separate from the driver poll tick. Every
// row a pass can settle has already been idle for the quiescence window, so a
// faster pass has nothing new to discover, and unlike a claim — which reads one
// apply — a pass scans the whole pending set.
//
// The election is an efficiency gate, not a safety one. Every write is already
// guarded and idempotent, so concurrent reapers would be correct; they would just
// each pay the full scan to settle rows the first one already handled.
//
// The retryable-task sweep reaps dead retryable tasks: failed_retryable task
// rows under a settled parent apply. A failed_retryable task promises a retry that
// only the parent's recovery path can dispatch, so once the parent settles the
// promise is dead — and the row poisons every reader that treats
// failed_retryable as "a retry is coming", most critically the control plane's
// remote-progress snapshot, which copies the row verbatim and reads it as a
// permanent retryable pause. Which rows qualify and how each write is guarded is
// the storage layer's contract — see storage.TaskStore.ReapStrandedRetryable.
// The sweep shape is the same as above, with its own election lock and a longer
// parent-quiescence window sized past the retryable-recovery freshness window.

// The abandoned-stopped sweep reaps a different kind of residue from the two
// above: not a row under a settled parent, but a whole apply that stopped and
// that nobody came back for.
//
// A stop is a resumable pause, so nothing ages a stopped apply out of stopped.
// Left alone it holds its target forever — the check stays in_progress and every
// later plan for the same target meets its hold. The operator who would resume
// it is the only thing that ends it, and sometimes there isn't one.
//
// A stop past the checkpoint bound is the case where waiting has stopped buying
// anything: the checkpoint can no longer be resumed, so a start would re-copy
// from zero exactly as a fresh apply would. Past that point the sweep resolves
// the apply the way an operator would, by cancelling it.
//
//	stopped ── superseded_by set ──▶ left alone, a successor owns the artifacts
//	   │
//	   ├─ control request pending ──▶ left alone, an operator's command is in flight
//	   │
//	   └─ untouched past the checkpoint bound ──▶ cancel
//
// The sweep cancels through the same path `schemabot cancel` uses rather than
// writing cancelled onto the apply row. Apply state is a projection of its task
// rows: a terminal state written over live task rows is re-derived away by the
// next projection, and the cancel path is what settles the tasks, releases the
// engine's work, publishes the terminal summary, and drives the stored check to
// its blocking conclusion. Reaping is a caller of that path, not a second
// implementation of it.

// StrandedReaperInterval is how often the reaper runs a pass. Override with
// SetStrandedReaperInterval.
const StrandedReaperInterval = 1 * time.Minute

// strandedReaperBatch bounds how many operation rows one pass settles, keeping a
// single pass's work bounded on a large backlog. One instance reaps per pass, so
// this is the fleet-wide drain rate per interval, not a per-driver rate.
const strandedReaperBatch = 200

// abandonedStoppedApplyAge is how long an apply must sit stopped, untouched,
// before the sweep cancels it. It is the Spirit checkpoint bound, which is the
// first thing to invalidate a stopped copy and the one SchemaBot pins: past it a
// start re-copies from zero, so the reap costs the operator nothing a start
// would have saved them.
const abandonedStoppedApplyAge = spirit.DefaultCheckpointMaxAge

// abandonedStoppedReaperBatch bounds how many stopped applies one pass cancels.
// Each cancel is a control-plane request and, where a data plane is reachable,
// an immediate engine call, so the bound is much smaller than the operation
// sweep's: a backlog drains over several passes rather than issuing a burst of
// engine calls in one.
const abandonedStoppedReaperBatch = 10

// abandonedStoppedReaperCaller attributes the cancel in the durable control
// request and the apply log, so an operator reading either can tell an automatic
// reap from a person's cancel without cross-referencing anything.
const abandonedStoppedReaperCaller = "schemabot:abandoned-stopped-reaper"

// SetStrandedReaperInterval sets how often the reaper runs a pass. Call before
// StartOperator so the reaper creates its ticker with the intended interval.
func (s *Service) SetStrandedReaperInterval(interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("stranded reaper interval must be positive")
	}
	s.operatorMu.Lock()
	defer s.operatorMu.Unlock()
	if s.stopRecovery != nil {
		return fmt.Errorf("operator already running")
	}
	s.strandedReaperEvery = interval
	return nil
}

// strandedReaperLoop runs a pass on every tick until the operator stops. It
// shares the driver lifecycle rather than having its own: the rows it settles are
// the residue of driving applies, so a process that does not run the operator has
// nothing to reap.
func (s *Service) strandedReaperLoop(ctx context.Context, stop <-chan struct{}, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	s.runStrandedReaperPass(ctx)

	for {
		select {
		case <-stop:
			s.logger.Debug("operator: reaper stopping")
			return
		case <-ctx.Done():
			s.logger.Debug("operator: reaper stopping", "error", ctx.Err())
			return
		case <-ticker.C:
			s.runStrandedReaperPass(ctx)
		}
	}
}

// runStrandedReaperPass runs one pass over every kind of dead row. It is
// best-effort maintenance: a storage error is logged and recorded, and the next
// pass retries. Losing the election is the expected outcome on every instance
// but one, so it is not an error.
//
// The sweeps run concurrently. They share nothing — separate election locks,
// separate connections, no row in common — and none may wait behind another: the
// retryable-task sweep is what frees a remote drive waiting out a dead pause, so
// a slow scan of the pending-operation set must not defer it to the next tick,
// and the abandoned-stopped sweep makes engine calls whose latency belongs to
// neither of the others. A failure in one likewise cannot starve the rest.
func (s *Service) runStrandedReaperPass(ctx context.Context) {
	var sweeps sync.WaitGroup
	sweeps.Go(func() { s.reapStrandedOperations(ctx) })
	sweeps.Go(func() { s.reapStrandedRetryableTasks(ctx) })
	sweeps.Go(func() { s.reapAbandonedStoppedApplies(ctx) })
	sweeps.Wait()
}

// reapSweep identifies one of the reaper's sweeps to the outcome handling they
// share: the sentinel its election returns when another instance holds the
// lock, the phrase naming what it settles, and the claim-failure reason a
// storage error ticks.
//
// busy is nil for a sweep that holds no election. A sweep whose every write is
// already idempotent needs none — concurrent instances converge on the same
// result — and the shared outcome handling must not read its clean pass as a
// lost election.
type reapSweep struct {
	busy          error
	subject       string
	failureReason string
}

var (
	strandedOperationSweep = reapSweep{
		busy:          storage.ErrStrandedReaperBusy,
		subject:       "stranded apply operations",
		failureReason: "stranded_reaper_error",
	}
	strandedRetryableTaskSweep = reapSweep{
		busy:          storage.ErrStrandedTaskReaperBusy,
		subject:       "stranded retryable tasks",
		failureReason: "stranded_task_reaper_error",
	}
	abandonedStoppedApplySweep = reapSweep{
		subject:       "abandoned stopped applies",
		failureReason: "abandoned_stopped_reaper_error",
	}
)

// recordSweepOutcome handles how a sweep ended, after its settlements have
// already been reported. The three endings stay apart because only the last is
// a fault: an unelected pass is the expected outcome on every instance but one,
// and a pass cut short by shutdown is a routine deploy, so neither may tick the
// claim-failure counter operators alert on.
func (s *Service) recordSweepOutcome(ctx context.Context, sweep reapSweep, reaped int, err error) {
	if sweep.busy != nil && errors.Is(err, sweep.busy) {
		s.logger.Debug("operator: another instance is reaping " + sweep.subject + "; skipping this pass")
		return
	}
	if ctx.Err() != nil {
		s.logger.Debug("operator: reaper sweep interrupted by shutdown",
			"sweep", sweep.subject, "reaped_before_shutdown", reaped, "error", err)
		return
	}
	if err != nil {
		s.logger.Error("operator: failed to reap "+sweep.subject,
			"reaped_before_failure", reaped, "error", err)
		metrics.RecordOperatorClaimFailure(ctx, sweep.failureReason)
	}
}

// reapStrandedOperations settles pending operation rows under settled parent
// applies, mirroring the parent's outcome onto them.
func (s *Service) reapStrandedOperations(ctx context.Context) {
	reaped, err := s.storage.ApplyOperations().ReapStranded(ctx, strandedReaperBatch)

	// Report what landed before handling the error. A failed pass still returns
	// the rows it settled before failing, and those writes are committed — an
	// operator asking who changed a settled apply's rows must find them.
	for _, settled := range reaped {
		parent, op := settled.Parent, settled.Operation
		s.logger.Info("operator: reaped a stranded apply operation to its parent apply's recorded outcome",
			append(parent.LogAttrs(),
				"apply_operation_id", op.ID,
				"operation_deployment", op.Deployment)...)
		metrics.RecordOperatorStrandedOperationReaped(ctx, parent.Database, op.Deployment, parent.Environment, parent.State)
	}

	s.recordSweepOutcome(ctx, strandedOperationSweep, len(reaped), err)
}

// reapStrandedRetryableTasks hardens failed_retryable task rows under settled
// parent applies to failed, retiring retry promises nothing will ever dispatch.
// Settlements are logged, not counted: the rows are the rare residue of a
// partial failure write, so a rate would read zero for weeks — a log-based
// monitor can count the line below if that day arrives.
func (s *Service) reapStrandedRetryableTasks(ctx context.Context) {
	reaped, err := s.storage.Tasks().ReapStrandedRetryable(ctx, strandedReaperBatch)

	// Report what landed before handling the error. A failed pass still returns
	// the rows it settled before failing, and those writes are committed — an
	// operator asking who changed a settled apply's task rows must find them.
	for _, settled := range reaped {
		parent, task := settled.Parent, settled.Task
		// task_state, not the parent's state: the line's own subject is the task
		// it hardened, while parent.LogAttrs() carries the settled verdict the
		// retry promise died under — which is as often completed or cancelled as
		// failed. A monitor keyed on the outcome needs the task's.
		s.logger.Info("operator: reaped a stranded retryable task to failed; its settled parent apply will never dispatch the retry",
			append(parent.LogAttrs(),
				"task_id", task.TaskIdentifier,
				"table", task.TableName,
				"task_state", task.State,
				"task_error", task.ErrorMessage)...)
	}

	s.recordSweepOutcome(ctx, strandedRetryableTaskSweep, len(reaped), err)
}

// reapAbandonedStoppedApplies cancels stopped applies that no successor and no
// operator command is coming for, once they are older than the checkpoint bound
// that made resuming them worth waiting for.
//
// The scan is read-only and the cancel goes through the ordinary control path,
// which records a durable request and re-reads the apply under its own guards.
// That makes a cancel the sweep issues indistinguishable, downstream, from one an
// operator typed, and it makes the sweep safe to run without an election: the
// scan skips an apply that already has a request pending, and two instances that
// scanned before either recorded one converge on the single request row rather
// than cancelling twice.
//
// Cancels are issued one at a time and each failure is confined to its own
// apply: a target whose data plane is unreachable must not stop the sweep from
// resolving abandoned applies on every other target.
func (s *Service) reapAbandonedStoppedApplies(ctx context.Context) {
	abandoned, err := s.storage.Applies().FindAbandonedStoppedApplies(ctx, abandonedStoppedApplyAge, abandonedStoppedReaperBatch)

	cancelled := 0
	for _, apply := range abandoned {
		if ctx.Err() != nil {
			break
		}
		if cancelErr := s.cancelAbandonedStoppedApply(ctx, apply); cancelErr != nil {
			// One apply the sweep could not resolve says nothing about the next:
			// the target may be unreachable, or an operator may have acted
			// between the scan and the cancel. The row stays stopped and the
			// next pass retries it.
			s.logger.Warn("operator: could not cancel an abandoned stopped apply; it stays stopped for the next reaper pass",
				append(apply.LogAttrs(), "stopped_for", time.Since(apply.UpdatedAt).String(), "error", cancelErr)...)
			continue
		}
		cancelled++
		s.logger.Info("operator: cancelled a stopped apply nobody resumed; its copy aged past the bound that made resuming it worth waiting for, so any fresh apply copies from zero either way",
			append(apply.LogAttrs(), "stopped_for", time.Since(apply.UpdatedAt).String())...)
	}

	s.recordSweepOutcome(ctx, abandonedStoppedApplySweep, cancelled, err)
}

// cancelAbandonedStoppedApply issues the reap as an ordinary cancel, attributed
// to the reaper.
func (s *Service) cancelAbandonedStoppedApply(ctx context.Context, apply *storage.Apply) error {
	resp, err := s.ExecuteCancel(ctx, apitypes.ControlRequest{
		ApplyID:     apply.ApplyIdentifier,
		Environment: apply.Environment,
		Caller:      abandonedStoppedReaperCaller,
	})
	if err != nil {
		return fmt.Errorf("cancel abandoned stopped apply %s: %w", apply.ApplyIdentifier, err)
	}
	if resp == nil {
		return fmt.Errorf("cancel abandoned stopped apply %s: no response", apply.ApplyIdentifier)
	}
	if !resp.Accepted {
		return fmt.Errorf("cancel abandoned stopped apply %s was not accepted: %s", apply.ApplyIdentifier, resp.ErrorMessage)
	}
	return nil
}
