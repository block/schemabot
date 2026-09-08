package api

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// The reaper is the operator's cleanup component: it settles rows no driver
// will ever take, so they cannot masquerade as live work. Each kind of row is
// its own independent sweep, and sweeps that want the same cadence share a
// per-tick pass; new cleanup responsibilities belong here.
//
// Every sweep is a maintenance writer rather than a driver: it writes rows it
// holds no lease on, so what keeps it off a driver's rows is reading that
// driver's lease and declining. That is the property to preserve when adding
// one — see the note on lease exclusion below.
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
// What keeps a reaper off a driver's rows is the lease, not the election and not
// the quiescence windows. Every sweep takes only rows whose lease is absent or
// stale by the claim path's own reckoning, which is the same test a driver
// applies before taking work from a peer — so the two writer classes exclude each
// other by one mechanism rather than by two that have to be kept in agreement.
// The windows sit on top of that, deciding when a row is worth looking at rather
// than whether it is safe to write.
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
//
// The retryable-expiry pass settles the other direction: the parent apply
// itself, when a failed_retryable apply has spent its retry budget or gone
// stale, together with the operation and task rows underneath it. It is the one
// sweep whose subject is an apply rather than the residue of one, and it runs on
// its own short cadence rather than in the shared pass — see
// RetryableExpiryInterval. Because it writes a whole apply's tree at once it
// cannot decide row by row: it excludes a live driver by taking only applies
// with no operation a driver is part-way through driving, and then takes the
// tree whole. It locks every operation and rechecks leases before writing;
// a fresh retryable lease is protected too. See storage.ApplyStore.ExpireRetryable.

// StrandedReaperInterval is how often the reaper runs a pass. Override with
// SetStrandedReaperInterval.
const StrandedReaperInterval = 1 * time.Minute

// strandedReaperBatch bounds how many operation rows one pass settles, keeping a
// single pass's work bounded on a large backlog. One instance reaps per pass, so
// this is the fleet-wide drain rate per interval, not a per-driver rate.
const strandedReaperBatch = 200

// RetryableExpiryInterval is how often the expiry pass runs. Override with
// SetRetryableExpiryInterval.
//
// It is far shorter than StrandedReaperInterval because the two passes wait on
// different things. Every row the stranded sweeps can settle has already sat out
// a quiescence window, so a faster pass finds nothing new. Expiry's budget arm
// has no such window: an apply qualifies the moment its last redispatch consumes
// the budget, and until it settles it holds the one-active-apply guard against
// every new apply for that database. A pass that costs one elected scan is worth
// running at the cadence a driver notices.
const RetryableExpiryInterval = OperatorPollInterval

// retryableExpiryBatch bounds how many applies one expiry pass settles. Sized
// like strandedReaperBatch and for the same reason: one instance expires per
// pass, so this is the fleet-wide drain rate per interval.
const retryableExpiryBatch = 200

// SetStrandedReaperInterval sets how often the stranded sweeps run. Call before
// StartOperator so the reaper creates its ticker with the intended interval.
func (s *Service) SetStrandedReaperInterval(interval time.Duration) error {
	return s.setReaperInterval(interval, "stranded reaper", &s.strandedReaperEvery)
}

// SetRetryableExpiryInterval sets how often the expiry pass runs. Call before
// StartOperator so the reaper creates its ticker with the intended interval.
func (s *Service) SetRetryableExpiryInterval(interval time.Duration) error {
	return s.setReaperInterval(interval, "retryable expiry", &s.retryableExpiryEvery)
}

func (s *Service) setReaperInterval(interval time.Duration, name string, into *time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("%s interval must be positive", name)
	}
	s.operatorMu.Lock()
	defer s.operatorMu.Unlock()
	if s.stopRecovery != nil {
		return fmt.Errorf("operator already running")
	}
	*into = interval
	return nil
}

// reaperLoop runs one named pass on every tick until the operator stops. It
// shares the driver lifecycle rather than having its own: the rows these passes
// settle are the residue of driving applies, so a process that does not run the
// operator has nothing to reap.
//
// Each cadence gets its own loop rather than a shared ticker that skips passes,
// so the slow sweeps are not woken on every fast tick only to decide they have
// nothing to do.
func (s *Service) reaperLoop(ctx context.Context, stop <-chan struct{}, interval time.Duration, name string, pass func(context.Context)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	pass(ctx)

	for {
		select {
		case <-stop:
			s.logger.Debug("operator: reaper stopping", "pass", name)
			return
		case <-ctx.Done():
			s.logger.Debug("operator: reaper stopping", "pass", name, "error", ctx.Err())
			return
		case <-ticker.C:
			pass(ctx)
		}
	}
}

// runStrandedReaperPass runs one pass over the three kinds of dead row. It is
// best-effort maintenance: a storage error is logged and recorded, and the next
// pass retries. Losing the election is the expected outcome on every instance
// but one, so it is not an error.
//
// The sweeps run concurrently. They share nothing — separate election locks,
// separate connections, no row in common — and none may wait behind another: the
// retryable-task sweep is what frees a remote drive waiting out a dead pause, so
// a slow scan of the pending-operation set must not defer it to the next tick. A
// failure in one likewise cannot starve the others.
func (s *Service) runStrandedReaperPass(ctx context.Context) {
	var sweeps sync.WaitGroup
	sweeps.Go(func() { s.reapStrandedOperations(ctx) })
	sweeps.Go(func() { s.reapStrandedRetryableTasks(ctx) })
	sweeps.Go(func() { s.reapStrandedActiveTasks(ctx) })
	sweeps.Wait()
}

// reapSweep identifies one of the reaper's sweeps to the outcome handling they
// share: the sentinel its election returns when another instance holds the
// lock, the phrase naming what it settles, and the claim-failure reason a
// storage error ticks.
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
	strandedActiveTaskSweep = reapSweep{
		busy:          storage.ErrStrandedActiveTaskReaperBusy,
		subject:       "stranded active tasks",
		failureReason: "stranded_active_task_reaper_error",
	}
	retryableExpirySweep = reapSweep{
		busy:          storage.ErrRetryableExpiryBusy,
		subject:       "expired retryable applies",
		failureReason: "expire_retryable_error",
	}
)

// recordSweepOutcome handles how a sweep ended, after its settlements have
// already been reported. The three endings stay apart because only the last is
// a fault: an unelected pass is the expected outcome on every instance but one,
// and a pass cut short by shutdown is a routine deploy, so neither may tick the
// claim-failure counter operators alert on.
func (s *Service) recordSweepOutcome(ctx context.Context, sweep reapSweep, reaped int, err error) {
	if errors.Is(err, sweep.busy) {
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

// runRetryableExpiryPass settles failed_retryable applies that have exhausted
// their retry budget or freshness window, along with the task and operation rows
// underneath them. It is best-effort maintenance on the same terms as the
// stranded sweeps: a storage error is logged and recorded, and the next pass
// retries.
//
// It is a pass of its own rather than a fourth sweep in runStrandedReaperPass
// because it wants a much shorter interval than the stranded sweeps do; see
// RetryableExpiryInterval.
func (s *Service) runRetryableExpiryPass(ctx context.Context) {
	expired, err := s.storage.Applies().ExpireRetryable(ctx, retryableExpiryBatch)

	// Report what landed before handling the error, as the stranded sweeps do:
	// an expiry that failed part-way still committed the applies it settled, and
	// those are the ones an operator asking why an apply stopped retrying needs
	// to find.
	for _, expiration := range expired {
		apply := expiration.Apply
		s.logger.Error("operator: retryable apply expired",
			append(apply.LogAttrs(),
				"attempt", apply.Attempt,
				"reason", expiration.Reason)...)
		metrics.RecordOperatorResumeFailure(ctx, apply.Database, apply.Deployment, apply.Environment, string(expiration.Reason))
		s.logApplyExpiration(ctx, apply, expiration.Reason)
	}

	s.recordSweepOutcome(ctx, retryableExpirySweep, len(expired), err)
}

// logApplyExpiration appends a durable apply log entry recording that operator
// recovery gave up on the apply. Expiry is what makes a retryable failure
// permanent, so without it the apply log ends on the last paused attempt and an
// operator reading the CLI or the PR summary sees the apply reach a terminal
// state with nothing stating why. Best-effort: a failed append must not stop the
// pass from expiring the remaining applies.
func (s *Service) logApplyExpiration(ctx context.Context, apply *storage.Apply, reason storage.RetryableExpirationReason) {
	s.appendApplyLog(ctx, s.logger, &storage.ApplyLog{
		ApplyID:   apply.ID,
		Level:     storage.LogLevelError,
		EventType: storage.LogEventError,
		Source:    storage.LogSourceSchemaBot,
		Message: fmt.Sprintf("Operator recovery gave up on the apply after %d of %d attempts (%s); it will not be retried automatically",
			apply.Attempt, storage.MaxRecoveryAttempts, reason),
		OldState:  state.Apply.FailedRetryable,
		NewState:  state.Apply.Failed,
		CreatedAt: s.clock.Now(),
	}, "why recovery stopped", apply.LogAttrs()...)
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

// reapStrandedActiveTasks settles task rows left in an active state under a
// settled, quiescent parent apply, mirroring the parent's outcome onto them.
// These are the rows that make a completed apply render a table still copying:
// their driver recorded the verdict on the parent and exited without closing
// them. Only a lease-class writer can correct them — a reader cannot, because
// correcting them is a write, and because a reader cannot tell a stranded row
// from a sibling still copying under an apply that a failed task already
// settled. The operation lease is what tells those apart.
//
// Settlements are logged, not counted, for the same reason as the retryable
// sweep's: they are the rare residue of a driver that stopped mid-write, so a
// rate would read zero for weeks.
func (s *Service) reapStrandedActiveTasks(ctx context.Context) {
	reaped, err := s.storage.Tasks().ReapStrandedActive(ctx, strandedReaperBatch)

	// Report what landed before handling the error. A failed pass still returns
	// the rows it settled before failing, and those writes are committed — an
	// operator asking who changed a settled apply's task rows must find them.
	for _, settled := range reaped {
		parent, task := settled.Parent, settled.Task
		s.logger.Info("operator: reaped a stranded active task to its parent apply's recorded outcome",
			append(parent.LogAttrs(),
				"task_id", task.TaskIdentifier,
				"table", task.TableName,
				"task_state", task.State)...)
	}

	s.recordSweepOutcome(ctx, strandedActiveTaskSweep, len(reaped), err)
}
