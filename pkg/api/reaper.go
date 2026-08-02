package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// The stranded-operation reaper settles apply_operations rows that nothing will
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

// StrandedReaperInterval is how often the reaper runs a pass. Override with
// SetStrandedReaperInterval.
const StrandedReaperInterval = 1 * time.Minute

// strandedReaperBatch bounds how many operation rows one pass settles, keeping a
// single pass's work bounded on a large backlog. One instance reaps per pass, so
// this is the fleet-wide drain rate per interval, not a per-driver rate.
const strandedReaperBatch = 200

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
			s.logger.Debug("operator: stranded-operation reaper stopping")
			return
		case <-ctx.Done():
			s.logger.Debug("operator: stranded-operation reaper stopping", "error", ctx.Err())
			return
		case <-ticker.C:
			s.runStrandedReaperPass(ctx)
		}
	}
}

// runStrandedReaperPass runs one pass. It is best-effort maintenance: a storage
// error is logged and recorded, and the next pass retries. Losing the election is
// the expected outcome on every instance but one, so it is not an error.
func (s *Service) runStrandedReaperPass(ctx context.Context) {
	reaped, err := s.storage.ApplyOperations().ReapStranded(ctx, strandedReaperBatch)
	if errors.Is(err, storage.ErrStrandedReaperBusy) {
		s.logger.Debug("operator: another instance is reaping stranded apply operations; skipping this pass")
		return
	}
	if err != nil {
		s.logger.Error("operator: failed to reap stranded apply operations", "error", err)
		metrics.RecordOperatorClaimFailure(ctx, "stranded_reaper_error")
		return
	}
	for _, settled := range reaped {
		parent, op := settled.Parent, settled.Operation
		s.logger.Info("operator: reaped a stranded apply operation to its parent apply's recorded outcome",
			append(parent.LogAttrs(),
				"apply_operation_id", op.ID,
				"operation_deployment", op.Deployment)...)
		metrics.RecordOperatorStrandedOperationReaped(ctx, parent.Database, parent.Deployment, parent.Environment, parent.State)
	}
}
