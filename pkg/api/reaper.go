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

// strandedReaperHeartbeatPasses is how many passes one heartbeat summary covers.
// Summarizing a window rather than narrating every pass keeps a healthy reaper to
// a few lines an hour at the default interval, which is quiet enough to leave on
// and frequent enough that its absence is noticeable.
const strandedReaperHeartbeatPasses = 15

// strandedReaperPassOutcome is what one pass did, for the heartbeat's tally.
type strandedReaperPassOutcome int

const (
	// strandedReaperPassRan is an elected pass that completed, whether or not it
	// found anything to settle.
	strandedReaperPassRan strandedReaperPassOutcome = iota
	// strandedReaperPassNotElected is a pass another instance was already running.
	strandedReaperPassNotElected
	// strandedReaperPassFailed is a pass that ended in a storage error.
	strandedReaperPassFailed
)

// strandedReaperHeartbeat tallies what the reaper's passes did so that a reaper
// with nothing to settle still says so.
//
// An idle pass is silent by design — it settles nothing, and an instance that
// loses the election has nothing to report at all — which leaves a healthy steady
// state indistinguishable from a reaper that stopped ticking. An operator reading
// hours of INFO logs finds neither, and telling those two apart is the first
// question during an incident. The heartbeat answers it without putting a line in
// front of them on every pass.
type strandedReaperHeartbeat struct {
	passes     int
	settled    int
	notElected int
	failed     int
}

// observe folds one pass into the current window and returns the summary to log
// once the window is full, or nil while it is still filling.
func (h *strandedReaperHeartbeat) observe(outcome strandedReaperPassOutcome, settled int) []any {
	h.passes++
	h.settled += settled
	switch outcome {
	case strandedReaperPassNotElected:
		h.notElected++
	case strandedReaperPassFailed:
		h.failed++
	case strandedReaperPassRan:
	}

	if h.passes < strandedReaperHeartbeatPasses {
		return nil
	}
	summary := []any{
		"passes", h.passes,
		"operations_settled", h.settled,
		"passes_not_elected", h.notElected,
		"passes_failed", h.failed,
	}
	*h = strandedReaperHeartbeat{}
	return summary
}

// strandedReaperLoop runs a pass on every tick until the operator stops. It
// shares the driver lifecycle rather than having its own: the rows it settles are
// the residue of driving applies, so a process that does not run the operator has
// nothing to reap.
func (s *Service) strandedReaperLoop(ctx context.Context, stop <-chan struct{}, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// State the reaper's cadence and batch size once at startup. Every later
	// heartbeat is a count of passes, and an operator reads that count against
	// this line to know what stretch of time it covers.
	s.logger.Info("operator: stranded-operation reaper started",
		"interval", interval.String(),
		"batch", strandedReaperBatch,
		"heartbeat_passes", strandedReaperHeartbeatPasses)

	var heartbeat strandedReaperHeartbeat
	s.runPassAndBeat(ctx, &heartbeat)

	for {
		select {
		case <-stop:
			s.logger.Info("operator: stranded-operation reaper stopping")
			return
		case <-ctx.Done():
			s.logger.Info("operator: stranded-operation reaper stopping", "error", ctx.Err())
			return
		case <-ticker.C:
			s.runPassAndBeat(ctx, &heartbeat)
		}
	}
}

// runPassAndBeat runs one pass and emits the heartbeat when its window closes.
func (s *Service) runPassAndBeat(ctx context.Context, heartbeat *strandedReaperHeartbeat) {
	outcome, settled := s.runStrandedReaperPass(ctx)
	if summary := heartbeat.observe(outcome, settled); summary != nil {
		s.logger.Info("operator: stranded-operation reaper heartbeat", summary...)
	}
}

// runStrandedReaperPass runs one pass, reporting what it did so the heartbeat can
// tally it. It is best-effort maintenance: a storage error is logged and
// recorded, and the next pass retries. Losing the election is the expected
// outcome on every instance but one, so it is not an error.
func (s *Service) runStrandedReaperPass(ctx context.Context) (strandedReaperPassOutcome, int) {
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

	if errors.Is(err, storage.ErrStrandedReaperBusy) {
		s.logger.Debug("operator: another instance is reaping stranded apply operations; skipping this pass")
		return strandedReaperPassNotElected, len(reaped)
	}
	if ctx.Err() != nil {
		// A pass interrupted by shutdown is a routine deploy, not a fault, and
		// must not tick the claim-failure counter operators alert on.
		s.logger.Debug("operator: stranded-operation reaper pass interrupted by shutdown",
			"reaped_before_shutdown", len(reaped), "error", err)
		return strandedReaperPassRan, len(reaped)
	}
	if err != nil {
		s.logger.Error("operator: failed to reap stranded apply operations",
			"reaped_before_failure", len(reaped), "error", err)
		metrics.RecordOperatorClaimFailure(ctx, "stranded_reaper_error")
		return strandedReaperPassFailed, len(reaped)
	}
	return strandedReaperPassRan, len(reaped)
}
