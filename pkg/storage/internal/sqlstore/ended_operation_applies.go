package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// An apply that defers its cutover holds every operation at the cutover until
// an operator runs it. When one operation ends before it gets there, the
// cutover can never run for all of them, and the others would wait at the
// cutover until the wait timed out, holding the target the whole time. These
// two sweeps end such an apply instead. The first finds it while operations
// are still running or waiting, so the reaper can record a stop on it as
// SchemaBot (storage.EndedOperationStopCaller); the drives already holding
// those operations carry the stop out and save each one's work. The second
// finds it once nothing is driven any more and settles it failed.

// endedOperationStopLockName elects the one instance that looks for applies to
// stop per pass. Instance-wide for the same reason as the reaper's other
// sweeps: the pass scans every target's applies.
const endedOperationStopLockName = "schemabot_ended_operation_stop"

var endedOperationStopSweep = strandedSweep{
	lockName: endedOperationStopLockName,
	busy:     storage.ErrEndedOperationStopBusy,
	subject:  "applies held by an ended operation",
}

// endedOperationSettleLockName elects the one instance that settles stopped
// applies per pass. A lock of its own, so the two sweeps never serialize on
// each other.
const endedOperationSettleLockName = "schemabot_ended_operation_settle"

var endedOperationSettleSweep = strandedSweep{
	lockName: endedOperationSettleLockName,
	busy:     storage.ErrEndedOperationSettleBusy,
	subject:  "applies stopped for an ended operation",
}

// heldByEndedOperationApplyStates are the parent states an apply holding work
// behind an ended operation projects to. A halted failure beside a sibling
// that started projects running_degraded, and a spent retry projects
// failed_retryable. Selecting on them lets the scan ride the (state,
// updated_at) index.
var heldByEndedOperationApplyStates = []string{
	state.Apply.RunningDegraded,
	state.Apply.FailedRetryable,
}

// quietOperationStates are the operation states no stop has to reach: settled,
// already stopped, or waiting on a retry. An apply whose operations are all in
// these states has nothing left running.
var quietOperationStates = append(append([]string{}, state.SettledApplyStates...),
	state.ApplyOperation.Stopped, state.ApplyOperation.FailedRetryable)

// settleableOperationStates adds pending to the quiet states for the settle
// sweep. A stop moves an operation nothing has claimed to stopped when a drive
// delivers it, but one no driver reaches would otherwise keep the apply
// running forever. The settle holds every operation row locked with no lease
// on any of them, so no driver can claim a pending one under it.
var settleableOperationStates = append(append([]string{}, quietOperationStates...), state.ApplyOperation.Pending)

// endedOperationSQL matches an operation, aliased op, of the apply in the
// enclosing query that ended before its cutover. An operation has ended when
// it failed under the halt policy, or when it is waiting on a retry that will
// never come because the apply spent its retry budget or freshness window. The
// second arm is the expiry selection of expireRetryable, read for one
// operation. A failure under continue or pause has not ended the apply: the
// policy says the other operations go on without it.
func (s *applyStore) endedOperationSQL(op string) (string, []any) {
	stale := s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, ParameterIntervalAmount(), IntervalDay)
	clause := fmt.Sprintf(`((%[1]s.state = ? AND %[1]s.on_failure = ?)
			OR (%[1]s.state = ? AND applies.state = ? AND (applies.attempt >= ? OR applies.updated_at < %[2]s)))`, op, stale)
	args := []any{
		state.ApplyOperation.Failed, storage.OnFailureHalt,
		state.ApplyOperation.FailedRetryable, state.Apply.FailedRetryable, maxRecoveryAttempts, retryableRecoveryFreshnessDays,
	}
	return clause, args
}

// ListAppliesHeldByEndedOperation elects one instance per pass and lists under
// the lock. See storage.ApplyStore for the contract.
func (s *applyStore) ListAppliesHeldByEndedOperation(ctx context.Context, databaseTypes []string, limit int) ([]*storage.EndedOperationApply, error) {
	return reapUnderElection(ctx, s.db, s.locker, endedOperationStopSweep,
		func(ctx context.Context) ([]*storage.EndedOperationApply, error) {
			return s.listAppliesHeldByEndedOperation(ctx, databaseTypes, limit)
		})
}

func (s *applyStore) listAppliesHeldByEndedOperation(ctx context.Context, databaseTypes []string, limit int) ([]*storage.EndedOperationApply, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("list applies held by an ended operation: limit must be positive, got %d", limit)
	}
	if len(databaseTypes) == 0 {
		return nil, fmt.Errorf("list applies held by an ended operation: no database types to list")
	}
	ended, endedArgs := s.endedOperationSQL("ended")
	args := stringArgs(heldByEndedOperationApplyStates)
	args = append(args, stringArgs(databaseTypes)...)
	args = append(args, endedArgs...)
	args = append(args, stringArgs(quietOperationStates)...)
	args = append(args, storage.ControlOperationStop, storage.ControlOperationCancel, storage.ControlRequestPending, limit)

	// A pending stop or cancel is already ending the apply, whoever asked for
	// it, so the apply is left to that request.
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT `+applyColumns+`
		FROM applies
		WHERE state IN (%s)
			AND database_type IN (%s)
			AND %s
			AND EXISTS (
				SELECT 1 FROM apply_operations ended
				WHERE ended.apply_id = applies.id AND %s
			)
			AND EXISTS (
				SELECT 1 FROM apply_operations held
				WHERE held.apply_id = applies.id AND held.state NOT IN (%s)
			)
			AND NOT EXISTS (
				SELECT 1 FROM apply_control_requests req
				WHERE req.apply_id = applies.id AND req.operation IN (?, ?) AND req.status = ?
			)
		ORDER BY updated_at
		LIMIT ?
	`, placeholders(len(heldByEndedOperationApplyStates)),
		placeholders(len(databaseTypes)),
		s.dialect.JSONBooleanIsTrue("applies.options", []string{"defer_cutover"}),
		ended,
		placeholders(len(quietOperationStates))), args...)
	if err != nil {
		return nil, fmt.Errorf("query applies held by an ended operation: %w", err)
	}
	applies, err := scanApplies(rows)
	utils.CloseAndLog(rows)
	if err != nil {
		return nil, fmt.Errorf("scan applies held by an ended operation: %w", err)
	}
	return s.withEndedOperations(ctx, s.db, applies)
}

// SettleAppliesStoppedForEndedOperation elects one instance per pass and
// settles under the lock. See storage.ApplyStore for the contract.
func (s *applyStore) SettleAppliesStoppedForEndedOperation(ctx context.Context, limit int) ([]*storage.EndedOperationApply, error) {
	return reapUnderElection(ctx, s.db, s.locker, endedOperationSettleSweep,
		func(ctx context.Context) ([]*storage.EndedOperationApply, error) {
			return s.settleAppliesStoppedForEndedOperation(ctx, limit)
		})
}

func (s *applyStore) settleAppliesStoppedForEndedOperation(ctx context.Context, limit int) ([]*storage.EndedOperationApply, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("settle applies stopped for an ended operation: limit must be positive, got %d", limit)
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin settle applies stopped for an ended operation transaction: %w", err)
	}
	defer rollbackTx(ctx, tx, "settle applies stopped for an ended operation")

	// Only the stop SchemaBot recorded for the ended operation marks an apply
	// as this sweep's. An operator's stop leaves the apply to the operator, who
	// may start it again. The apply is taken only once every operation is quiet
	// and no driver holds one, for the reasons expireRetryable gives: the rows
	// of a live drive are its own.
	ended, endedArgs := s.endedOperationSQL("ended")
	args := stringArgs(heldByEndedOperationApplyStates)
	args = append(args, storage.ControlOperationStop, storage.ControlRequestPending, storage.EndedOperationStopCaller)
	args = append(args, endedArgs...)
	args = append(args, stringArgs(settleableOperationStates)...)
	args = append(args, limit)
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
		SELECT `+applyColumns+`
		FROM applies
		WHERE state IN (%s)
			AND %s
			AND EXISTS (
				SELECT 1 FROM apply_control_requests req
				WHERE req.apply_id = applies.id AND req.operation = ? AND req.status = ? AND req.requested_by = ?
			)
			AND EXISTS (
				SELECT 1 FROM apply_operations ended
				WHERE ended.apply_id = applies.id AND %s
			)
			AND NOT EXISTS (
				SELECT 1 FROM apply_operations busy
				WHERE busy.apply_id = applies.id AND busy.state NOT IN (%s)
			)
			AND %s
		ORDER BY updated_at
		LIMIT ?
		FOR UPDATE
	`, placeholders(len(heldByEndedOperationApplyStates)),
		s.dialect.JSONBooleanIsTrue("applies.options", []string{"defer_cutover"}),
		ended,
		placeholders(len(settleableOperationStates)),
		undrivenApplyGate(s.dialect)), args...)
	if err != nil {
		return nil, fmt.Errorf("query applies stopped for an ended operation: %w", err)
	}
	applies, err := scanApplies(rows)
	utils.CloseAndLog(rows)
	if err != nil {
		return nil, fmt.Errorf("scan applies stopped for an ended operation: %w", err)
	}
	if len(applies) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit empty settle of applies stopped for an ended operation: %w", err)
		}
		return nil, nil
	}

	// The selection reads operation leases without locking the rows, so lock
	// them and read the leases again, as expireRetryable does. An apply a
	// driver claimed after the scan is left for a later pass.
	candidateIDs := make([]int64, 0, len(applies))
	for _, apply := range applies {
		candidateIDs = append(candidateIDs, apply.ID)
	}
	undriven, err := s.lockUndrivenApplies(ctx, tx, candidateIDs)
	if err != nil {
		return nil, err
	}
	eligible := make([]*storage.Apply, 0, len(applies))
	for _, apply := range applies {
		if undriven[apply.ID] {
			eligible = append(eligible, apply)
		}
	}
	if len(eligible) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit deferred settle of applies stopped for an ended operation: %w", err)
		}
		return nil, nil
	}

	settled, err := s.withEndedOperations(ctx, tx, eligible)
	if err != nil {
		return nil, err
	}
	for _, apply := range settled {
		if err := settleStoppedForEndedOperation(ctx, tx, apply); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit settle of applies stopped for an ended operation: %w", err)
	}
	now := time.Now()
	for _, apply := range settled {
		apply.SettledFrom = apply.Apply.State
		apply.Apply.State = state.Apply.Failed
		apply.Apply.CompletedAt = &now
		apply.Apply.UpdatedAt = now
	}
	return settled, nil
}

// settleStoppedForEndedOperation writes one apply's settlement inside tx, which
// holds the apply and every one of its operations locked.
//
// The stopped operations settle failed and name the operation they failed for.
// Their tasks keep their stopped state: the copy each one made is saved, and a
// new apply of the same change resumes it. An operation still waiting on a
// retry settles failed with its tasks, as expiry settles it. An operation or
// task that never started is cancelled rather than failed, because it did no
// work to fail.
//
// The stop SchemaBot recorded is left pending here, and so is any cutover an
// operator asked for. The reaper answers both once the apply is failed, through
// the same settlement every other path uses.
func settleStoppedForEndedOperation(ctx context.Context, tx *rebindTx, settled *storage.EndedOperationApply) error {
	apply := settled.Apply
	if _, err := tx.ExecContext(ctx, `
		UPDATE tasks
		SET state = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE apply_id = ? AND state = ?
	`, state.Task.Cancelled, apply.ID, state.Task.Pending); err != nil {
		return fmt.Errorf("cancel pending tasks of apply %s stopped for ended operation %d: %w", apply.ApplyIdentifier, settled.EndedOperation.ID, err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE tasks
		SET state = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE apply_id = ? AND state = ?
	`, state.Task.Failed, apply.ID, state.Task.FailedRetryable); err != nil {
		return fmt.Errorf("fail retryable tasks of apply %s stopped for ended operation %d: %w", apply.ApplyIdentifier, settled.EndedOperation.ID, err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE apply_operations
		SET state = ?, ended_by_operation_id = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE apply_id = ? AND state = ?
	`, state.ApplyOperation.Failed, settled.EndedOperation.ID, apply.ID, state.ApplyOperation.Stopped); err != nil {
		return fmt.Errorf("fail operations of apply %s stopped for ended operation %d: %w", apply.ApplyIdentifier, settled.EndedOperation.ID, err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE apply_operations
		SET state = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE apply_id = ? AND state = ?
	`, state.ApplyOperation.Failed, apply.ID, state.ApplyOperation.FailedRetryable); err != nil {
		return fmt.Errorf("fail retryable operations of apply %s stopped for ended operation %d: %w", apply.ApplyIdentifier, settled.EndedOperation.ID, err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE apply_operations
		SET state = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE apply_id = ? AND state = ?
	`, state.ApplyOperation.Cancelled, apply.ID, state.ApplyOperation.Pending); err != nil {
		return fmt.Errorf("cancel pending operations of apply %s stopped for ended operation %d: %w", apply.ApplyIdentifier, settled.EndedOperation.ID, err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE applies
		SET state = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE id = ?
	`, state.Apply.Failed, apply.ID); err != nil {
		return fmt.Errorf("fail apply %s stopped for ended operation %d: %w", apply.ApplyIdentifier, settled.EndedOperation.ID, err)
	}
	return nil
}

// queryer is what withEndedOperations reads through: the pool for the list
// sweep, the settle transaction for the settle sweep.
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// withEndedOperations pairs each apply with the operation that ended it: the
// first, by id, that failed under the halt policy, else the first waiting on a
// spent retry. The selection already established that one exists, so an apply
// with none means its rows changed under a read that held no lock, and the
// apply is left for a later pass.
func (s *applyStore) withEndedOperations(ctx context.Context, q queryer, applies []*storage.Apply) ([]*storage.EndedOperationApply, error) {
	if len(applies) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(applies))
	for _, apply := range applies {
		args = append(args, apply.ID)
	}
	rows, err := q.QueryContext(ctx, fmt.Sprintf(`
		SELECT `+applyOperationColumns+`
		FROM apply_operations
		WHERE apply_id IN (%s)
		ORDER BY apply_id, id
	`, placeholders(len(applies))), args...)
	if err != nil {
		return nil, fmt.Errorf("query operations of %d applies held by an ended operation: %w", len(applies), err)
	}
	ops, err := scanApplyOperations(rows)
	utils.CloseAndLog(rows)
	if err != nil {
		return nil, fmt.Errorf("scan operations of %d applies held by an ended operation: %w", len(applies), err)
	}
	byApply := make(map[int64][]*storage.ApplyOperation, len(applies))
	for _, op := range ops {
		byApply[op.ApplyID] = append(byApply[op.ApplyID], op)
	}
	out := make([]*storage.EndedOperationApply, 0, len(applies))
	for _, apply := range applies {
		ended := firstEndedOperation(byApply[apply.ID])
		if ended == nil {
			continue
		}
		out = append(out, &storage.EndedOperationApply{Apply: apply, EndedOperation: ended})
	}
	return out, nil
}

// firstEndedOperation picks the operation that ended an apply from its
// operations in id order. A halted failure is the cause when there is one; a
// spent retry is the cause only when nothing failed outright, since the
// selection admits it only under a failed_retryable parent.
func firstEndedOperation(ops []*storage.ApplyOperation) *storage.ApplyOperation {
	for _, op := range ops {
		if state.IsState(op.State, state.ApplyOperation.Failed) && op.OnFailure == storage.OnFailureHalt {
			return op
		}
	}
	for _, op := range ops {
		if state.IsState(op.State, state.ApplyOperation.FailedRetryable) {
			return op
		}
	}
	return nil
}
