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
	held, args := s.heldByEndedOperationSQL(databaseTypes)
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE `+held+`
		ORDER BY updated_at
		LIMIT ?
	`, args...)
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

// heldByEndedOperationSQL matches an apply, in the enclosing query's
// applies, that ListAppliesHeldByEndedOperation takes for the given database
// types. expireRetryable passes over the same applies, so the two sweeps never
// select one apply between them.
func (s *applyStore) heldByEndedOperationSQL(databaseTypes []string) (string, []any) {
	ended, endedArgs := s.endedOperationSQL("ended")
	args := stringArgs(heldByEndedOperationApplyStates)
	args = append(args, stringArgs(databaseTypes)...)
	args = append(args, stringArgs(quietOperationStates)...)
	args = append(args, stringArgs(quietOperationStates)...)
	args = append(args, endedArgs...)
	args = append(args, stringArgs(quietOperationStates)...)
	args = append(args, state.ApplyOperation.Pending)
	args = append(args, state.ApplyOperation.WaitingForCutover, storage.CutoverPolicyBarrier, storage.CutoverPolicyParallel)
	args = append(args, storage.ControlOperationStop, storage.ControlOperationCancel, storage.ControlRequestPending)

	// The cutover runs for one rollout member, a deployment and target, at a
	// time, so an ended operation holds only the operations of its own member.
	// A later member waiting behind it waits on the rollout's barrier, which
	// leaves the decision to the operator, so an apply with any such member is
	// not this sweep's.
	//
	// Every held operation that is copying must be held by a live drive,
	// since the drive is what delivers the stop and saves the copy. A copying
	// operation nobody drives is left to recovery, which claims it again and
	// brings it back here, or, under a spent retry, to retry expiry. An
	// operation parked at the cutover needs no live drive: recovery
	// never claims it again on its own, but the claim of a parked operation
	// takes it once a stop is pending, and that drive delivers the stop. A
	// pending operation needs no drive; stop reconciliation settles it
	// stopped. An apply held only by parked or pending operations has no
	// driver, which retry expiry would otherwise take too, so expiry passes
	// over every apply this matches.
	//
	// A pending stop or cancel is already ending the apply, whoever asked for
	// it, so the apply is left to that request.
	clause := fmt.Sprintf(`applies.state IN (%s)
			AND applies.database_type IN (%s)
			AND %s
			AND EXISTS (
				SELECT 1 FROM apply_operations held
				WHERE held.apply_id = applies.id AND held.state NOT IN (%s)
			)
			AND NOT EXISTS (
				SELECT 1 FROM apply_operations held
				WHERE held.apply_id = applies.id AND held.state NOT IN (%s)
					AND NOT EXISTS (
						SELECT 1 FROM apply_operations ended
						WHERE ended.apply_id = held.apply_id
							AND ended.deployment = held.deployment
							AND ended.target = held.target
							AND %s
					)
			)
			AND NOT EXISTS (
				SELECT 1 FROM apply_operations held
				WHERE held.apply_id = applies.id AND held.state NOT IN (%s) AND held.state <> ?
					AND NOT (held.state = ? AND held.cutover_policy IN (?, ?))
					AND NOT (%s)
			)
			AND NOT EXISTS (
				SELECT 1 FROM apply_control_requests req
				WHERE req.apply_id = applies.id AND req.operation IN (?, ?) AND req.status = ?
			)`, placeholders(len(heldByEndedOperationApplyStates)),
		placeholders(len(databaseTypes)),
		s.dialect.JSONBooleanIsTrue("applies.options", []string{"defer_cutover"}),
		placeholders(len(quietOperationStates)),
		placeholders(len(quietOperationStates)),
		ended,
		placeholders(len(quietOperationStates)),
		freshLeaseSQL(s.dialect, "held"))
	return clause, args
}

// freshLeaseSQL matches an operation, aliased op, that a live drive holds: the
// lease undrivenApplyGate looks for, read for one operation.
func freshLeaseSQL(d Dialect, op string) string {
	freshLeaseAfter := d.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime,
		LiteralIntervalAmount(uint64(storage.ApplyLeaseStaleAfter.Microseconds())), IntervalMicrosecond)
	return fmt.Sprintf("%[1]s.lease_owner <> '' AND %[1]s.updated_at >= %[2]s", op, freshLeaseAfter)
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
	// may start it again, and a pending cancel leaves it to the cancel, which
	// still has engine work to cancel. The apply is taken only once every
	// operation is quiet and no driver holds one, for the reasons
	// expireRetryable gives: the rows of a live drive are its own.
	//
	// A pending operation counts as quiet only with evidence it never started:
	// no engine handle, and no task of the apply doing work. A task still
	// running means a stop has yet to reach an engine, so the apply is left to
	// stop reconciliation, which delivers it.
	ended, endedArgs := s.endedOperationSQL("ended")
	settledTaskStates := append([]string{state.Task.Pending, state.Task.Stopped, state.Task.FailedRetryable}, state.TerminalTaskStates...)
	args := stringArgs(heldByEndedOperationApplyStates)
	args = append(args, storage.ControlOperationStop, storage.ControlRequestPending, storage.EndedOperationStopCaller)
	args = append(args, storage.ControlOperationCancel, storage.ControlRequestPending)
	args = append(args, endedArgs...)
	args = append(args, stringArgs(settleableOperationStates)...)
	args = append(args, state.ApplyOperation.Pending)
	args = append(args, stringArgs(settledTaskStates)...)
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
			AND NOT EXISTS (
				SELECT 1 FROM apply_control_requests req
				WHERE req.apply_id = applies.id AND req.operation = ? AND req.status = ?
			)
			AND EXISTS (
				SELECT 1 FROM apply_operations ended
				WHERE ended.apply_id = applies.id AND %s
			)
			AND NOT EXISTS (
				SELECT 1 FROM apply_operations busy
				WHERE busy.apply_id = applies.id AND busy.state NOT IN (%s)
			)
			AND NOT EXISTS (
				SELECT 1 FROM apply_operations handled
				WHERE handled.apply_id = applies.id AND handled.state = ? AND COALESCE(handled.external_id, '') <> ''
			)
			AND NOT EXISTS (
				SELECT 1 FROM tasks working
				WHERE working.apply_id = applies.id AND working.state NOT IN (%s)
			)
			AND %s
		ORDER BY updated_at
		LIMIT ?
		FOR UPDATE
	`, placeholders(len(heldByEndedOperationApplyStates)),
		s.dialect.JSONBooleanIsTrue("applies.options", []string{"defer_cutover"}),
		ended,
		placeholders(len(settleableOperationStates)),
		placeholders(len(settledTaskStates)),
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
	cancelling, err := appliesWithPendingCancel(ctx, tx, candidateIDs)
	if err != nil {
		return nil, err
	}
	eligible := make([]*storage.Apply, 0, len(applies))
	for _, apply := range applies {
		if undriven[apply.ID] && !cancelling[apply.ID] {
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

// appliesWithPendingCancel reads, inside tx, which of the given applies an
// operator is cancelling. Recording a cancel holds the apply row shared, so
// once tx holds the rows for update a cancel has either committed, and this
// read sees it, or waits for tx. The selection's own check reads without a
// lock and can miss a cancel that committed while it waited.
func appliesWithPendingCancel(ctx context.Context, tx *rebindTx, applyIDs []int64) (map[int64]bool, error) {
	args := make([]any, 0, len(applyIDs)+2)
	for _, id := range applyIDs {
		args = append(args, id)
	}
	args = append(args, storage.ControlOperationCancel, storage.ControlRequestPending)
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
		SELECT DISTINCT apply_id FROM apply_control_requests
		WHERE apply_id IN (%s) AND operation = ? AND status = ?
	`, placeholders(len(applyIDs))), args...)
	if err != nil {
		return nil, fmt.Errorf("query pending cancels of %d applies stopped for an ended operation: %w", len(applyIDs), err)
	}
	defer utils.CloseAndLog(rows)
	cancelling := make(map[int64]bool)
	for rows.Next() {
		var applyID int64
		if err := rows.Scan(&applyID); err != nil {
			return nil, fmt.Errorf("scan pending cancel of %d applies stopped for an ended operation: %w", len(applyIDs), err)
		}
		cancelling[applyID] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pending cancels of %d applies stopped for an ended operation: %w", len(applyIDs), err)
	}
	return cancelling, nil
}

// settleStoppedForEndedOperation writes one apply's settlement inside tx, which
// holds the apply and every one of its operations locked.
//
// An operation that never started is cancelled rather than failed, because it
// did no work to fail. That includes one a stop moved from pending to stopped:
// no engine handle, no start time, and no task of it that left pending.
//
// The other stopped operations settle failed and name the operation they
// failed for, the one that ended their own rollout member. Their tasks keep
// their stopped state: the copy each one made is saved, and a new apply of the
// same change resumes it. An operation still waiting on a retry settles failed
// with its tasks, as expiry settles it.
//
// The stop SchemaBot recorded is left pending here, and so is any cutover an
// operator asked for. The reaper answers both once the apply is failed, through
// the same settlement every other path uses.
func settleStoppedForEndedOperation(ctx context.Context, tx *rebindTx, settled *storage.EndedOperationApply) error {
	apply := settled.Apply
	if _, err := tx.ExecContext(ctx, `
		UPDATE apply_operations
		SET state = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE apply_id = ? AND state IN (?, ?)
			AND COALESCE(external_id, '') = ''
			AND started_at IS NULL
			AND NOT EXISTS (
				SELECT 1 FROM tasks
				WHERE tasks.apply_operation_id = apply_operations.id AND tasks.state <> ?
			)
	`, state.ApplyOperation.Cancelled, apply.ID, state.ApplyOperation.Pending, state.ApplyOperation.Stopped, state.Task.Pending); err != nil {
		return fmt.Errorf("cancel never-started operations of apply %s stopped for ended operation %d: %w", apply.ApplyIdentifier, settled.EndedOperation.ID, err)
	}
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
	if err := failStoppedForMemberFailure(ctx, tx, settled); err != nil {
		return err
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

// rolloutMember is the deployment and target a rollout cuts over together.
type rolloutMember struct {
	deployment string
	target     string
}

// failStoppedForMemberFailure fails each stopped operation of the apply and
// names the operation that ended its own rollout member. A member with no
// ended operation of its own names the one that ended the apply.
func failStoppedForMemberFailure(ctx context.Context, tx *rebindTx, settled *storage.EndedOperationApply) error {
	apply := settled.Apply
	rows, err := tx.QueryContext(ctx, `
		SELECT `+applyOperationColumns+`
		FROM apply_operations
		WHERE apply_id = ?
		ORDER BY id
	`, apply.ID)
	if err != nil {
		return fmt.Errorf("query operations of apply %s stopped for ended operation %d: %w", apply.ApplyIdentifier, settled.EndedOperation.ID, err)
	}
	ops, err := scanApplyOperations(rows)
	utils.CloseAndLog(rows)
	if err != nil {
		return fmt.Errorf("scan operations of apply %s stopped for ended operation %d: %w", apply.ApplyIdentifier, settled.EndedOperation.ID, err)
	}
	byMember := make(map[rolloutMember][]*storage.ApplyOperation)
	var members []rolloutMember
	for _, op := range ops {
		m := rolloutMember{deployment: op.Deployment, target: op.Target}
		if _, ok := byMember[m]; !ok {
			members = append(members, m)
		}
		byMember[m] = append(byMember[m], op)
	}
	for _, m := range members {
		endedBy := settled.EndedOperation.ID
		if ended := firstEndedOperation(byMember[m]); ended != nil {
			endedBy = ended.ID
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE apply_operations
			SET state = ?, ended_by_operation_id = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
			WHERE apply_id = ? AND deployment = ? AND target = ? AND state = ?
		`, state.ApplyOperation.Failed, endedBy, apply.ID, m.deployment, m.target, state.ApplyOperation.Stopped); err != nil {
			return fmt.Errorf("fail operations of apply %s deployment %s target %s stopped for ended operation %d: %w",
				apply.ApplyIdentifier, m.deployment, m.target, endedBy, err)
		}
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
