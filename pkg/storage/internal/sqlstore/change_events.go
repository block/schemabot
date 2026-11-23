package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/changeevents"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

const changeEventStorageTimeout = 250 * time.Millisecond

type changeEventRecorder struct {
	db     *rebindDB
	sender *changeevents.Sender
	app    func(string) string
	logger *slog.Logger
}

func insertChangeEventLog(ctx context.Context, tx queryExecer, applyID int64, operationID *int64, kind, outcome, description string, metadata []byte) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO apply_logs (apply_id, apply_operation_id, level, event_type, source, message, new_state, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, applyID, nullInt64Ptr(operationID), storage.LogLevelInfo, kind, storage.LogSourceSchemaBot, description,
		nullString(outcome), nullJSON(metadata))
	if err != nil {
		return fmt.Errorf("record %s in apply audit log: %w", kind, err)
	}
	return nil
}

// ObserveOperationDispatch observes the local engine handoff on this drive. Only
// a fresh attempt offers a start event. A recovered drive can still observe the
// handoff itself and report the outcome it commits.
func (s *Storage) ObserveOperationDispatch(ctx context.Context, apply *storage.Apply) {
	r := s.changeEvents
	if r == nil || ctx.Err() != nil {
		return
	}
	startEnabled := r.sender.Enabled(changeevents.OperationStarted, apply.Environment)
	if !startEnabled && !r.sender.Enabled(changeevents.OperationFinished, apply.Environment) {
		return
	}
	lease, ok := storage.OperationLeaseFromContext(ctx)
	attempt := storage.OperationChangeAttemptFromContext(ctx)
	if !ok || !lease.Valid() || lease.ApplyID != apply.ID || attempt == nil || lease.OperationID != attempt.OperationID {
		return
	}
	if !attempt.ObserveDispatch() || !attempt.Starts || !startEnabled {
		return
	}

	r.sender.Offer(r.operationEvent(apply, attempt, changeevents.OperationStarted, ""))
}

func (r *changeEventRecorder) finishOperation(ctx context.Context, operationID int64, outcome string) {
	if r == nil || !r.sender.Selects(changeevents.OperationFinished) ||
		(!state.IsState(outcome, state.ApplyOperation.FailedRetryable) && !state.IsState(outcome, state.SettledApplyStates...)) {
		return
	}
	attempt := storage.OperationChangeAttemptFromContext(ctx)
	if attempt == nil || attempt.OperationID != operationID || !attempt.ObservedDispatch() {
		return
	}

	r.record(context.WithoutCancel(ctx), changeevents.OperationFinished, func(ctx context.Context, tx *rebindTx) (changeevents.Event, error) {
		op, err := scanApplyOperation(tx.QueryRowContext(ctx, `SELECT `+applyOperationColumns+` FROM apply_operations WHERE id = ? FOR UPDATE`, operationID))
		if err != nil || op == nil {
			return changeevents.Event{}, err
		}
		apply, err := scanApply(tx.QueryRowContext(ctx, `SELECT `+applyColumns+` FROM applies WHERE id = ?`, op.ApplyID))
		if err != nil || apply == nil {
			return changeevents.Event{}, err
		}
		if !r.sender.Enabled(changeevents.OperationFinished, apply.Environment) {
			return changeevents.Event{}, nil
		}
		recorded, err := operationFinishRecorded(ctx, tx, op, attempt.Number)
		if err != nil || recorded {
			return changeevents.Event{}, err
		}
		event := r.operationEvent(apply, attempt, changeevents.OperationFinished, outcome)
		marker := []byte(`{"attempt":` + strconv.Itoa(attempt.Number) + `}`)
		return event, insertChangeEventLog(ctx, tx, apply.ID, &op.ID, event.Type, outcome, event.Description, marker)
	})
}

func (r *changeEventRecorder) operationEvent(apply *storage.Apply, attempt *storage.OperationChangeAttempt, kind, outcome string) changeevents.Event {
	event := changeevents.Event{
		Type: kind, App: r.application(apply.GetOptions().ChangeEventApp, apply.Database), Environment: apply.Environment,
		OccurredAtNanos: time.Now().UnixNano(),
		Description: fmt.Sprintf("%s apply=%s operation=%d deployment=%s attempt=%d", kind,
			apply.ApplyIdentifier, attempt.OperationID, attempt.Deployment, attempt.Number),
	}
	if outcome != "" {
		event.Description += " outcome=" + outcome
	}
	event.Description += changeEventCaller(apply.Caller, apply.Repository, apply.PullRequest)
	return event
}

func operationFinishRecorded(ctx context.Context, tx *rebindTx, op *storage.ApplyOperation, attempt int) (bool, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT metadata FROM apply_logs
		WHERE apply_id = ? AND apply_operation_id = ? AND event_type = ?
	`, op.ApplyID, op.ID, changeevents.OperationFinished)
	if err != nil {
		return false, err
	}
	defer utils.CloseAndLog(rows)
	for rows.Next() {
		var metadata []byte
		if err := rows.Scan(&metadata); err != nil {
			return false, err
		}
		var marker struct {
			Attempt int `json:"attempt"`
		}
		if err := json.Unmarshal(metadata, &marker); err != nil {
			return false, fmt.Errorf("decode operation finish marker: %w", err)
		}
		if marker.Attempt == attempt {
			return true, nil
		}
	}
	return false, rows.Err()
}

func (r *changeEventRecorder) finishApply(ctx context.Context, applyID int64, outcome string) {
	if r == nil || !r.sender.Selects(changeevents.ApplyFinished) || !state.IsState(outcome, state.SettledApplyStates...) {
		return
	}
	r.record(context.WithoutCancel(ctx), changeevents.ApplyFinished, func(ctx context.Context, tx *rebindTx) (changeevents.Event, error) {
		apply, err := scanApply(tx.QueryRowContext(ctx, `SELECT `+applyColumns+` FROM applies WHERE id = ? FOR UPDATE`, applyID))
		if err != nil || apply == nil {
			return changeevents.Event{}, err
		}
		// A keyed apply created by remote dispatch is the worker's copy. Only
		// the control-plane parent reports the aggregate outcome.
		if strings.HasPrefix(apply.IdempotencyKey, "schemabot:v1:") || !state.IsState(apply.State, outcome) ||
			!r.sender.Enabled(changeevents.ApplyFinished, apply.Environment) {
			return changeevents.Event{}, nil
		}
		done, err := hasChangeEventLog(ctx, tx, applyID, `apply_operation_id IS NULL AND event_type = ? AND new_state = ?`, changeevents.ApplyFinished, outcome)
		if err != nil || done {
			return changeevents.Event{}, err
		}
		summary, err := changeEventDeploymentSummary(ctx, tx, applyID, outcome)
		if err != nil {
			return changeevents.Event{}, err
		}
		event := changeevents.Event{
			Type: changeevents.ApplyFinished, App: r.application("", apply.Database), Environment: apply.Environment,
			OccurredAtNanos: time.Now().UnixNano(),
			Description: fmt.Sprintf("%s apply=%s outcome=%s%s%s", changeevents.ApplyFinished, apply.ApplyIdentifier,
				outcome, summary, changeEventCaller(apply.Caller, apply.Repository, apply.PullRequest)),
		}
		return event, insertChangeEventLog(ctx, tx, applyID, nil, event.Type, outcome, event.Description, nil)
	})
}

func (r *changeEventRecorder) application(app, database string) string {
	if app == "" && r.app != nil {
		app = r.app(database)
	}
	if app == "" {
		return database
	}
	return app
}

func changeEventCaller(caller, repository string, pullRequest int) string {
	var description string
	if caller != "" {
		description = " caller=" + caller
	}
	if repository != "" && pullRequest > 0 {
		description += fmt.Sprintf(" https://github.com/%s/pull/%d", repository, pullRequest)
	}
	return description
}

func hasChangeEventLog(ctx context.Context, tx *rebindTx, applyID int64, predicate string, args ...any) (bool, error) {
	var exists int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM apply_logs WHERE apply_id = ? AND `+predicate+` LIMIT 1`,
		append([]any{applyID}, args...)...).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return exists == 1, err
}

func changeEventDeploymentSummary(ctx context.Context, tx *rebindTx, applyID int64, outcome string) (string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT deployment, state FROM apply_operations WHERE apply_id = ?`, applyID)
	if err != nil {
		return "", err
	}
	defer utils.CloseAndLog(rows)
	deployments := make(map[string]map[string]int)
	for rows.Next() {
		var deployment, outcome string
		if err := rows.Scan(&deployment, &outcome); err != nil {
			return "", err
		}
		if deployments[deployment] == nil {
			deployments[deployment] = make(map[string]int)
		}
		deployments[deployment][outcome]++
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(deployments) == 0 {
		return "", nil
	}
	var summary []string
	for _, deployment := range slices.Sorted(maps.Keys(deployments)) {
		outcomes := deployments[deployment]
		var counts []string
		for _, status := range slices.Sorted(maps.Keys(outcomes)) {
			counts = append(counts, fmt.Sprintf("%s:%d", status, outcomes[status]))
		}
		summary = append(summary, deployment+"("+strings.Join(counts, ",")+")")
	}
	// A single-operation driver may commit its parent before writing its
	// child verdict. In that case the aggregate verdict is also the child's.
	if len(deployments) == 1 {
		for deployment, outcomes := range deployments {
			for status, count := range outcomes {
				if len(outcomes) == 1 && count == 1 && !state.IsState(status, state.SettledApplyStates...) {
					return " deployments=" + deployment + "(" + outcome + ":1)", nil
				}
			}
		}
	}
	return " deployments=" + strings.Join(summary, ","), nil
}

func (r *changeEventRecorder) record(ctx context.Context, kind string, makeEvent func(context.Context, *rebindTx) (changeevents.Event, error)) {
	ctx, cancel := context.WithTimeout(ctx, changeEventStorageTimeout)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err == nil {
		defer rollbackTx(ctx, tx, "record change event")
		var event changeevents.Event
		event, err = makeEvent(ctx, tx)
		if err == nil && event.Type != "" {
			err = tx.Commit()
			if err == nil {
				r.sender.Offer(event)
			}
		}
	}
	if err != nil && r.logger != nil {
		r.logger.Warn("could not record change event", "event", kind, "error", err)
	}
}
