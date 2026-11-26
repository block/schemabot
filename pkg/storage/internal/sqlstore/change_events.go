package sqlstore

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/changeevents"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/google/uuid"
)

const changeEventStorageTimeout = 250 * time.Millisecond

type changeEventRecorder struct {
	store  *Storage
	sender *changeevents.Sender
	app    func(string) string
	logger *slog.Logger
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

func (r *changeEventRecorder) finishOperation(ctx context.Context, applyID int64, attempt *storage.OperationChangeAttempt, outcome string) {
	if !attempt.ObserveFinish() {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), changeEventStorageTimeout)
	defer cancel()
	apply, err := r.store.Applies().Get(ctx, applyID)
	if err != nil {
		r.logReadError(changeevents.OperationFinished, applyID, err)
		return
	}
	if apply != nil && r.sender.Enabled(changeevents.OperationFinished, apply.Environment) {
		r.sender.Offer(r.operationEvent(apply, attempt, changeevents.OperationFinished, outcome))
	}
}

func (r *changeEventRecorder) operationEvent(apply *storage.Apply, attempt *storage.OperationChangeAttempt, kind, outcome string) changeevents.Event {
	event := r.newEvent(apply, kind, outcome, apply.GetOptions().ChangeEventApp, time.Now().UnixNano())
	event.Details.OperationID = attempt.OperationID
	event.Details.Deployment = attempt.Deployment
	event.Details.Attempt = attempt.Number
	event.Description = fmt.Sprintf("%s apply=%s operation=%d deployment=%s attempt=%d", kind,
		apply.ApplyIdentifier, attempt.OperationID, attempt.Deployment, attempt.Number)
	if outcome != "" {
		event.Description += " outcome=" + outcome
	}
	event.Description += changeEventCaller(apply.Caller, apply.Repository, apply.PullRequest)
	return event
}

// selectsApplyFinish uses only the requested state and destination filters.
// The recorder checks rollout settlement after the write commits.
func (r *changeEventRecorder) selectsApplyFinish(apply *storage.Apply) bool {
	return r != nil && state.IsState(apply.State, state.SettledApplyStates...) &&
		r.sender.Enabled(changeevents.ApplyFinished, apply.Environment)
}

func (r *changeEventRecorder) rolloutFailureSettled(ctx context.Context, applyID int64, ops []*storage.ApplyOperation) (bool, error) {
	if len(ops) < 2 {
		return true, nil
	}
	released, err := storage.ReleaseLatched(ctx, r.store, applyID, ops)
	if err != nil {
		return false, err
	}
	children := make([]state.RolloutOperation, len(ops))
	for i, op := range ops {
		children[i] = op.RolloutOperation(released)
	}
	return state.IsState(state.DeriveRolloutApplyState(state.RolloutChildren(children)), state.Apply.Failed), nil
}

// finishApply offers an event after a committed transition. A direct failure
// checks settlement against the same rows it uses for the summary.
func (r *changeEventRecorder) finishApply(ctx context.Context, applyID int64, outcome string, directWrite bool) {
	if r == nil || !r.sender.Selects(changeevents.ApplyFinished) || !state.IsState(outcome, state.SettledApplyStates...) {
		return
	}
	if event, ready := r.loadApplyFinish(context.WithoutCancel(ctx), applyID, outcome, directWrite, time.Now().UnixNano()); ready {
		r.sender.Offer(event)
	}
}

// offerReapedApplyFinish queues the committed outcome so the reaper can
// return its batch before any reads for event delivery.
func (r *changeEventRecorder) offerReapedApplyFinish(apply *storage.Apply) {
	if !r.selectsApplyFinish(apply) {
		return
	}
	applyID, outcome, occurredAt := apply.ID, apply.State, time.Now().UnixNano()
	r.sender.OfferDeferred(changeevents.ApplyFinished, apply.Environment, func(ctx context.Context) (changeevents.Event, bool) {
		return r.loadApplyFinish(ctx, applyID, outcome, false, occurredAt)
	})
}

func (r *changeEventRecorder) loadApplyFinish(ctx context.Context, applyID int64, outcome string, directWrite bool, occurredAt int64) (changeevents.Event, bool) {
	ctx, cancel := context.WithTimeout(ctx, changeEventStorageTimeout)
	defer cancel()
	apply, err := r.store.Applies().Get(ctx, applyID)
	if err != nil {
		r.logReadError(changeevents.ApplyFinished, applyID, err)
		return changeevents.Event{}, false
	}
	// A keyed apply created by remote dispatch is the worker's copy. Only
	// the control-plane parent reports the aggregate outcome.
	if apply == nil || strings.HasPrefix(apply.IdempotencyKey, "schemabot:v1:") ||
		!state.IsState(apply.State, outcome) || !r.sender.Enabled(changeevents.ApplyFinished, apply.Environment) {
		return changeevents.Event{}, false
	}
	ops, err := r.store.ApplyOperations().ListByApply(ctx, applyID)
	if err != nil {
		r.logReadError(changeevents.ApplyFinished, applyID, err)
		return changeevents.Event{}, false
	}
	if directWrite && state.IsState(outcome, state.Apply.Failed) {
		settled, err := r.rolloutFailureSettled(ctx, applyID, ops)
		if err != nil {
			r.logReadError(changeevents.ApplyFinished, applyID, err)
		}
		if !settled {
			return changeevents.Event{}, false
		}
	}
	summary := changeEventDeploymentSummary(ops, outcome)
	event := r.newEvent(apply, changeevents.ApplyFinished, outcome, "", occurredAt)
	completedAt := apply.UpdatedAt
	if apply.CompletedAt != nil {
		completedAt = *apply.CompletedAt
	}
	event.Details.CompletedAt = completedAt.UTC()
	event.Description = fmt.Sprintf("%s apply=%s outcome=%s%s%s", changeevents.ApplyFinished, apply.ApplyIdentifier,
		outcome, summary, changeEventCaller(apply.Caller, apply.Repository, apply.PullRequest))
	return event, true
}

func (r *changeEventRecorder) newEvent(apply *storage.Apply, kind, outcome, app string, occurredAt int64) changeevents.Event {
	return changeevents.Event{
		Type: kind, App: r.application(app, apply.Database), Environment: apply.Environment,
		OccurredAtNanos: occurredAt,
		Details: changeevents.EventDetails{
			EventID: uuid.NewString(), ApplyID: apply.ApplyIdentifier, Outcome: outcome,
			Database: apply.Database, DatabaseType: apply.DatabaseType, Repository: apply.Repository, PullRequest: apply.PullRequest,
		},
	}
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

func changeEventDeploymentSummary(ops []*storage.ApplyOperation, outcome string) string {
	if len(ops) == 0 {
		return ""
	}
	deployments := make(map[string]map[string]int)
	for _, op := range ops {
		if deployments[op.Deployment] == nil {
			deployments[op.Deployment] = make(map[string]int)
		}
		deployments[op.Deployment][op.State]++
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
					return " deployments=" + deployment + "(" + outcome + ":1)"
				}
			}
		}
	}
	return " deployments=" + strings.Join(summary, ",")
}

func (r *changeEventRecorder) logReadError(kind string, applyID int64, err error) {
	r.logger.Warn("could not read change event data", "event", kind, "apply_id", applyID, "error", err)
}
