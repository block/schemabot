package storage

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/block/schemabot/pkg/caller"
	"github.com/block/schemabot/pkg/changeevents"
	"github.com/block/schemabot/pkg/state"
)

// ChangeEventDispatch identifies the control-plane apply and operations at the
// execution site. A remote apply can accept several operations, so the first
// dispatch stores all correlations for its deployment in the apply options.
type ChangeEventDispatch struct {
	ApplyID        string           `json:"apply_id"`
	App            string           `json:"app,omitempty"`
	Deployment     string           `json:"deployment"`
	OperationIDs   map[string]int64 `json:"operation_ids"`
	PullRequestURL string           `json:"pull_request_url,omitempty"`
	AttemptOffset  int              `json:"attempt_offset,omitempty"`
}

// OperationDispatchJournal records the dispatch boundary under the operation
// lease. It distinguishes a claim that never reached the engine from a
// reattachment to an attempt a previous driver dispatched.
type OperationDispatchJournal interface {
	RecordOperationDispatch(ctx context.Context, lease OperationLease, attempt int) (first bool, err error)
	HasOperationDispatch(ctx context.Context, lease OperationLease, attempt int) (bool, error)
}

const changeEventJournalTimeout = 100 * time.Millisecond

// ChangeEvents publishes lifecycle snapshots. A nil publisher disables events.
type ChangeEvents struct {
	Sink    changeevents.Sink
	Accepts func(changeevents.Type, string) bool
	AppFor  func(database string) string
	Journal OperationDispatchJournal
	Logger  *slog.Logger
}

func (p *ChangeEvents) appFor(database string) string {
	if p.AppFor != nil {
		if app := p.AppFor(database); app != "" {
			return app
		}
	}
	return database
}

func changeDescription(description, caller, pullRequestURL string) string {
	if caller != "" {
		description += fmt.Sprintf(" caller=%q", caller)
	}
	if pullRequestURL != "" {
		description += " " + pullRequestURL
	}
	return description
}

// ChangeEventPullRequestURL returns the PR link for a PR-backed apply.
func (apply *Apply) ChangeEventPullRequestURL() string {
	if apply.Repository == "" || apply.PullRequest <= 0 {
		return ""
	}
	return caller.PullRequestURL(apply.Repository, apply.PullRequest)
}

type changeEventApplySnapshot struct {
	apply *Apply
	ops   []*ApplyOperation
}

type changeEventApplyKey struct{}

// WithApplyChangeEventSnapshot supplies the rows used to calculate an outcome.
// The storage writer uses them synchronously after the winning commit.
func WithApplyChangeEventSnapshot(ctx context.Context, apply *Apply, ops []*ApplyOperation) context.Context {
	return context.WithValue(ctx, changeEventApplyKey{}, changeEventApplySnapshot{apply: apply, ops: ops})
}

// OfferRecoveredOperationFinished publishes a stale attempt's outcome when
// the caller's pre-write snapshot identifies an operation that reached dispatch.
func (p *ChangeEvents) OfferRecoveredOperationFinished(ctx context.Context, operationID int64, outcome string) {
	if p == nil {
		return
	}
	snapshot, _ := ctx.Value(changeEventApplyKey{}).(changeEventApplySnapshot)
	if snapshot.apply == nil || len(snapshot.ops) != 1 || snapshot.ops[0].ID != operationID {
		return
	}
	ctx = p.WithOperationChangeEvents(ctx, snapshot.apply, snapshot.ops[0])
	OfferOperationFinished(ctx, operationID, outcome)
}

// OfferApplyFinished publishes the aggregate verdict committed by the control
// plane. Dispatch-created execution records do not own that aggregate.
func (p *ChangeEvents) OfferApplyFinished(ctx context.Context, apply *Apply, outcome string) {
	if p == nil || !state.IsState(outcome, state.SettledApplyStates...) {
		return
	}
	snapshot, _ := ctx.Value(changeEventApplyKey{}).(changeEventApplySnapshot)
	if apply == nil {
		apply = snapshot.apply
	}
	if apply == nil || apply.IdempotencyKey != "" || apply.GetOptions().ChangeEventDispatch != nil {
		return
	}
	if p.Accepts != nil && !p.Accepts(changeevents.ApplyFinished, apply.Environment) {
		return
	}
	var deployments string
	if snapshot.apply != nil && snapshot.apply.ID == apply.ID && len(snapshot.ops) > 0 {
		deployments = summarizeDeployments(snapshot.ops)
	} else if apply.Deployment != "" {
		deployments = fmt.Sprintf("%s[%s]", apply.Deployment, outcome)
	}
	description := fmt.Sprintf("apply.finished apply=%s outcome=%s", apply.ApplyIdentifier, outcome)
	if deployments != "" {
		description += " deployments=" + deployments
	}
	p.Sink.Offer(changeevents.New(changeevents.ApplyFinished, p.appFor(apply.Database), apply.Environment,
		changeDescription(description, apply.Caller, apply.ChangeEventPullRequestURL())))
}

func summarizeDeployments(ops []*ApplyOperation) string {
	counts := make(map[string]map[string]int)
	for _, op := range ops {
		if counts[op.Deployment] == nil {
			counts[op.Deployment] = make(map[string]int)
		}
		counts[op.Deployment][op.State]++
	}
	deployments := make([]string, 0, len(counts))
	for _, deployment := range slices.Sorted(maps.Keys(counts)) {
		outcomes := make([]string, 0, len(counts[deployment]))
		for _, outcome := range slices.Sorted(maps.Keys(counts[deployment])) {
			outcomes = append(outcomes, fmt.Sprintf("%s:%d", outcome, counts[deployment][outcome]))
		}
		deployments = append(deployments, fmt.Sprintf("%s[%s]", deployment, strings.Join(outcomes, ",")))
	}
	return strings.Join(deployments, ",")
}

type operationChangeKey struct{}

type operationChange struct {
	sink              changeevents.Sink
	journal           OperationDispatchJournal
	logger            *slog.Logger
	lease             OperationLease
	attempt           int
	engineWorkKnown   bool
	app, environment  string
	description       string
	caller, prURL     string
	newAttempt        bool
	previouslyStarted bool
	previouslySettled bool
	dispatchChecked   atomic.Bool
	dispatched        atomic.Bool
	finished          atomic.Bool
}

// WithOperationChangeEvents tracks one locally executed claim across the
// driver's engine calls and the operation store's outcome write.
func (p *ChangeEvents) WithOperationChangeEvents(ctx context.Context, apply *Apply, op *ApplyOperation) context.Context {
	if p == nil {
		return ctx
	}
	if p.Accepts != nil && !p.Accepts(changeevents.OperationStarted, apply.Environment) &&
		!p.Accepts(changeevents.OperationFinished, apply.Environment) {
		return ctx
	}
	app := p.appFor(apply.Database)
	applyID, operationID, deployment := apply.ApplyIdentifier, op.ID, op.Deployment
	prURL := apply.ChangeEventPullRequestURL()
	attempt := op.Attempt + 1
	if dispatch := apply.GetOptions().ChangeEventDispatch; dispatch != nil {
		applyID, operationID, deployment = dispatch.ApplyID, dispatch.OperationIDs[op.OperationKey], dispatch.Deployment
		if operationID <= 0 {
			return ctx
		}
		if dispatch.App != "" {
			app = dispatch.App
		}
		prURL = dispatch.PullRequestURL
		attempt += max(dispatch.AttemptOffset, 0)
	} else if apply.IdempotencyKey != "" {
		// A dispatch from a peer predating event correlation has no control-plane
		// identifiers. Local identifiers would appear to describe a separate apply.
		return ctx
	}
	event := &operationChange{
		sink: p.Sink, journal: p.Journal, logger: p.Logger,
		lease: op.Lease(), attempt: op.Attempt,
		app: app, environment: apply.Environment,
		engineWorkKnown: !op.StartsAttempt && (op.EngineResumeContext != "" || op.EngineResumeMetadata != ""),
		description:     fmt.Sprintf("apply=%s operation=%d deployment=%s attempt=%d", applyID, operationID, deployment, attempt),
		caller:          apply.Caller, prURL: prURL,
		newAttempt:        op.StartsAttempt,
		previouslyStarted: op.StartedAt != nil && !op.StartsAttempt,
		previouslySettled: state.IsState(op.State, state.SettledApplyStates...) ||
			state.IsState(op.State, state.ApplyOperation.FailedRetryable),
	}
	return context.WithValue(ctx, operationChangeKey{}, event)
}

// OfferOperationStarted runs immediately before local engine dispatch. A drive
// can dispatch many statements under one claim and only the first offers start.
func OfferOperationStarted(ctx context.Context) {
	event, _ := ctx.Value(operationChangeKey{}).(*operationChange)
	if event == nil || ctx.Err() != nil || !event.dispatchChecked.CompareAndSwap(false, true) {
		return
	}
	first := event.newAttempt
	if event.journal != nil {
		// The marker is independent of delivery and of the engine's outcome.
		// Bound the observation so storage contention cannot hold the drive.
		journalCtx, cancel := context.WithTimeout(ctx, changeEventJournalTimeout)
		defer cancel()
		var err error
		first, err = event.journal.RecordOperationDispatch(journalCtx, event.lease, event.attempt)
		if err != nil {
			event.logJournalError(ctx, err)
			return
		}
	}
	event.dispatched.Store(true)
	if first && !event.engineWorkKnown {
		event.offer(changeevents.OperationStarted, "")
	}
}

// OfferOperationFinished runs after the claimed operation's outcome commits.
// Stopping parks resumable work in the same attempt. A subsequent cancel or
// completion can still end that attempt.
func OfferOperationFinished(ctx context.Context, operationID int64, outcome string) {
	event, _ := ctx.Value(operationChangeKey{}).(*operationChange)
	if event == nil || event.lease.OperationID != operationID || event.finished.Load() {
		return
	}
	if !state.IsState(outcome, state.SettledApplyStates...) && !state.IsState(outcome, state.ApplyOperation.FailedRetryable) {
		return
	}
	if !event.dispatched.Load() && !event.recoverDispatch(ctx) {
		return
	}
	if event.finished.CompareAndSwap(false, true) {
		event.offer(changeevents.OperationFinished, outcome)
	}
}

func (e *operationChange) recoverDispatch(ctx context.Context) bool {
	if e.previouslySettled {
		return false
	}
	if e.journal == nil {
		return e.previouslyStarted
	}
	journalCtx, cancel := context.WithTimeout(ctx, changeEventJournalTimeout)
	defer cancel()
	known, err := e.journal.HasOperationDispatch(journalCtx, e.lease, e.attempt)
	if err != nil {
		e.logJournalError(ctx, err)
		return false
	}
	return known
}

func (e *operationChange) logJournalError(ctx context.Context, err error) {
	if e.logger != nil {
		e.logger.WarnContext(ctx, "change event dispatch journal unavailable", "operation_id", e.lease.OperationID, "error", err)
	}
}

func (e *operationChange) offer(kind changeevents.Type, outcome string) {
	description := string(kind) + " " + e.description
	if outcome != "" {
		description += " outcome=" + outcome
	}
	e.sink.Offer(changeevents.New(kind, e.app, e.environment, changeDescription(description, e.caller, e.prURL)))
}
