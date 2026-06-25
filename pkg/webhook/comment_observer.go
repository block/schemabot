package webhook

import (
	"context"
	"sync"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/clock"
	"github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// CommentObserver implements tern.ProgressObserver by posting PR comments.
// It replaces the separate watchApplyProgress goroutine — the progress poller
// in the tern layer calls OnProgress/OnTerminal directly, so one goroutine
// handles both execution and comment posting.
//
// Rate-limits progress updates to avoid excessive GitHub API calls.
// Errors from GitHub are logged but never block the schema change.
type CommentObserver struct {
	ghClient       github.GitHubClientFactory
	stor           storage.Storage
	repo           string
	pr             int
	installationID int64
	applyID        int64
	applyLease     storage.ApplyLease
	deferCutover   bool
	supportChannel api.SupportChannelConfig
	logger         interface {
		Info(msg string, args ...any)
		Error(msg string, args ...any)
	}

	// OnTerminalHook is called after the summary comment is posted.
	// Used by the webhook handler to update check runs on terminal state.
	// Optional — nil means no hook.
	OnTerminalHook func(apply *storage.Apply)

	// clock is the time source used for adaptive rate-limit math. Defaults
	// to clock.Real{} in NewCommentObserver; tests may inject a *clock.Fake
	// via clock.NewFake(start).
	clock clock.Clock

	// aggregateTerminalCASWinner marks a one-shot observer used by the operator
	// to publish a multi-operation apply's single terminal summary after it won
	// the aggregate projection compare-and-swap. Such a driver holds the
	// operation lease, not the parent apply lease, so the per-driver apply-lease
	// authority does not apply; the won CAS is the authority. It bypasses the
	// apply-lease checks and lease-scoped storage writes accordingly.
	aggregateTerminalCASWinner bool

	mu                sync.Mutex
	lastProgressPost  time.Time
	lastState         string
	lastRowsCopied    int64
	stagnantTicks     int
	hasCutoverComment bool
	resumeRotated     bool
}

const (
	// Adaptive polling intervals — same as watchApplyProgress.
	activeInterval   = 5 * time.Second
	stagnantInterval = 30 * time.Second
	stagnantThresh   = 3 // consecutive unchanged ticks before slowing down
)

// CommentObserverConfig holds the parameters for creating a CommentObserver.
type CommentObserverConfig struct {
	GHClient       github.GitHubClientFactory
	Storage        storage.Storage
	Repo           string
	PR             int
	InstallationID int64
	ApplyID        int64
	ApplyLease     storage.ApplyLease
	DeferCutover   bool
	SupportChannel api.SupportChannelConfig
	Logger         interface {
		Info(msg string, args ...any)
		Error(msg string, args ...any)
	}

	// OnTerminalHook is called after the summary comment is posted.
	// Used to update check runs on terminal state.
	OnTerminalHook func(apply *storage.Apply)

	// Clock is the time source for adaptive rate-limit math. Optional —
	// nil or typed-nil defaults to clock.Real{} (via clock.Default). Tests
	// inject a *clock.Fake via clock.NewFake(start) to make the
	// stagnant / active transition observable without sleeping.
	Clock clock.Clock
}

// SetApplyID sets the apply ID after the apply record is created.
// Called before the observer is registered for progress notifications.
func (o *CommentObserver) SetApplyID(id int64) {
	o.applyID = id
}

// logError logs an observer error with the identifying fields operators need
// to correlate GitHub side effects with an apply: repo, PR, and the apply
// identifier. Without them, a log search scoped to one apply silently misses
// every GitHub-side failure for that apply.
func (o *CommentObserver) logError(apply *storage.Apply, msg string, args ...any) {
	fields := []any{
		"repo", o.repo,
		"pr", o.pr,
	}
	if apply != nil {
		fields = append(fields,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
		)
	}
	o.logger.Error(msg, append(fields, args...)...)
}

func (o *CommentObserver) logInfo(apply *storage.Apply, msg string, args ...any) {
	fields := []any{
		"repo", o.repo,
		"pr", o.pr,
	}
	if apply != nil {
		fields = append(fields,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
		)
	}
	o.logger.Info(msg, append(fields, args...)...)
}

// NewCommentObserver creates a new CommentObserver for posting PR comments.
func NewCommentObserver(cfg CommentObserverConfig) *CommentObserver {
	clk := clock.Default(cfg.Clock)
	return &CommentObserver{
		ghClient:       cfg.GHClient,
		stor:           cfg.Storage,
		repo:           cfg.Repo,
		pr:             cfg.PR,
		installationID: cfg.InstallationID,
		applyID:        cfg.ApplyID,
		applyLease:     cfg.ApplyLease,
		deferCutover:   cfg.DeferCutover,
		supportChannel: cfg.SupportChannel,
		logger:         cfg.Logger,
		OnTerminalHook: cfg.OnTerminalHook,
		clock:          clk,
	}
}

// NewAggregateTerminalCommentObserver builds a one-shot observer for the
// operator to publish a multi-operation apply's single terminal summary after it
// won the aggregate projection compare-and-swap. The CAS win — not a parent
// apply lease — is the authority, so this observer bypasses the per-driver
// apply-lease checks. Only OnTerminal is meant to be called on it.
func NewAggregateTerminalCommentObserver(cfg CommentObserverConfig) *CommentObserver {
	o := NewCommentObserver(cfg)
	o.aggregateTerminalCASWinner = true
	return o
}

// OnProgress is called on each progress poller tick. Rate-limits updates
// to avoid excessive GitHub API calls. Handles the comment lifecycle:
// progress edits, cutover comment creation, and state-change notifications.
func (o *CommentObserver) OnProgress(apply *storage.Apply, tasks []*storage.Task) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.leaseStillOwnsObserver(apply, "progress") {
		return
	}

	now := o.clock.Now()
	currentState := apply.State

	// Check if a cutover comment was posted by an external handler.
	// This must happen before the CuttingOver branch below — without it,
	// the observer would post a duplicate cutover comment.
	if !o.hasCutoverComment {
		checkCtx, checkCancel := context.WithTimeout(context.Background(), 2*time.Second)
		cutover, err := o.stor.ApplyComments().Get(checkCtx, o.applyID, state.Comment.Cutover)
		if err != nil {
			o.logError(apply, "observer: failed to check for cutover comment", "error", err)
		} else if cutover != nil {
			o.hasCutoverComment = true
		}
		checkCancel()
	}

	// Post cutover comment when entering cutting_over with defer_cutover,
	// but only if one hasn't been posted already.
	if currentState == state.Apply.CuttingOver && o.shouldDeferCutover(apply) && !o.hasCutoverComment {
		body := o.formatStatusComment(apply, tasks)
		o.postAndTrackComment(apply, state.Comment.Cutover, body)
		o.hasCutoverComment = true
		o.lastState = currentState
		return
	}

	// If cutover was triggered, stop editing — the progress comment is frozen
	// and OnTerminal will handle the cutover comment completion.
	if o.hasCutoverComment {
		return
	}

	// A summary comment present while the apply is active again means the apply
	// was stopped and has resumed — stopped is the only terminal state that
	// returns to active. Post a fresh progress comment to track the resumed row
	// copy and leave the prior comment frozen at "Stopped" as the record of where
	// the apply paused.
	if !state.IsTerminalApplyState(apply.State) && o.rotateProgressCommentForResume(apply, tasks) {
		o.lastState = currentState
		o.lastProgressPost = now
		o.stagnantTicks = 0
		return
	}

	// Adaptive rate limiting — ported from watchApplyProgress.
	// Edit every 5s when progress is moving, slow to 30s when stagnant.
	var totalRows int64
	for _, t := range tasks {
		totalRows += t.RowsCopied
	}

	interval := activeInterval
	if o.stagnantTicks >= stagnantThresh {
		interval = stagnantInterval
	}

	if totalRows == o.lastRowsCopied && currentState == o.lastState {
		o.stagnantTicks++
		if o.stagnantTicks >= stagnantThresh && now.Sub(o.lastProgressPost) < stagnantInterval {
			return // stagnant — skip edit
		}
		if now.Sub(o.lastProgressPost) < interval {
			return // not time yet
		}
	} else {
		o.stagnantTicks = 0
		o.lastRowsCopied = totalRows
		if now.Sub(o.lastProgressPost) < activeInterval && currentState == o.lastState {
			return // active but not time yet (unless state changed)
		}
	}

	o.lastState = currentState
	o.lastProgressPost = now

	// Edit the progress comment
	body := o.formatStatusComment(apply, tasks)
	o.editTrackedComment(apply, state.Comment.Progress, body)
}

// OnTerminal is called when the apply reaches a terminal state.
// Edits the active comment to final state, posts summary comment,
// and updates check runs.
func (o *CommentObserver) OnTerminal(apply *storage.Apply, tasks []*storage.Task) {
	if !o.leaseStillOwnsObserver(apply, "terminal") {
		return
	}
	// Determine which comment to edit to final state.
	// If a cutover comment exists, edit that and leave the progress comment
	// frozen at its last state. Otherwise edit the progress comment.
	activeCommentState := state.Comment.Progress
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cutover, err := o.stor.ApplyComments().Get(ctx, o.applyID, state.Comment.Cutover)
	if err != nil {
		o.logError(apply, "observer: failed to check for cutover comment on terminal", "error", err)
	} else if cutover != nil {
		activeCommentState = state.Comment.Cutover
	}

	// Load the operation rows once and reuse them for the ownership decision and
	// both comment renders below, so the terminal path reads apply_operations a
	// single time per callback.
	ops, opsErr := o.stor.ApplyOperations().ListByApply(ctx, o.applyID)
	shardsByTable := o.shardsByTable(ctx, apply, ops)

	if activeCommentState == state.Comment.Cutover {
		// The cutover comment IS the completion comment, so editing it to the
		// summary format is the terminal publish — there is no separate summary
		// comment to duplicate. Always edit it to its terminal rendering so it
		// never stays frozen at "cutting over"; for a multi-operation apply the
		// aggregate CAS-winner observer re-edits it with the full task set. The
		// summary marker is always upserted so FindMissingSummaryComment (outbox
		// query) doesn't false-positive on restart for cutover applies.
		finalBody := o.summaryCommentFromOps(apply, ops, opsErr, tasks, shardsByTable)
		o.editTrackedComment(apply, activeCommentState, finalBody)
		o.markSummaryPosted(apply, activeCommentState)
	} else {
		// Edit the progress comment to its final state (completed bars / error).
		// This is the per-operation status freeze, not the apply-level summary, so
		// it always runs; the aggregate publisher re-edits it with the full task
		// set for multi-operation applies.
		finalBody := o.statusCommentFromOps(apply, ops, opsErr, tasks, shardsByTable)
		o.editTrackedComment(apply, activeCommentState, finalBody)

		// Post a separate summary comment. A new comment is more reliable than
		// an edit — GitHub renders edits with a delay, but new comments appear
		// immediately and trigger notifications for PR subscribers. A
		// multi-operation (fan-out) apply publishes its single apply-level
		// summary through the operator's aggregate CAS-winner observer instead
		// (see publishTerminalSummaryIfWon / NewAggregateTerminalCommentObserver),
		// so a per-driver observer holding one operation's task slice must defer
		// to it rather than post a duplicate, partial summary.
		if o.shouldPublishSeparateSummary(apply, ops, opsErr) {
			summaryBody := o.summaryCommentFromOps(apply, ops, opsErr, tasks, shardsByTable)
			o.postAndTrackComment(apply, state.Comment.Summary, summaryBody)
		}
	}

	// Run terminal hook (e.g., update check runs)
	if !o.leaseStillOwnsObserver(apply, "terminal hook") {
		return
	}
	if o.OnTerminalHook != nil {
		o.OnTerminalHook(apply)
	}
}

// shouldPublishSeparateSummary reports whether this observer owns the separate
// apply-level terminal summary comment for a non-cutover apply, given the apply's
// already-loaded operation rows. The aggregate CAS-winner observer (built by
// NewAggregateTerminalCommentObserver after winning the non-terminal→terminal
// projection CAS) always owns it. A per-driver observer owns it only for a
// single-operation apply: a multi-operation apply has its summary published once
// by the aggregate observer, which re-derives the parent from every
// apply_operation and renders the full task set, so a per-driver observer here —
// holding one operation's task slice — must defer to it rather than post a
// duplicate, partial summary. On a load failure it returns false so no partial
// or duplicate summary is posted; startup reconciliation
// (FindMissingSummaryComment) repairs a genuinely missing one.
func (o *CommentObserver) shouldPublishSeparateSummary(apply *storage.Apply, ops []*storage.ApplyOperation, opsErr error) bool {
	if o.aggregateTerminalCASWinner {
		return true
	}
	if opsErr != nil {
		o.logError(apply, "observer: failed to load apply operations for terminal summary ownership; leaving summary to reconciliation",
			"error", opsErr)
		return false
	}
	if len(ops) > 1 {
		o.logInfo(apply, "observer: deferring terminal summary to aggregate publisher for multi-operation apply",
			"operation_count", len(ops))
		return false
	}
	return true
}

// formatStatusComment renders the apply's progress/cutover status comment,
// choosing the single- or multi-deployment layout by the apply's operation-row
// count via formatApplyStatusComment. It loads the operation rows (as returned
// by ListByApply) so a multi-deployment apply renders the aggregated comment;
// a single operation (every apply today, until the fan-out lands) renders the
// single-deployment layout byte-for-byte. A load failure falls back to the
// single-deployment layout so a transient storage error never blocks a comment
// update.
func (o *CommentObserver) formatStatusComment(apply *storage.Apply, tasks []*storage.Task) string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ops, err := o.stor.ApplyOperations().ListByApply(ctx, o.applyID)
	return o.statusCommentFromOps(apply, ops, err, tasks, o.shardsByTable(ctx, apply, ops))
}

// shardsByTable loads an apply's per-shard detail rows and groups them by table
// for the compact per-shard summary in the PR comment. Only sharded engines
// write these rows; MySQL never does, so it is skipped and any other engine is
// queried, returning an empty map (and so no shard summary) when an apply has no
// shard rows. Best-effort: a failed load for one operation just omits its shards
// from this render rather than failing the comment.
func (o *CommentObserver) shardsByTable(ctx context.Context, apply *storage.Apply, ops []*storage.ApplyOperation) map[string][]*storage.Task {
	if apply == nil || apply.DatabaseType == storage.DatabaseTypeMySQL {
		return nil
	}
	byTable := map[string][]*storage.Task{}
	for _, op := range ops {
		if err := ctx.Err(); err != nil {
			o.logError(apply, "comment per-shard summary will omit remaining operations' shards: context done", "error", err)
			break
		}
		opID := op.ID
		shardTasks, err := o.stor.Tasks().GetShardProgressByApplyOperationID(ctx, opID)
		if err != nil {
			o.logError(apply, "comment per-shard summary will omit an operation's shards: failed to load shard rows", "apply_operation_id", opID, "error", err)
			continue
		}
		for _, st := range shardTasks {
			key := shardCommentTableKey(&opID, st.Namespace, st.TableName)
			byTable[key] = append(byTable[key], st)
		}
	}
	return byTable
}

// statusCommentFromOps renders the status comment from already-loaded operation
// rows, applying the same single-deployment fallback as formatStatusComment when
// the load failed. Callers that already hold the operation set (e.g. OnTerminal)
// use this to avoid re-reading apply_operations.
func (o *CommentObserver) statusCommentFromOps(apply *storage.Apply, ops []*storage.ApplyOperation, opsErr error, tasks []*storage.Task, shardsByTable map[string][]*storage.Task) string {
	if opsErr != nil {
		o.logger.Error("observer: failed to load apply operations for comment dispatch; rendering single-deployment layout",
			"apply_id", o.applyID, "error", opsErr)
		return formatProgressComment(apply, tasks, shardsByTable)
	}
	return formatApplyStatusComment(apply, ops, tasks, o.resolveDisplay(apply, ops), shardsByTable)
}

// resolveDisplay projects the apply's per-operation engine display state (VSchema
// status + deploy-request URL) for comment rendering. It uses a short, independent
// deadline so a slow storage read degrades to a comment without these fields
// rather than blocking the update.
func (o *CommentObserver) resolveDisplay(apply *storage.Apply, ops []*storage.ApplyOperation) map[int64]operationDisplay {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return resolveDisplayByOperation(ctx, o.stor, apply, ops)
}

// formatTerminalSummaryComment renders the apply's terminal summary comment,
// choosing the single- or multi-deployment layout by the apply's operation-row
// count via formatApplySummaryComment. It loads the operation rows (as returned
// by ListByApply) so a multi-deployment apply renders the aggregated summary;
// a single operation (every apply today, until the fan-out lands) renders the
// single-deployment summary byte-for-byte. A load failure falls back to the
// single-deployment summary so a transient storage error never blocks the
// terminal comment.
func (o *CommentObserver) formatTerminalSummaryComment(apply *storage.Apply) string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ops, err := o.stor.ApplyOperations().ListByApply(ctx, o.applyID)
	return o.summaryCommentFromOps(apply, ops, err, nil, o.shardsByTable(ctx, apply, ops))
}

// summaryCommentFromOps renders the terminal summary from already-loaded
// operation rows, applying the same single-deployment fallback as
// formatTerminalSummaryComment when the load failed. Callers that already hold
// the operation set (e.g. OnTerminal) use this to avoid re-reading
// apply_operations.
func (o *CommentObserver) summaryCommentFromOps(apply *storage.Apply, ops []*storage.ApplyOperation, opsErr error, tasks []*storage.Task, shardsByTable map[string][]*storage.Task) string {
	if opsErr != nil {
		o.logger.Error("observer: failed to load apply operations for summary comment dispatch; rendering single-deployment layout",
			"apply_id", o.applyID, "error", opsErr)
		return formatSummaryComment(apply, tasks, shardsByTable)
	}
	return formatApplySummaryComment(apply, ops, tasks, o.resolveDisplay(apply, ops), shardsByTable)
}

func (o *CommentObserver) shouldDeferCutover(apply *storage.Apply) bool {
	return o.deferCutover || apply.GetOptions().DeferCutover
}

func (o *CommentObserver) leaseStillOwnsObserver(apply *storage.Apply, operation string) bool {
	// The aggregate terminal observer is invoked by the operator that already won
	// the non-terminal→terminal projection CAS. That driver holds the operation
	// lease, not the parent apply lease, so the per-driver apply-lease authority
	// does not apply here — the won CAS is the authority for this one publish.
	if o.aggregateTerminalCASWinner {
		return true
	}
	// PR apply observers are created before the durable apply row is claimed, so
	// they may not have a lease at construction time. Once progress callbacks pass
	// the claimed apply, fall back to the apply's current lease and use it as the
	// authority for external GitHub writes.
	lease := o.applyLease
	if !lease.Valid() && apply != nil {
		lease = apply.Lease()
	}
	if !lease.Valid() {
		o.logError(apply, "observer: apply lease unavailable; skipping GitHub side effect",
			"operation", operation)
		return false
	}

	// GitHub comments and check updates are side effects outside MySQL's
	// transaction boundary. Re-check the apply lease immediately before each
	// side effect so a stale driver cannot publish progress, terminal comments,
	// or check updates after a newer operator owner has claimed the apply.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := o.stor.Applies().CheckLease(ctx, lease); err != nil {
		o.logError(apply, "observer: apply lease no longer owns apply; skipping GitHub side effect",
			"operation", operation,
			"lease_owner", lease.Owner,
			"error", err)
		return false
	}
	return true
}

func (o *CommentObserver) contextWithApplyLease(ctx context.Context, apply *storage.Apply) context.Context {
	// The aggregate terminal observer holds the operation lease, not the parent
	// apply lease. Attaching an apply lease it does not hold would make every
	// comment-recording write fail closed. Pass the context through unchanged so
	// these writes take storage's no-apply-lease path; the won projection CAS,
	// not an apply lease, authorizes this one terminal publish.
	if o.aggregateTerminalCASWinner {
		return ctx
	}
	// Storage writes that record GitHub side effects must use the same lease as
	// the observer-side lease checks above. Attach the resolved lease even if it
	// is invalid so storage fails closed instead of performing an unleased write.
	lease := o.applyLease
	if !lease.Valid() && apply != nil {
		lease = apply.Lease()
	}
	return storage.WithApplyLease(ctx, lease)
}

// editTrackedComment looks up a stored comment ID and edits it.
func (o *CommentObserver) editTrackedComment(apply *storage.Apply, commentState string, body string) {
	if !o.leaseStillOwnsObserver(apply, "lookup comment before edit") {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	comment, err := o.stor.ApplyComments().Get(ctx, o.applyID, commentState)
	if err != nil {
		o.logError(apply, "observer: failed to look up comment for edit", "error", err, "comment_state", commentState)
		return
	}
	if comment == nil {
		// No tracked comment for this state — nothing to edit.
		// This is expected when the progress comment hasn't been posted yet
		// (e.g., first OnProgress tick before the handler posts it).
		return
	}
	if !o.leaseStillOwnsObserver(apply, "create GitHub client before edit") {
		return
	}

	client, err := o.ghClient.ForInstallation(o.installationID)
	if err != nil {
		o.logError(apply, "observer: failed to create GitHub client", "error", err)
		return
	}
	if !o.leaseStillOwnsObserver(apply, "edit GitHub comment") {
		return
	}

	if err := client.EditIssueComment(ctx, o.repo, comment.GitHubCommentID, o.renderPRComment(body)); err != nil {
		o.logError(apply, "observer: failed to edit comment", "error", err, "comment_state", commentState)
		return
	}
	if !o.leaseStillOwnsObserver(apply, "record edited GitHub comment") {
		return
	}

	// Track the edit for audit/debugging
	if err := o.stor.ApplyComments().IncrementEditCount(o.contextWithApplyLease(ctx, apply), o.applyID, commentState); err != nil {
		o.logError(apply, "observer: failed to increment edit count", "error", err, "comment_state", commentState)
	}
}

// rotateProgressCommentForResume posts a fresh progress comment when a resumed
// apply is detected, so the resumed row copy is tracked in a new comment instead
// of re-editing the comment frozen at "Stopped". The signal is durable and
// cross-pod safe: OnTerminal writes a summary comment when an apply stops, so an
// active apply with a summary comment present has resumed. On rotation it posts a
// new progress comment — postAndTrackComment overwrites the tracked progress
// comment id, so later progress edits land on the new comment while the prior one
// stays frozen as the record — and consumes the summary marker so it rotates
// exactly once and the eventual terminal summary is posted fresh. Returns true
// when it rotated.
func (o *CommentObserver) rotateProgressCommentForResume(apply *storage.Apply, tasks []*storage.Task) bool {
	if o.resumeRotated {
		// This observer already rotated for the current resume. Guard against
		// re-rotating (and posting duplicate fresh comments) on later ticks if the
		// summary-marker delete below failed to land. A fresh observer on a later
		// drive claim starts with this unset and rotates once more, bounding any
		// duplicate to one per drive rather than one per tick.
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	summary, err := o.stor.ApplyComments().Get(ctx, o.applyID, state.Comment.Summary)
	if err != nil {
		o.logError(apply, "observer: failed to check for summary comment before resume rotation", "error", err)
		return false
	}
	if summary == nil || summary.SupersededAt != nil {
		// No active summary comment — either the apply has not been stopped, or a
		// prior resume already consumed the marker. Nothing to rotate. This is the
		// common path on every progress tick.
		return false
	}

	body := o.formatStatusComment(apply, tasks)
	o.postAndTrackComment(apply, state.Comment.Progress, body)
	o.resumeRotated = true

	if err := o.stor.ApplyComments().Supersede(o.contextWithApplyLease(ctx, apply), o.applyID, state.Comment.Summary); err != nil {
		o.logError(apply, "observer: failed to consume summary marker after resume rotation", "error", err)
		return true
	}
	o.logger.Info("observer: posted fresh progress comment for resumed apply",
		"apply_id", o.applyID, "repo", o.repo, "pr", o.pr, "state", apply.State)
	return true
}

// postAndTrackComment creates a comment and stores its ID.
func (o *CommentObserver) postAndTrackComment(apply *storage.Apply, commentState string, body string) {
	if !o.leaseStillOwnsObserver(apply, "create GitHub client before post") {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := o.ghClient.ForInstallation(o.installationID)
	if err != nil {
		o.logError(apply, "observer: failed to create GitHub client", "error", err)
		return
	}
	if !o.leaseStillOwnsObserver(apply, "post GitHub comment") {
		return
	}

	commentID, err := client.CreateIssueComment(ctx, o.repo, o.pr, o.renderPRComment(body))
	if err != nil {
		o.logError(apply, "observer: failed to post comment", "error", err, "comment_state", commentState)
		return
	}
	if !o.leaseStillOwnsObserver(apply, "record posted GitHub comment") {
		return
	}

	comment := &storage.ApplyComment{
		ApplyID:         o.applyID,
		CommentState:    commentState,
		GitHubCommentID: commentID,
	}
	if err := o.stor.ApplyComments().Upsert(o.contextWithApplyLease(ctx, apply), comment); err != nil {
		o.logError(apply, "observer: failed to store comment ID", "error", err, "comment_state", commentState)
	}
}

func (o *CommentObserver) renderPRComment(body string) string {
	return appendSupportChannelFooter(body, o.supportChannel)
}

// markSummaryPosted upserts a summary marker record in apply_comments.
// Used for cutover applies where the cutover comment serves as the summary —
// no separate summary is posted, but the marker satisfies the
// FindMissingSummaryComment outbox query.
func (o *CommentObserver) markSummaryPosted(apply *storage.Apply, editedCommentState string) {
	if !o.leaseStillOwnsObserver(apply, "lookup comment before summary marker") {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	edited, err := o.stor.ApplyComments().Get(ctx, o.applyID, editedCommentState)
	if err != nil {
		o.logError(apply, "observer: failed to look up comment for summary marker", "error", err, "comment_state", editedCommentState)
		return
	}
	if edited == nil {
		// The edited comment doesn't exist in storage — can't create a marker
		// without a GitHub comment ID to reference.
		o.logError(apply, "observer: no comment found to create summary marker from",
			"comment_state", editedCommentState)
		return
	}

	marker := &storage.ApplyComment{
		ApplyID:         o.applyID,
		CommentState:    state.Comment.Summary,
		GitHubCommentID: edited.GitHubCommentID,
	}
	if !o.leaseStillOwnsObserver(apply, "record summary marker") {
		return
	}
	if err := o.stor.ApplyComments().Upsert(o.contextWithApplyLease(ctx, apply), marker); err != nil {
		o.logError(apply, "observer: failed to upsert summary marker", "error", err, "comment_state", state.Comment.Summary)
	}
}
