package webhook

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/clock"
	"github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
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
	tenant         string
	logger         interface {
		Debug(msg string, args ...any)
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

	// authorityOwner identifies this process to the durable progress-comment
	// authority claim (see ClaimProgressCommentAuthority). It is
	// process-scoped, not observer-instance-scoped, so a replacement observer
	// in the same process (e.g. after a re-registration) takes the authority
	// over immediately instead of waiting out its predecessor's staleness
	// window, while observers on other pods still hand over only through the
	// claim.
	authorityOwner string

	mu                sync.Mutex
	lastProgressPost  time.Time
	lastState         string
	lastRowsCopied    int64
	stagnantTicks     int
	hasCutoverComment bool
	resumeRotated     bool

	// resumeSupersedePending marks that this observer's resume rotation posted
	// its fresh comment but failed to consume the summary marker. The marker is
	// the durable "summary owed" signal: left active with a posted comment ID,
	// the next terminal publish would lose both claim legs and the completion
	// summary would never post. Later ticks retry only the supersede until it
	// lands.
	resumeSupersedePending bool

	// rotatedPhases records the control phases this observer already rotated
	// for (or confirmed the tracked comment postdates), keyed by phase name,
	// so later ticks skip the storage re-read. Guarded by mu like the other
	// per-drive rotation flags.
	rotatedPhases map[string]bool

	// cutoverRotated marks that this observer already rotated in a fresh
	// progress comment after a deferred cutover completed (or confirmed the
	// tracked comment postdates the cutover), so later ticks skip the storage
	// re-read.
	cutoverRotated bool

	// pendingRotation remembers a fresh progress comment that is live on the PR
	// but whose tracking write failed, so later ticks adopt it (retry the write
	// with the known comment ID) instead of posting a duplicate.
	pendingRotation *pendingProgressRotation

	// authorityMu guards the per-callback memo of the durable progress-comment
	// authority decision below. It is separate from mu because OnProgress holds
	// mu for its entire tick while OnTerminal runs without it, and the gate is
	// reached from inside both.
	authorityMu sync.Mutex
	// authorityDecided marks that the authority decision was already made
	// during the current observer callback, so the gate's later invocations on
	// the same callback reuse authorityHeld instead of re-reading storage and
	// re-writing the claim row. Each callback starts with a fresh decision.
	authorityDecided bool
	authorityHeld    bool
}

// pendingProgressRotation identifies a rotation progress comment that was
// posted but not tracked: the comment exists on the PR while the stored row
// still points at its predecessor. Adoption retries only the tracking write —
// never another post — so duplicates stay bounded.
type pendingProgressRotation struct {
	// commentID is the posted comment's GitHub ID.
	commentID int64
	// phase is the control phase the comment was posted in (empty when none),
	// recorded on the tracked row at adoption so the durable rotation signal
	// survives the retried write.
	phase string
	// reason selects the frozen rendering the superseded comment is owed.
	reason supersededProgressReason
	// supersededCommentID is the prior tracked comment, still owed its freeze.
	supersededCommentID int64
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

	// Tenant is the deployment's tenant identity, carried into every pasteable
	// command hint the observer's comments render. Empty on single-tenant
	// deployments.
	Tenant string

	Logger interface {
		Debug(msg string, args ...any)
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
		tenant:         cfg.Tenant,
		logger:         cfg.Logger,
		OnTerminalHook: cfg.OnTerminalHook,
		clock:          clk,
		authorityOwner: storage.LeaseOwnerProcess() + "/comment-observer",
	}
}

// NewAggregateTerminalCommentObserver builds a one-shot observer for the
// operator to publish an apply's single terminal summary after it won the
// aggregate projection compare-and-swap. The CAS win — not a parent apply
// lease — is the authority for the comment edits, so this observer bypasses
// the per-driver apply-lease checks; the separate summary post is additionally
// serialized by the summary-marker claim against any still-live per-driver
// observer. Only OnTerminal is meant to be called on it.
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
	o.resetProgressCommentAuthorityDecision()
	if !o.leaseStillOwnsObserver(apply, "progress") {
		return
	}

	now := o.clock.Now()
	currentState := apply.State

	// Check if a cutover comment was posted by an external handler. A
	// superseded row is an already-rotated-away prompt from an earlier drive,
	// not a live cutover comment — treating it as live would re-mute the
	// observer. This must happen before the deferred-cutover-gate branch below
	// — it is the restart-time dedup: an apply can park at the gate for days,
	// and without the durable-row check every fresh observer on a restart or
	// re-claimed drive would post a duplicate prompt.
	if !o.hasCutoverComment {
		checkCtx, checkCancel := context.WithTimeout(context.Background(), 2*time.Second)
		cutover, err := o.stor.ApplyComments().Get(checkCtx, o.applyID, state.Comment.Cutover)
		if err != nil {
			o.logError(apply, "observer: failed to check for cutover comment", "error", err)
		} else if cutover != nil && cutover.SupersededAt == nil {
			if o.cutoverRotated {
				// This observer already rotated away from the prompt, so the row
				// can only still read live because its supersede write failed.
				// Retry consuming it and keep editing the fresh comment instead
				// of re-muting on the spent prompt.
				o.logInfo(apply, "observer: cutover comment still live after rotation; retrying supersede")
				o.supersedeCutoverComment(checkCtx, apply)
			} else {
				o.hasCutoverComment = true
			}
		}
		checkCancel()
	}

	// A prior tick's rotation comment may be live on the PR but untracked.
	// Adopt it before considering any rotation — including the post-cutover
	// one below — so the tracking write is retried with the known comment ID
	// instead of a rotation reading the stale tracked row and posting a
	// duplicate, or a later adoption overwriting the row a rotation just
	// wrote. While adoption keeps failing, rotations stay blocked but the
	// tick's normal progress edit below still lands on the prior comment —
	// the tracked one.
	adopted := o.pendingRotation == nil || o.adoptPendingRotationComment(apply)

	// Post the cutover prompt when the apply is at the deferred-cutover gate,
	// but only if one hasn't been posted already. A failed post leaves
	// hasCutoverComment unset so the next tick retries, instead of muting the
	// observer for the rest of the drive over one transient GitHub error.
	if atDeferredCutoverGate(currentState) && o.shouldDeferCutover(apply) && !o.hasCutoverComment {
		body := o.formatStatusComment(apply, tasks)
		if _, posted, _ := o.postAndTrackComment(apply, state.Comment.Cutover, body, nil); posted {
			o.hasCutoverComment = true
		}
		o.lastState = currentState
		return
	}

	// While the cutover prompt is live, stop editing — the prompt is the
	// active comment and the progress comment stays frozen. Rotate a fresh
	// progress comment in only when the apply has provably moved past the
	// cutover gate into a post-cutover phase (revert window, reverting,
	// skipping revert): the prompt's call to action is spent, and the operator
	// looks at the bottom of the PR for the effect of the cutover command.
	// Terminal states are left to OnTerminal, which completes the cutover
	// comment itself. Every other state keeps the observer muted — a restart
	// recovery re-copying a parked apply, a retryable failure at the gate, a
	// stop — because no cutover has happened, so a rotation would fold the
	// unanswered prompt under a false "Cutover complete" record.
	if o.hasCutoverComment {
		if !postCutoverPhase(controlPhase(apply.State)) {
			return
		}
		if adopted && o.rotateProgressCommentAfterCutover(apply, tasks) {
			o.lastState = currentState
			o.lastProgressPost = now
			o.stagnantTicks = 0
		}
		return
	}

	if adopted {
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

		// A control operation taking effect (revert, skip-revert) likewise gets a
		// fresh progress comment: the user just issued the command, so its effect
		// belongs at the bottom of the PR timeline, with the old comment frozen at
		// its pre-operation state as the record.
		if o.rotateProgressCommentForControlPhase(apply, tasks) {
			o.lastState = currentState
			o.lastProgressPost = now
			o.stagnantTicks = 0
			return
		}
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
	o.resetProgressCommentAuthorityDecision()
	if !o.leaseStillOwnsObserver(apply, "terminal") {
		return
	}
	// A rotation's fresh comment may be live on the PR but still untracked —
	// the tracking write failed and no later progress tick landed the
	// adoption. This is the last chance to adopt it: on success the terminal
	// rendering below edits the fresh comment and its predecessor is frozen.
	// If the adoption still fails, the terminal rendering lands on the tracked
	// (prior) comment and the fresh comment stays at its last progress
	// rendering on the PR.
	if o.pendingRotation != nil && !o.adoptPendingRotationComment(apply) {
		o.logError(apply, "observer: posted-but-untracked rotation comment could not be adopted before the terminal publish; it stays at its last progress rendering",
			"github_comment_id", o.pendingRotation.commentID)
	}
	// Determine which comment to edit to final state.
	// If a cutover comment exists, edit that and leave the progress comment
	// frozen at its last state. Otherwise edit the progress comment.
	activeCommentState := state.Comment.Progress
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// A superseded cutover row was already rotated away into a fold — the
	// tracked progress comment is the active one, not the spent prompt.
	cutover, err := o.stor.ApplyComments().Get(ctx, o.applyID, state.Comment.Cutover)
	if err != nil {
		o.logError(apply, "observer: failed to check for cutover comment on terminal", "error", err)
	} else if cutover != nil && cutover.SupersededAt == nil {
		activeCommentState = state.Comment.Cutover
		// A live row whose tracked progress comment records a post-cutover
		// phase is a spent prompt whose supersede write failed: the rotation
		// already happened and the fresh progress comment is the active one.
		// Consume the stale row and complete the fresh comment instead of
		// overwriting the folded prompt with the terminal summary — this
		// durable check covers a fresh observer whose only call is OnTerminal.
		// On a tracked-row read failure the prompt stays the active comment,
		// the same routing a genuinely died-at-the-gate apply gets.
		tracked, terr := o.stor.ApplyComments().Get(ctx, o.applyID, state.Comment.Progress)
		if terr != nil {
			o.logError(apply, "observer: failed to load tracked progress comment on terminal; completing the cutover comment", "error", terr)
		} else if tracked != nil && postCutoverPhase(trackedPhase(tracked)) {
			o.supersedeCutoverComment(ctx, apply)
			activeCommentState = state.Comment.Progress
		}
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
		finalBody := o.summaryCommentFromOps(ctx, apply, ops, opsErr, tasks, shardsByTable)
		o.editTrackedComment(apply, activeCommentState, finalBody)
		o.markSummaryPosted(apply, activeCommentState)
		// The prompt is the completion comment, but the tracked progress
		// comment still gets its final per-operation status freeze — it last
		// rendered the cutover gate and its pasteable cutover command, and must
		// not keep advertising a spent gate as live.
		o.finalizeTrackedProgressCommentAtTerminal(apply, cutover.GitHubCommentID, ops, opsErr, tasks, shardsByTable)
		if state.IsState(apply.State, state.Apply.Stopped) {
			// Stopped is the only terminal state that returns to active. The
			// prompt now carries the stop record, so consume its row —
			// otherwise a resumed drive finds a live prompt row and mutes
			// behind the stop record instead of tracking the resumed copy in
			// the fresh comment the resume rotation posts.
			o.supersedeCutoverComment(ctx, apply)
		}
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
			summaryBody := o.summaryCommentFromOps(ctx, apply, ops, opsErr, tasks, shardsByTable)
			o.publishClaimedSummary(apply, summaryBody)
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

// finalizeTrackedProgressCommentAtTerminal edits the tracked progress comment
// to its final per-operation status rendering when the cutover prompt is the
// completion comment. The prompt carries the terminal summary, and the
// progress comment gets the same status freeze the non-cutover terminal path
// writes, so no comment on the timeline keeps rendering the cutover gate as
// live. It runs for every terminal state, including stopped — a stopped
// rendering is a correct record, and the resume rotation posts the fresh
// comment when the apply returns to active. A superseded row was already
// folded into a details block pointing at its successor and must not be
// unfolded by an edit; a row tracking the prompt's own GitHub comment already
// carries the terminal summary and must not be edited over it.
func (o *CommentObserver) finalizeTrackedProgressCommentAtTerminal(apply *storage.Apply, promptCommentID int64, ops []*storage.ApplyOperation, opsErr error, tasks []*storage.Task, shardsByTable map[string][]*storage.Task) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tracked, err := o.stor.ApplyComments().Get(ctx, o.applyID, state.Comment.Progress)
	if err != nil {
		o.logError(apply, "observer: failed to load tracked progress comment for its terminal status freeze; it stays at its last progress rendering", "error", err)
		return
	}
	if tracked == nil {
		// Nothing renders the gate, so there is nothing to freeze.
		o.logInfo(apply, "observer: no tracked progress comment to freeze at terminal")
		return
	}
	if tracked.SupersededAt != nil {
		o.logInfo(apply, "observer: tracked progress comment already superseded; skipping its terminal status freeze")
		return
	}
	if tracked.GitHubCommentID == promptCommentID {
		o.logInfo(apply, "observer: tracked progress comment is the cutover prompt; the terminal summary already covers it")
		return
	}
	finalBody := o.statusCommentFromOps(apply, ops, opsErr, tasks, shardsByTable)
	o.editTrackedComment(apply, state.Comment.Progress, finalBody)
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
	var body string
	if opsErr != nil {
		o.logger.Error("observer: failed to load apply operations for comment dispatch; rendering single-deployment layout",
			"apply_id", o.applyID, "error", opsErr)
		body = formatProgressComment(apply, tasks, shardsByTable, o.tenant)
	} else {
		body = formatApplyStatusComment(apply, ops, o.resolveReleased(apply, ops), tasks, o.resolveDisplay(apply, ops), shardsByTable, o.resolveVSchemaDiffs(apply, ops), o.tenant)
	}
	return body + controlRejectionSection(context.Background(), o.stor, o.logger, apply, body)
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

// resolveReleased reports whether the apply's paused rollout has been released
// open, for comment rendering. It uses a short, independent deadline so a slow
// storage read degrades to an unreleased (paused) render rather than blocking
// the update.
func (o *CommentObserver) resolveReleased(apply *storage.Apply, ops []*storage.ApplyOperation) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return releasedForApply(ctx, o.stor, apply, ops, o.logger)
}

// resolveVSchemaDiffs loads the stored plan's per-namespace VSchema diffs for
// a sharded apply's comment rendering. It uses a short, independent deadline
// so a slow storage read degrades to a comment without diffs rather than
// blocking the update.
func (o *CommentObserver) resolveVSchemaDiffs(apply *storage.Apply, ops []*storage.ApplyOperation) map[string]string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return resolveShardedVSchemaDiffs(ctx, o.stor, apply, ops)
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
	return o.summaryCommentFromOps(ctx, apply, ops, err, nil, o.shardsByTable(ctx, apply, ops))
}

// summaryCommentFromOps renders the terminal summary from already-loaded
// operation rows, applying the same single-deployment fallback as
// formatTerminalSummaryComment when the load failed. Callers that already hold
// the operation set (e.g. OnTerminal) use this to avoid re-reading
// apply_operations. A failed apply's summary carries the collapsed recent-logs
// section, appended after whichever layout rendered, so triage data lands on
// the PR without an extra operator step.
func (o *CommentObserver) summaryCommentFromOps(ctx context.Context, apply *storage.Apply, ops []*storage.ApplyOperation, opsErr error, tasks []*storage.Task, shardsByTable map[string][]*storage.Task) string {
	var body string
	if opsErr != nil {
		o.logger.Error("observer: failed to load apply operations for summary comment dispatch; rendering single-deployment layout",
			"apply_id", o.applyID, "error", opsErr)
		body = formatSummaryComment(apply, tasks, shardsByTable, o.tenant)
	} else {
		body = formatApplySummaryComment(apply, ops, o.resolveReleased(apply, ops), tasks, o.resolveDisplay(apply, ops), shardsByTable, o.resolveVSchemaDiffs(apply, ops), o.tenant)
	}
	body += controlRejectionSection(ctx, o.stor, o.logger, apply, body)
	return body + failureLogsSection(ctx, o.stor, o.logger, apply, body)
}

func (o *CommentObserver) shouldDeferCutover(apply *storage.Apply) bool {
	return o.deferCutover || apply.GetOptions().DeferCutover
}

// atDeferredCutoverGate reports whether the apply is at the deferred-cutover
// gate, where the cutover prompt comment belongs. waiting_for_cutover is the
// parked state the apply holds until the operator's cutover command;
// cutting_over is the transient window while the cutover executes. A tick can
// first observe the apply in either state, so the prompt posts from both.
func atDeferredCutoverGate(applyState string) bool {
	return state.IsState(applyState, state.Apply.WaitingForCutover, state.Apply.CuttingOver)
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
		// No parent apply lease exists anywhere — not on this observer and not
		// on the apply row. For an apply whose work runs under operation
		// leases, that is the normal shape between dispatch waves, so the
		// durable progress-comment authority decides instead of the lease.
		return o.progressCommentAuthorityOwnsObserver(apply, operation)
	}

	// GitHub comments and check updates are side effects outside MySQL's
	// transaction boundary. Re-check the apply lease immediately before each
	// side effect so a stale driver cannot publish progress, terminal comments,
	// or check updates after a newer operator owner has claimed the apply.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := o.stor.Applies().CheckLease(ctx, lease); err != nil {
		if apply != nil && !apply.Lease().Valid() {
			// This observer's construction-time lease no longer matches, and
			// the apply row records no parent lease at all — the lease was
			// released back (an operation-scoped drive holds the parent only
			// transiently per dispatch wave), not claimed by a newer owner.
			// The durable progress-comment authority decides instead.
			return o.progressCommentAuthorityOwnsObserver(apply, operation)
		}
		o.logError(apply, "observer: apply lease no longer owns apply; skipping GitHub side effect",
			"operation", operation,
			"lease_owner", lease.Owner,
			"error", err)
		return false
	}
	return true
}

// progressCommentAuthorityOwnsObserver reports whether this observer may
// perform a GitHub side effect for an apply whose parent apply lease is
// legitimately unheld. An apply whose operations are dispatched under
// operation leases holds the parent lease only transiently per dispatch wave,
// so the progress comment would otherwise go silent for the whole rollout and
// operators would read a live apply as dead. The authority granted here is
// durable and cross-pod safe: a compare-and-swap ownership recorded on the
// tracked progress comment row (see ClaimProgressCommentAuthority), so among
// authority-path observers at most one at a time edits the comment and a
// crashed holder hands over only after its heartbeat goes stale. A
// lease-admitted observer is governed by the lease checks instead and never
// touches the recorded authority. It is granted only while operation-scoped
// work is in flight — an apply that holds (or should hold) a parent lease
// stays governed by the lease checks.
//
// The decision is made once per observer callback against freshly read
// storage rows — including a re-read of the parent lease columns, so a
// dispatch wave that re-claimed the parent since the poller's snapshot denies
// the authority — then reused by the callback's remaining side-effect checks.
// Claiming once per callback also renews the holder's heartbeat well inside
// its staleness window.
func (o *CommentObserver) progressCommentAuthorityOwnsObserver(apply *storage.Apply, operation string) bool {
	if apply == nil {
		o.logError(apply, "observer: apply lease unavailable and no apply loaded to resolve progress-comment authority; skipping GitHub side effect",
			"operation", operation)
		return false
	}
	o.authorityMu.Lock()
	if o.authorityDecided {
		held := o.authorityHeld
		o.authorityMu.Unlock()
		if !held {
			o.logger.Debug("observer: progress-comment authority already denied this callback; skipping GitHub side effect",
				append(apply.LogAttrs(), "operation", operation, "authority_owner", o.authorityOwner)...)
		}
		return held
	}
	o.authorityMu.Unlock()

	held := o.decideProgressCommentAuthority(apply, operation)
	o.authorityMu.Lock()
	o.authorityDecided, o.authorityHeld = true, held
	o.authorityMu.Unlock()
	return held
}

// resetProgressCommentAuthorityDecision discards the previous callback's
// authority decision so the next gate invocation decides afresh.
func (o *CommentObserver) resetProgressCommentAuthorityDecision() {
	o.authorityMu.Lock()
	o.authorityDecided = false
	o.authorityHeld = false
	o.authorityMu.Unlock()
}

// decideProgressCommentAuthority performs the storage reads and the claim
// behind progressCommentAuthorityOwnsObserver, in fail-closed order: no
// operation-scoped work in flight denies, a fresh parent-lease re-read
// showing a holder or a terminal apply denies, and only then is the durable
// claim attempted.
func (o *CommentObserver) decideProgressCommentAuthority(apply *storage.Apply, operation string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	inFlight, err := o.operationScopedWorkInFlight(ctx, apply)
	if err != nil {
		o.logger.Error("observer: failed to determine whether operation-scoped work is in flight; skipping GitHub side effect",
			append(apply.LogAttrs(), "operation", operation, "error", err)...)
		return false
	}
	if !inFlight {
		o.logger.Error("observer: apply lease unavailable and no operation-scoped work in flight; skipping GitHub side effect",
			append(apply.LogAttrs(), "operation", operation)...)
		return false
	}

	// The apply passed in is the poller's snapshot, up to a tick old. Re-read
	// the row before claiming: a dispatch wave may have re-claimed the parent
	// lease since the snapshot — its holder is governed by the lease checks
	// and owns the comment while the lease lasts — or the projection may have
	// settled the apply terminal, handing the comment to the terminal publish.
	fresh, err := o.stor.Applies().Get(ctx, o.applyID)
	if err != nil {
		o.logger.Error("observer: failed to re-read the apply for the progress-comment authority; skipping GitHub side effect",
			append(apply.LogAttrs(), "operation", operation, "error", err)...)
		return false
	}
	if fresh == nil {
		o.logger.Error("observer: apply row no longer exists for the progress-comment authority; skipping GitHub side effect",
			append(apply.LogAttrs(), "operation", operation)...)
		return false
	}
	if fresh.Lease().Valid() {
		o.logger.Debug("observer: parent apply lease re-claimed since the poller snapshot; the lease holder owns the comment, skipping GitHub side effect",
			append(apply.LogAttrs(), "operation", operation, "lease_owner", fresh.LeaseOwner)...)
		return false
	}
	if state.IsTerminalApplyState(fresh.State) {
		o.logger.Debug("observer: apply settled terminal since the poller snapshot; the terminal publish owns the comment, skipping GitHub side effect",
			append(apply.LogAttrs(), "operation", operation, "fresh_state", fresh.State)...)
		return false
	}

	held, err := o.stor.ApplyComments().ClaimProgressCommentAuthority(ctx, o.applyID, o.authorityOwner)
	if err != nil {
		o.logger.Error("observer: failed to claim progress-comment authority; skipping GitHub side effect",
			append(apply.LogAttrs(), "operation", operation, "authority_owner", o.authorityOwner, "error", err)...)
		return false
	}
	if !held {
		// The claim was not won: either another observer holds a fresh
		// authority (its edits carry the PR), or no tracked progress comment
		// row exists yet to claim (nothing to edit either way). Expected on
		// every peer pod polling the same apply, hence Debug.
		o.logger.Debug("observer: progress-comment authority not won (held by another observer, or no tracked comment row yet); skipping GitHub side effect",
			append(apply.LogAttrs(), "operation", operation, "authority_owner", o.authorityOwner)...)
		return false
	}
	return true
}

// operationScopedWorkInFlight reports whether the apply's schema change work
// is still in flight under operation-scoped dispatch — the one shape whose
// parent apply lease is legitimately unheld mid-apply. True when the apply is
// non-terminal and either its generation manifest still lists keys with no
// attached operation (the dispatcher owes more dispatch waves) or the apply
// has multiple attached operations with a keyed row not yet terminal. A
// single attached operation is deliberately not counted, matching the
// operator's drive-mode split: that shape drives under the parent apply
// lease, so an unheld lease there means no driver and the lease checks stay
// authoritative. (The one single-operation drive that runs under the
// operation lease — a task-less operation — fails closed here too; it has no
// task progress to report and its terminal comment is published by the
// aggregate projection winner.) Whole-deployment operations (empty key) are
// likewise not counted, whatever their number.
func (o *CommentObserver) operationScopedWorkInFlight(ctx context.Context, apply *storage.Apply) (bool, error) {
	if state.IsTerminalApplyState(apply.State) {
		return false, nil
	}
	ops, err := o.stor.ApplyOperations().ListByApply(ctx, o.applyID)
	if err != nil {
		return false, fmt.Errorf("load apply operations for progress-comment authority of apply %s: %w", apply.ApplyIdentifier, err)
	}
	if len(apply.MissingExpectedOperationKeys(ops)) > 0 {
		return true, nil
	}
	if len(ops) <= 1 {
		return false, nil
	}
	for _, op := range ops {
		if op.OperationKey != "" && !state.IsApplyOperationTerminal(op.State) {
			return true, nil
		}
	}
	return false, nil
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
	// An apply row that records no parent lease has no lease to attach: the
	// side-effect gate admitted this write under the progress-comment
	// authority, so it takes storage's no-apply-lease path — the claim, not an
	// apply lease, authorizes it, mirroring the aggregate CAS winner above.
	if apply != nil && !apply.Lease().Valid() {
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
// comment id, so later progress edits land on the new comment while the prior
// one is folded into a details block pointing at its successor (with the freeze
// it owes recorded in the same tracking write) — and consumes the summary
// marker so it rotates exactly once and the eventual terminal summary is
// posted fresh. Returns true when it rotated.
func (o *CommentObserver) rotateProgressCommentForResume(apply *storage.Apply, tasks []*storage.Task) bool {
	if o.resumeRotated {
		// This observer already rotated for the current resume. Guard against
		// re-rotating (and posting duplicate fresh comments) on later ticks if the
		// summary-marker supersede failed to land — retry only the supersede so
		// the marker cannot sit active for the rest of the drive. A fresh
		// observer on a later drive claim starts with this unset and rotates once
		// more, bounding any duplicate to one per drive rather than one per tick.
		if o.resumeSupersedePending {
			o.supersedeResumeSummaryMarker(apply)
		}
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
	if summary.GitHubCommentID == 0 {
		// The marker is a claim sentinel: a terminal-summary publish is in
		// flight. Rotating now would supersede the sentinel mid-publish and
		// transfer the claim, so wait for the marker to record its posted
		// comment before rotating. A crashed publisher's abandoned sentinel is
		// recovered by the stale-claim machinery, not by this rotation.
		o.logInfo(apply, "observer: summary claim sentinel is live; deferring resume rotation until the summary posts")
		return false
	}

	tracked, err := o.stor.ApplyComments().Get(ctx, o.applyID, state.Comment.Progress)
	if err != nil {
		o.logError(apply, "observer: failed to load tracked progress comment before resume rotation", "error", err)
		return false
	}
	var pendingFreeze *int64
	if tracked != nil {
		if tracked.PendingFreezeCommentID != nil {
			// A superseded comment from an earlier rotation is still owed its
			// frozen rendering — the freeze edit failed or the pod died before it
			// landed. Retry it now; on success the marker is cleared.
			o.freezeSupersededProgressComment(apply, *tracked.PendingFreezeCommentID, tracked.GitHubCommentID,
				supersededByPriorRotation)
		}
		pendingFreeze = &tracked.GitHubCommentID
	}

	body := o.formatStatusComment(apply, tasks)
	newCommentID, posted, trackedNew := o.postAndTrackComment(apply, state.Comment.Progress, body, pendingFreeze)
	if !posted {
		// postAndTrackComment logged the failure. The summary marker is left in
		// place — it is the durable signal that a resume rotation is owed — so
		// the next tick retries instead of consuming the marker for a comment
		// that never landed.
		return false
	}
	o.resumeRotated = true

	if trackedNew && tracked != nil {
		o.freezeSupersededProgressComment(apply, tracked.GitHubCommentID, newCommentID, supersededByResume)
	}
	// When the fresh comment posted but its tracking write failed
	// (postAndTrackComment logged it), the prior comment is still the tracked
	// live one, so it must not be folded; no freeze marker was recorded either,
	// since the marker rides the tracking write.

	o.supersedeResumeSummaryMarker(apply)
	o.logger.Info("observer: posted fresh progress comment for resumed apply",
		"apply_id", o.applyID, "repo", o.repo, "pr", o.pr, "state", apply.State)
	return true
}

// supersedeResumeSummaryMarker consumes the summary marker after a resume
// rotation under its own deadline: the rotation's GitHub round-trips can spend
// most of the tick's context budget, and this write is the durable record that
// the rotation happened — left unconsumed, the marker keeps advertising a
// summary that was already rotated away, and the next terminal publish would
// lose both claim legs and never post its summary. On failure the pending flag
// keeps later ticks retrying just this write until it lands.
func (o *CommentObserver) supersedeResumeSummaryMarker(apply *storage.Apply) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := o.stor.ApplyComments().Supersede(o.contextWithApplyLease(ctx, apply), o.applyID, state.Comment.Summary); err != nil {
		o.resumeSupersedePending = true
		o.logError(apply, "observer: failed to consume summary marker after resume rotation; retrying on the next tick", "error", err)
		return
	}
	o.resumeSupersedePending = false
}

// Control-operation phases recorded on tracked progress comments
// (ApplyComment.PostedPhase) and used to decide comment rotation. The phase is
// derived from engine-agnostic apply state so every engine gets the same PR
// UX.
const (
	phaseNone              = ""
	phaseWaitingForCutover = "waiting_for_cutover"
	phaseCuttingOver       = "cutting_over"
	phaseRevertWindow      = "revert_window"
	phaseReverting         = "reverting"
	phaseSkippingRevert    = "skipping_revert"
)

// controlPhase maps an apply state to the control-operation phase recorded on
// progress comments posted in that state. States outside the cutover/revert
// lifecycle carry no phase.
func controlPhase(applyState string) string {
	switch state.NormalizeState(applyState) {
	case state.Apply.WaitingForCutover:
		return phaseWaitingForCutover
	case state.Apply.CuttingOver:
		return phaseCuttingOver
	case state.Apply.RevertWindow:
		return phaseRevertWindow
	case state.Apply.Reverting:
		return phaseReverting
	case state.Apply.SkippingRevert:
		return phaseSkippingRevert
	}
	return phaseNone
}

// phaseRotatesProgressComment reports whether an apply entering the phase can
// be the effect of an operator control command that warrants rotating the
// progress comment. Reverting always is — it has no non-operator entry path.
// Skipping-revert only sometimes is, so it additionally requires the durable
// operator-origin signal (see phaseIsOperatorIssued). The cutover-driven
// phases are not listed here: entering the revert window after an
// auto-cutover involves no operator command (the comment transitions in
// place), and the operator-triggered deferred cutover rotates through
// rotateProgressCommentAfterCutover, keyed on the cutover prompt comment.
func phaseRotatesProgressComment(phase string) bool {
	return phase == phaseReverting || phase == phaseSkippingRevert
}

// phaseIsOperatorIssued reports whether the apply entered the phase as the
// effect of an operator control command — the situation comment rotation
// exists for: an operator watching the bottom of the PR timeline for the
// effect of the command they just issued. Reverting is always
// operator-issued. Skipping-revert is operator-issued only when a durable
// skip-revert control request exists for the apply: the phase is also entered
// when the revert window expires on its own or when the apply was submitted
// with skip-revert chosen upfront, and neither involves a command whose
// effect an operator is waiting to see — the progress comment transitions in
// place.
func (o *CommentObserver) phaseIsOperatorIssued(ctx context.Context, phase string) (bool, error) {
	if phase != phaseSkippingRevert {
		return true, nil
	}
	req, err := o.stor.ControlRequests().GetByOperation(ctx, o.applyID, storage.ControlOperationSkipRevert)
	if err != nil {
		return false, fmt.Errorf("load skip-revert control request: %w", err)
	}
	return req != nil, nil
}

// postCutoverPhase reports whether a recorded phase means the comment was
// posted after the apply moved past the cutover gate — the durable sign that a
// post-cutover rotation already happened.
func postCutoverPhase(phase string) bool {
	return phase == phaseRevertWindow || phase == phaseReverting || phase == phaseSkippingRevert
}

// trackedPhase returns the phase recorded on a tracked comment row, treating
// nil (a row predating phase tracking) as no phase — the comment predates
// whatever phase the apply is in now.
func trackedPhase(tracked *storage.ApplyComment) string {
	if tracked.PostedPhase == nil {
		return phaseNone
	}
	return *tracked.PostedPhase
}

// rotateProgressCommentForControlPhase posts a fresh progress comment when a
// control operation's phase (revert, skip-revert) has taken effect, so the
// operation is tracked in a new comment at the bottom of the PR timeline
// instead of re-editing the comment last rendered for the prior phase. The
// signal is durable and cross-pod safe: every tracked progress comment records
// the phase the apply was in when it was posted, so an apply in a control
// phase whose tracked comment records a different phase still predates it. The
// fresh comment records the phase, so later ticks — and fresh observers on
// later drive claims — see the rotation already happened. Returns true when it
// rotated.
func (o *CommentObserver) rotateProgressCommentForControlPhase(apply *storage.Apply, tasks []*storage.Task) bool {
	phase := controlPhase(apply.State)
	if !phaseRotatesProgressComment(phase) {
		// Not in a phase that rotates — the common path on every progress tick.
		return false
	}
	if o.rotatedPhases[phase] {
		// This observer already rotated for the phase (or confirmed the tracked
		// comment postdates it) — answered from memory instead of re-reading
		// storage on every later tick. A fresh observer on a later drive claim
		// starts with this unset and re-checks the durable record.
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	operatorIssued, err := o.phaseIsOperatorIssued(ctx, phase)
	if err != nil {
		o.logError(apply, "observer: failed to determine whether the control phase was operator-issued; rotation will be re-checked on the next tick",
			"error", err, "phase", phase)
		return false
	}
	if !operatorIssued {
		// The apply entered the phase on its own — no operator command whose
		// effect needs surfacing at the bottom of the PR. The progress comment
		// transitions in place; remembered so later ticks skip the re-check.
		o.markPhaseRotated(phase)
		o.logInfo(apply, "observer: control phase was not operator-issued; progress comment transitions in place", "phase", phase)
		return false
	}

	tracked, err := o.stor.ApplyComments().Get(ctx, o.applyID, state.Comment.Progress)
	if err != nil {
		o.logError(apply, "observer: failed to load tracked progress comment before control-phase rotation",
			"error", err, "phase", phase)
		return false
	}
	if tracked == nil {
		// No tracked progress comment to rotate away from — the handler may
		// still be posting the initial one. The next posted comment records the
		// phase itself.
		o.logInfo(apply, "observer: no tracked progress comment yet; skipping control-phase rotation check", "phase", phase)
		return false
	}
	if tracked.PendingFreezeCommentID != nil {
		// A superseded comment from an earlier rotation is still owed its
		// frozen rendering — the freeze edit failed or the pod died before it
		// landed. Retry it now (even when the tracked comment already records
		// this phase and no new rotation follows); on success the marker is
		// cleared.
		o.freezeSupersededProgressComment(apply, *tracked.PendingFreezeCommentID, tracked.GitHubCommentID,
			supersededByPriorRotation)
	}
	if trackedPhase(tracked) == phase {
		// The tracked comment was posted while the apply was already in this
		// phase — it already tracks the operation, across restarts and drive
		// re-claims.
		o.markPhaseRotated(phase)
		return false
	}

	body := o.formatStatusComment(apply, tasks)
	newCommentID, posted, trackedNew := o.postAndTrackComment(apply, state.Comment.Progress, body, &tracked.GitHubCommentID)
	if !posted {
		// postAndTrackComment logged the failure. The tracked comment still
		// predates the phase, so the next tick retries the rotation.
		return false
	}
	if !trackedNew {
		// The fresh comment is on the PR, but the tracked row still points at
		// the prior comment, which therefore must not be folded. Remember the
		// fresh comment so later ticks retry only the tracking write —
		// adoption — instead of posting a duplicate; the phase is marked
		// rotated once the row tracks it.
		o.pendingRotation = &pendingProgressRotation{
			commentID:           newCommentID,
			phase:               phase,
			reason:              supersededReasonForPhase(phase),
			supersededCommentID: tracked.GitHubCommentID,
		}
		o.logError(apply, "observer: fresh progress comment posted for control phase but not tracked; progress edits continue on the prior comment until the next tick adopts it",
			"phase", phase, "github_comment_id", newCommentID)
		return true
	}
	o.markPhaseRotated(phase)

	o.freezeSupersededProgressComment(apply, tracked.GitHubCommentID, newCommentID, supersededReasonForPhase(phase))

	o.logger.Info("observer: posted fresh progress comment for control phase",
		"apply_id", o.applyID, "repo", o.repo, "pr", o.pr, "state", apply.State, "phase", phase)
	return true
}

// markPhaseRotated records in-memory that this observer rotated (or confirmed
// the tracked comment already covers) the given control phase, so later ticks
// skip the storage re-read.
func (o *CommentObserver) markPhaseRotated(phase string) {
	if o.rotatedPhases == nil {
		o.rotatedPhases = make(map[string]bool)
	}
	o.rotatedPhases[phase] = true
}

// supersededReasonForPhase selects the frozen rendering for the comment a
// control-phase rotation supersedes.
func supersededReasonForPhase(phase string) supersededProgressReason {
	if phase == phaseSkippingRevert {
		return supersededBySkipRevert
	}
	return supersededByRevert
}

// rotateProgressCommentAfterCutover posts a fresh progress comment when a
// deferred cutover has completed into a still-active apply (e.g. the revert
// window): the operator issued the cutover command, so the post-cutover phase
// belongs in a new comment at the bottom of the PR timeline, with the spent
// cutover prompt frozen into a fold pointing at it. The live cutover row is
// the durable marker that this rotation is owed — it is consumed (superseded)
// only after the fresh comment lands, so a crash retries on the next tick, and
// a fresh observer on a later drive claim finds either the superseded row or
// the tracked comment's recorded post-cutover phase and does not rotate again.
// Returns true when it posted the fresh comment.
func (o *CommentObserver) rotateProgressCommentAfterCutover(apply *storage.Apply, tasks []*storage.Task) bool {
	if o.cutoverRotated {
		// This observer already rotated after the cutover (or confirmed the
		// tracked comment postdates it). Guard against re-reading storage on
		// later ticks; a fresh observer on a later drive claim starts unset
		// and re-checks the durable record.
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cutover, err := o.stor.ApplyComments().Get(ctx, o.applyID, state.Comment.Cutover)
	if err != nil {
		o.logError(apply, "observer: failed to load cutover comment before post-cutover rotation", "error", err)
		return false
	}
	if cutover == nil {
		// The prompt posted but its tracking write failed, so its comment ID is
		// unknown and cannot be folded or superseded. Unmute so progress edits
		// resume in the existing tracked comment — degraded (the spent prompt
		// stays unfolded) but not silent.
		o.logError(apply, "observer: cutover comment untracked after cutover completed; resuming progress edits without rotation")
		o.cutoverRotated = true
		o.hasCutoverComment = false
		return false
	}

	tracked, err := o.stor.ApplyComments().Get(ctx, o.applyID, state.Comment.Progress)
	if err != nil {
		o.logError(apply, "observer: failed to load tracked progress comment before post-cutover rotation", "error", err)
		return false
	}

	if cutover.SupersededAt != nil || (tracked != nil && postCutoverPhase(trackedPhase(tracked))) {
		// The rotation already happened — a prior drive (or an earlier tick
		// whose supersede write raced a crash) posted the post-cutover comment.
		// Retry any fold still owed, then resume normal editing.
		if tracked != nil && tracked.PendingFreezeCommentID != nil {
			o.freezeSupersededProgressComment(apply, *tracked.PendingFreezeCommentID, tracked.GitHubCommentID,
				supersededByPriorRotation)
		}
		if cutover.SupersededAt == nil {
			o.supersedeCutoverComment(ctx, apply)
		}
		o.cutoverRotated = true
		o.hasCutoverComment = false
		return false
	}

	if tracked != nil && tracked.PendingFreezeCommentID != nil {
		// A superseded comment from an earlier rotation is still owed its
		// frozen rendering — the freeze edit failed or the pod died before it
		// landed. Retry it now; on success the marker is cleared.
		o.freezeSupersededProgressComment(apply, *tracked.PendingFreezeCommentID, tracked.GitHubCommentID,
			supersededByPriorRotation)
	}

	body := o.formatStatusComment(apply, tasks)
	newCommentID, posted, trackedNew := o.postAndTrackComment(apply, state.Comment.Progress, body, &cutover.GitHubCommentID)
	if !posted {
		// postAndTrackComment logged the failure. The live cutover row is left
		// in place — it is the durable signal that this rotation is owed — so
		// the next tick retries.
		return false
	}
	if !trackedNew {
		// The fresh comment is on the PR, but the tracked row still points at
		// the pre-cutover comment and the live cutover row remains the durable
		// marker that this rotation is owed. Remember the fresh comment so the
		// next tick adopts it — retrying only the tracking write, never
		// another post — after which the already-rotated branch above folds
		// the prompt and consumes the row. Consuming the row now would orphan
		// the fresh comment: nothing else adopts an untracked comment, and no
		// recorded phase would stop a later observer from rotating again. The
		// prompt is not folded either: the freeze marker rides the tracking
		// write, so a fold now could never be retried.
		o.pendingRotation = &pendingProgressRotation{
			commentID:           newCommentID,
			phase:               controlPhase(apply.State),
			reason:              supersededByCutover,
			supersededCommentID: cutover.GitHubCommentID,
		}
		o.logError(apply, "observer: fresh post-cutover progress comment posted but not tracked; the cutover prompt stays live until the next tick adopts the fresh comment",
			"github_comment_id", newCommentID)
		return true
	}
	o.cutoverRotated = true
	o.hasCutoverComment = false

	o.freezeSupersededProgressComment(apply, cutover.GitHubCommentID, newCommentID, supersededByCutover)
	o.supersedeCutoverComment(ctx, apply)

	o.logInfo(apply, "observer: posted fresh progress comment after deferred cutover", "state", apply.State)
	return true
}

// supersedeCutoverComment consumes the live cutover row after a post-cutover
// rotation. Failure is logged and left for the durable dedupe to absorb: the
// tracked comment's recorded post-cutover phase stops a later observer from
// rotating again even while the row still reads live.
func (o *CommentObserver) supersedeCutoverComment(ctx context.Context, apply *storage.Apply) {
	if err := o.stor.ApplyComments().Supersede(o.contextWithApplyLease(ctx, apply), o.applyID, state.Comment.Cutover); err != nil {
		o.logError(apply, "observer: failed to supersede cutover comment after post-cutover rotation", "error", err)
	}
}

// supersededProgressReason names why a tracked progress comment was rotated
// away from, selecting the frozen rendering written over it.
type supersededProgressReason int

const (
	// supersededByPriorRotation is the retry reason: the pending-freeze marker
	// records which comment is owed a fold but not which rotation superseded
	// it, so a retry renders the generic fold instead of guessing a headline.
	// It is the zero value, so an unset reason renders that generic fold
	// rather than an unearned headline.
	supersededByPriorRotation supersededProgressReason = iota
	supersededByResume
	supersededByRevert
	supersededBySkipRevert
	// supersededByCutover freezes the spent cutover prompt after a deferred
	// cutover completed into a still-active apply, pointing at the fresh
	// progress comment that tracks the post-cutover phase.
	supersededByCutover
)

// frozenSupersededProgressBody renders the folded body written over a
// superseded progress comment, headlined by the reason it was rotated away
// from.
func (o *CommentObserver) frozenSupersededProgressBody(reason supersededProgressReason, newCommentID int64, previousBody string) string {
	data := templates.SupersededProgressData{
		Repo:         o.repo,
		PR:           o.pr,
		NewCommentID: newCommentID,
		PreviousBody: previousBody,
	}
	switch reason {
	case supersededByResume:
		return templates.RenderResumeSupersededProgressComment(data)
	case supersededByRevert:
		return templates.RenderRevertSupersededProgressComment(data)
	case supersededBySkipRevert:
		return templates.RenderSkipRevertSupersededProgressComment(data)
	case supersededByCutover:
		return templates.RenderCutoverSupersededComment(data)
	}
	return templates.RenderSupersededProgressComment(data)
}

// adoptPendingRotationComment retries the tracking write for a fresh progress
// comment that was posted but never recorded, pointing the tracked row (and
// the freeze owed to its predecessor) at the already-live comment. Returns
// true when the row now tracks the pending comment; on failure the pending
// record is kept so the next tick retries — no path posts another comment.
func (o *CommentObserver) adoptPendingRotationComment(apply *storage.Apply) bool {
	p := o.pendingRotation
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	comment := &storage.ApplyComment{
		ApplyID:                o.applyID,
		CommentState:           state.Comment.Progress,
		GitHubCommentID:        p.commentID,
		PostedPhase:            &p.phase,
		PendingFreezeCommentID: &p.supersededCommentID,
	}
	if err := o.stor.ApplyComments().Upsert(o.contextWithApplyLease(ctx, apply), comment); err != nil {
		o.logError(apply, "observer: failed to adopt posted-but-untracked progress comment; progress edits continue on the prior comment until a retry lands",
			"github_comment_id", p.commentID, "phase", p.phase, "error", err)
		return false
	}
	o.pendingRotation = nil
	if p.phase != phaseNone {
		o.markPhaseRotated(p.phase)
	}
	o.logInfo(apply, "observer: adopted posted-but-untracked progress comment; progress edits now land on it",
		"github_comment_id", p.commentID, "phase", p.phase)
	o.freezeSupersededProgressComment(apply, p.supersededCommentID, p.commentID, p.reason)
	return true
}

// freezeSupersededProgressComment collapses a progress comment that is no
// longer tracked into a folded details block whose summary points readers at
// the comment that replaced it. Collapsing rather than deleting keeps the
// pre-change progress on the PR as a record while decluttering the timeline;
// without the fold a reader has no way to tell a frozen comment from a live
// one. reason selects the fold's headline. A failure leaves the tracked row's
// pending-freeze marker in place (it is written alongside the successor's
// tracking), so a later tick or a later drive's observer retries; the marker is
// cleared only once the frozen rendering is on GitHub.
func (o *CommentObserver) freezeSupersededProgressComment(apply *storage.Apply, oldCommentID, newCommentID int64, reason supersededProgressReason) {
	if !o.leaseStillOwnsObserver(apply, "create GitHub client before freezing superseded comment") {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := o.ghClient.ForInstallation(o.installationID)
	if err != nil {
		o.logError(apply, "observer: failed to create GitHub client to freeze superseded progress comment", "error", err)
		return
	}
	oldBody, err := client.GetIssueComment(ctx, o.repo, oldCommentID)
	if err != nil {
		o.logError(apply, "observer: failed to load superseded progress comment before freezing it",
			"error", err, "github_comment_id", oldCommentID)
		return
	}
	// A retry after a failed marker clear finds the frozen body already on
	// GitHub; re-rendering it would fold the frozen body inside another fold,
	// so only a still-live body is edited.
	if !templates.IsSupersededProgressComment(oldBody) {
		if !o.leaseStillOwnsObserver(apply, "freeze superseded progress comment") {
			return
		}
		frozen := o.frozenSupersededProgressBody(reason, newCommentID, oldBody)
		if err := client.EditIssueComment(ctx, o.repo, oldCommentID, frozen); err != nil {
			o.logError(apply, "observer: failed to freeze superseded progress comment; the pending-freeze marker stays set and the next observer pass that reads the tracked row retries",
				"error", err, "github_comment_id", oldCommentID)
			return
		}
	}
	if err := o.stor.ApplyComments().ClearPendingFreeze(o.contextWithApplyLease(ctx, apply), o.applyID, state.Comment.Progress); err != nil {
		o.logError(apply, "observer: failed to clear pending-freeze marker after freezing superseded progress comment; a later retry finds the comment already frozen and clears the marker",
			"error", err, "github_comment_id", oldCommentID)
	}
}

// postAndTrackComment creates a comment and stores its ID, recording the
// apply's control phase on progress comments so a later control operation is
// detectable. pendingFreezeCommentID, when non-nil, records in the same tracking
// write that the named predecessor comment is still owed its frozen rendering;
// pass nil for posts that supersede nothing. Returns the GitHub comment ID,
// whether the post landed on the PR, and whether the tracking row
// was updated to point at it — a posted but untracked comment exists on the PR
// while later edits still target the prior comment.
func (o *CommentObserver) postAndTrackComment(apply *storage.Apply, commentState string, body string, pendingFreezeCommentID *int64) (commentID int64, posted, tracked bool) {
	if !o.leaseStillOwnsObserver(apply, "create GitHub client before post") {
		return 0, false, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := o.ghClient.ForInstallation(o.installationID)
	if err != nil {
		o.logError(apply, "observer: failed to create GitHub client", "error", err)
		return 0, false, false
	}
	if !o.leaseStillOwnsObserver(apply, "post GitHub comment") {
		return 0, false, false
	}

	commentID, _, err = client.CreateIssueComment(ctx, o.repo, o.pr, o.renderPRComment(body))
	if err != nil {
		o.logError(apply, "observer: failed to post comment", "error", err, "comment_state", commentState)
		return 0, false, false
	}
	if !o.leaseStillOwnsObserver(apply, "record posted GitHub comment") {
		// The comment is live on the PR; only the tracking write is being
		// skipped. Report it posted-but-untracked — the contract callers use to
		// bound duplicates — rather than pretending the post never happened.
		return commentID, true, false
	}

	comment := &storage.ApplyComment{
		ApplyID:                o.applyID,
		CommentState:           commentState,
		GitHubCommentID:        commentID,
		PendingFreezeCommentID: pendingFreezeCommentID,
	}
	if commentState == state.Comment.Progress {
		phase := controlPhase(apply.State)
		comment.PostedPhase = &phase
	}
	if err := o.stor.ApplyComments().Upsert(o.contextWithApplyLease(ctx, apply), comment); err != nil {
		o.logError(apply, "observer: failed to store comment ID", "error", err, "comment_state", commentState)
		return commentID, true, false
	}
	return commentID, true, true
}

func (o *CommentObserver) renderPRComment(body string) string {
	return appendSupportChannelFooter(body, o.supportChannel)
}

// publishClaimedSummary posts the separate apply-level terminal summary
// comment under the atomic summary-marker claim. Multiple writers can reach a
// terminal apply's summary step — the origin driver's observer, the aggregate
// CAS-winner observer, and stop reconciliation's publisher — so the claim, not
// the apply lease, is the exactly-once authority here: whichever writer wins
// the claim posts the one summary, and every loser skips. The storage writes
// are deliberately lease-free — a writer whose apply lease was re-claimed (for
// example by stop reconciliation) must still be able to lose the claim cleanly,
// and the winner must be able to record its post.
func (o *CommentObserver) publishClaimedSummary(apply *storage.Apply, body string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	won, err := o.stor.ApplyComments().ClaimSummaryComment(ctx, o.applyID)
	if err != nil {
		o.logError(apply, "observer: failed to claim terminal summary; summary not posted, left for reconciliation",
			"error", err)
		return
	}
	if !won {
		o.logInfo(apply, "observer: terminal summary already claimed or posted by another writer; skipping")
		return
	}

	client, err := o.ghClient.ForInstallation(o.installationID)
	if err != nil {
		o.logError(apply, "observer: failed to create GitHub client for claimed terminal summary; releasing claim",
			"error", err)
		o.releaseSummaryClaim(ctx, apply)
		return
	}
	commentID, _, err := client.CreateIssueComment(ctx, o.repo, o.pr, o.renderPRComment(body))
	if err != nil {
		o.logError(apply, "observer: failed to post claimed terminal summary; releasing claim",
			"error", err)
		o.releaseSummaryClaim(ctx, apply)
		return
	}

	marker := &storage.ApplyComment{
		ApplyID:         o.applyID,
		CommentState:    state.Comment.Summary,
		GitHubCommentID: commentID,
	}
	if err := o.stor.ApplyComments().Upsert(ctx, marker); err != nil {
		// The comment is live on the PR but the marker still looks like a claim
		// sentinel, so once it goes stale, reconciliation will reclaim it and may
		// post one duplicate — the bounded-duplicate contract for a lost tracking
		// write.
		o.logError(apply, "observer: posted terminal summary but failed to record its comment ID",
			"error", err, "comment_id", commentID)
	}
}

// releaseSummaryClaim returns a won-but-unused summary claim so another
// publisher or startup reconciliation can retry immediately instead of waiting
// out the stale-claim window.
func (o *CommentObserver) releaseSummaryClaim(ctx context.Context, apply *storage.Apply) {
	if err := o.stor.ApplyComments().ReleaseSummaryClaim(ctx, o.applyID); err != nil {
		o.logError(apply, "observer: failed to release terminal summary claim; reconciliation will reclaim it once stale",
			"error", err)
	}
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
