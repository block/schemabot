package webhook

import (
	"context"
	"sync"
	"time"

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
	deferCutover   bool
	logger         interface {
		Info(msg string, args ...any)
		Error(msg string, args ...any)
	}

	// OnTerminalHook is called after the summary comment is posted.
	// Used by the webhook handler to update check runs on terminal state.
	// Optional — nil means no hook.
	OnTerminalHook func(apply *storage.Apply)

	mu                sync.Mutex
	lastProgressPost  time.Time
	lastState         string
	lastRowsCopied    int64
	stagnantTicks     int
	hasCutoverComment bool
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
	DeferCutover   bool
	Logger         interface {
		Info(msg string, args ...any)
		Error(msg string, args ...any)
	}

	// OnTerminalHook is called after the summary comment is posted.
	// Used to update check runs on terminal state.
	OnTerminalHook func(apply *storage.Apply)
}

// SetApplyID sets the apply ID after the apply record is created.
// Called before the observer is registered for progress notifications.
func (o *CommentObserver) SetApplyID(id int64) {
	o.applyID = id
}

// NewCommentObserver creates a new CommentObserver for posting PR comments.
func NewCommentObserver(cfg CommentObserverConfig) *CommentObserver {
	return &CommentObserver{
		ghClient:       cfg.GHClient,
		stor:           cfg.Storage,
		repo:           cfg.Repo,
		pr:             cfg.PR,
		installationID: cfg.InstallationID,
		applyID:        cfg.ApplyID,
		deferCutover:   cfg.DeferCutover,
		logger:         cfg.Logger,
		OnTerminalHook: cfg.OnTerminalHook,
	}
}

// OnProgress is called on each progress poller tick. Rate-limits updates
// to avoid excessive GitHub API calls. Handles the comment lifecycle:
// progress edits, cutover comment creation, and state-change notifications.
func (o *CommentObserver) OnProgress(apply *storage.Apply, tasks []*storage.Task) {
	o.mu.Lock()
	defer o.mu.Unlock()

	now := time.Now()
	currentState := apply.State

	// Check if a cutover comment was posted by an external handler.
	// This must happen before the CuttingOver branch below — without it,
	// the observer would post a duplicate cutover comment.
	if !o.hasCutoverComment {
		checkCtx, checkCancel := context.WithTimeout(context.Background(), 2*time.Second)
		cutover, err := o.stor.ApplyComments().Get(checkCtx, o.applyID, state.Comment.Cutover)
		if err != nil {
			o.logger.Error("observer: failed to check for cutover comment", "error", err)
		} else if cutover != nil {
			o.hasCutoverComment = true
		}
		checkCancel()
	}

	// Post cutover comment when entering cutting_over with defer_cutover,
	// but only if one hasn't been posted already.
	if currentState == state.Apply.CuttingOver && o.shouldDeferCutover(apply) && !o.hasCutoverComment {
		body := formatCutoverComment(apply, tasks)
		o.postAndTrackComment(state.Comment.Cutover, body)
		o.hasCutoverComment = true
		o.lastState = currentState
		return
	}

	// If cutover was triggered, stop editing — the progress comment is frozen
	// and OnTerminal will handle the cutover comment completion.
	if o.hasCutoverComment {
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
	body := formatProgressComment(apply, tasks)
	o.editTrackedComment(state.Comment.Progress, body)
}

// OnTerminal is called when the apply reaches a terminal state.
// Edits the active comment to final state, posts summary comment,
// and updates check runs.
func (o *CommentObserver) OnTerminal(apply *storage.Apply, tasks []*storage.Task) {
	// Determine which comment to edit to final state.
	// If a cutover comment exists, edit that and leave the progress comment
	// frozen at its last state. Otherwise edit the progress comment.
	activeCommentState := state.Comment.Progress
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cutover, err := o.stor.ApplyComments().Get(ctx, o.applyID, state.Comment.Cutover)
	if err != nil {
		o.logger.Error("observer: failed to check for cutover comment on terminal", "error", err)
	} else if cutover != nil {
		activeCommentState = state.Comment.Cutover
	}

	if activeCommentState == state.Comment.Cutover {
		// Cutover comment gets the summary format — Apply ID, DDL, success message.
		// No separate summary needed since the cutover comment IS the completion comment.
		finalBody := formatSummaryComment(apply, tasks)
		o.editTrackedComment(activeCommentState, finalBody)

		// Upsert a summary marker so FindMissingSummaryComment (outbox query)
		// doesn't false-positive on restart for cutover applies.
		o.markSummaryPosted(activeCommentState)
	} else {
		// Edit the progress comment to its final state (completed bars / error).
		finalBody := formatProgressComment(apply, tasks)
		o.editTrackedComment(activeCommentState, finalBody)

		// Post a separate summary comment. A new comment is more reliable than
		// an edit — GitHub renders edits with a delay, but new comments appear
		// immediately and trigger notifications for PR subscribers.
		summaryBody := formatSummaryComment(apply, tasks)
		o.postAndTrackComment(state.Comment.Summary, summaryBody)
	}

	// Run terminal hook (e.g., update check runs)
	if o.OnTerminalHook != nil {
		o.OnTerminalHook(apply)
	}
}

func (o *CommentObserver) shouldDeferCutover(apply *storage.Apply) bool {
	return o.deferCutover || apply.GetOptions().DeferCutover
}

// editTrackedComment looks up a stored comment ID and edits it.
func (o *CommentObserver) editTrackedComment(commentState string, body string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	comment, err := o.stor.ApplyComments().Get(ctx, o.applyID, commentState)
	if err != nil {
		o.logger.Error("observer: failed to look up comment for edit", "error", err, "comment_state", commentState)
		return
	}
	if comment == nil {
		// No tracked comment for this state — nothing to edit.
		// This is expected when the progress comment hasn't been posted yet
		// (e.g., first OnProgress tick before the handler posts it).
		return
	}

	client, err := o.ghClient.ForInstallation(o.installationID)
	if err != nil {
		o.logger.Error("observer: failed to create GitHub client", "error", err)
		return
	}

	if err := client.EditIssueComment(ctx, o.repo, comment.GitHubCommentID, body); err != nil {
		o.logger.Error("observer: failed to edit comment", "error", err, "comment_state", commentState)
		return
	}

	// Track the edit for audit/debugging
	if err := o.stor.ApplyComments().IncrementEditCount(ctx, o.applyID, commentState); err != nil {
		o.logger.Error("observer: failed to increment edit count", "error", err)
	}
}

// postAndTrackComment creates a comment and stores its ID.
func (o *CommentObserver) postAndTrackComment(commentState string, body string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := o.ghClient.ForInstallation(o.installationID)
	if err != nil {
		o.logger.Error("observer: failed to create GitHub client", "error", err)
		return
	}

	commentID, err := client.CreateIssueComment(ctx, o.repo, o.pr, body)
	if err != nil {
		o.logger.Error("observer: failed to post comment", "error", err, "comment_state", commentState)
		return
	}

	comment := &storage.ApplyComment{
		ApplyID:         o.applyID,
		CommentState:    commentState,
		GitHubCommentID: commentID,
	}
	if err := o.stor.ApplyComments().Upsert(ctx, comment); err != nil {
		o.logger.Error("observer: failed to store comment ID", "error", err)
	}
}

// markSummaryPosted upserts a summary marker record in apply_comments.
// Used for cutover applies where the cutover comment serves as the summary —
// no separate summary is posted, but the marker satisfies the
// FindMissingSummaryComment outbox query.
func (o *CommentObserver) markSummaryPosted(editedCommentState string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	edited, err := o.stor.ApplyComments().Get(ctx, o.applyID, editedCommentState)
	if err != nil {
		o.logger.Error("observer: failed to look up comment for summary marker", "error", err)
		return
	}
	if edited == nil {
		// The edited comment doesn't exist in storage — can't create a marker
		// without a GitHub comment ID to reference.
		o.logger.Error("observer: no comment found to create summary marker from",
			"comment_state", editedCommentState, "apply_id", o.applyID)
		return
	}

	marker := &storage.ApplyComment{
		ApplyID:         o.applyID,
		CommentState:    state.Comment.Summary,
		GitHubCommentID: edited.GitHubCommentID,
	}
	if err := o.stor.ApplyComments().Upsert(ctx, marker); err != nil {
		o.logger.Error("observer: failed to upsert summary marker", "error", err)
	}
}
