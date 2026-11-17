package webhook

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/block/schemabot/pkg/caller"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// rolloutProgress is how far each in-progress rollout has got across its
// targets, keyed by apply ID, for the aggregate Check Run's text. It is display
// only: it never decides the aggregate's status or conclusion, so a failed read
// renders the Check Run without the counts rather than changing what it gates.
// A nil map reads as "no progress known".
type rolloutProgress map[int64]targetTally

// targetTally counts a rollout's targets by where each one stands, and names
// the apply's live progress comment so the Check Run can point at the detail.
// Targets that already had the change ran nothing and are not counted. It is
// comparable, so a fold can tell whether the progress moved while it published.
type targetTally struct {
	Total      int
	Complete   int
	InProgress int
	Queued     int
	Failed     int
	Stopped    int
	Cancelled  int
	Reverted   int
	// ProgressCommentID is the GitHub comment the apply's progress is tracked
	// on, zero when none is tracked.
	ProgressCommentID int64
}

// tallyFor returns the tally for the apply that owns c, when c is a check of a
// rollout across several targets with an apply in progress.
func (p rolloutProgress) tallyFor(c *storage.Check) (targetTally, bool) {
	if c.ApplyID == 0 || c.RolloutTargets <= 1 {
		return targetTally{}, false
	}
	t, ok := p[c.ApplyID]
	if !ok || t.Total == 0 {
		return targetTally{}, false
	}
	return t, true
}

// tallyTargets counts an apply's targets from its operations. A target's
// operations (one per table step, say) settle into one state for the target.
// A group finalizer is not a target, and a target whose every operation was
// settled because it already had the change ran nothing, so neither counts.
func tallyTargets(ops []*storage.ApplyOperation) targetTally {
	var order []string
	states := map[string][]string{}
	converged := map[string]bool{}
	for _, op := range ops {
		if op.OperationKind == storage.ApplyOperationKindGroupFinalizer {
			continue
		}
		key := op.Target
		if key == "" {
			key = op.Deployment
		}
		if _, seen := states[key]; !seen {
			order = append(order, key)
			converged[key] = true
		}
		states[key] = append(states[key], op.State)
		converged[key] = converged[key] && op.AlreadyConverged
	}

	var t targetTally
	for _, key := range order {
		if converged[key] {
			continue
		}
		t.Total++
		switch derived := state.DeriveApplyState(states[key]); {
		case state.IsState(derived, state.ApplyOperation.Completed):
			t.Complete++
		case state.IsState(derived, state.ApplyOperation.Pending):
			t.Queued++
		case state.IsState(derived, state.ApplyOperation.Failed):
			t.Failed++
		case state.IsState(derived, state.ApplyOperation.Stopped):
			t.Stopped++
		case state.IsState(derived, state.ApplyOperation.Cancelled):
			t.Cancelled++
		case state.IsState(derived, state.ApplyOperation.Reverted):
			t.Reverted++
		default:
			t.InProgress++
		}
	}
	return t
}

// phrases is the tally in words, in the order a reader acts on them: what
// failed, what is running, what is held, what is still to run, then what is
// done.
func (t targetTally) phrases() []string {
	var parts []string
	add := func(n int, word string) {
		if n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", n, word))
		}
	}
	add(t.Failed, "failed")
	add(t.InProgress, "in progress")
	add(t.Stopped, "stopped")
	add(t.Queued, "queued")
	add(t.Cancelled, "cancelled")
	add(t.Reverted, "reverted")
	add(t.Complete, "complete")
	return parts
}

// rolloutDoneSuffix is the title's count of finished targets: ": 5 done".
// Before any target finishes it adds nothing, since the title already says
// the apply is in progress.
func rolloutDoneSuffix(c *storage.Check, progress rolloutProgress) string {
	t, ok := progress.tallyFor(c)
	if !ok || t.Complete == 0 {
		return ""
	}
	return fmt.Sprintf(": %d done", t.Complete)
}

// rolloutStatusLabel is the Status cell of a rollout still in progress:
// "In progress · 5 of 32 done", or plain "In progress" before any target
// finishes.
func rolloutStatusLabel(c *storage.Check, progress rolloutProgress) string {
	label := conclusionEmoji(c.Status, c.Conclusion)
	t, ok := progress.tallyFor(c)
	if !ok || t.Complete == 0 {
		return label
	}
	return fmt.Sprintf("%s · %d of %d done", label, t.Complete, t.Total)
}

// renderRolloutProgress writes a line under the Database table for each
// rollout still in progress: its targets counted by where each stands, and a
// link to the progress comment that names them.
func renderRolloutProgress(dbChecks []*storage.Check, progress rolloutProgress, stopped stoppedApplyIDs) string {
	var sb strings.Builder
	for _, c := range dbChecks {
		if c.Status != checkStatusInProgress || stopped.holds(c) {
			continue
		}
		t, ok := progress.tallyFor(c)
		if !ok {
			continue
		}
		fmt.Fprintf(&sb, "- `%s` targets: %d (%s)", c.DatabaseName, t.Total, strings.Join(t.phrases(), ", "))
		if t.ProgressCommentID != 0 && c.Repository != "" && c.PullRequest != 0 {
			fmt.Fprintf(&sb, " · [progress](%s#issuecomment-%d)", caller.PullRequestURL(c.Repository, c.PullRequest), t.ProgressCommentID)
		}
		sb.WriteString("\n")
	}
	if sb.Len() == 0 {
		return ""
	}
	return "\n" + sb.String()
}

// rolloutProgressForPR reads how far each in-progress rollout among the
// contributions has got. A failed read returns no progress rather than an
// error: the counts are display only, so the Check Run is still published,
// without them. The error is logged.
func (h *Handler) rolloutProgressForPR(ctx context.Context, repo string, pr int, checks []*storage.Check) rolloutProgress {
	progress, err := h.loadRolloutProgress(ctx, checks)
	if err != nil {
		h.logger.Warn("aggregate check run will not count rollout targets; failed to load the rollout's operations",
			"repo", repo, "pr", pr, "error", err)
		return nil
	}
	return progress
}

// loadRolloutProgress tallies the targets of every apply that holds an
// in-progress check of a rollout across several targets.
func (h *Handler) loadRolloutProgress(ctx context.Context, checks []*storage.Check) (rolloutProgress, error) {
	var applyIDs []int64
	seen := map[int64]bool{}
	for _, c := range checks {
		if c.Status != checkStatusInProgress || c.ApplyID == 0 || c.RolloutTargets <= 1 || seen[c.ApplyID] {
			continue
		}
		seen[c.ApplyID] = true
		applyIDs = append(applyIDs, c.ApplyID)
	}
	if len(applyIDs) == 0 {
		return nil, nil
	}

	stor := h.service.Storage()
	ops, err := stor.ApplyOperations().ListByApplies(ctx, applyIDs)
	if err != nil {
		return nil, fmt.Errorf("list operations of %d in-progress rollout applies: %w", len(applyIDs), err)
	}
	byApply := map[int64][]*storage.ApplyOperation{}
	for _, op := range ops {
		byApply[op.ApplyID] = append(byApply[op.ApplyID], op)
	}

	progress := rolloutProgress{}
	for _, id := range applyIDs {
		t := tallyTargets(byApply[id])
		comment, err := stor.ApplyComments().Get(ctx, id, state.Comment.Progress)
		if err != nil {
			return nil, fmt.Errorf("load the progress comment of an in-progress rollout apply: %w", err)
		}
		if comment != nil && comment.SupersededAt == nil {
			t.ProgressCommentID = comment.GitHubCommentID
		}
		progress[id] = t
	}
	return progress, nil
}

// rolloutTargetsFinishedHook returns the observer hook that republishes an
// apply's aggregate Check Run when one more of its rollout's targets finishes.
// The publish runs off the observer's goroutine on a copy of the apply, so the
// progress tick never waits on GitHub. One refresh runs at a time for each
// observer; a tick that finds one running is declined and offered again later.
func (h *Handler) rolloutTargetsFinishedHook() func(*storage.Apply) bool {
	var refreshing atomic.Bool
	return func(apply *storage.Apply) bool {
		if apply.Repository == "" || apply.PullRequest == 0 || apply.InstallationID == 0 {
			h.logger.Debug("rollout target finished; no Check Run to refresh: apply has no GitHub destination",
				apply.LogAttrs()...)
			return true
		}
		if !refreshing.CompareAndSwap(false, true) {
			h.logger.Debug("rollout target finished; a Check Run refresh for this apply is still running, the next progress tick offers it again",
				apply.LogAttrs()...)
			return false
		}
		claimed := *apply
		h.goSafe(claimed.Repository, claimed.PullRequest, claimed.InstallationID, "", func() {
			defer refreshing.Store(false)
			h.refreshRolloutCheckRun(context.Background(), &claimed)
		})
		return true
	}
}

// refreshRolloutCheckRun republishes the aggregate Check Run of a rollout
// still in progress so it shows how many targets have finished. The stored
// check state is not written.
//
// The refresh folds from stored check state it read before publishing, so the
// apply can finish while it publishes: the terminal path writes the stored
// check state, publishes, and then this refresh's in-progress run lands last
// and holds the PR blocked. To converge, the refresh re-reads the stored check
// state once it has published and, if it is no longer in progress, publishes
// again. Whoever settled the row wrote it before publishing, so either that
// publish follows this re-read, or this second publish carries the settled
// state.
func (h *Handler) refreshRolloutCheckRun(ctx context.Context, a *storage.Apply) {
	fields := applyCheckLogFields(a, "rollout target finished")
	if _, _, published := h.publishApplyAggregate(ctx, a, fields, true); !published {
		return
	}
	check, err := h.service.Storage().Checks().Get(ctx, a.Repository, a.PullRequest, a.Environment, a.DatabaseType, a.Database)
	if err != nil {
		h.logger.Warn("rollout Check Run refresh not re-checked against stored check state; if the apply finished while it published, the Check Run shows in progress until the next fold",
			append(fields(), "error", err)...)
		return
	}
	if check == nil {
		h.logger.Warn("rollout Check Run refresh found no stored check state after publishing; the Check Run shows in progress until the next fold",
			fields()...)
		return
	}
	if check.Status == checkStatusInProgress {
		h.logger.Debug("rollout Check Run refreshed; the stored check state is still in progress", fields()...)
		return
	}
	h.logger.Info("republishing aggregate check: the stored check state settled while the rollout progress refresh published",
		append(fields(), "check_status", check.Status, "check_conclusion", check.Conclusion)...)
	h.publishApplyAggregate(ctx, a, fields, false)
}
