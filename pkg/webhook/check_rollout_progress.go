package webhook

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/block/schemabot/pkg/caller"
	"github.com/block/schemabot/pkg/routing"
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

// tallyTargets counts an apply's targets from its operations. A target is a
// rollout member, named by its deployment and target together, and its
// operations (one per table step, say) settle into one state for it.
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
		key := routing.ExecutionTarget{Deployment: op.Deployment, Target: op.Target}.MemberID()
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
// link to the progress comment that names them. The lines stay within limit
// bytes; rollouts past it are counted on one closing line.
func renderRolloutProgress(dbChecks []*storage.Check, progress rolloutProgress, stopped stoppedApplyIDs, limit int) string {
	var lines []string
	for _, c := range dbChecks {
		if c.Status != checkStatusInProgress || stopped.holds(c) {
			continue
		}
		t, ok := progress.tallyFor(c)
		if !ok {
			continue
		}
		line := fmt.Sprintf("- `%s` targets: %d (%s)", c.DatabaseName, t.Total, strings.Join(t.phrases(), ", "))
		if t.ProgressCommentID != 0 && c.Repository != "" && c.PullRequest != 0 {
			line += fmt.Sprintf(" · [progress](%s#issuecomment-%d)", caller.PullRequestURL(c.Repository, c.PullRequest), t.ProgressCommentID)
		}
		lines = append(lines, line+"\n")
	}
	if len(lines) == 0 {
		return ""
	}
	// The closing line is at most this long, so it always fits once reserved.
	const omittedReserve = len("- … 1000000 more rollouts in progress\n")
	var sb strings.Builder
	sb.WriteString("\n")
	for i, line := range lines {
		if sb.Len()+len(line)+omittedReserve > limit {
			fmt.Fprintf(&sb, "- … %d more rollouts in progress\n", len(lines)-i)
			break
		}
		sb.WriteString(line)
	}
	return sb.String()
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
// Starting the refresh counts as reporting the finished targets, so a refresh
// that then fails, on a GitHub error for example, leaves the Check Run's counts
// as they were until the next target finishes or the aggregate is folded. Only
// the text is late: a refresh never changes what the run gates.
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

// refreshRolloutCheckRun rewrites the aggregate Check Run of a rollout still
// in progress so it shows how many targets have finished. It renders from what
// the last fold stored and spends two GitHub requests, however large the PR.
// When the stored render cannot be trusted for the run, or the state it was
// rendered from moved while it published (the apply finished, another
// database settled, an apply stopped), it folds in full instead, so the run
// ends on the current state rather than on the refresh's text. The stored
// check state is not written.
func (h *Handler) refreshRolloutCheckRun(ctx context.Context, a *storage.Apply) {
	fields := applyCheckLogFields(a, "rollout target finished")
	client, err := h.clientForRepo(a.Repository, a.InstallationID)
	if err != nil {
		h.logger.Warn("rollout counts not refreshed on the aggregate check run; cannot resolve GitHub App client",
			append(fields(), "error", err)...)
		return
	}
	target := h.aggregateTargetForEnvironment(a.Repository, a.Environment)
	refresh, err := h.refreshAggregateRolloutCounts(ctx, client, a.Repository, a.PullRequest, target.environment, target.name)
	if err != nil {
		h.logger.Warn("rollout counts not refreshed on the aggregate check run; the run shows the previous counts until the next fold",
			append(fields(), "check_name", target.name, "error", err)...)
		return
	}
	if refresh != rolloutCountsNeedFold {
		return
	}
	h.logger.Info("folding the aggregate check in full to show the rollout counts",
		append(fields(), "check_name", target.name)...)
	h.publishApplyAggregate(ctx, a, fields, false)
}
