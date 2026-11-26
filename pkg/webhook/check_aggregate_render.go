package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// aggregateRender is everything an aggregate Check Run's title and summary are
// rendered from. A fold stores it on the aggregate row with the state it
// published, so the text can be rendered again with newer rollout counts
// without repeating the fold's reads of the PR, its files, and its
// participants. It is display only: nothing in it decides what the aggregate
// gates.
type aggregateRender struct {
	// CheckRunID, HeadSHA, Status, and Conclusion are what the fold published
	// the text with. A refresh renders only while the stored aggregate row
	// still says the same, so it never renders text for state that moved on.
	CheckRunID int64
	HeadSHA    string
	Status     string
	Conclusion string

	// Contributions are the rows the summary's tables list, after stale-commit
	// normalization and without the prior environments' contributions.
	Contributions []*storage.Check
	Stopped       stoppedApplyIDs
	Progress      rolloutProgress

	// PrerequisiteNotice is the line naming prior environments the aggregate
	// waits on, empty when it waits on none.
	PrerequisiteNotice string
	// AwaitingCurrentCommit is set when the fold holds rows recorded for
	// another commit and nothing is running on the current one.
	AwaitingCurrentCommit bool

	// Inputs is the stored per-database check state the fold read, keyed by
	// row ID, so a refresh can tell whether any of it moved since.
	Inputs map[int64]renderInput

	// Tenant is the deployment's own tenant, carried by the commands the
	// summary names. It is not stored: each render reads it from the
	// deployment's config, so a stored render never names another tenant.
	Tenant string `json:"-"`
}

// renderInput is the part of one stored per-database row that the fold's
// text depends on. A row the fold read as a prior environment's prerequisite
// was relabeled for the fold, so its stored blocking reason is not recorded
// or compared.
type renderInput struct {
	Prerequisite bool

	Environment  string
	HeadSHA      string
	ApplyID      int64
	Status       string
	Conclusion   string
	Blocking     string
	Change       string
	RolloutTotal int
}

func renderInputOf(c *storage.Check) renderInput {
	return renderInput{
		Environment: c.Environment, HeadSHA: c.HeadSHA, ApplyID: c.ApplyID,
		Status: c.Status, Conclusion: c.Conclusion, Blocking: c.BlockingReason,
		Change: c.ChangeSummary, RolloutTotal: c.RolloutTargets,
	}
}

// renderInputsOf records the stored per-database rows among the fold's
// inputs. Rows a participant reported and the aggregate rows are not stored
// per-database state, so they are left out.
func renderInputsOf(checks []*storage.Check) map[int64]renderInput {
	inputs := map[int64]renderInput{}
	for _, c := range checks {
		if c.ID == 0 || c.DatabaseType == aggregateSentinel {
			continue
		}
		input := renderInputOf(c)
		if c.BlockingReason == prerequisiteContribution {
			input.Prerequisite = true
			input.Blocking = ""
		}
		inputs[c.ID] = input
	}
	return inputs
}

// inputsMoved reports whether the stored per-database rows a render was
// folded from have changed: a row it read was rewritten or removed, or a row
// for its environment appeared.
func inputsMoved(render aggregateRender, environment string, stored []*storage.Check) bool {
	seen := 0
	for _, c := range stored {
		if c.DatabaseType == aggregateSentinel {
			continue
		}
		input, read := render.Inputs[c.ID]
		if !read {
			if environment == aggregateSentinel || c.Environment == environment {
				return true
			}
			continue
		}
		seen++
		current := renderInputOf(c)
		if input.Prerequisite {
			current.Prerequisite = true
			current.Blocking = ""
		}
		if input != current {
			return true
		}
	}
	return seen != len(render.Inputs)
}

// text renders the Check Run's title and summary.
func (r aggregateRender) text() (title, summary string) {
	title, summary = aggregateSummary(r.Contributions, r.Conclusion, r.Stopped, r.Progress, r.Tenant)
	if r.PrerequisiteNotice != "" {
		title = "Waiting for prerequisite environments"
		summary = appendPrerequisiteSummary(summary, r.PrerequisiteNotice)
	}
	if r.AwaitingCurrentCommit {
		title = awaitingCurrentCommitTitle
	}
	return title, summary
}

// rolloutCountsRefresh is what refreshing an aggregate Check Run's rollout
// counts did.
type rolloutCountsRefresh int

const (
	// rolloutCountsUnchanged: the counts the run shows are current, or the
	// aggregate shows none.
	rolloutCountsUnchanged rolloutCountsRefresh = iota
	// rolloutCountsPublished: the run's text was rewritten with new counts.
	rolloutCountsPublished
	// rolloutCountsNeedFold: the stored render cannot be trusted for this run,
	// so only a full fold can publish the counts.
	rolloutCountsNeedFold
)

// refreshAggregateRolloutCounts rewrites the text of an environment's
// aggregate Check Run with the rollout counts read now, rendered from what the
// last fold stored. It costs two GitHub requests: one to find the newest run
// and one to rewrite its text. It never sends a status or conclusion, so it
// cannot change what the run gates; it reports rolloutCountsNeedFold whenever
// the stored render does not match the stored aggregate row or the live run,
// and the caller folds instead.
//
// Another writer can settle a database, stop an apply, or fold the aggregate
// while the refresh publishes, and the refresh's older text can then land
// after that writer's. Every such writer stores its state before it
// publishes, so once the text is sent the refresh reads that state again: if
// any of it moved since the render was folded, it reports
// rolloutCountsNeedFold so the caller folds from the current state.
func (h *Handler) refreshAggregateRolloutCounts(
	ctx context.Context, client *ghclient.InstallationClient,
	repo string, pr int, environment, checkName string,
) (rolloutCountsRefresh, error) {
	fields := func() []any {
		return []any{"repo", repo, "pr", pr, "environment", environment, "check_name", checkName}
	}
	if !h.shouldPublishChecks(ctx, repo, "aggregate_rollout_counts") {
		return rolloutCountsUnchanged, nil
	}
	checks := h.service.Storage().Checks()
	row, err := checks.Get(ctx, repo, pr, environment, aggregateSentinel, aggregateSentinel)
	if err != nil {
		return rolloutCountsUnchanged, fmt.Errorf("load aggregate check state for %s#%d (env %s): %w", repo, pr, environment, err)
	}
	if row == nil {
		h.logger.Debug("rollout counts not refreshed: no stored aggregate check state", fields()...)
		return rolloutCountsUnchanged, nil
	}
	if row.BlockingReason != "" || row.Status != checkStatusInProgress || row.CheckRunID == 0 {
		h.logger.Debug("rollout counts not refreshed: the aggregate is not an in-progress run that shows counts",
			append(fields(), "check_status", row.Status, "blocking_reason", row.BlockingReason, "check_run_id", row.CheckRunID)...)
		return rolloutCountsUnchanged, nil
	}

	raw, err := checks.GetAggregateRender(ctx, repo, pr, environment, aggregateSentinel, aggregateSentinel)
	if err != nil {
		return rolloutCountsUnchanged, fmt.Errorf("load aggregate render for %s#%d (env %s): %w", repo, pr, environment, err)
	}
	if raw == nil {
		h.logger.Info("rollout counts need a full fold: the aggregate row has no stored render",
			append(fields(), "check_run_id", row.CheckRunID)...)
		return rolloutCountsNeedFold, nil
	}
	var render aggregateRender
	if err := json.Unmarshal(raw, &render); err != nil {
		h.logger.Warn("rollout counts need a full fold: the stored aggregate render does not decode",
			append(fields(), "check_run_id", row.CheckRunID, "error", err)...)
		return rolloutCountsNeedFold, nil
	}
	if render.Inputs == nil {
		h.logger.Info("rollout counts need a full fold: the stored aggregate render does not record the state it was folded from",
			append(fields(), "check_run_id", row.CheckRunID)...)
		return rolloutCountsNeedFold, nil
	}
	if !renderDescribesRow(render, row) {
		h.logger.Info("rollout counts need a full fold: the stored aggregate render was published with different state",
			append(fields(), "check_run_id", row.CheckRunID, "render_check_run_id", render.CheckRunID,
				"head_sha", row.HeadSHA, "render_head_sha", render.HeadSHA,
				"check_status", row.Status, "render_status", render.Status)...)
		return rolloutCountsNeedFold, nil
	}

	stopped, err := h.loadStoppedApplies(ctx, repo, pr)
	if err != nil {
		return rolloutCountsUnchanged, fmt.Errorf("load stopped applies for %s#%d: %w", repo, pr, err)
	}
	if !maps.Equal(stopped, render.Stopped) {
		h.logger.Info("rollout counts need a full fold: an apply stopped or started again since the last fold", fields()...)
		return rolloutCountsNeedFold, nil
	}
	progress, err := h.loadRolloutProgress(ctx, render.Contributions)
	if err != nil {
		return rolloutCountsUnchanged, fmt.Errorf("load rollout progress for %s#%d (env %s): %w", repo, pr, environment, err)
	}
	if maps.Equal(progress, render.Progress) {
		h.logger.Debug("rollout counts not refreshed: the run already shows the current counts", fields()...)
		return rolloutCountsUnchanged, nil
	}
	render.Progress = progress
	render.Tenant = h.deploymentTenant()
	title, summary := render.text()

	run, _, err := client.FindCheckRunByName(ctx, repo, render.HeadSHA, checkName)
	if err != nil {
		return rolloutCountsUnchanged, fmt.Errorf("resolve newest aggregate check run %q for %s#%d@%s: %w", checkName, repo, pr, render.HeadSHA, err)
	}
	if run == nil {
		h.logger.Info("rollout counts need a full fold: the stored aggregate check run is not on the commit",
			append(fields(), "check_run_id", row.CheckRunID, "head_sha", render.HeadSHA)...)
		return rolloutCountsNeedFold, nil
	}
	if run.ID != render.CheckRunID || run.Status == checkStatusCompleted {
		h.logger.Info("rollout counts need a full fold: the newest aggregate check run is not the in-progress run the render was published on",
			append(fields(), "check_run_id", row.CheckRunID, "live_check_run_id", run.ID, "live_status", run.Status)...)
		return rolloutCountsNeedFold, nil
	}
	if err := client.UpdateCheckRun(ctx, repo, run.ID, ghclient.CheckRunOptions{
		Name:   checkName,
		Output: &ghclient.CheckRunOutput{Title: title, Summary: summary},
	}); err != nil {
		return rolloutCountsUnchanged, fmt.Errorf("rewrite rollout counts on aggregate check run %d for %s#%d (env %s): %w", run.ID, repo, pr, environment, err)
	}
	h.logger.Info("aggregate check run rollout counts refreshed",
		append(fields(), "check_run_id", run.ID, "head_sha", render.HeadSHA)...)

	moved, err := h.renderStateMoved(ctx, repo, pr, environment, raw, render)
	if err != nil {
		h.logger.Warn("rollout counts refresh not re-checked against the state it was rendered from; folding the aggregate in full so the run cannot keep older text",
			append(fields(), "check_run_id", run.ID, "error", err)...)
		return rolloutCountsNeedFold, nil
	}
	if moved {
		h.logger.Info("rollout counts need a full fold: the aggregate's state moved while the refresh published",
			append(fields(), "check_run_id", run.ID)...)
		return rolloutCountsNeedFold, nil
	}
	return rolloutCountsPublished, nil
}

// renderStateMoved reads again everything a render was folded from that a
// concurrent writer can move: the aggregate row and its render, the stopped
// applies, and the stored per-database rows.
func (h *Handler) renderStateMoved(ctx context.Context, repo string, pr int, environment string, raw []byte, render aggregateRender) (bool, error) {
	checks := h.service.Storage().Checks()
	current, err := checks.GetAggregateRender(ctx, repo, pr, environment, aggregateSentinel, aggregateSentinel)
	if err != nil {
		return false, fmt.Errorf("reload aggregate render for %s#%d (env %s): %w", repo, pr, environment, err)
	}
	if !bytes.Equal(current, raw) {
		return true, nil
	}
	row, err := checks.Get(ctx, repo, pr, environment, aggregateSentinel, aggregateSentinel)
	if err != nil {
		return false, fmt.Errorf("reload aggregate check state for %s#%d (env %s): %w", repo, pr, environment, err)
	}
	if row == nil || row.BlockingReason != "" || !renderDescribesRow(render, row) {
		return true, nil
	}
	stopped, err := h.loadStoppedApplies(ctx, repo, pr)
	if err != nil {
		return false, fmt.Errorf("reload stopped applies for %s#%d: %w", repo, pr, err)
	}
	if !maps.Equal(stopped, render.Stopped) {
		return true, nil
	}
	stored, err := checks.GetByPR(ctx, repo, pr)
	if err != nil {
		return false, fmt.Errorf("reload stored check state for %s#%d: %w", repo, pr, err)
	}
	return inputsMoved(render, environment, stored), nil
}

// renderDescribesRow reports whether a stored render was published with the
// state the stored aggregate row holds now. A writer that moves the row
// without a fold leaves an older render behind, and that render no longer
// describes the run.
func renderDescribesRow(render aggregateRender, row *storage.Check) bool {
	return render.CheckRunID == row.CheckRunID &&
		render.HeadSHA == row.HeadSHA &&
		render.Status == row.Status &&
		render.Conclusion == row.Conclusion
}

// aggregateTargetForEnvironment names the aggregate Check Run an apply in
// environment contributes to: the environment's own run when this instance
// publishes one per environment, the single run otherwise.
func (h *Handler) aggregateTargetForEnvironment(repo, environment string) aggregateCheckTarget {
	base := h.aggregateCheckNameForRepo(repo)
	config, ok := h.serverConfig()
	if !ok || len(config.AllowedEnvironments) == 0 {
		return aggregateCheckTarget{name: base, environment: aggregateSentinel}
	}
	return aggregateCheckTarget{name: aggregateCheckNameForEnv(base, environment), environment: environment}
}
