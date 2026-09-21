package webhook

import (
	"context"
	"fmt"
	"sort"
	"strings"

	ghclient "github.com/block/schemabot/pkg/github"
)

type onboardingGateResult struct {
	baseSHA    string
	conclusion string
	title      string
	summary    string
}

func (h *Handler) evaluateOnboardingGate(ctx context.Context, client *ghclient.InstallationClient, repo, headSHA, baseRef string) (onboardingGateResult, error) {
	baseSHA, err := client.ResolveBranchTip(ctx, repo, baseRef)
	if err != nil {
		return onboardingGateResult{}, fmt.Errorf("resolve current base branch %s: %w", baseRef, err)
	}
	result, err := h.evaluateOnboardingGateAtBase(ctx, client, repo, headSHA, baseSHA)
	result.baseSHA = baseSHA
	return result, err
}

func (h *Handler) evaluateOnboardingGateAtBase(ctx context.Context, client *ghclient.InstallationClient, repo, headSHA, baseSHA string) (onboardingGateResult, error) {
	baseConfigs, err := client.FindAllConfigs(ctx, repo, baseSHA)
	if err != nil {
		return onboardingGateResult{}, fmt.Errorf("discover base configs at %s: %w", baseSHA, err)
	}
	headConfigs, err := client.FindAllConfigs(ctx, repo, headSHA)
	if err != nil {
		return onboardingGateResult{}, fmt.Errorf("discover head configs at %s: %w", headSHA, err)
	}
	introduced, err := ghclient.IntroducedConfigs(baseConfigs, headConfigs)
	if err != nil {
		return onboardingGateFailure("Onboarding classification failed", err.Error()), nil
	}
	if len(introduced) == 0 {
		return onboardingGateResult{
			conclusion: checkConclusionSuccess,
			title:      "Onboarding gate not applicable",
			summary:    "Not applicable: no database configuration introduced.",
		}, nil
	}

	var failures []string
	var verified []string
	for _, discovered := range introduced {
		database := discovered.Config.Database
		baseline := discovered.Config.LegacyBaseline
		if baseline == nil {
			h.logger.Debug("skipping optional legacy verification because no baseline is configured", "repo", repo, "database", database, "head_sha", headSHA)
			continue
		}
		if err := baseline.Validate(); err != nil {
			failures = append(failures, fmt.Sprintf("- `%s` (`%s`): %s", database, discovered.Path, err))
			continue
		}
		changes, historyErr := client.LegacyPathChangesSinceAnchor(ctx, repo, baseline.BaseCommit, baseSHA, baseline.LegacyPaths)
		if historyErr != nil {
			if ghclient.IsUnavailableError(historyErr) {
				return onboardingGateResult{}, fmt.Errorf("verify legacy baseline for %s: %w", database, historyErr)
			}
			failures = append(failures, fmt.Sprintf("- `%s`: %s", database, historyErr))
			continue
		}
		if len(changes) > 0 {
			failures = append(failures, renderLegacyPathChanges(database, changes)...)
			continue
		}
		verified = append(verified, fmt.Sprintf("- `%s`: legacy paths are unchanged through base `%s`.", database, shortSHA(baseSHA)))
	}

	if len(failures) > 0 {
		sort.Strings(failures)
		return onboardingGateFailure(
			"Onboarding legacy baseline is stale or invalid",
			strings.Join(failures, "\n")+"\n\nCorrect invalid `legacy_baseline` metadata. If the legacy source changed, refresh the declarative schema from the current base and advance `legacy_baseline.base_commit` in the same commit.",
		), nil
	}
	if len(verified) == 0 {
		return onboardingGateResult{
			conclusion: checkConclusionSuccess,
			title:      "Onboarding gate not applicable",
			summary:    "Not applicable: no introduced database configures legacy verification.",
		}, nil
	}
	sort.Strings(verified)
	return onboardingGateResult{
		conclusion: checkConclusionSuccess,
		title:      "Onboarding legacy baseline is current",
		summary: strings.Join(verified, "\n") +
			"\n\nProduction convergence is enforced independently by the production SchemaBot plan check.",
	}, nil
}

func onboardingGateFailure(title, summary string) onboardingGateResult {
	return onboardingGateResult{conclusion: checkConclusionFailure, title: title, summary: summary}
}

func renderLegacyPathChanges(database string, changes []ghclient.LegacyPathChange) []string {
	lines := make([]string, 0, len(changes))
	for _, change := range changes {
		title := change.Title
		if title == "" {
			title = "commit changed the legacy path"
		}
		lines = append(lines, fmt.Sprintf("- `%s`: `%s` changed `%s` — %s", database, shortSHA(change.Commit), change.Path, title))
	}
	return lines
}

func shortSHA(sha string) string {
	if len(sha) <= 12 {
		return sha
	}
	return sha[:12]
}

// gateAggregateSuccess verifies onboarding at every PR aggregate publication
// that would otherwise pass. Plans, apply completion, participant re-folds, and
// no-schema checks all use this boundary, so none can clear an onboarding
// failure without re-evaluating it. Participants leave this repo-wide gate to
// their aggregate leader.
//
// A failed read or a moving base produces a failing check and a retryable error.
// A superseded PR head produces no write. The caller publishes the blocking
// result before returning the error to its retry owner.
func (h *Handler) gateAggregateSuccess(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, opts *ghclient.CheckRunOptions) (bool, error) {
	if opts.Conclusion != checkConclusionSuccess || h.isAggregateParticipant(repo) {
		return true, nil
	}
	prInfo, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		return h.failAggregateOnboarding(repo, pr, headSHA, opts, fmt.Errorf("read PR for onboarding verification: %w", err))
	}
	if prInfo.HeadSHA != headSHA || prInfo.IsClosed() {
		h.logger.Info("skipping onboarding publication for a superseded or closed PR", "repo", repo, "pr", pr, "head_sha", headSHA, "current_head_sha", prInfo.HeadSHA)
		return false, nil
	}
	if prInfo.BaseRef == "" {
		return h.failAggregateOnboarding(repo, pr, headSHA, opts, fmt.Errorf("PR %s#%d has no base branch", repo, pr))
	}
	result, evaluationErr := h.evaluateOnboardingGate(ctx, client, repo, headSHA, prInfo.BaseRef)
	if evaluationErr != nil {
		return h.failAggregateOnboarding(repo, pr, headSHA, opts, evaluationErr)
	}

	current, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		return h.failAggregateOnboarding(repo, pr, headSHA, opts, fmt.Errorf("re-read PR before publishing onboarding result: %w", err))
	}
	if current.HeadSHA != headSHA || current.IsClosed() {
		h.logger.Info("discarding onboarding result because the PR moved or closed during verification", "repo", repo, "pr", pr, "head_sha", headSHA, "current_head_sha", current.HeadSHA)
		return false, nil
	}
	if current.BaseRef != prInfo.BaseRef {
		return h.failAggregateOnboarding(repo, pr, headSHA, opts, fmt.Errorf("PR base branch changed during onboarding verification: %s to %s", prInfo.BaseRef, current.BaseRef))
	}
	if result.conclusion == checkConclusionSuccess {
		// Resolve the branch itself: the PR's base metadata is not the pinned
		// branch-tip read that the history and config comparison evaluated.
		currentBaseSHA, err := client.ResolveBranchTip(ctx, repo, current.BaseRef)
		if err != nil {
			return h.failAggregateOnboarding(repo, pr, headSHA, opts, fmt.Errorf("re-read base branch before publishing onboarding result: %w", err))
		}
		if currentBaseSHA != result.baseSHA {
			return h.failAggregateOnboarding(repo, pr, headSHA, opts, fmt.Errorf("base branch %s advanced during onboarding verification: %s to %s", current.BaseRef, result.baseSHA, currentBaseSHA))
		}
		return true, nil
	}
	opts.Status = checkStatusCompleted
	opts.Conclusion = checkConclusionFailure
	opts.Output = &ghclient.CheckRunOutput{Title: result.title, Summary: sanitizeCheckRunErrorSummary(result.summary)}
	h.logger.Info("onboarding verification blocks aggregate", "repo", repo, "pr", pr, "head_sha", headSHA, "base_sha", result.baseSHA)
	return true, nil
}

func (h *Handler) failAggregateOnboarding(repo string, pr int, headSHA string, opts *ghclient.CheckRunOptions, err error) (bool, error) {
	opts.Status = checkStatusCompleted
	opts.Conclusion = checkConclusionFailure
	opts.Output = &ghclient.CheckRunOutput{
		Title:   "Onboarding verification unavailable",
		Summary: "SchemaBot could not verify onboarding against the current base branch. Retry this check; see server logs for details.",
	}
	h.logger.Error("onboarding verification failed; aggregate will block merge", "repo", repo, "pr", pr, "head_sha", headSHA, "error", err)
	return true, fmt.Errorf("verify onboarding for %s#%d@%s: %w", repo, pr, headSHA, err)
}
