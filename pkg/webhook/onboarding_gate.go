package webhook

import (
	"context"
	"fmt"
	"sort"
	"strings"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
)

const onboardingCheckName = "SchemaBot onboarding"

type onboardingGateResult struct {
	conclusion string
	title      string
	summary    string
}

func (h *Handler) runOnboardingGate(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA, baseRef string) error {
	if h.isAggregateParticipant(repo) || !h.shouldPublishChecks(ctx, repo, "onboarding_gate") {
		return nil
	}
	result, evaluationErr := h.evaluateOnboardingGate(ctx, client, repo, headSHA, baseRef)
	if result.title == "" {
		result = onboardingGateFailure("Onboarding verification unavailable", evaluationErr.Error())
	}
	if err := h.publishOnboardingCheck(ctx, client, repo, pr, headSHA, baseRef, result); err != nil {
		return err
	}
	return evaluationErr
}

func (h *Handler) evaluateOnboardingGate(ctx context.Context, client *ghclient.InstallationClient, repo, headSHA, baseRef string) (onboardingGateResult, error) {
	baseSHA, err := client.ResolveBranchTip(ctx, repo, baseRef)
	if err != nil {
		return onboardingGateResult{}, fmt.Errorf("resolve current base branch %s: %w", baseRef, err)
	}
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
			strings.Join(failures, "\n")+"\n\nRefresh the declarative schema from the current base and advance `legacy_baseline.base_commit` in the same commit.",
		), nil
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

func (h *Handler) publishOnboardingCheck(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA, baseRef string, result onboardingGateResult) error {
	current, err := client.FetchPullRequestNoCache(ctx, repo, pr)
	if err != nil {
		return fmt.Errorf("re-read PR before publishing onboarding check: %w", err)
	}
	if current.HeadSHA != headSHA || current.BaseRef != baseRef {
		h.logger.Info("discarding onboarding check because the PR moved during verification",
			"repo", repo, "pr", pr, "evaluated_head_sha", headSHA, "current_head_sha", current.HeadSHA,
			"evaluated_base_ref", baseRef, "current_base_ref", current.BaseRef)
		return nil
	}
	opts := ghclient.CheckRunOptions{
		Name:       onboardingCheckName,
		Status:     checkStatusCompleted,
		Conclusion: result.conclusion,
		Output: &ghclient.CheckRunOutput{
			Title:   result.title,
			Summary: result.summary,
		},
	}
	existing, _, findErr := client.FindCheckRunByName(ctx, repo, headSHA, onboardingCheckName)
	if findErr != nil {
		h.logger.Warn("could not look up existing onboarding check; creating a new one",
			"repo", repo, "pr", pr, "head_sha", headSHA, "error", findErr)
	}
	action := "created"
	if findErr == nil && existing != nil {
		if err := client.UpdateCheckRun(ctx, repo, existing.ID, opts); err != nil {
			return fmt.Errorf("update onboarding check for %s#%d@%s: %w", repo, pr, headSHA, err)
		}
		action = "updated"
	} else if _, err := client.CreateCheckRun(ctx, repo, headSHA, opts); err != nil {
		return fmt.Errorf("create onboarding check for %s#%d@%s: %w", repo, pr, headSHA, err)
	}
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:  "onboarding_gate",
		Repository: repo,
		Status:     result.conclusion,
	})
	h.logger.Info("published onboarding gate", "repo", repo, "pr", pr, "head_sha", headSHA,
		"conclusion", result.conclusion, "action", action)
	return nil
}

func (h *Handler) postPassingMergeGroupOnboardingCheck(ctx context.Context, client *ghclient.InstallationClient, repo, headSHA string) error {
	opts := ghclient.CheckRunOptions{
		Name:       onboardingCheckName,
		Status:     checkStatusCompleted,
		Conclusion: checkConclusionSuccess,
		Output: &ghclient.CheckRunOutput{
			Title:   "Onboarding verified before merge queue",
			Summary: "The pull request onboarding gate passed before this change entered the merge queue.",
		},
	}
	existing, _, findErr := client.FindCheckRunByName(ctx, repo, headSHA, onboardingCheckName)
	if findErr == nil && existing != nil {
		return client.UpdateCheckRun(ctx, repo, existing.ID, opts)
	}
	_, err := client.CreateCheckRun(ctx, repo, headSHA, opts)
	return err
}
