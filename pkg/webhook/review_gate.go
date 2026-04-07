package webhook

import (
	"context"
	"fmt"
	"strings"

	"github.com/hmarr/codeowners"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// ReviewGateResult contains the outcome of a review gate check.
type ReviewGateResult struct {
	Approved bool
	Owners   []string // CODEOWNERS owner display names
	PRAuthor string
}

// enforceReviewGate runs the review gate check and posts the appropriate comment if blocked.
// Returns true if the apply was blocked (caller should return), false if it may proceed.
func (h *Handler) enforceReviewGate(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult, environment, requestedBy, commandName string) bool {
	gateResult, err := h.checkReviewGate(ctx, client, repo, pr, schemaResult.SchemaPath)
	if err != nil {
		h.logger.Error("review gate check failed", "error", err)
		h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
			RequestedBy: requestedBy,
			Environment: environment,
			CommandName: commandName,
			ErrorDetail: "Review gate check failed: " + err.Error() + ". An admin can run schema changes directly via the CLI to bypass the review gate.",
		}))
		return true
	}
	if gateResult != nil && !gateResult.Approved {
		h.postComment(repo, pr, installationID, templates.RenderReviewRequired(templates.ReviewGateData{
			Database:    schemaResult.Database,
			Environment: environment,
			RequestedBy: requestedBy,
			Owners:      gateResult.Owners,
			PRAuthor:    gateResult.PRAuthor,
		}))
		return true
	}
	return false
}

// checkReviewGate checks if the PR has CODEOWNERS approval for the given schema path.
// Returns nil if review gating is disabled (apply proceeds).
// Returns a result with Approved=true if gate passes.
// Returns a result with Approved=false if gate blocks.
// schemaPath is the repo-relative path to the database's schema directory (e.g. "schema/payments").
func (h *Handler) checkReviewGate(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, schemaPath string) (*ReviewGateResult, error) {
	enabled, err := h.isReviewGateEnabled(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("check review gate setting: %w", err)
	}
	if !enabled {
		return nil, nil
	}

	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("fetch PR info: %w", err)
	}

	// Fetch CODEOWNERS from the base branch (not head) to prevent a PR from
	// relaxing its own approval requirements by modifying CODEOWNERS.
	ruleset, err := client.FetchCodeownersRuleset(ctx, repo, prInfo.BaseRef)
	if err != nil {
		return nil, fmt.Errorf("fetch CODEOWNERS: %w", err)
	}

	reviews, err := client.ListReviews(ctx, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("fetch PR reviews: %w", err)
	}

	approvedReviewers := ghclient.GetApprovedReviewers(reviews)
	h.logger.Info("review gate: fetched reviews", "repo", repo, "pr", pr, "approved_by", approvedReviewers, "pr_author", prInfo.User)

	// Filter out self-approval
	var validApprovers []string
	for _, reviewer := range approvedReviewers {
		if !strings.EqualFold(reviewer, prInfo.User) {
			validApprovers = append(validApprovers, reviewer)
		}
	}

	// Determine owners: path-scoped if CODEOWNERS exists, otherwise nil.
	// Match against a representative file within the schema directory so that
	// CODEOWNERS directory patterns (e.g. "schema/payments/") match correctly.
	var owners []codeowners.Owner
	if ruleset != nil && schemaPath != "" {
		matchPath := strings.TrimSuffix(schemaPath, "/") + "/.schema"
		var err error
		owners, err = ghclient.MatchCodeownersPath(ruleset, matchPath)
		if err != nil {
			return nil, fmt.Errorf("match CODEOWNERS for %s: %w", schemaPath, err)
		}
		h.logger.Info("review gate: matched CODEOWNERS for path", "schema_path", schemaPath, "owners", ghclient.OwnerNames(owners))
	}
	if owners == nil && ruleset != nil {
		owners = ghclient.OwnersFromRuleset(ruleset)
		h.logger.Info("review gate: no path-specific match, using all CODEOWNERS", "owners", ghclient.OwnerNames(owners))
	}

	// No CODEOWNERS file — require any non-self approval
	if len(owners) == 0 {
		approved := len(validApprovers) > 0
		h.logger.Info("review gate: no CODEOWNERS, requiring any approval", "approved", approved, "valid_approvers", validApprovers)
		return &ReviewGateResult{
			Approved: approved,
			PRAuthor: prInfo.User,
		}, nil
	}

	// Build the set of individual logins that satisfy the gate.
	// Team owners are expanded to their members via GitHub API.
	ownerSet := make(map[string]bool, len(owners))
	for _, o := range owners {
		if ghclient.IsTeamOwner(o) {
			org, slug := ghclient.TeamParts(o)
			members, err := client.ListTeamMembers(ctx, org, slug)
			if err != nil {
				return nil, fmt.Errorf("expand team %s: %w", o.String(), err)
			}
			for _, m := range members {
				ownerSet[strings.ToLower(m)] = true
			}
		} else {
			ownerSet[strings.ToLower(o.Value)] = true
		}
	}

	ownerNames := ghclient.OwnerNames(owners)
	for _, reviewer := range validApprovers {
		if ownerSet[strings.ToLower(reviewer)] {
			h.logger.Info("review gate: approved", "repo", repo, "pr", pr, "approved_by", reviewer, "owners", ownerNames)
			return &ReviewGateResult{
				Approved: true,
				Owners:   ownerNames,
				PRAuthor: prInfo.User,
			}, nil
		}
	}

	h.logger.Info("review gate: blocked", "repo", repo, "pr", pr, "valid_approvers", validApprovers, "required_owners", ownerNames)
	return &ReviewGateResult{
		Approved: false,
		Owners:   ownerNames,
		PRAuthor: prInfo.User,
	}, nil
}

// isReviewGateEnabled checks settings for the review gate toggle.
// Checks repo-specific key first (require_review:<repo>), then global (require_review).
func (h *Handler) isReviewGateEnabled(ctx context.Context, repo string) (bool, error) {
	settings := h.service.Storage().Settings()

	repoSetting, err := settings.Get(ctx, "require_review:"+repo)
	if err != nil {
		return false, err
	}
	if repoSetting != nil {
		enabled := repoSetting.Value == "true"
		h.logger.Debug("review gate: repo-specific setting", "repo", repo, "enabled", enabled)
		return enabled, nil
	}

	globalSetting, err := settings.Get(ctx, "require_review")
	if err != nil {
		return false, err
	}
	if globalSetting != nil {
		enabled := globalSetting.Value == "true"
		h.logger.Debug("review gate: global setting", "enabled", enabled)
		return enabled, nil
	}

	h.logger.Debug("review gate: no setting found, disabled by default")
	return false, nil
}
