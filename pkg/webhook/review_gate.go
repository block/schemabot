package webhook

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hmarr/codeowners"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// ReviewGateResult contains the outcome of a review gate check.
type ReviewGateResult struct {
	Approved          bool
	RequiredReviewers []string
	PRAuthor          string
}

// enforceReviewGate runs the review gate check and posts the appropriate comment if blocked.
// Returns true if the apply was blocked (caller should return), false if it may proceed.
func (h *Handler) enforceReviewGate(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult, environment, requestedBy, commandName string) bool {
	gateResult, err := h.checkReviewGate(ctx, client, repo, pr, schemaResult.Database, schemaResult.SchemaPath)
	if err != nil {
		h.logger.Error("review gate check failed", "error", err)
		h.postCommandError(repo, pr, installationID, commandName, environment, requestedBy, reviewGateErrorDetail(err))
		return true
	}
	if gateResult != nil && !gateResult.Approved {
		h.postComment(repo, pr, installationID, templates.RenderReviewRequired(templates.ReviewGateData{
			Database:    schemaResult.Database,
			Environment: environment,
			RequestedBy: requestedBy,
			Reviewers:   gateResult.RequiredReviewers,
			PRAuthor:    gateResult.PRAuthor,
		}))
		return true
	}
	return false
}

func reviewGateErrorDetail(err error) string {
	detail := "Review gate check failed: " + err.Error()
	if errors.Is(err, ghclient.ErrTeamMembershipUnreadable) {
		detail += ". If approval is granted through a GitHub team, verify the GitHub App can read organization members and team membership."
	}
	return detail
}

// checkReviewGate checks if the PR has approval from a configured review policy principal.
// Returns nil if review gating is disabled (apply proceeds).
// Returns a result with Approved=true if gate passes.
// Returns a result with Approved=false if gate blocks.
// schemaPath is the repo-relative path to the database's schema directory (e.g. "schema/payments").
func (h *Handler) checkReviewGate(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, database, schemaPath string) (*ReviewGateResult, error) {
	if !h.isReviewGateEnabled(repo) {
		return nil, nil
	}

	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("fetch PR info: %w", err)
	}

	reviews, err := client.ListReviews(ctx, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("fetch PR reviews: %w", err)
	}

	policy, err := h.loadReviewGatePolicy(ctx, client, repo, prInfo.BaseRef, database, schemaPath)
	if err != nil {
		return nil, err
	}
	if len(policy.RequiredReviewers) == 0 {
		return nil, fmt.Errorf("review policy has no configured reviewers for database %q", database)
	}

	approvedReviewers := ghclient.GetApprovedReviewers(reviews)
	h.logger.Info("review gate: fetched reviews",
		"repo", repo, "pr", pr, "database", database,
		"approved_by", approvedReviewers, "pr_author", prInfo.User)

	var validApprovers []string
	for _, reviewer := range approvedReviewers {
		if !strings.EqualFold(reviewer, prInfo.User) {
			validApprovers = append(validApprovers, reviewer)
		}
	}

	for _, reviewer := range validApprovers {
		matched, principal, err := policy.Matches(ctx, client, reviewer)
		if err != nil {
			return nil, err
		}
		if matched {
			h.logger.Info("review gate: approved",
				"repo", repo, "pr", pr, "database", database,
				"approved_by", reviewer, "matched_principal", principal,
				"required_reviewers", policy.RequiredReviewers)
			return &ReviewGateResult{
				Approved:          true,
				RequiredReviewers: policy.RequiredReviewers,
				PRAuthor:          prInfo.User,
			}, nil
		}
	}

	h.logger.Info("review gate: blocked",
		"repo", repo, "pr", pr, "database", database,
		"valid_approvers", validApprovers, "required_reviewers", policy.RequiredReviewers)
	return &ReviewGateResult{
		Approved:          false,
		RequiredReviewers: policy.RequiredReviewers,
		PRAuthor:          prInfo.User,
	}, nil
}

type reviewGatePolicy struct {
	AdminTeams         []string
	AdminUsers         []string
	RepoAdminTeams     []string
	RepoAdminUsers     []string
	OperatorTeams      []string
	OperatorUsers      []string
	CodeownerReviewers map[string]struct{}
	RequiredReviewers  []string
}

func (p reviewGatePolicy) Matches(ctx context.Context, client *ghclient.InstallationClient, reviewer string) (bool, string, error) {
	if len(p.AdminTeams) > 0 {
		matched, principal, err := actorInAnyTeam(ctx, client, p.AdminTeams, reviewer)
		if err != nil {
			return false, "", err
		}
		if matched {
			return true, principal, nil
		}
	}
	if matched, principal := matchedUserPrincipal(p.AdminUsers, reviewer); matched {
		return true, principal, nil
	}
	if len(p.RepoAdminTeams) > 0 {
		matched, principal, err := actorInAnyTeam(ctx, client, p.RepoAdminTeams, reviewer)
		if err != nil {
			return false, "", err
		}
		if matched {
			return true, principal, nil
		}
	}
	if matched, principal := matchedUserPrincipal(p.RepoAdminUsers, reviewer); matched {
		return true, principal, nil
	}
	if len(p.OperatorTeams) > 0 {
		matched, principal, err := actorInAnyTeam(ctx, client, p.OperatorTeams, reviewer)
		if err != nil {
			return false, "", err
		}
		if matched {
			return true, principal, nil
		}
	}
	if matched, principal := matchedUserPrincipal(p.OperatorUsers, reviewer); matched {
		return true, principal, nil
	}
	if _, ok := p.CodeownerReviewers[strings.ToLower(reviewer)]; ok {
		return true, "CODEOWNERS", nil
	}
	return false, "", nil
}

func (h *Handler) loadReviewGatePolicy(ctx context.Context, client *ghclient.InstallationClient, repo, baseRef, database, schemaPath string) (reviewGatePolicy, error) {
	if h.service == nil || h.service.Config() == nil {
		return reviewGatePolicy{}, fmt.Errorf("server config is unavailable")
	}
	config := h.service.Config()
	reviewPolicy := config.ReviewPolicy
	repoAdminTeams, repoAdminUsers := config.RepoAdmins(repo)
	policy := reviewGatePolicy{
		AdminTeams:         reviewPolicy.AdminTeams,
		AdminUsers:         reviewPolicy.AdminUsers,
		RepoAdminTeams:     repoAdminTeams,
		RepoAdminUsers:     repoAdminUsers,
		CodeownerReviewers: make(map[string]struct{}),
	}
	appendRequiredReviewers := func(values ...[]string) {
		for _, group := range values {
			for _, value := range group {
				policy.RequiredReviewers = appendUniqueString(policy.RequiredReviewers, value)
			}
		}
	}
	appendRequiredReviewers(reviewPolicy.AdminTeams, reviewPolicy.AdminUsers, repoAdminTeams, repoAdminUsers)

	if config.ReviewPolicyIncludesDatabaseOperators() {
		dbConfig := config.Database(database)
		if dbConfig == nil {
			return reviewGatePolicy{}, fmt.Errorf("database %q is not configured for review policy", database)
		}
		policy.OperatorTeams = dbConfig.OperatorTeams
		policy.OperatorUsers = dbConfig.OperatorUsers
		appendRequiredReviewers(dbConfig.OperatorTeams, dbConfig.OperatorUsers)
	}

	if reviewPolicy.IncludeCodeowners {
		owners, err := matchReviewGateCodeowners(ctx, client, repo, baseRef, schemaPath)
		if err != nil {
			return reviewGatePolicy{}, err
		}
		for _, owner := range owners {
			if ghclient.IsTeamOwner(owner) {
				org, slug := ghclient.TeamParts(owner)
				members, err := client.ListTeamMembers(ctx, org, slug)
				if err != nil {
					return reviewGatePolicy{}, fmt.Errorf("expand CODEOWNERS team %s: %w", owner.String(), err)
				}
				for _, member := range members {
					policy.CodeownerReviewers[strings.ToLower(member)] = struct{}{}
				}
			} else {
				policy.CodeownerReviewers[strings.ToLower(owner.Value)] = struct{}{}
			}
		}
		appendRequiredReviewers(ghclient.OwnerNames(owners))
	}

	return policy, nil
}

func matchReviewGateCodeowners(ctx context.Context, client *ghclient.InstallationClient, repo, baseRef, schemaPath string) ([]codeowners.Owner, error) {
	ruleset, err := client.FetchCodeownersRuleset(ctx, repo, baseRef)
	if err != nil {
		return nil, fmt.Errorf("fetch CODEOWNERS: %w", err)
	}
	if ruleset == nil {
		return nil, nil
	}

	var owners []codeowners.Owner
	if schemaPath != "" {
		matchPath := strings.TrimSuffix(schemaPath, "/") + "/.schema"
		owners, err = ghclient.MatchCodeownersPath(ruleset, matchPath)
		if err != nil {
			return nil, fmt.Errorf("match CODEOWNERS for %s: %w", schemaPath, err)
		}
	}
	if owners == nil {
		owners = ghclient.OwnersFromRuleset(ruleset)
	}
	return owners, nil
}

// isReviewGateEnabled checks server config for the review gate toggle.
func (h *Handler) isReviewGateEnabled(repo string) bool {
	enabled := h.service != nil && h.service.Config().ReviewPolicyEnabled()
	h.logger.Debug("review gate: server config", "repo", repo, "enabled", enabled)
	return enabled
}

func appendUniqueString(values []string, value string) []string {
	for _, existing := range values {
		if strings.EqualFold(existing, value) {
			return values
		}
	}
	return append(values, value)
}
