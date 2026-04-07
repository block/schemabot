package github

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	gh "github.com/google/go-github/v68/github"
	"github.com/hmarr/codeowners"
)

// Review states returned by the GitHub API.
const (
	ReviewApproved         = "APPROVED"
	ReviewChangesRequested = "CHANGES_REQUESTED"
	ReviewCommented        = "COMMENTED"
	ReviewDismissed        = "DISMISSED"
)

// ReviewInfo holds a single PR review.
type ReviewInfo struct {
	User        string
	State       string // One of the Review* constants.
	SubmittedAt time.Time
}

// ListReviews fetches all reviews for a PR (paginated).
func (ic *InstallationClient) ListReviews(ctx context.Context, repo string, pr int) ([]*ReviewInfo, error) {
	owner, repoName := splitRepo(repo)
	opts := &gh.ListOptions{PerPage: 100}
	var allReviews []*ReviewInfo

	for {
		reviews, resp, err := ic.client.PullRequests.ListReviews(ctx, owner, repoName, pr, opts)
		if err != nil {
			return nil, fmt.Errorf("list PR reviews: %w", err)
		}
		for _, r := range reviews {
			allReviews = append(allReviews, &ReviewInfo{
				User:        r.GetUser().GetLogin(),
				State:       r.GetState(),
				SubmittedAt: r.GetSubmittedAt().Time,
			})
		}
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}

	return allReviews, nil
}

// GetApprovedReviewers returns usernames whose latest review is APPROVED.
// Reviews are processed in chronological order; only the most recent review
// per user counts.
func GetApprovedReviewers(reviews []*ReviewInfo) []string {
	// Track each user's latest review by timestamp
	type latestReview struct {
		State       string
		SubmittedAt time.Time
	}
	latest := make(map[string]latestReview)
	for _, r := range reviews {
		if r.User == "" {
			continue
		}
		key := strings.ToLower(r.User)
		if cur, ok := latest[key]; !ok || r.SubmittedAt.After(cur.SubmittedAt) {
			latest[key] = latestReview{State: r.State, SubmittedAt: r.SubmittedAt}
		}
	}

	var approved []string
	for user, info := range latest {
		if strings.EqualFold(info.State, ReviewApproved) {
			approved = append(approved, user)
		}
	}
	return approved
}

// FetchCodeownersRuleset fetches CODEOWNERS from standard locations and returns a parsed ruleset.
// Uses github.com/hmarr/codeowners for standard CODEOWNERS pattern matching.
// Returns nil (not error) if no CODEOWNERS file exists.
func (ic *InstallationClient) FetchCodeownersRuleset(ctx context.Context, repo, ref string) (codeowners.Ruleset, error) {
	codeownerPaths := []string{
		".github/CODEOWNERS",
		"CODEOWNERS",
		"docs/CODEOWNERS",
	}

	for _, p := range codeownerPaths {
		content, err := ic.FetchFileContent(ctx, repo, p, ref)
		if err != nil {
			if IsNotFoundError(err) {
				continue
			}
			return nil, fmt.Errorf("fetch CODEOWNERS from %s: %w", p, err)
		}
		ruleset, err := codeowners.ParseFile(bytes.NewBufferString(content))
		if err != nil {
			return nil, fmt.Errorf("parse CODEOWNERS from %s: %w", p, err)
		}
		return ruleset, nil
	}

	return nil, nil
}

// FetchCodeowners fetches CODEOWNERS from standard locations in the repo.
// Returns all owners across all rules (not path-scoped).
// Returns empty slice (not error) if no CODEOWNERS file exists.
func (ic *InstallationClient) FetchCodeowners(ctx context.Context, repo, ref string) ([]string, error) {
	ruleset, err := ic.FetchCodeownersRuleset(ctx, repo, ref)
	if err != nil {
		return nil, err
	}
	if ruleset == nil {
		return nil, nil
	}
	return OwnerNames(OwnersFromRuleset(ruleset)), nil
}

// MatchCodeownersPath finds the owners for a specific file path using CODEOWNERS rules.
// Last matching rule wins (standard CODEOWNERS semantics).
// Returns nil, nil if no rule matches. Returns an error if pattern matching fails.
func MatchCodeownersPath(ruleset codeowners.Ruleset, filePath string) ([]codeowners.Owner, error) {
	rule, err := ruleset.Match(filePath)
	if err != nil {
		return nil, fmt.Errorf("match CODEOWNERS path %s: %w", filePath, err)
	}
	if rule == nil {
		return nil, nil
	}
	return rule.Owners, nil
}

// OwnersFromRuleset extracts all unique owners from a ruleset.
func OwnersFromRuleset(ruleset codeowners.Ruleset) []codeowners.Owner {
	seen := make(map[string]bool)
	var owners []codeowners.Owner
	for _, rule := range ruleset {
		for _, o := range rule.Owners {
			key := strings.ToLower(o.String())
			if !seen[key] {
				seen[key] = true
				owners = append(owners, o)
			}
		}
	}
	return owners
}

// IsTeamOwner returns true if the owner is a GitHub team (org/team format).
func IsTeamOwner(o codeowners.Owner) bool {
	return o.Type == codeowners.TeamOwner
}

// OwnerName returns the owner name without @ prefix.
func OwnerName(o codeowners.Owner) string {
	return strings.TrimPrefix(o.String(), "@")
}

// TeamParts splits a team owner "org/team" into org and team slug.
func TeamParts(o codeowners.Owner) (org, slug string) {
	parts := strings.SplitN(o.Value, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return o.Value, ""
}

// ParseCodeowners parses CODEOWNERS content and returns unique owner names without @ prefix.
func ParseCodeowners(content string) []string {
	ruleset, err := codeowners.ParseFile(bytes.NewBufferString(content))
	if err != nil {
		return nil
	}
	return OwnerNames(OwnersFromRuleset(ruleset))
}

// OwnerNames converts a slice of codeowners.Owner to string names without @ prefix.
func OwnerNames(owners []codeowners.Owner) []string {
	result := make([]string, len(owners))
	for i, o := range owners {
		result[i] = OwnerName(o)
	}
	return result
}

// ListTeamMembers returns the login names of all members of a GitHub team.
// The owner parameter is the org slug (e.g. "octocat"), and slug is the team
// slug (e.g. "dba-team"). Returns an error if the team is not found.
func (ic *InstallationClient) ListTeamMembers(ctx context.Context, org, slug string) ([]string, error) {
	opts := &gh.TeamListTeamMembersOptions{ListOptions: gh.ListOptions{PerPage: 100}}
	var members []string

	for {
		users, resp, err := ic.client.Teams.ListTeamMembersBySlug(ctx, org, slug, opts)
		if err != nil {
			return nil, fmt.Errorf("list team members %s/%s: %w", org, slug, err)
		}
		for _, u := range users {
			members = append(members, u.GetLogin())
		}
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}

	return members, nil
}

// RequestReviewers requests reviews from users and/or teams on a PR.
func (ic *InstallationClient) RequestReviewers(ctx context.Context, repo string, pr int, users, teams []string) error {
	owner, repoName := splitRepo(repo)
	_, _, err := ic.client.PullRequests.RequestReviewers(ctx, owner, repoName, pr, gh.ReviewersRequest{
		Reviewers:     users,
		TeamReviewers: teams,
	})
	if err != nil {
		return fmt.Errorf("request reviewers: %w", err)
	}
	return nil
}
