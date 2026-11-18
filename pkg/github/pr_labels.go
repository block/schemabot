package github

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	gh "github.com/google/go-github/v86/github"
)

// PRLabelState records the latest matching label and revision events visible
// in the terminal page of the GitHub timeline. The latter distinguishes revisions
// that return to a previously seen head or base.
type PRLabelState struct {
	EventID    int64
	RevisionID int64
	Present    bool
	Actor      string
	ChangedAt  time.Time
}

func (p *PullRequestInfo) HasLabel(label string) bool {
	return slices.ContainsFunc(p.Labels, func(name string) bool { return strings.EqualFold(name, label) })
}

func (ic *InstallationClient) FetchPRLabelState(ctx context.Context, repo string, pr int, label string) (PRLabelState, error) {
	owner, name := splitRepo(repo)
	opts := &gh.ListOptions{PerPage: 100}
	// Jump directly to the last page. Only a single terminal page proves
	// continuity from a visible label event through the latest revision.
	for {
		var response *gh.Response
		events, err := retryGitHubUnavailableRead(ctx, ic, metrics.GitHubOperationUnknown, "list PR label and revision events", []any{"repo", repo, "pr", pr}, func(ctx context.Context) ([]*gh.Timeline, error) {
			events, resp, err := ic.client.Issues.ListIssueTimeline(ctx, owner, name, pr, opts)
			response = resp
			return events, classifyGitHubAPIError(err)
		})
		if err != nil {
			return PRLabelState{}, fmt.Errorf("list PR timeline for %s#%d: %w", repo, pr, err)
		}
		if response.NextPage != 0 {
			if opts.Page != 0 || response.LastPage <= 1 {
				// The tail moved or GitHub supplied no last-page link.
				// An empty state cannot authorize the acknowledgment.
				return PRLabelState{}, nil
			}
			opts.Page = response.LastPage
			continue
		}
		var state PRLabelState
		for _, event := range events {
			switch event.GetEvent() {
			case "head_ref_force_pushed", "base_ref_force_pushed", "base_ref_changed":
				state.RevisionID = max(state.RevisionID, event.GetID())
			case "labeled", "unlabeled":
				if event.GetID() > state.EventID && strings.EqualFold(event.GetLabel().GetName(), label) {
					state.EventID = event.GetID()
					state.Present = event.GetEvent() == "labeled"
					state.Actor = event.GetActor().GetLogin()
					state.ChangedAt = event.GetCreatedAt().Time
				}
			}
		}
		return state, nil
	}
}

func (ic *InstallationClient) RemovePRLabel(ctx context.Context, repo string, pr int, label string) error {
	owner, name := splitRepo(repo)
	_, err := ic.client.Issues.RemoveLabelForIssue(ctx, owner, name, pr, label)
	if err != nil && !IsNotFoundError(err) {
		return fmt.Errorf("remove label %q from %s#%d: %w", label, repo, pr, err)
	}
	return nil
}

// FindOwnPRComment lets a retry recover a comment whose remote create
// succeeded before its ID could be committed to local storage.
func (ic *InstallationClient) FindOwnPRComment(ctx context.Context, repo string, pr int, marker string) (int64, error) {
	slug := ic.loadAppSlug()
	if slug == "" {
		return 0, nil
	}
	owner, name := splitRepo(repo)
	opts := &gh.IssueListCommentsOptions{ListOptions: gh.ListOptions{PerPage: 100}}
	for {
		var response *gh.Response
		comments, err := retryGitHubUnavailableRead(ctx, ic, metrics.GitHubOperationUnknown, "find SchemaBot PR comment", []any{"repo", repo, "pr", pr}, func(ctx context.Context) ([]*gh.IssueComment, error) {
			comments, resp, err := ic.client.Issues.ListComments(ctx, owner, name, pr, opts)
			response = resp
			return comments, classifyGitHubAPIError(err)
		})
		if err != nil {
			return 0, fmt.Errorf("list comments for %s#%d: %w", repo, pr, err)
		}
		for _, comment := range comments {
			if strings.EqualFold(comment.GetUser().GetLogin(), slug+"[bot]") && strings.HasPrefix(comment.GetBody(), marker) {
				return comment.GetID(), nil
			}
		}
		if response.NextPage == 0 {
			return 0, nil
		}
		opts.Page = response.NextPage
	}
}
