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

// PRSchemaEvents is the current label action and latest retarget or history
// rewrite that could let a previously seen head/base pair return.
type PRSchemaEvents struct {
	LabelEventID    int64
	LabelPresent    bool
	LabelActor      string
	LabelChangedAt  time.Time
	RevisionEventID int64
}

func (p *PullRequestInfo) HasLabel(label string) bool {
	return slices.ContainsFunc(p.labels, func(name string) bool { return strings.EqualFold(name, label) })
}

// FetchPRSchemaEvents reads the authoritative PR timeline. The PR endpoint
// alone cannot distinguish a removed and reapplied label from its first use.
func (ic *InstallationClient) FetchPRSchemaEvents(ctx context.Context, repo string, pr int, label string) (PRSchemaEvents, error) {
	owner, name := splitRepo(repo)
	opts := &gh.ListOptions{PerPage: 100}
	var state PRSchemaEvents
	for {
		var response *gh.Response
		events, err := retryGitHubUnavailableRead(ctx, ic, metrics.GitHubOperationUnknown, "list PR schema exception events", []any{"repo", repo, "pr", pr}, func(ctx context.Context) ([]*gh.Timeline, error) {
			events, resp, err := ic.client.Issues.ListIssueTimeline(ctx, owner, name, pr, opts)
			response = resp
			return events, classifyGitHubAPIError(err)
		})
		if err != nil {
			return PRSchemaEvents{}, fmt.Errorf("list PR timeline for %s#%d: %w", repo, pr, err)
		}
		for _, event := range events {
			switch event.GetEvent() {
			case "base_ref_changed", "head_ref_force_pushed", "base_ref_force_pushed":
				state.RevisionEventID = max(state.RevisionEventID, event.GetID())
			case "labeled", "unlabeled":
				if label != "" && event.GetID() > state.LabelEventID && strings.EqualFold(event.GetLabel().GetName(), label) {
					state.LabelEventID = event.GetID()
					state.LabelPresent = event.GetEvent() == "labeled"
					state.LabelActor = event.GetActor().GetLogin()
					state.LabelChangedAt = event.GetCreatedAt().Time
				}
			}
		}
		if response.NextPage == 0 {
			return state, nil
		}
		opts.Page = response.NextPage
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
