package webhook

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/block/schemabot/pkg/github"
)

// commentLister lists a PR's comments. *github.InstallationClient satisfies it;
// the narrow interface keeps CommentStateSource testable without a GitHub
// client.
type commentLister interface {
	ListIssueComments(ctx context.Context, repo string, pr int) ([]github.IssueComment, error)
}

// CommentStateSource reads per-tenant state from a pull request's comments,
// implementing TenantStateSource over the PR-comment transport. It is the
// read side of the comment adapter: it lists the PR's comments, parses the
// tenant-state block out of each, and returns the states reported for the
// current head SHA.
type CommentStateSource struct {
	client commentLister
	logger *slog.Logger
}

// NewCommentStateSource returns a TenantStateSource backed by a PR's comments.
func NewCommentStateSource(client commentLister, logger *slog.Logger) *CommentStateSource {
	return &CommentStateSource{client: client, logger: logger}
}

var _ TenantStateSource = (*CommentStateSource)(nil)

// StatesForPR returns the tenant states reported on the PR for headSHA. It fails
// closed per comment: an unparseable block is skipped (the tenant is treated as
// not-reported) rather than trusted, and a block stamped with a different SHA is
// ignored as stale. A list error is returned so the caller can fail closed
// overall rather than fold a partial view.
func (s *CommentStateSource) StatesForPR(ctx context.Context, repo string, pr int, headSHA string) ([]TenantState, error) {
	comments, err := s.client.ListIssueComments(ctx, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("read tenant-state comments for %s#%d: %w", repo, pr, err)
	}

	// State is keyed by (tenant, environment): a tenant publishes one block per
	// environment it participates in, so two blocks from the same tenant for
	// different environments are both kept.
	type tenantEnv struct{ tenant, environment string }
	var states []TenantState
	seen := make(map[tenantEnv]struct{})
	for _, comment := range comments {
		state, found, parseErr := parseTenantStateBlock(comment.Body)
		if !found {
			// Not a tenant-state comment (ordinary PR chatter); skip silently.
			continue
		}
		if parseErr != nil {
			s.logger.Warn("skipping unparseable tenant-state comment; tenant treated as not reported",
				"repo", repo, "pr", pr, "comment_id", comment.ID, "error", parseErr)
			continue
		}
		if state.SHA != headSHA {
			s.logger.Debug("skipping stale-SHA tenant-state comment",
				"repo", repo, "pr", pr, "tenant", state.Tenant, "environment", state.Environment,
				"comment_sha", state.SHA, "head_sha", headSHA)
			continue
		}
		key := tenantEnv{tenant: state.Tenant, environment: state.Environment}
		if _, dup := seen[key]; dup {
			s.logger.Warn("duplicate tenant-state comment for (tenant, environment) at head SHA; keeping the first",
				"repo", repo, "pr", pr, "tenant", state.Tenant, "environment", state.Environment, "comment_id", comment.ID)
			continue
		}
		seen[key] = struct{}{}
		states = append(states, state)
	}
	return states, nil
}
