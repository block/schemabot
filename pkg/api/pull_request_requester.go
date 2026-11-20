package api

import (
	"context"
	"strings"
)

// PullRequestRequester is who asks for a freeze override from a pull request
// command, and where.
type PullRequestRequester struct {
	// Login is the commenter's GitHub login.
	Login string
	// Repository and PullRequest name the pull request the command is on.
	Repository  string
	PullRequest int
	// Author is the pull request author's login, who may not approve.
	Author string
}

func (r PullRequestRequester) complete() bool {
	return strings.TrimSpace(r.Login) != "" && strings.TrimSpace(r.Repository) != "" &&
		r.PullRequest > 0 && strings.TrimSpace(r.Author) != ""
}

type pullRequestRequesterKey struct{}

// WithPullRequestOverrideRequester marks ctx as carrying a pull request
// command whose commenter asks for an override. Only the webhook calls it,
// after verifying the delivery's signature; the key is unexported so no HTTP
// request can set it. It is not auth.WithVerifiedUser: a GitHub login is not
// a forward-auth subject and must not pass for one.
func WithPullRequestOverrideRequester(ctx context.Context, requester PullRequestRequester) context.Context {
	return context.WithValue(ctx, pullRequestRequesterKey{}, requester)
}

// pullRequestOverrideRequester returns the requester ctx carries, and whether
// it carries one at all. A requester missing any field is returned too, for
// the caller to refuse: a pull request request never goes without its author.
func pullRequestOverrideRequester(ctx context.Context) (PullRequestRequester, bool) {
	requester, ok := ctx.Value(pullRequestRequesterKey{}).(PullRequestRequester)
	return requester, ok
}
