package github

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/block/schemabot/pkg/metrics"
)

// ErrListenOnlyRepository is returned for a GitHub request SchemaBot refused
// to send because the target repository is configured listen-only.
var ErrListenOnlyRepository = errors.New("repository is listen-only, so SchemaBot does not write to it")

// ListenOnlyRepos reports the repositories SchemaBot reads from but never
// writes to. A listen-only repository receives the same webhook processing
// and GitHub reads as any other, which lets a deployment measure what a
// repository costs in GitHub requests without changing what its pull requests
// show.
type ListenOnlyRepos interface {
	// HasListenOnlyRepos reports whether any repository is listen-only. When
	// none is, the client sends every request without inspecting it.
	HasListenOnlyRepos() bool
	IsRepoListenOnly(repo string) bool
}

// WithListenOnlyRepos refuses every GitHub request that is not a read when it
// targets a repository repos reports as listen-only. The refusal happens in
// the HTTP transport, below every client method, so a write path added later
// is covered without being listed here.
func WithListenOnlyRepos(repos ListenOnlyRepos) ClientOption {
	return func(c *Client) {
		c.listenOnlyRepos = repos
	}
}

// listenOnlyTransport sends a request only when it is a read or does not
// target a listen-only repository. It sits outside the rate-limit transport,
// so a refused request never waits on a GitHub cooldown. A refused request is counted on the GitHub
// request metric with status suppressed, so a listen-only deployment still
// shows how many writes it would have made.
type listenOnlyTransport struct {
	base           http.RoundTripper
	repos          ListenOnlyRepos
	installationID int64
	appSlug        func() string
}

func newListenOnlyTransport(base http.RoundTripper, repos ListenOnlyRepos, installationID int64, appSlug func() string) http.RoundTripper {
	if repos == nil || !repos.HasListenOnlyRepos() {
		return base
	}
	return &listenOnlyTransport{
		base:           base,
		repos:          repos,
		installationID: installationID,
		appSlug:        appSlug,
	}
}

func (t *listenOnlyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	metricCtx, ok := githubRateLimitContextFrom(req.Context())
	if !ok {
		metricCtx = githubRateLimitContextFromRequest(req)
	}
	if t.maySend(req, metricCtx) {
		return t.base.RoundTrip(req)
	}

	// A RoundTripper must close the request body even when it sends nothing.
	if req.Body != nil {
		if err := req.Body.Close(); err != nil {
			return nil, fmt.Errorf("close body of suppressed %s %s for %s: %w", req.Method, metricCtx.operation, metricCtx.repository, err)
		}
	}
	githubApp := ""
	if t.appSlug != nil {
		githubApp = t.appSlug()
	}
	metrics.RecordGitHubRequest(req.Context(), metrics.GitHubRequestSample{
		Operation:      metricCtx.operation,
		Category:       gitHubRequestCategory(metricCtx.operation),
		Resource:       gitHubRateLimitResource(githubRateLimitResourceFromRequest(req)),
		Repository:     metricCtx.repository,
		GitHubApp:      githubApp,
		InstallationID: t.installationID,
		Status:         metrics.GitHubRequestStatusSuppressed,
	})
	target := metricCtx.repository
	if target == "" {
		target = "a request that names no repository"
	}
	return nil, fmt.Errorf("%s %s for %s: %w", req.Method, metricCtx.operation, target, ErrListenOnlyRepository)
}

// maySend reports whether a request can reach GitHub. Reads are always sent.
// A write is sent when it targets a repository that is not listen-only, or
// when it names no repository at all, such as an installation token exchange.
// A GraphQL request carries its repository only in the request context, so a
// GraphQL write without one is refused: its target cannot be told apart from
// a listen-only repository.
func (t *listenOnlyTransport) maySend(req *http.Request, metricCtx githubRateLimitContext) bool {
	if isGitHubReadRequest(req, metricCtx.operation) {
		return true
	}
	if metricCtx.repository != "" {
		return !t.repos.IsRepoListenOnly(metricCtx.repository)
	}
	return githubRateLimitResourceFromRequest(req) != metrics.GitHubRateLimitResourceGraphQL
}

// isGitHubReadRequest reports whether a request only reads from GitHub. REST
// reads are GET and HEAD. GraphQL sends queries and mutations alike as POST,
// so a GraphQL request counts as a read only when its operation is a known
// read; an unknown GraphQL operation is refused rather than guessed at.
func isGitHubReadRequest(req *http.Request, operation string) bool {
	switch req.Method {
	case http.MethodGet, http.MethodHead:
		return true
	case http.MethodPost:
		return githubRateLimitResourceFromRequest(req) == metrics.GitHubRateLimitResourceGraphQL &&
			gitHubRequestCategory(operation) == metrics.GitHubRequestCategoryRead
	default:
		return false
	}
}
