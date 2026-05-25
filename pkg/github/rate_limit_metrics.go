package github

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/block/schemabot/pkg/metrics"
)

const (
	headerRateLimitResource  = "X-RateLimit-Resource"
	headerRateLimitLimit     = "X-RateLimit-Limit"
	headerRateLimitRemaining = "X-RateLimit-Remaining"
	headerRateLimitUsed      = "X-RateLimit-Used"
)

type githubRateLimitContextKey struct{}

type githubRateLimitContext struct {
	operation  string
	repository string
}

func withGitHubRateLimitContext(ctx context.Context, operation, repository string) context.Context {
	return context.WithValue(ctx, githubRateLimitContextKey{}, githubRateLimitContext{
		operation:  operation,
		repository: repository,
	})
}

func githubRateLimitContextFrom(ctx context.Context) (githubRateLimitContext, bool) {
	if v, ok := ctx.Value(githubRateLimitContextKey{}).(githubRateLimitContext); ok {
		return v, true
	}
	return githubRateLimitContext{}, false
}

type githubMetricsTransport struct {
	base           http.RoundTripper
	installationID int64
	appSlug        func() string
}

func newGitHubMetricsTransport(base http.RoundTripper, installationID int64, appSlug func() string) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &githubMetricsTransport{
		base:           base,
		installationID: installationID,
		appSlug:        appSlug,
	}
}

func (t *githubMetricsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if resp != nil {
		recordGitHubResponseHeaders(req.Context(), req, resp, t.installationID, t.githubApp())
	}
	return resp, err
}

func (t *githubMetricsTransport) githubApp() string {
	if t.appSlug == nil {
		return ""
	}
	return t.appSlug()
}

func recordGitHubResponseHeaders(ctx context.Context, req *http.Request, resp *http.Response, installationID int64, githubApp string) {
	metricCtx, hasMetricCtx := githubRateLimitContextFrom(ctx)
	if !hasMetricCtx {
		metricCtx = githubRateLimitContextFromRequest(req)
	}

	resource := gitHubRateLimitResource(githubRateLimitResourceFromHeaders(req, resp))
	metrics.RecordGitHubRequest(ctx, metrics.GitHubRequestSample{
		Operation:      metricCtx.operation,
		Category:       gitHubRequestCategory(metricCtx.operation),
		Resource:       resource,
		Repository:     metricCtx.repository,
		GitHubApp:      githubApp,
		InstallationID: installationID,
		Status:         gitHubResponseStatus(resp),
	})

	limit, hasLimit := parseGitHubRateLimitHeader(resp.Header, headerRateLimitLimit)
	remaining, hasRemaining := parseGitHubRateLimitHeader(resp.Header, headerRateLimitRemaining)
	if !hasLimit || !hasRemaining {
		return
	}

	used, hasUsed := parseGitHubRateLimitHeader(resp.Header, headerRateLimitUsed)
	if !hasUsed {
		used = limit - remaining
	}

	metrics.RecordGitHubRateLimit(ctx, metrics.GitHubRateLimitSample{
		Operation:      metricCtx.operation,
		Resource:       resource,
		Repository:     metricCtx.repository,
		GitHubApp:      githubApp,
		InstallationID: installationID,
		Limit:          limit,
		Remaining:      remaining,
		Used:           used,
	})
}

func parseGitHubRateLimitHeader(header http.Header, key string) (int64, bool) {
	raw := header.Get(key)
	if raw == "" {
		return 0, false
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

func githubRateLimitContextFromRequest(req *http.Request) githubRateLimitContext {
	if req.URL == nil {
		return githubRateLimitContext{operation: metrics.GitHubOperationUnknown}
	}
	parts := githubPathSegments(req.URL.Path)
	if len(parts) > 0 && parts[len(parts)-1] == "graphql" {
		return githubRateLimitContext{operation: metrics.GitHubOperationUnknown}
	}

	if metricCtx, ok := githubRateLimitContextFromRoute(req.Method, parts); ok {
		return metricCtx
	}

	return githubRateLimitContext{
		operation:  metrics.GitHubOperationUnknown,
		repository: githubRepositoryFromPathSegments(parts),
	}
}

type githubRoutePattern struct {
	method    string
	path      string
	operation string
}

var githubRoutePatterns = []githubRoutePattern{
	{
		method:    http.MethodGet,
		path:      "/app",
		operation: metrics.GitHubOperationFetchAppSlug,
	},
	{
		method:    http.MethodPost,
		path:      "/app/installations/{installation_id}/access_tokens",
		operation: metrics.GitHubOperationCreateInstallationAccessToken,
	},
	{
		method:    http.MethodPost,
		path:      "/repos/{owner}/{repo}/issues/comments/{comment_id}/reactions",
		operation: metrics.GitHubOperationAddCommentReaction,
	},
	{
		method:    http.MethodPatch,
		path:      "/repos/{owner}/{repo}/issues/comments/{comment_id}",
		operation: metrics.GitHubOperationEditIssueComment,
	},
	{
		method:    http.MethodPost,
		path:      "/repos/{owner}/{repo}/issues/{issue_number}/comments",
		operation: metrics.GitHubOperationCreateIssueComment,
	},
	{
		method:    http.MethodGet,
		path:      "/repos/{owner}/{repo}/pulls/{pull_number}",
		operation: metrics.GitHubOperationFetchPullRequest,
	},
	{
		method:    http.MethodGet,
		path:      "/repos/{owner}/{repo}/pulls/{pull_number}/files",
		operation: metrics.GitHubOperationListPRFiles,
	},
	{
		method:    http.MethodGet,
		path:      "/repos/{owner}/{repo}/pulls/{pull_number}/reviews",
		operation: metrics.GitHubOperationListReviews,
	},
	{
		method:    http.MethodPost,
		path:      "/repos/{owner}/{repo}/pulls/{pull_number}/requested_reviewers",
		operation: metrics.GitHubOperationRequestReviewers,
	},
	{
		method:    http.MethodPost,
		path:      "/repos/{owner}/{repo}/check-runs",
		operation: metrics.GitHubOperationCreateCheckRun,
	},
	{
		method:    http.MethodPatch,
		path:      "/repos/{owner}/{repo}/check-runs/{check_run_id}",
		operation: metrics.GitHubOperationUpdateCheckRun,
	},
	{
		method:    http.MethodGet,
		path:      "/repos/{owner}/{repo}/commits/{ref}/check-runs",
		operation: metrics.GitHubOperationListCheckRunsForRef,
	},
	{
		method:    http.MethodGet,
		path:      "/repos/{owner}/{repo}/git/trees/{tree_sha}",
		operation: metrics.GitHubOperationFetchGitTree,
	},
	{
		method:    http.MethodGet,
		path:      "/repos/{owner}/{repo}/git/blobs/{file_sha}",
		operation: metrics.GitHubOperationFetchBlob,
	},
	{
		method:    http.MethodGet,
		path:      "/repos/{owner}/{repo}/contents/{path...}",
		operation: metrics.GitHubOperationFetchFileContent,
	},
	{
		method:    http.MethodGet,
		path:      "/orgs/{org}/teams/{team_slug}/members",
		operation: metrics.GitHubOperationListTeamMembers,
	},
	{
		method:    http.MethodGet,
		path:      "/orgs/{org}/teams/{team_slug}/memberships/{username}",
		operation: metrics.GitHubOperationGetTeamMembership,
	},
}

func githubPathSegments(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}
	parts := strings.Split(path, "/")
	if len(parts) >= 2 && parts[0] == "api" && parts[1] == "v3" {
		return parts[2:]
	}
	return parts
}

func githubRateLimitContextFromRoute(method string, parts []string) (githubRateLimitContext, bool) {
	for _, pattern := range githubRoutePatterns {
		captures, ok := pattern.match(method, parts)
		if !ok {
			continue
		}
		return githubRateLimitContext{
			operation:  pattern.operation,
			repository: githubRepositoryFromRouteCaptures(captures),
		}, true
	}
	return githubRateLimitContext{}, false
}

func (p githubRoutePattern) match(method string, parts []string) (map[string]string, bool) {
	if method != p.method {
		return nil, false
	}

	patternParts := githubPathSegments(p.path)
	captures := make(map[string]string)
	for i, patternPart := range patternParts {
		name, trailing, isCapture := githubRouteCapture(patternPart)
		if trailing {
			if i != len(patternParts)-1 {
				return nil, false
			}
			if len(parts) < i {
				return nil, false
			}
			captures[name] = strings.Join(parts[i:], "/")
			return captures, true
		}
		if len(parts) <= i {
			return nil, false
		}
		if isCapture {
			captures[name] = parts[i]
			continue
		}
		if parts[i] != patternPart {
			return nil, false
		}
	}
	return captures, len(parts) == len(patternParts)
}

func githubRouteCapture(segment string) (string, bool, bool) {
	if !strings.HasPrefix(segment, "{") || !strings.HasSuffix(segment, "}") {
		return "", false, false
	}
	name := strings.TrimSuffix(strings.TrimPrefix(segment, "{"), "}")
	trailing := strings.HasSuffix(name, "...")
	name = strings.TrimSuffix(name, "...")
	if name == "" {
		return "", false, false
	}
	return name, trailing, true
}

func githubRepositoryFromRouteCaptures(captures map[string]string) string {
	owner, hasOwner := captures["owner"]
	repo, hasRepo := captures["repo"]
	if !hasOwner || !hasRepo {
		return ""
	}
	return owner + "/" + repo
}

func githubRepositoryFromPathSegments(parts []string) string {
	repoIndex := githubRepositorySegmentIndex(parts)
	if repoIndex < 0 {
		return ""
	}
	return parts[repoIndex+1] + "/" + parts[repoIndex+2]
}

func githubRepositorySegmentIndex(parts []string) int {
	for i := 0; i+2 < len(parts); i++ {
		if parts[i] == "repos" {
			return i
		}
	}
	return -1
}

func githubRateLimitResourceFromHeaders(req *http.Request, resp *http.Response) string {
	resource := strings.ToLower(resp.Header.Get(headerRateLimitResource))
	if resource != "" {
		return resource
	}
	if req.URL != nil && strings.HasSuffix(req.URL.Path, "/graphql") {
		return metrics.GitHubRateLimitResourceGraphQL
	}
	return metrics.GitHubRateLimitResourceCore
}

func gitHubRateLimitResource(resource string) string {
	resource = strings.ToLower(resource)
	if resource == "" {
		return metrics.GitHubRateLimitResourceCore
	}
	return resource
}

func gitHubRequestCategory(operation string) string {
	switch operation {
	case metrics.GitHubOperationAddCommentReaction,
		metrics.GitHubOperationCreateCheckRun,
		metrics.GitHubOperationCreateIssueComment,
		metrics.GitHubOperationEditIssueComment,
		metrics.GitHubOperationRequestReviewers,
		metrics.GitHubOperationUpdateCheckRun:
		return metrics.GitHubRequestCategoryWrite
	case metrics.GitHubOperationCreateInstallationAccessToken:
		return metrics.GitHubRequestCategoryAuth
	case metrics.GitHubOperationFetchAppSlug,
		metrics.GitHubOperationFetchBlob,
		metrics.GitHubOperationFetchFileContent,
		metrics.GitHubOperationFetchGitTree,
		metrics.GitHubOperationFetchPullRequest,
		metrics.GitHubOperationGetTeamMembership,
		metrics.GitHubOperationGraphQLStatusCheckRollup,
		metrics.GitHubOperationListCheckRunsForRef,
		metrics.GitHubOperationListPRFiles,
		metrics.GitHubOperationListReviews,
		metrics.GitHubOperationListTeamMembers:
		return metrics.GitHubRequestCategoryRead
	default:
		return metrics.GitHubRequestCategoryUnknown
	}
}

func gitHubResponseStatus(resp *http.Response) string {
	if resp == nil {
		return metrics.GitHubRequestStatusUnknown
	}
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return metrics.GitHubRequestStatusSuccess
	}
	return metrics.GitHubRequestStatusError
}
