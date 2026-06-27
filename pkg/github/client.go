// Package github provides a GitHub API client for SchemaBot webhook integration.
// It uses GitHub App authentication via ghinstallation to manage PR comments,
// check runs, and fetch repository content.
package github

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/gofri/go-github-ratelimit/v2/github_ratelimit"
	"github.com/gofri/go-github-ratelimit/v2/github_ratelimit/github_secondary_ratelimit"
	gh "github.com/google/go-github/v86/github"
)

// GitHubClientFactory creates installation-scoped GitHub clients.
// Production uses *Client (JWT auth via ghinstallation); tests use a fake with httptest.
type GitHubClientFactory interface {
	ForInstallation(installationID int64) (*InstallationClient, error)
}

// Client handles GitHub App-level operations and creates per-installation clients.
type Client struct {
	appID      int64
	privateKey []byte
	logger     *slog.Logger

	// appSlug is the GitHub App's slug, fetched from GitHub at startup
	// (best-effort — may be empty if the initial fetch failed). It is
	// stored as atomic.Pointer[string] so concurrent ForInstallation and
	// InstallationClient.isOwnAppSlug calls observe consistent values
	// without holding a lock on the hot read path. The pointer is non-nil
	// after NewClient returns (holds the empty string if the fetch failed).
	appSlug atomic.Pointer[string]

	// trustedCheckAppSlugs lists sibling SchemaBot GitHub App slugs whose
	// Check Runs this deployment trusts in addition to its own App. Static
	// configuration set at construction; never mutated afterwards.
	trustedCheckAppSlugs []string

	// slugFetchMu serialises slug-fetch attempts so concurrent
	// ForInstallation callers do not thundering-herd retry on startup
	// failure. lastSlugAttempt is read+written only under this mutex.
	slugFetchMu     sync.Mutex
	lastSlugAttempt time.Time

	// installations caches per-installation clients keyed by installationID
	// so the underlying http.Client, gh.Client, and
	// ghinstallation transport (and its installation-token cache) are
	// reused across webhook deliveries instead of being reconstructed on
	// every call.
	installationsMu sync.Mutex
	installations   map[int64]*InstallationClient

	// checkStatusSingleflight coalesces concurrent GetPRCheckStatuses
	// calls for the same (repo, sha) into a single upstream request via
	// singleflight. Shared across every InstallationClient this factory
	// produces so concurrent webhook deliveries and command bursts
	// targeting the same commit collapse to one shared GitHub read even
	// though each delivery may spawn a fresh InstallationClient.
	// Deliberately not a TTL cache: check status is mutable for a SHA
	// (reruns, late-arriving checks, branch-protection adding required
	// checks), so any memoisation window would risk converting a
	// now-failing gate into a passing one.
	checkStatusSingleflight *CheckStatusSingleflight
}

// ClientOption configures optional Client behavior at construction.
type ClientOption func(*Client)

// WithTrustedCheckAppSlugs configures the GitHub App slugs of sibling
// SchemaBot deployments whose Check Runs this deployment trusts in addition
// to its own App. Cross-deployment gates (such as the prior-environment
// promotion gate) accept Check Runs created by these Apps.
func WithTrustedCheckAppSlugs(slugs []string) ClientOption {
	return func(c *Client) {
		c.trustedCheckAppSlugs = slugs
	}
}

// loadAppSlug returns the current app slug, or empty if not yet fetched.
func (c *Client) loadAppSlug() string {
	if p := c.appSlug.Load(); p != nil {
		return *p
	}
	return ""
}

// storeAppSlug atomically updates the app slug.
func (c *Client) storeAppSlug(slug string) {
	c.appSlug.Store(&slug)
}

// slugFetchRetryCooldown is how long to wait between retry attempts when the
// app slug couldn't be fetched at startup (e.g., GitHub was temporarily down).
const slugFetchRetryCooldown = 5 * time.Second

// NewClient creates a new GitHub App client.
//
// Fetches the app's slug from GitHub. If the slug can't be fetched (e.g., GitHub
// is down), the server still starts but PR applies are blocked by the check gate
// since we can't identify our own checks.
//
// The returned Client memoises the *InstallationClient it produces by
// installationID so the underlying http.Client, gh.Client, and
// ghinstallation transport (and its installation-token cache) are reused across
// webhook deliveries. It also owns a CheckStatusSingleflight that is shared
// across every InstallationClient it produces, so concurrent webhook
// deliveries and command bursts targeting the same (repo, sha) collapse to a
// single upstream GitHub read.
func NewClient(appID int64, privateKey []byte, logger *slog.Logger, opts ...ClientOption) *Client {
	c := &Client{
		appID:                   appID,
		privateKey:              privateKey,
		logger:                  logger,
		installations:           make(map[int64]*InstallationClient),
		checkStatusSingleflight: NewCheckStatusSingleflight(),
	}
	for _, opt := range opts {
		opt(c)
	}
	// Seed the atomic with the empty string so loadAppSlug never returns
	// from a nil pointer.
	c.storeAppSlug("")

	// Fetch the app slug so we can identify our own check runs.
	// Non-fatal: if GitHub is down, the server still starts but the check gate won't
	// exclude own checks (PR applies may be blocked until the slug is fetched).
	c.fetchAppSlug()

	return c
}

// fetchAppSlug fetches the app slug from GitHub via GET /app.
// On failure, logs an error and leaves appSlug empty.
func (c *Client) fetchAppSlug() {
	c.slugFetchMu.Lock()
	c.lastSlugAttempt = time.Now()
	c.slugFetchMu.Unlock()

	appBaseTransport := newGitHubRateLimitTransport(newGitHubMetricsTransport(http.DefaultTransport, 0, c.loadAppSlug))
	appTransport, err := ghinstallation.NewAppsTransport(appBaseTransport, c.appID, c.privateKey)
	if err != nil {
		c.logger.Error("failed to create app transport for slug fetch", "error", err)
		return
	}
	appClient := gh.NewClient(&http.Client{Transport: appTransport, Timeout: 10 * time.Second})
	appClient.DisableRateLimitCheck = true
	ctx := context.Background()
	app, _, err := appClient.Apps.Get(ctx, "")
	if err != nil {
		c.logger.Error("failed to fetch app slug from GitHub — check gate will not exclude own checks",
			"app_id", c.appID, "error", err)
		return
	}
	slug := app.GetSlug()
	c.storeAppSlug(slug)
	c.logger.Info("fetched GitHub App slug", "slug", slug)
}

// ForInstallation returns a GitHub client scoped to a specific installation,
// reusing the cached client for that installationID when one already exists.
// The ghinstallation library handles JWT generation, token exchange, caching,
// and refresh automatically; reusing the InstallationClient additionally
// preserves HTTP keep-alive, the ghinstallation token cache, and any shared
// per-installation state (such as PR-info cache hits) across webhook deliveries.
//
// The cached client's appSlug is refreshed on every call so a Client that
// recovers its slug after a startup failure does not strand existing
// InstallationClients with an empty slug.
func (c *Client) ForInstallation(installationID int64) (*InstallationClient, error) {
	// Retry slug fetch if it failed at startup (e.g., GitHub was down).
	// Rate-limited to once per 5 seconds to avoid hammering GitHub during
	// an outage while still recovering quickly once it's back.
	if c.loadAppSlug() == "" {
		c.slugFetchMu.Lock()
		shouldRetry := time.Since(c.lastSlugAttempt) > slugFetchRetryCooldown
		c.slugFetchMu.Unlock()
		if shouldRetry {
			c.logger.Info("app slug not yet fetched, retrying")
			c.fetchAppSlug()
		}
		if c.loadAppSlug() == "" {
			c.logger.Error("app slug unavailable — check gate will block PR applies if own checks are not passing")
		}
	}

	slug := c.loadAppSlug()

	c.installationsMu.Lock()
	defer c.installationsMu.Unlock()

	if existing, ok := c.installations[installationID]; ok {
		// Refresh the cached client's slug snapshot atomically so a slug
		// recovery during the lifetime of this process propagates to
		// clients constructed before recovery — without racing concurrent
		// isOwnAppSlug reads on the same InstallationClient.
		existing.storeAppSlug(slug)
		return existing, nil
	}

	baseTransport := newGitHubRateLimitTransport(newGitHubMetricsTransport(http.DefaultTransport, installationID, c.loadAppSlug))
	installationTransport, err := ghinstallation.New(baseTransport, c.appID, installationID, c.privateKey)
	if err != nil {
		return nil, fmt.Errorf("create installation transport for installation %d: %w", installationID, err)
	}
	httpc := &http.Client{Transport: installationTransport, Timeout: 30 * time.Second}
	ghClient := gh.NewClient(httpc)
	ghClient.DisableRateLimitCheck = true
	ic := &InstallationClient{
		client:                  ghClient,
		logger:                  c.logger,
		installationID:          installationID,
		trustedCheckAppSlugs:    c.trustedCheckAppSlugs,
		checkStatusSingleflight: c.checkStatusSingleflight,
	}
	ic.storeAppSlug(slug)
	c.installations[installationID] = ic
	c.logger.Info("constructed installation client",
		"installation_id", installationID, "app_slug", slug)
	return ic, nil
}

// NewInstallationClient creates an InstallationClient from a pre-configured go-github client.
// Used in tests to point at httptest.Server; production uses Client.ForInstallation().
func NewInstallationClient(client *gh.Client, logger *slog.Logger) *InstallationClient {
	return NewInstallationClientWithSlug(client, logger, "")
}

// NewInstallationClientWithSlug creates an InstallationClient with an explicit
// app slug and, optionally, trusted sibling SchemaBot App slugs.
func NewInstallationClientWithSlug(client *gh.Client, logger *slog.Logger, appSlug string, trustedCheckAppSlugs ...string) *InstallationClient {
	ic := &InstallationClient{
		client:               client,
		logger:               logger,
		trustedCheckAppSlugs: trustedCheckAppSlugs,
	}
	ic.storeAppSlug(appSlug)
	return ic
}

// InstallationClient wraps a go-github client scoped to a specific GitHub App installation.
type InstallationClient struct {
	client *gh.Client
	logger *slog.Logger

	installationID int64

	// appSlug is the GitHub App's slug used to identify own check runs.
	// Stored as atomic.Pointer[string] because cached InstallationClients
	// are reused across webhook deliveries and ForInstallation may refresh
	// this field after a slug recovery while concurrent isOwnAppSlug reads
	// run on other goroutines.
	appSlug atomic.Pointer[string]

	// trustedCheckAppSlugs lists sibling SchemaBot GitHub App slugs whose
	// Check Runs this deployment trusts in addition to its own App. Static
	// configuration copied from the parent Client; never mutated.
	trustedCheckAppSlugs []string

	// checkStatusSingleflight is owned by the parent Client factory and
	// shared across every InstallationClient it produces so concurrent
	// fetches collapse across the short-lived InstallationClients
	// spawned per webhook delivery. It delivers identity-independent
	// rows; IsSchemaBot is re-derived per call against this client's
	// appSlug snapshot, so a shared fetch delivered to N waiters with
	// different appSlug snapshots is classified correctly for each.
	// Optional: when nil, GetPRCheckStatuses bypasses the coalescer
	// (e.g. tests).
	checkStatusSingleflight *CheckStatusSingleflight
}

// loadAppSlug returns the current app slug, or empty if not yet set.
func (ic *InstallationClient) loadAppSlug() string {
	if p := ic.appSlug.Load(); p != nil {
		return *p
	}
	return ""
}

// storeAppSlug atomically updates the app slug.
func (ic *InstallationClient) storeAppSlug(slug string) {
	ic.appSlug.Store(&slug)
}

const githubSecondaryRateLimitMaxSleep = 5 * time.Second

func newGitHubRateLimitTransport(base http.RoundTripper) http.RoundTripper {
	return github_ratelimit.New(base,
		github_secondary_ratelimit.WithSingleSleepLimit(githubSecondaryRateLimitMaxSleep, nil),
	)
}

const githubUnavailableReadRetryMaxAttempts = 3

var githubUnavailableReadRetryDelay = 200 * time.Millisecond

func retryGitHubUnavailableRead[T any](ctx context.Context, logger *slog.Logger, operation string, logAttrs []any, read func(context.Context) (T, error)) (T, error) {
	var zero T
	for attempt := 1; attempt <= githubUnavailableReadRetryMaxAttempts; attempt++ {
		result, err := read(ctx)
		if err == nil {
			if attempt > 1 && logger != nil {
				logger.Info("GitHub read succeeded after retry",
					append(logAttrs, "operation", operation, "attempt", attempt)...)
			}
			return result, nil
		}
		if !IsUnavailableError(err) || attempt == githubUnavailableReadRetryMaxAttempts || ctx.Err() != nil {
			return zero, err
		}
		delay := githubUnavailableReadRetryDelay * time.Duration(attempt)
		if logger != nil {
			logger.Warn("retrying unavailable GitHub read",
				append(logAttrs, "operation", operation, "attempt", attempt, "max_attempts", githubUnavailableReadRetryMaxAttempts, "delay", delay, "error", err)...)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, fmt.Errorf("wait to retry GitHub read %s: %w", operation, ctx.Err())
		case <-timer.C:
		}
	}
	return zero, fmt.Errorf("retry GitHub read %s: exhausted attempts", operation)
}

// AppSlug returns the GitHub App slug associated with this installation
// client, when known. It is best-effort: startup can proceed before the slug
// is fetched, so callers should tolerate an empty string.
func (ic *InstallationClient) AppSlug() string {
	return ic.loadAppSlug()
}

// IsNotFoundError checks if an error is a GitHub API 404 Not Found error.
func IsNotFoundError(err error) bool {
	var ghErr *gh.ErrorResponse
	if errors.As(err, &ghErr) {
		return ghErr.Response != nil && ghErr.Response.StatusCode == http.StatusNotFound
	}
	return false
}

// ErrGitHubUnavailable identifies GitHub API availability failures that should
// be retried once GitHub becomes reachable again.
var ErrGitHubUnavailable = errors.New("github unavailable")

type githubUnavailableError struct {
	err error
}

func (e *githubUnavailableError) Error() string {
	return fmt.Sprintf("%s: %v", ErrGitHubUnavailable, e.err)
}

func (e *githubUnavailableError) Unwrap() error {
	return e.err
}

func (e *githubUnavailableError) Is(target error) bool {
	return target == ErrGitHubUnavailable
}

// IsUnavailableError returns true when the error chain contains a GitHub API
// availability failure such as a network failure, timeout, or 5xx response.
func IsUnavailableError(err error) bool {
	return errors.Is(err, ErrGitHubUnavailable)
}

func classifyGitHubAPIError(err error) error {
	if err == nil {
		return nil
	}
	if isGitHubUnavailable(err) {
		return &githubUnavailableError{err: err}
	}
	return err
}

func isGitHubUnavailable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var ghErr *gh.ErrorResponse
	if errors.As(err, &ghErr) && ghErr.Response != nil {
		status := ghErr.Response.StatusCode
		return status >= http.StatusInternalServerError
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr)
}

// splitRepo splits "owner/repo" into owner and repo parts.
func splitRepo(repo string) (owner, repoName string) {
	parts := strings.SplitN(repo, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return repo, ""
}

// CreateIssueComment posts a comment on a PR/issue.
func (ic *InstallationClient) CreateIssueComment(ctx context.Context, repo string, pr int, body string) (int64, error) {
	owner, repoName := splitRepo(repo)
	comment, _, err := ic.client.Issues.CreateComment(ctx, owner, repoName, pr, &gh.IssueComment{
		Body: new(body),
	})
	if err != nil {
		return 0, fmt.Errorf("create issue comment: %w", err)
	}
	return comment.GetID(), nil
}

// EditIssueComment edits an existing PR/issue comment.
func (ic *InstallationClient) EditIssueComment(ctx context.Context, repo string, commentID int64, body string) error {
	owner, repoName := splitRepo(repo)
	_, _, err := ic.client.Issues.EditComment(ctx, owner, repoName, commentID, &gh.IssueComment{
		Body: new(body),
	})
	if err != nil {
		return fmt.Errorf("edit issue comment: %w", err)
	}
	return nil
}

// AddReactionToComment adds a reaction emoji to a comment.
func (ic *InstallationClient) AddReactionToComment(ctx context.Context, repo string, commentID int64, reaction string) error {
	owner, repoName := splitRepo(repo)
	_, _, err := ic.client.Reactions.CreateIssueCommentReaction(ctx, owner, repoName, commentID, reaction)
	if err != nil {
		return fmt.Errorf("add reaction: %w", err)
	}
	return nil
}

// PullRequestInfo holds relevant PR metadata.
type PullRequestInfo struct {
	HeadRef string
	HeadSHA string
	BaseRef string
	BaseSHA string
	User    string
}

// FetchPullRequest is the dedupe-friendly variant. It honours the
// request-scoped PR-info cache attached to ctx via WithPRInfoCache, so
// repeated calls for the same (repo, pr) within one webhook delivery
// collapse to a single upstream GitHub round trip. Use this for
// discovery / gate work where consistency-within-a-delivery is required
// and the cached snapshot is by construction not stale (the cache lives
// and dies with the delivery's ctx, and a new commit triggers a new
// delivery with its own fresh cache).
//
// For safety re-checks where correctness requires the *current* GitHub
// HEAD — e.g. the auto-confirm / apply-confirm revalidation, where a
// new commit pushed after discovery must downgrade to manual
// confirmation — call FetchPullRequestNoCache instead. Picking the
// right method at the call site keeps the intent explicit and avoids
// hidden ctx flags.
//
// Callers without a request-scoped cache on ctx (tests, ad-hoc usage)
// fall through to a raw fetch on every call.
func (ic *InstallationClient) FetchPullRequest(ctx context.Context, repo string, pr int) (*PullRequestInfo, error) {
	cache := prInfoCacheFromContext(ctx)
	if cache == nil {
		return ic.fetchPullRequest(ctx, repo, pr)
	}
	if info, ok := cache.get(repo, pr); ok {
		// Hand each caller its own copy so a caller mutating the returned
		// struct cannot affect another caller's view within the same scope.
		copyOf := *info
		return &copyOf, nil
	}
	info, err := ic.fetchPullRequest(ctx, repo, pr)
	if err != nil {
		return nil, err
	}
	cache.set(repo, pr, info)
	return info, nil
}

// FetchPullRequestNoCache is the revalidation-friendly variant. It always
// issues a fresh GitHub request, bypassing any request-scoped PR-info
// cache attached via WithPRInfoCache. Use this where correctness requires
// the current GitHub HEAD — for example, the apply -y auto-confirm and
// apply-confirm SHA re-checks, where a stale cached HeadSHA would let
// the apply proceed against schema files fetched at an earlier commit
// instead of downgrading to manual confirmation.
//
// Paired with FetchPullRequest (dedupe-friendly) so the call site
// declares its intent without any hidden ctx-flag magic: discovery work
// calls FetchPullRequest, safety re-checks call FetchPullRequestNoCache.
func (ic *InstallationClient) FetchPullRequestNoCache(ctx context.Context, repo string, pr int) (*PullRequestInfo, error) {
	return ic.fetchPullRequest(ctx, repo, pr)
}

func (ic *InstallationClient) fetchPullRequest(ctx context.Context, repo string, pr int) (*PullRequestInfo, error) {
	owner, repoName := splitRepo(repo)
	ghPR, err := retryGitHubUnavailableRead(ctx, ic.logger, "fetch pull request", []any{"repo", repo, "pr", pr}, func(ctx context.Context) (*gh.PullRequest, error) {
		ghPR, _, err := ic.client.PullRequests.Get(ctx, owner, repoName, pr)
		if err != nil {
			return nil, fmt.Errorf("fetch pull request %s#%d: %w", repo, pr, classifyGitHubAPIError(err))
		}
		return ghPR, nil
	})
	if err != nil {
		return nil, err
	}
	return &PullRequestInfo{
		HeadRef: ghPR.GetHead().GetRef(),
		HeadSHA: ghPR.GetHead().GetSHA(),
		BaseRef: ghPR.GetBase().GetRef(),
		BaseSHA: ghPR.GetBase().GetSHA(),
		User:    ghPR.GetUser().GetLogin(),
	}, nil
}

// PRFile represents a file changed in a PR.
type PRFile struct {
	Filename         string
	PreviousFilename string
	Status           string // added, removed, modified, renamed
}

// GitHub caps pull request file listings at this documented maximum and does
// not provide a separate completeness marker for larger PRs. Treat reaching
// the cap as incomplete so schema discovery fails closed instead of assuming
// there are no managed schema changes later in the list.
const maxGitHubPRFiles = 3000

// GitHub includes at most this many changed files on compare responses.
// Treat reaching the cap as incomplete so comment suppression fails open.
const maxGitHubCompareFiles = 300

// FetchPRFiles gets the list of files changed in a PR.
func (ic *InstallationClient) FetchPRFiles(ctx context.Context, repo string, pr int) ([]PRFile, error) {
	owner, repoName := splitRepo(repo)
	opts := &gh.ListOptions{PerPage: 100}
	var allFiles []PRFile

	for {
		readResult, err := retryGitHubUnavailableRead(ctx, ic.logger, "list PR files", []any{"repo", repo, "pr", pr, "page", opts.Page}, func(ctx context.Context) (struct {
			files []*gh.CommitFile
			resp  *gh.Response
		}, error) {
			ghFiles, resp, err := ic.client.PullRequests.ListFiles(ctx, owner, repoName, pr, opts)
			if err != nil {
				return struct {
					files []*gh.CommitFile
					resp  *gh.Response
				}{}, fmt.Errorf("list PR files: %w", classifyGitHubAPIError(err))
			}
			return struct {
				files []*gh.CommitFile
				resp  *gh.Response
			}{files: ghFiles, resp: resp}, nil
		})
		if err != nil {
			return nil, err
		}
		for _, f := range readResult.files {
			allFiles = append(allFiles, PRFile{
				Filename:         f.GetFilename(),
				PreviousFilename: f.GetPreviousFilename(),
				Status:           f.GetStatus(),
			})
		}
		if len(allFiles) >= maxGitHubPRFiles {
			return nil, fmt.Errorf("list PR files for %s#%d reached GitHub API limit: %w", repo, pr, ErrPRFilesIncomplete)
		}
		if readResult.resp.NextPage == 0 {
			break
		}
		opts.Page = readResult.resp.NextPage
	}

	return allFiles, nil
}

// FetchChangedFilesBetween gets the files changed between two refs.
func (ic *InstallationClient) FetchChangedFilesBetween(ctx context.Context, repo string, base, head string) ([]PRFile, error) {
	owner, repoName := splitRepo(repo)
	readResult, err := retryGitHubUnavailableRead(ctx, ic.logger, "compare commits", []any{"repo", repo, "base", base, "head", head}, func(ctx context.Context) (*gh.CommitsComparison, error) {
		comparison, _, err := ic.client.Repositories.CompareCommits(ctx, owner, repoName, base, head, nil)
		if err != nil {
			return nil, fmt.Errorf("compare commits %s...%s: %w", base, head, classifyGitHubAPIError(err))
		}
		return comparison, nil
	})
	if err != nil {
		return nil, err
	}

	files := make([]PRFile, 0, len(readResult.Files))
	for _, f := range readResult.Files {
		files = append(files, PRFile{
			Filename:         f.GetFilename(),
			PreviousFilename: f.GetPreviousFilename(),
			Status:           f.GetStatus(),
		})
	}
	if len(files) >= maxGitHubCompareFiles {
		return nil, fmt.Errorf("compare commits %s...%s for %s reached GitHub API file limit: %w", base, head, repo, ErrPRFilesIncomplete)
	}
	return files, nil
}

// CheckRunOptions contains options for creating or updating a GitHub Check Run.
type CheckRunOptions struct {
	Name       string
	Status     string // "queued", "in_progress", "completed"
	Conclusion string // "success", "failure", "neutral", "action_required"
	Output     *CheckRunOutput
	Actions    []CheckRunAction
}

// CheckRunOutput is the detailed output of a check run.
type CheckRunOutput struct {
	Title   string
	Summary string
	Text    string
}

// CheckRunAction is a clickable action button in a check run.
type CheckRunAction struct {
	Label       string
	Description string
	Identifier  string
}

// CreateCheckRun creates a GitHub Check Run. Returns the check run ID.
func (ic *InstallationClient) CreateCheckRun(ctx context.Context, repo, headSHA string, opts CheckRunOptions) (int64, error) {
	owner, repoName := splitRepo(repo)

	createOpts := gh.CreateCheckRunOptions{
		Name:    opts.Name,
		HeadSHA: headSHA,
		Status:  new(opts.Status),
	}

	if opts.Status == "completed" {
		createOpts.Conclusion = new(opts.Conclusion)
	}

	if opts.Output != nil {
		createOpts.Output = &gh.CheckRunOutput{
			Title:   new(opts.Output.Title),
			Summary: new(opts.Output.Summary),
		}
		if opts.Output.Text != "" {
			createOpts.Output.Text = new(opts.Output.Text)
		}
	}

	for _, action := range opts.Actions {
		createOpts.Actions = append(createOpts.Actions, &gh.CheckRunAction{
			Label:       action.Label,
			Description: action.Description,
			Identifier:  action.Identifier,
		})
	}

	result, _, err := ic.client.Checks.CreateCheckRun(ctx, owner, repoName, createOpts)
	if err != nil {
		return 0, fmt.Errorf("create check run: %w", err)
	}
	return result.GetID(), nil
}

// UpdateCheckRun updates an existing GitHub Check Run.
func (ic *InstallationClient) UpdateCheckRun(ctx context.Context, repo string, checkRunID int64, opts CheckRunOptions) error {
	owner, repoName := splitRepo(repo)

	updateOpts := gh.UpdateCheckRunOptions{
		Name: opts.Name,
	}

	if opts.Status != "" {
		updateOpts.Status = new(opts.Status)
	}

	if opts.Status == "completed" {
		updateOpts.Conclusion = new(opts.Conclusion)
	}

	if opts.Output != nil {
		updateOpts.Output = &gh.CheckRunOutput{
			Title:   new(opts.Output.Title),
			Summary: new(opts.Output.Summary),
		}
		if opts.Output.Text != "" {
			updateOpts.Output.Text = new(opts.Output.Text)
		}
	}

	for _, action := range opts.Actions {
		updateOpts.Actions = append(updateOpts.Actions, &gh.CheckRunAction{
			Label:       action.Label,
			Description: action.Description,
			Identifier:  action.Identifier,
		})
	}

	_, _, err := ic.client.Checks.UpdateCheckRun(ctx, owner, repoName, checkRunID, updateOpts)
	if err != nil {
		return fmt.Errorf("update check run: %w", err)
	}
	return nil
}

// CheckRunResult holds the key fields from a GitHub Check Run.
type CheckRunResult struct {
	ID         int64
	Name       string
	Status     string // "queued", "in_progress", "completed"
	Conclusion string // "success", "failure", "neutral", "action_required"
}

// FindCheckRunByName searches for a check run on a specific commit by name.
// Only check runs created by a trusted SchemaBot GitHub App are considered:
// this deployment's own App plus any configured trusted sibling deployment
// Apps. A same-named check run created by any other app (for example a GitHub
// Actions job configured with a matching name) can never satisfy a gate that
// relies on this lookup; the slugs of such apps are returned so callers can
// tell "no check exists" apart from "a check exists but its App is not
// trusted" and surface the right remediation. When multiple trusted-app runs
// share the name, the most recently created one (highest check run ID) is
// returned.
//
// Returns a nil run if no trusted-app check run matches. Returns an error
// when the own app slug is unknown and no trusted sibling slugs are
// configured — check run ownership cannot be verified against anything, and
// callers must fail closed instead of trusting a same-named run.
func (ic *InstallationClient) FindCheckRunByName(ctx context.Context, repo, headSHA, checkName string) (*CheckRunResult, []string, error) {
	if ic.loadAppSlug() == "" && len(ic.trustedCheckAppSlugs) == 0 {
		return nil, nil, fmt.Errorf("find check run %q on %s@%s: GitHub App slug is unavailable and no trusted sibling App slugs are configured, check run ownership cannot be verified", checkName, repo, headSHA)
	}

	owner, repoName := splitRepo(repo)
	ctx = withGitHubRateLimitContext(ctx, metrics.GitHubOperationListCheckRunsForRef, repo)
	opts := &gh.ListCheckRunsOptions{
		CheckName: new(checkName),
		ListOptions: gh.ListOptions{
			PerPage: 100,
		},
	}

	var newest *CheckRunResult
	var untrustedApps []string
	seenUntrusted := make(map[string]struct{})
	for {
		result, resp, err := ic.client.Checks.ListCheckRunsForRef(ctx, owner, repoName, headSHA, opts)
		if err != nil {
			return nil, nil, fmt.Errorf("list check runs named %q on %s@%s: %w", checkName, repo, headSHA, err)
		}
		if result != nil {
			for _, run := range result.CheckRuns {
				if !ic.isTrustedCheckAppSlug(run.GetApp().GetSlug()) {
					ic.logger.Warn("ignoring same-named check run created by an untrusted GitHub App",
						"repo", repo, "head_sha", headSHA, "check_name", checkName,
						"check_run_id", run.GetID(), "app_slug", run.GetApp().GetSlug())
					slug := run.GetApp().GetSlug()
					if _, seen := seenUntrusted[slug]; !seen {
						seenUntrusted[slug] = struct{}{}
						untrustedApps = append(untrustedApps, slug)
					}
					continue
				}
				if newest == nil || run.GetID() > newest.ID {
					newest = &CheckRunResult{
						ID:         run.GetID(),
						Name:       run.GetName(),
						Status:     run.GetStatus(),
						Conclusion: run.GetConclusion(),
					}
				}
			}
		}
		if resp == nil || resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}

	return newest, untrustedApps, nil
}

// PRCheckStatus represents the status of a single PR check (check run or commit status).
type PRCheckStatus struct {
	Name        string
	Status      string // "completed", "in_progress", "queued"
	Conclusion  string // "success", "failure", "neutral", "skipped", etc.
	AppSlug     string // creating GitHub App slug; empty for commit statuses
	IsSchemaBot bool   // true if this check was created by a trusted SchemaBot GitHub App
}

// isOwnAppSlug returns true if the given slug belongs to this SchemaBot
// instance. An empty slug never matches — both to handle StatusContext rows
// (which have no App) and to fail closed when ic.appSlug has not yet been
// fetched, so cached rows are never misclassified as SchemaBot's own.
func (ic *InstallationClient) isOwnAppSlug(slug string) bool {
	if slug == "" {
		return false
	}
	own := ic.loadAppSlug()
	if own == "" {
		return false
	}
	return strings.EqualFold(slug, own)
}

// isTrustedCheckAppSlug returns true if the given slug belongs to a SchemaBot
// deployment this instance trusts: its own App or a configured trusted sibling
// deployment App. An empty slug never matches, so StatusContext rows (which
// have no App) and unverifiable rows fail closed.
func (ic *InstallationClient) isTrustedCheckAppSlug(slug string) bool {
	if slug == "" {
		return false
	}
	if ic.isOwnAppSlug(slug) {
		return true
	}
	for _, trusted := range ic.trustedCheckAppSlugs {
		if strings.EqualFold(slug, trusted) {
			return true
		}
	}
	return false
}

// GetPRCheckStatuses fetches check runs and commit statuses for a ref via REST.
// SchemaBot check runs are identified via the GitHub App slug (more reliable
// than name matching): this deployment's own App plus any configured trusted
// sibling deployment Apps. Sibling deployments' aggregate checks are governed
// by SchemaBot's own promotion and merge gates, so they are classified as
// SchemaBot checks rather than treated as external CI.
//
// Concurrent calls for the same (repo, ref) collapse to a single upstream
// GitHub read via the Client-shared singleflight (when configured), so
// a webhook delivery or command burst that fans out to multiple gate
// checks for the same commit shares the upstream work — even across the
// short-lived InstallationClients spawned per delivery.
//
// requiredChecks is optional. When provided, commit statuses are fetched first;
// if they contain every configured required check, check runs are not fetched.
// This keeps status-only gates from depending on unrelated check-run access.
//
// The singleflight delivers identity-independent rows; IsSchemaBot is re-derived
// per call against this client's appSlug snapshot so a shared fetch delivered to
// N waiters with different appSlug snapshots is classified correctly for each.
func (ic *InstallationClient) GetPRCheckStatuses(ctx context.Context, repo string, ref string, requiredChecks []string) ([]PRCheckStatus, error) {
	var (
		rows []CheckStatusRow
		err  error
	)
	if ic.checkStatusSingleflight == nil {
		rows, err = ic.fetchPRCheckStatuses(ctx, repo, ref, requiredChecks)
	} else {
		// The singleflight supplies its own ctx to the fetch so a
		// caller cancelling cannot abort the shared GitHub request and
		// fail unrelated waiters.
		rows, err = ic.checkStatusSingleflight.Do(ctx, repo, checkStatusSingleflightKey(ref, requiredChecks), func(fetchCtx context.Context) ([]CheckStatusRow, error) {
			return ic.fetchPRCheckStatuses(fetchCtx, repo, ref, requiredChecks)
		})
	}
	if err != nil {
		return nil, err
	}

	out := make([]PRCheckStatus, len(rows))
	for i, r := range rows {
		out[i] = PRCheckStatus{
			Name:        r.Name,
			Status:      r.Status,
			Conclusion:  r.Conclusion,
			AppSlug:     r.AppSlug,
			IsSchemaBot: ic.isTrustedCheckAppSlug(r.AppSlug),
		}
	}
	return out, nil
}

// CheckStatusAccessDiagnostic captures what SchemaBot can prove about the
// installation token's ability to read the REST resources underlying the PR
// check-status gate.
type CheckStatusAccessDiagnostic struct {
	ChecksReadable         bool
	CommitStatusesReadable bool
	ChecksError            string
	CommitStatusesError    string
	MissingPermissions     []string
}

// DiagnoseCheckStatusAccess probes REST endpoints used to reconstruct a PR's
// checks/statuses. It is intended for error reporting after check-status reads
// fail, not for the hot success path.
func (ic *InstallationClient) DiagnoseCheckStatusAccess(ctx context.Context, repo string, ref string) CheckStatusAccessDiagnostic {
	owner, repoName := splitRepo(repo)
	d := CheckStatusAccessDiagnostic{}

	checksCtx := withGitHubRateLimitContext(ctx, metrics.GitHubOperationListCheckRunsForRef, repo)
	if _, _, err := ic.client.Checks.ListCheckRunsForRef(checksCtx, owner, repoName, ref, &gh.ListCheckRunsOptions{ListOptions: gh.ListOptions{PerPage: 1}}); err != nil {
		d.ChecksError = err.Error()
		if isResourceNotAccessible(err) {
			d.MissingPermissions = append(d.MissingPermissions, "Checks: Read")
		}
	} else {
		d.ChecksReadable = true
	}

	statusesCtx := withGitHubRateLimitContext(ctx, metrics.GitHubOperationGetCombinedStatus, repo)
	if _, _, err := ic.client.Repositories.GetCombinedStatus(statusesCtx, owner, repoName, ref, &gh.ListOptions{PerPage: 1}); err != nil {
		d.CommitStatusesError = err.Error()
		if isResourceNotAccessible(err) {
			d.MissingPermissions = append(d.MissingPermissions, "Commit statuses: Read")
		}
	} else {
		d.CommitStatusesReadable = true
	}

	return d
}

func isResourceNotAccessible(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Resource not accessible")
}

// fetchPRCheckStatuses performs the actual REST reads for GetPRCheckStatuses,
// returning identity-independent rows suitable for sharing across
// InstallationClients with different appSlug snapshots.
func (ic *InstallationClient) fetchPRCheckStatuses(ctx context.Context, repo string, ref string, requiredChecks []string) ([]CheckStatusRow, error) {
	owner, repoName := splitRepo(repo)

	var out []CheckStatusRow
	statusRows, err := ic.fetchCommitStatusRows(ctx, owner, repoName, repo, ref)
	if err != nil {
		return nil, err
	}
	out = append(out, statusRows...)

	if len(requiredChecks) > 0 && checkRowsContainRequiredChecks(statusRows, requiredChecks) {
		return out, nil
	}

	checkRows, err := ic.fetchCheckRunRows(ctx, owner, repoName, repo, ref)
	if err != nil {
		return nil, err
	}
	out = append(out, checkRows...)
	return out, nil
}

func (ic *InstallationClient) fetchCommitStatusRows(ctx context.Context, owner, repoName, repo, ref string) ([]CheckStatusRow, error) {
	ctx = withGitHubRateLimitContext(ctx, metrics.GitHubOperationGetCombinedStatus, repo)
	opts := &gh.ListOptions{PerPage: 100}
	var out []CheckStatusRow
	for {
		combined, resp, err := ic.client.Repositories.GetCombinedStatus(ctx, owner, repoName, ref, opts)
		if err != nil {
			return nil, fmt.Errorf("get combined commit status for %s@%s: %w", repo, ref, err)
		}
		if combined != nil {
			for _, status := range combined.Statuses {
				state, conclusion := mapLegacyStatusState(status.GetState())
				out = append(out, CheckStatusRow{
					Name:       status.GetContext(),
					Status:     state,
					Conclusion: conclusion,
				})
			}
		}
		if resp == nil || resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}
	return out, nil
}

func (ic *InstallationClient) fetchCheckRunRows(ctx context.Context, owner, repoName, repo, ref string) ([]CheckStatusRow, error) {
	ctx = withGitHubRateLimitContext(ctx, metrics.GitHubOperationListCheckRunsForRef, repo)
	filter := "latest"
	opts := &gh.ListCheckRunsOptions{Filter: &filter, ListOptions: gh.ListOptions{PerPage: 100}}
	var out []CheckStatusRow
	for {
		result, resp, err := ic.client.Checks.ListCheckRunsForRef(ctx, owner, repoName, ref, opts)
		if err != nil {
			return nil, fmt.Errorf("list check runs for %s@%s: %w", repo, ref, err)
		}
		if result != nil {
			for _, run := range result.CheckRuns {
				out = append(out, CheckStatusRow{
					Name:       run.GetName(),
					Status:     strings.ToLower(run.GetStatus()),
					Conclusion: strings.ToLower(run.GetConclusion()),
					AppSlug:    run.GetApp().GetSlug(),
				})
			}
		}
		if resp == nil || resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}
	return out, nil
}

func checkRowsContainRequiredChecks(rows []CheckStatusRow, requiredChecks []string) bool {
	seen := make(map[string]bool, len(rows))
	for _, row := range rows {
		seen[row.Name] = true
	}
	for _, required := range requiredChecks {
		if !seen[required] {
			return false
		}
	}
	return true
}

func checkStatusSingleflightKey(ref string, requiredChecks []string) string {
	if len(requiredChecks) == 0 {
		return ref
	}
	sortedRequiredChecks := append([]string(nil), requiredChecks...)
	sort.Strings(sortedRequiredChecks)
	return ref + "\x00" + strings.Join(sortedRequiredChecks, "\x00")
}

// mapLegacyStatusState maps a commit status state (EXPECTED, ERROR, FAILURE,
// PENDING, SUCCESS) to the (status, conclusion) pair used by PRCheckStatus, so
// downstream filters can treat check runs and legacy statuses uniformly.
func mapLegacyStatusState(state string) (status, conclusion string) {
	switch strings.ToUpper(state) {
	case "PENDING", "EXPECTED":
		return "in_progress", ""
	case "SUCCESS":
		return "completed", "success"
	case "FAILURE":
		return "completed", "failure"
	case "ERROR":
		return "completed", "error"
	default:
		return "completed", strings.ToLower(state)
	}
}

// TreeEntry represents a single entry in a Git tree.
type TreeEntry struct {
	Path string
	Mode string
	Type string // "blob" for files, "tree" for directories
	SHA  string
	Size int
}

// gitSymlinkMode is the git tree file mode for a symbolic link. Symlinks appear
// as "blob" tree entries with this mode; their content is the target path.
const gitSymlinkMode = "120000"

// FetchGitTree fetches the entire directory tree in one API call using recursive mode.
func (ic *InstallationClient) FetchGitTree(ctx context.Context, repo, treeSHA string) ([]TreeEntry, bool, error) {
	owner, repoName := splitRepo(repo)
	ghTree, err := retryGitHubUnavailableRead(ctx, ic.logger, "fetch git tree", []any{"repo", repo, "tree_sha", treeSHA}, func(ctx context.Context) (*gh.Tree, error) {
		ghTree, _, err := ic.client.Git.GetTree(ctx, owner, repoName, treeSHA, true)
		if err != nil {
			return nil, fmt.Errorf("fetch git tree: %w", classifyGitHubAPIError(err))
		}
		return ghTree, nil
	})
	if err != nil {
		return nil, false, err
	}

	entries := make([]TreeEntry, len(ghTree.Entries))
	for i, entry := range ghTree.Entries {
		entries[i] = TreeEntry{
			Path: entry.GetPath(),
			Mode: entry.GetMode(),
			Type: entry.GetType(),
			SHA:  entry.GetSHA(),
			Size: entry.GetSize(),
		}
	}
	return entries, ghTree.GetTruncated(), nil
}

// FetchBlobContent fetches file content using the Git Blob API.
func (ic *InstallationClient) FetchBlobContent(ctx context.Context, repo, blobSHA string) (string, error) {
	owner, repoName := splitRepo(repo)
	blob, err := retryGitHubUnavailableRead(ctx, ic.logger, "fetch blob", []any{"repo", repo, "blob_sha", blobSHA}, func(ctx context.Context) (*gh.Blob, error) {
		blob, _, err := ic.client.Git.GetBlob(ctx, owner, repoName, blobSHA)
		if err != nil {
			return nil, fmt.Errorf("fetch blob: %w", classifyGitHubAPIError(err))
		}
		return blob, nil
	})
	if err != nil {
		return "", err
	}

	content := blob.GetContent()
	if blob.GetEncoding() == "base64" {
		decoded, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(content, "\n", ""))
		if err != nil {
			return "", fmt.Errorf("decode base64 blob: %w", err)
		}
		return string(decoded), nil
	}
	return content, nil
}

// FetchFileContent gets file content from GitHub Contents API at a specific ref.
func (ic *InstallationClient) FetchFileContent(ctx context.Context, repo, filePath, ref string) (string, error) {
	owner, repoName := splitRepo(repo)
	opts := &gh.RepositoryContentGetOptions{Ref: ref}
	fileContent, err := retryGitHubUnavailableRead(ctx, ic.logger, "fetch file content", []any{"repo", repo, "path", filePath, "ref", ref}, func(ctx context.Context) (*gh.RepositoryContent, error) {
		fileContent, _, _, err := ic.client.Repositories.GetContents(ctx, owner, repoName, filePath, opts)
		if err != nil {
			return nil, fmt.Errorf("fetch file content: %w", classifyGitHubAPIError(err))
		}
		return fileContent, nil
	})
	if err != nil {
		return "", err
	}
	if fileContent == nil {
		return "", fmt.Errorf("file not found: %s", filePath)
	}
	content, err := fileContent.GetContent()
	if err != nil {
		return "", fmt.Errorf("decode file content: %w", err)
	}
	return content, nil
}

// GitHub caps the Contents API directory listing at this documented maximum and
// does not paginate or provide a completeness marker for larger directories.
// Treat reaching the cap as incomplete so schema discovery fails closed instead
// of silently dropping the lexicographic tail, which would surface as spurious
// DROP TABLE proposals or missed changes in the declarative differ.
const maxGitHubDirEntries = 1000

func (ic *InstallationClient) fetchDirectoryContents(ctx context.Context, repo, dirPath, ref string) ([]*gh.RepositoryContent, error) {
	owner, repoName := splitRepo(repo)
	opts := &gh.RepositoryContentGetOptions{Ref: ref}
	readResult, err := retryGitHubUnavailableRead(ctx, ic.logger, "fetch directory content", []any{"repo", repo, "path", dirPath, "ref", ref}, func(ctx context.Context) (struct {
		fileContent      *gh.RepositoryContent
		directoryContent []*gh.RepositoryContent
	}, error) {
		fileContent, directoryContent, _, err := ic.client.Repositories.GetContents(ctx, owner, repoName, dirPath, opts)
		if err != nil {
			return struct {
				fileContent      *gh.RepositoryContent
				directoryContent []*gh.RepositoryContent
			}{}, fmt.Errorf("fetch directory content at %s: %w", dirPath, classifyGitHubAPIError(err))
		}
		return struct {
			fileContent      *gh.RepositoryContent
			directoryContent []*gh.RepositoryContent
		}{fileContent: fileContent, directoryContent: directoryContent}, nil
	})
	if err != nil {
		return nil, err
	}
	if readResult.fileContent != nil {
		return nil, fmt.Errorf("expected directory at %s, found file", dirPath)
	}
	if len(readResult.directoryContent) >= maxGitHubDirEntries {
		return nil, fmt.Errorf("list schema directory %s in repo %s ref %s reached GitHub Contents API limit: %w", dirPath, repo, ref, ErrDirListingCapped)
	}
	return readResult.directoryContent, nil
}

// GitHubFile represents a file fetched from GitHub API.
type GitHubFile struct {
	Name    string
	Content string
	Path    string
}

// fileResult holds the result of a parallel file fetch.
type fileResult struct {
	file GitHubFile
	err  error
}

// FetchSchemaFilesOptimized fetches schema files by walking the configured schema directory.
// Accepts both flat files (single namespace) and namespace subdirectories (multiple namespaces).
//
// Supported layouts (see docs/namespaces.md):
//
//	MySQL — single namespace:
//	  schema/payments/schemabot.yaml        ← config can live inside namespace dir
//	  schema/payments/transactions.sql
//
//	MySQL — multiple namespaces:
//	  schema/schemabot.yaml                 ← config at schema root
//	  schema/payments/transactions.sql
//	  schema/payments_audit/audit_log.sql
//
//	Vitess — multiple keyspaces:
//	  schema/schemabot.yaml                 ← config at schema root
//	  schema/commerce/orders.sql
//	  schema/customers/users.sql
func (ic *InstallationClient) FetchSchemaFilesOptimized(ctx context.Context, repo string, headSHA, schemaPath, dbType string) ([]GitHubFile, error) {
	locators, err := ic.schemaFileLocators(ctx, repo, headSHA, schemaPath)
	if err != nil {
		if IsNotFoundError(err) {
			return ic.fetchSchemaFilesFromTree(ctx, repo, headSHA, schemaPath)
		}
		return nil, err
	}

	var filesToFetch []schemaFileLocator
	for _, loc := range locators {
		if !isManagedSchemaFile(loc.displayPath) {
			continue
		}
		filesToFetch = append(filesToFetch, loc)
	}

	if len(filesToFetch) == 0 {
		return []GitHubFile{}, nil
	}

	// Fetch all file contents in parallel with concurrency limit. Content is
	// fetched from fetchPath (the real path on the branch — the symlink target
	// for files reached through a symlinked namespace, since the Contents API
	// does not traverse symlinked directories), but each file is labeled with
	// displayPath so namespace grouping uses the symlink name.
	results := make(chan fileResult, len(filesToFetch))
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 10)

	for _, loc := range filesToFetch {
		wg.Go(func() {
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			content, err := ic.FetchFileContent(ctx, repo, loc.fetchPath, headSHA)
			if err != nil {
				results <- fileResult{err: fmt.Errorf("fetch %s: %w", loc.fetchPath, err)}
				return
			}
			results <- fileResult{
				file: GitHubFile{
					Name:    path.Base(loc.displayPath),
					Content: content,
					Path:    loc.displayPath,
				},
			}
		})
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var files []GitHubFile
	var fetchErr error
	for result := range results {
		if result.err != nil {
			fetchErr = result.err
			continue
		}
		files = append(files, result.file)
	}
	if fetchErr != nil {
		return nil, fetchErr
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})
	return files, nil
}

func (ic *InstallationClient) fetchSchemaFilesFromTree(ctx context.Context, repo string, headSHA, schemaPath string) ([]GitHubFile, error) {
	entries, truncated, err := ic.FetchGitTree(ctx, repo, headSHA)
	if err != nil {
		return nil, fmt.Errorf("fetch git tree: %w", err)
	}
	if truncated {
		return nil, fmt.Errorf("fetch schema files from %s in repo %s ref %s: %w", schemaPath, repo, headSHA, ErrGitTreeTruncated)
	}

	var filesToFetch []TreeEntry
	schemaPathPrefix := schemaPath + "/"
	for _, entry := range entries {
		if entry.Type != "blob" {
			continue
		}
		if !strings.HasPrefix(entry.Path, schemaPathPrefix) {
			continue
		}
		// A symbolic link under the schema root (such as a symlinked namespace
		// directory) is recorded in the git tree as a blob whose content is the
		// target path, not the target's files — which live elsewhere in the tree
		// and outside schemaPathPrefix. The tree fallback cannot resolve it, so
		// fail closed rather than silently dropping the symlinked schema. The
		// Contents API path (schemaFileLocators) resolves these.
		if entry.Mode == gitSymlinkMode {
			return nil, fmt.Errorf("schema path %s in repo %s ref %s contains a symlink the git-tree fallback cannot resolve: %s", schemaPath, repo, headSHA, entry.Path)
		}
		if !isManagedSchemaFile(entry.Path) {
			continue
		}

		relativePath := strings.TrimPrefix(entry.Path, schemaPathPrefix)
		hasNamespaceDir := strings.Contains(relativePath, "/")
		if !hasNamespaceDir || strings.Count(relativePath, "/") == 1 {
			filesToFetch = append(filesToFetch, entry)
		}
	}

	results := make(chan fileResult, len(filesToFetch))
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 10)
	for _, entry := range filesToFetch {
		wg.Go(func() {
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			content, err := ic.FetchBlobContent(ctx, repo, entry.SHA)
			if err != nil {
				results <- fileResult{err: fmt.Errorf("fetch %s: %w", entry.Path, err)}
				return
			}
			results <- fileResult{
				file: GitHubFile{
					Name:    path.Base(entry.Path),
					Content: content,
					Path:    entry.Path,
				},
			}
		})
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var files []GitHubFile
	var fetchErr error
	for result := range results {
		if result.err != nil {
			fetchErr = result.err
			continue
		}
		files = append(files, result.file)
	}
	if fetchErr != nil {
		return nil, fetchErr
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})
	return files, nil
}

// schemaFileLocator points at one schema file. fetchPath is where its content
// lives on the branch (used to GET content); displayPath is the path under the
// schema root used for namespace grouping. The two differ only when the file is
// reached through a symlinked namespace directory: fetchPath is the real symlink
// target, displayPath keeps the symlink (keyspace) name.
type schemaFileLocator struct {
	fetchPath   string
	displayPath string
}

// schemaFileLocators lists every schema file directly under the schema root and
// its namespace subdirectories. A namespace entry may be a real directory or a
// symlink to a directory elsewhere in the repository; symlinked namespaces are
// resolved so their files are discovered under the symlink's name. This is the
// server-side equivalent of the CLI following symlinks on the local filesystem.
func (ic *InstallationClient) schemaFileLocators(ctx context.Context, repo, ref, schemaPath string) ([]schemaFileLocator, error) {
	rootEntries, err := ic.fetchDirectoryContents(ctx, repo, schemaPath, ref)
	if err != nil {
		return nil, err
	}

	var locators []schemaFileLocator
	for _, entry := range rootEntries {
		switch entry.GetType() {
		case "file":
			// Flat layout: a file directly under the schema root.
			locators = append(locators, schemaFileLocator{fetchPath: entry.GetPath(), displayPath: entry.GetPath()})
		case "dir":
			subEntries, err := ic.fetchDirectoryContents(ctx, repo, entry.GetPath(), ref)
			if err != nil {
				return nil, fmt.Errorf("fetch schema namespace directory %s: %w", entry.GetPath(), err)
			}
			for _, sub := range subEntries {
				if sub.GetType() != "file" {
					ic.logger.Debug("skipping non-file entry in schema namespace directory",
						"repo", repo, "ref", ref, "path", sub.GetPath(), "type", sub.GetType())
					continue
				}
				locators = append(locators, schemaFileLocator{fetchPath: sub.GetPath(), displayPath: sub.GetPath()})
			}
		case "symlink":
			symlinked, err := ic.symlinkedNamespaceLocators(ctx, repo, ref, schemaPath, entry)
			if err != nil {
				return nil, err
			}
			locators = append(locators, symlinked...)
		default:
			ic.logger.Debug("skipping unsupported schema entry type",
				"repo", repo, "ref", ref, "path", entry.GetPath(), "type", entry.GetType())
		}
	}

	sort.Slice(locators, func(i, j int) bool {
		return locators[i].displayPath < locators[j].displayPath
	})
	return locators, nil
}

// symlinkedNamespaceLocators resolves a symlinked namespace directory under the
// schema root and returns a locator for each file inside the symlink target,
// keyed under the symlink's own name. The target must be a repository-relative
// directory that stays inside the repository; absolute targets and targets that
// escape the repository root are rejected.
func (ic *InstallationClient) symlinkedNamespaceLocators(ctx context.Context, repo, ref, schemaPath string, entry *gh.RepositoryContent) ([]schemaFileLocator, error) {
	symlinkName := path.Base(entry.GetPath())
	target := strings.TrimSpace(entry.GetTarget())
	if target == "" {
		// The Contents API does not populate a symlink's target in a directory
		// listing — only when the symlink path is fetched directly. Resolve it
		// with a follow-up fetch before giving up.
		content, _, err := ic.fetchRepositoryContents(ctx, repo, entry.GetPath(), ref)
		if err != nil {
			return nil, fmt.Errorf("resolve schema namespace symlink %s target: %w", entry.GetPath(), err)
		}
		if content != nil {
			target = strings.TrimSpace(content.GetTarget())
		}
	}
	if target == "" {
		return nil, fmt.Errorf("schema namespace symlink %s has empty target", entry.GetPath())
	}
	if strings.HasPrefix(target, "/") {
		return nil, fmt.Errorf("schema namespace symlink %s points to absolute path %s", entry.GetPath(), target)
	}
	resolved := path.Clean(path.Join(schemaPath, target))
	if !schemaPathWithinDirectory(".", resolved) {
		return nil, fmt.Errorf("schema namespace symlink %s points outside the repository: %s", entry.GetPath(), target)
	}
	if resolved == "." || resolved == path.Clean(schemaPath) || resolved == path.Clean(entry.GetPath()) {
		return nil, fmt.Errorf("schema namespace symlink %s must point to a distinct directory, not the repository root, its own schema root, or itself: %s", entry.GetPath(), target)
	}

	targetEntries, err := ic.fetchDirectoryContents(ctx, repo, resolved, ref)
	if err != nil {
		return nil, fmt.Errorf("resolve schema namespace symlink %s target %s: %w", entry.GetPath(), resolved, err)
	}

	locators := make([]schemaFileLocator, 0, len(targetEntries))
	for _, sub := range targetEntries {
		if sub.GetType() != "file" {
			ic.logger.Debug("skipping non-file entry in symlinked schema namespace target",
				"repo", repo, "ref", ref, "symlink", entry.GetPath(), "target_path", sub.GetPath(), "type", sub.GetType())
			continue
		}
		locators = append(locators, schemaFileLocator{
			fetchPath:   sub.GetPath(),
			displayPath: path.Join(schemaPath, symlinkName, path.Base(sub.GetPath())),
		})
	}
	return locators, nil
}

func isManagedSchemaFile(filePath string) bool {
	return strings.HasSuffix(filePath, ".sql") || path.Base(filePath) == "vschema.json"
}
