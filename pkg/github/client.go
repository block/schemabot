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
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/bradleyfalzon/ghinstallation/v2"
	gh "github.com/google/go-github/v68/github"
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
}

// NewClient creates a new GitHub App client.
func NewClient(appID int64, privateKey []byte, logger *slog.Logger) *Client {
	return &Client{
		appID:      appID,
		privateKey: privateKey,
		logger:     logger,
	}
}

// ForInstallation creates a GitHub client scoped to a specific installation.
// The ghinstallation library handles JWT generation, token exchange, caching,
// and refresh automatically.
func (c *Client) ForInstallation(installationID int64) (*InstallationClient, error) {
	transport, err := ghinstallation.New(http.DefaultTransport, c.appID, installationID, c.privateKey)
	if err != nil {
		return nil, fmt.Errorf("create installation transport: %w", err)
	}
	return &InstallationClient{
		client: gh.NewClient(&http.Client{Transport: transport, Timeout: 30 * time.Second}),
		logger: c.logger,
	}, nil
}

// NewInstallationClient creates an InstallationClient from a pre-configured go-github client.
// Used in tests to point at httptest.Server; production uses Client.ForInstallation().
func NewInstallationClient(client *gh.Client, logger *slog.Logger) *InstallationClient {
	return &InstallationClient{client: client, logger: logger}
}

// InstallationClient wraps a go-github client scoped to a specific GitHub App installation.
type InstallationClient struct {
	client *gh.Client
	logger *slog.Logger
}

// IsNotFoundError checks if an error is a GitHub API 404 Not Found error.
func IsNotFoundError(err error) bool {
	var ghErr *gh.ErrorResponse
	if errors.As(err, &ghErr) {
		return ghErr.Response != nil && ghErr.Response.StatusCode == http.StatusNotFound
	}
	return false
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

// FetchPullRequest gets PR information.
func (ic *InstallationClient) FetchPullRequest(ctx context.Context, repo string, pr int) (*PullRequestInfo, error) {
	owner, repoName := splitRepo(repo)
	ghPR, _, err := ic.client.PullRequests.Get(ctx, owner, repoName, pr)
	if err != nil {
		return nil, fmt.Errorf("fetch pull request: %w", err)
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
	Filename string
	Status   string // added, removed, modified, renamed
}

// FetchPRFiles gets the list of files changed in a PR.
func (ic *InstallationClient) FetchPRFiles(ctx context.Context, repo string, pr int) ([]PRFile, error) {
	owner, repoName := splitRepo(repo)
	opts := &gh.ListOptions{PerPage: 100}
	var allFiles []PRFile

	for {
		ghFiles, resp, err := ic.client.PullRequests.ListFiles(ctx, owner, repoName, pr, opts)
		if err != nil {
			return nil, fmt.Errorf("list PR files: %w", err)
		}
		for _, f := range ghFiles {
			allFiles = append(allFiles, PRFile{
				Filename: f.GetFilename(),
				Status:   f.GetStatus(),
			})
		}
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}

	return allFiles, nil
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

// TreeEntry represents a single entry in a Git tree.
type TreeEntry struct {
	Path string
	Mode string
	Type string // "blob" for files, "tree" for directories
	SHA  string
	Size int
}

// FetchGitTree fetches the entire directory tree in one API call using recursive mode.
func (ic *InstallationClient) FetchGitTree(ctx context.Context, repo, treeSHA string) ([]TreeEntry, bool, error) {
	owner, repoName := splitRepo(repo)
	ghTree, _, err := ic.client.Git.GetTree(ctx, owner, repoName, treeSHA, true)
	if err != nil {
		return nil, false, fmt.Errorf("fetch git tree: %w", err)
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
	blob, _, err := ic.client.Git.GetBlob(ctx, owner, repoName, blobSHA)
	if err != nil {
		return "", fmt.Errorf("fetch blob: %w", err)
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
	fileContent, _, _, err := ic.client.Repositories.GetContents(ctx, owner, repoName, filePath, opts)
	if err != nil {
		return "", fmt.Errorf("fetch file content: %w", err)
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

// FetchSchemaFilesOptimized fetches schema files using Tree API + parallel blob fetching.
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
	entries, _, err := ic.FetchGitTree(ctx, repo, headSHA)
	if err != nil {
		return nil, fmt.Errorf("fetch git tree: %w", err)
	}

	// Filter tree entries to find schema files under schemaPath
	var filesToFetch []TreeEntry
	schemaPathPrefix := schemaPath + "/"

	for _, entry := range entries {
		if entry.Type != "blob" {
			continue
		}
		if !strings.HasPrefix(entry.Path, schemaPathPrefix) {
			continue
		}
		if !strings.HasSuffix(entry.Path, ".sql") && !strings.HasSuffix(entry.Path, "vschema.json") {
			continue
		}

		relativePath := strings.TrimPrefix(entry.Path, schemaPathPrefix)
		hasNamespaceDir := strings.Contains(relativePath, "/")

		// Accept both flat files (single namespace) and namespace subdirs (multiple namespaces).
		// Only allow one level of nesting (schema/namespace/table.sql, not schema/a/b/table.sql).
		if !hasNamespaceDir || strings.Count(relativePath, "/") == 1 {
			filesToFetch = append(filesToFetch, entry)
		}
	}

	if len(filesToFetch) == 0 {
		return []GitHubFile{}, nil
	}

	// Fetch all file contents in parallel with concurrency limit
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

	return files, nil
}
