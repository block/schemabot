package livee2e

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/go-github/v86/github"
)

// CheckRun is the state of one named Check Run on a commit.
type CheckRun struct {
	Status     string
	Conclusion string
	Title      string
}

// PullRequest is an open pull request the sweep may close.
type PullRequest struct {
	Number int
	Branch string
}

// GitHub is the part of the GitHub API the runner drives a test pull request
// through. Every call names a repository-relative object; the repository is
// fixed when the client is built.
type GitHub interface {
	BranchHead(ctx context.Context, branch string) (string, error)
	CreateBranch(ctx context.Context, branch, sha string) error
	DeleteBranch(ctx context.Context, branch string) error
	// PutFile creates or replaces a file on branch and returns the new head
	// commit SHA.
	PutFile(ctx context.Context, branch, path, message string, content []byte) (string, error)
	OpenPullRequest(ctx context.Context, head, base, title, body string) (int, error)
	ClosePullRequest(ctx context.Context, number int) error
	OpenPullRequests(ctx context.Context, branchPrefix string) ([]PullRequest, error)
	CreateComment(ctx context.Context, number int, body string) (int64, error)
	Comments(ctx context.Context, number int) ([]Comment, error)
	// CheckRun returns the newest Check Run named name that the GitHub App
	// appSlug posted on sha, or nil when none exists yet.
	CheckRun(ctx context.Context, sha, name, appSlug string) (*CheckRun, error)
}

// apiGitHub implements GitHub with the REST API.
type apiGitHub struct {
	client *github.Client
	owner  string
	repo   string
}

// NewGitHub returns a GitHub client for repo ("owner/name") that
// authenticates with token.
func NewGitHub(repo, token string) (GitHub, error) {
	owner, name, ok := strings.Cut(repo, "/")
	if !ok || owner == "" || name == "" || strings.Contains(name, "/") {
		return nil, fmt.Errorf("repository %q is not in owner/name form", repo)
	}
	if token == "" {
		return nil, errors.New("a GitHub token is required")
	}
	httpClient := &http.Client{Timeout: 30 * time.Second}
	return &apiGitHub{
		client: github.NewClient(httpClient).WithAuthToken(token),
		owner:  owner,
		repo:   name,
	}, nil
}

func (g *apiGitHub) BranchHead(ctx context.Context, branch string) (string, error) {
	ref, _, err := g.client.Git.GetRef(ctx, g.owner, g.repo, "heads/"+branch)
	if err != nil {
		return "", fmt.Errorf("read head of branch %s in %s/%s: %w", branch, g.owner, g.repo, err)
	}
	return ref.GetObject().GetSHA(), nil
}

func (g *apiGitHub) CreateBranch(ctx context.Context, branch, sha string) error {
	_, _, err := g.client.Git.CreateRef(ctx, g.owner, g.repo, github.CreateRef{Ref: "refs/heads/" + branch, SHA: sha})
	if err != nil {
		return fmt.Errorf("create branch %s at %s in %s/%s: %w", branch, sha, g.owner, g.repo, err)
	}
	return nil
}

func (g *apiGitHub) DeleteBranch(ctx context.Context, branch string) error {
	resp, err := g.client.Git.DeleteRef(ctx, g.owner, g.repo, "heads/"+branch)
	if err == nil {
		return nil
	}
	deleteErr := fmt.Errorf("delete branch %s in %s/%s: %w", branch, g.owner, g.repo, err)
	if resp == nil || resp.StatusCode != http.StatusUnprocessableEntity {
		return deleteErr
	}
	// GitHub answers 422 both when the branch is already gone, as when
	// repository settings or an earlier sweep deleted a closed pull request's
	// branch, and when it refuses the deletion. Only a branch that no longer
	// exists counts as deleted.
	_, getResp, getErr := g.client.Git.GetRef(ctx, g.owner, g.repo, "heads/"+branch)
	if getErr != nil && getResp != nil && getResp.StatusCode == http.StatusNotFound {
		return nil
	}
	return deleteErr
}

func (g *apiGitHub) PutFile(ctx context.Context, branch, path, message string, content []byte) (string, error) {
	opts := &github.RepositoryContentFileOptions{
		Message: new(message),
		Content: content,
		Branch:  new(branch),
	}
	existing, _, resp, err := g.client.Repositories.GetContents(ctx, g.owner, g.repo, path, &github.RepositoryContentGetOptions{Ref: branch})
	switch {
	case err == nil && existing == nil:
		return "", fmt.Errorf("%s on branch %s in %s/%s is a directory, not a file", path, branch, g.owner, g.repo)
	case err == nil:
		opts.SHA = existing.SHA
	case resp != nil && resp.StatusCode == http.StatusNotFound:
		// A new file: the create call takes no blob SHA.
	default:
		return "", fmt.Errorf("read %s on branch %s in %s/%s: %w", path, branch, g.owner, g.repo, err)
	}
	result, _, err := g.client.Repositories.UpdateFile(ctx, g.owner, g.repo, path, opts)
	if err != nil {
		return "", fmt.Errorf("write %s on branch %s in %s/%s: %w", path, branch, g.owner, g.repo, err)
	}
	return result.GetSHA(), nil
}

func (g *apiGitHub) OpenPullRequest(ctx context.Context, head, base, title, body string) (int, error) {
	pr, _, err := g.client.PullRequests.Create(ctx, g.owner, g.repo, &github.NewPullRequest{
		Title: new(title),
		Head:  new(head),
		Base:  new(base),
		Body:  new(body),
	})
	if err != nil {
		return 0, fmt.Errorf("open pull request from %s into %s in %s/%s: %w", head, base, g.owner, g.repo, err)
	}
	return pr.GetNumber(), nil
}

func (g *apiGitHub) ClosePullRequest(ctx context.Context, number int) error {
	_, _, err := g.client.PullRequests.Edit(ctx, g.owner, g.repo, number, &github.PullRequest{State: new("closed")})
	if err != nil {
		return fmt.Errorf("close pull request %s/%s#%d: %w", g.owner, g.repo, number, err)
	}
	return nil
}

func (g *apiGitHub) OpenPullRequests(ctx context.Context, branchPrefix string) ([]PullRequest, error) {
	opts := &github.PullRequestListOptions{State: "open", ListOptions: github.ListOptions{PerPage: 100}}
	var out []PullRequest
	for {
		prs, resp, err := g.client.PullRequests.List(ctx, g.owner, g.repo, opts)
		if err != nil {
			return nil, fmt.Errorf("list open pull requests in %s/%s: %w", g.owner, g.repo, err)
		}
		for _, pr := range prs {
			// Repository names are case-insensitive, so the canonical name
			// can differ in case from the configured one. A fork's pull
			// request still differs in owner and stays out.
			if !strings.EqualFold(pr.GetHead().GetRepo().GetFullName(), g.owner+"/"+g.repo) {
				continue
			}
			branch := pr.GetHead().GetRef()
			if strings.HasPrefix(branch, branchPrefix) {
				out = append(out, PullRequest{Number: pr.GetNumber(), Branch: branch})
			}
		}
		if resp.NextPage == 0 {
			return out, nil
		}
		opts.Page = resp.NextPage
	}
}

func (g *apiGitHub) CreateComment(ctx context.Context, number int, body string) (int64, error) {
	c, _, err := g.client.Issues.CreateComment(ctx, g.owner, g.repo, number, &github.IssueComment{Body: new(body)})
	if err != nil {
		return 0, fmt.Errorf("comment on pull request %s/%s#%d: %w", g.owner, g.repo, number, err)
	}
	return c.GetID(), nil
}

func (g *apiGitHub) Comments(ctx context.Context, number int) ([]Comment, error) {
	opts := &github.IssueListCommentsOptions{ListOptions: github.ListOptions{PerPage: 100}}
	var out []Comment
	for {
		comments, resp, err := g.client.Issues.ListComments(ctx, g.owner, g.repo, number, opts)
		if err != nil {
			return nil, fmt.Errorf("list comments on pull request %s/%s#%d: %w", g.owner, g.repo, number, err)
		}
		for _, c := range comments {
			out = append(out, Comment{
				ID:     c.GetID(),
				Author: c.GetUser().GetLogin(),
				IsBot:  c.GetUser().GetType() == "Bot",
				Body:   c.GetBody(),
			})
		}
		if resp.NextPage == 0 {
			return out, nil
		}
		opts.Page = resp.NextPage
	}
}

func (g *apiGitHub) CheckRun(ctx context.Context, sha, name, appSlug string) (*CheckRun, error) {
	opts := &github.ListCheckRunsOptions{CheckName: new(name), ListOptions: github.ListOptions{PerPage: 100}}
	// Another App can post a run with the same name, so only appSlug's runs
	// count. A rerun adds a run, and the newest has the highest ID.
	var newest *github.CheckRun
	for {
		runs, resp, err := g.client.Checks.ListCheckRunsForRef(ctx, g.owner, g.repo, sha, opts)
		if err != nil {
			return nil, fmt.Errorf("list check runs named %q on %s in %s/%s: %w", name, sha, g.owner, g.repo, err)
		}
		for _, run := range runs.CheckRuns {
			if run.GetApp().GetSlug() == appSlug && (newest == nil || run.GetID() > newest.GetID()) {
				newest = run
			}
		}
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}
	if newest == nil {
		return nil, nil
	}
	return &CheckRun{
		Status:     newest.GetStatus(),
		Conclusion: newest.GetConclusion(),
		Title:      newest.GetOutput().GetTitle(),
	}, nil
}

// githubUnavailable reports whether err is GitHub refusing or failing a call
// for reasons of its own: a primary or secondary rate limit, or a server
// error. Such a failure says nothing about SchemaBot. The client does not
// retry these itself, because retrying a comment could post a command twice.
func githubUnavailable(err error) bool {
	var rateLimit *github.RateLimitError
	var secondary *github.AbuseRateLimitError
	if errors.As(err, &rateLimit) || errors.As(err, &secondary) {
		return true
	}
	var resp *github.ErrorResponse
	if !errors.As(err, &resp) || resp.Response == nil {
		return false
	}
	code := resp.Response.StatusCode
	return code == http.StatusTooManyRequests || code >= http.StatusInternalServerError
}
