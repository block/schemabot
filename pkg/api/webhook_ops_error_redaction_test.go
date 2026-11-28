package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
)

// An operator inspecting a pull request's check while SchemaBot's storage is
// unreachable gets a 500 with a fixed message naming the request and saying to
// retry. The dial error, which names the storage address, stays in the server
// log with the operation, the repository, and the pull request.
func TestWebhookOpsRouteAnswersFixedMessageWhenStorageFails(t *testing.T) {
	store := &inspectStorage{
		checks:  &inspectCheckStore{err: errors.New("dial tcp 10.0.0.5:3306: i/o timeout")},
		applies: &inspectApplyStore{},
	}
	svc := New(store, inspectTestConfig(), nil, slog.New(slog.DiscardHandler))
	svc.checksInspectClientFor = func(context.Context, *ServerConfig, string, *slog.Logger) (checksInspectClient, error) {
		return &inspectGitHubClient{prInfo: &ghclient.PullRequestInfo{HeadSHA: "abc123", State: "open"}}, nil
	}
	logs := captureServiceLogs(svc)

	rec := inspectQueryAs(t, svc, "", "/api/checks/inspect?repo=acme/store&pull_request=7&environment=staging")

	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	resp := decodeErrorResponse(t, rec)
	assert.Equal(t, `checks inspect failed for repository "acme/store": SchemaBot hit an internal error. Retry in a moment. `+
		`If it keeps failing, a SchemaBot operator can find the cause in the server log.`, resp.Error)
	assert.NotContains(t, rec.Body.String(), "3306")

	line := requireLogLine(t, decodeLogLines(t, logs.Bytes()), "webhook operator request failed")
	assert.Equal(t, "ERROR", line["level"])
	assert.Equal(t, "checks_inspect", line["operation"])
	assert.Equal(t, "acme/store", line["repo"])
	assert.InDelta(t, 7, line["pr"], 0)
	assert.Equal(t, "staging", line["environment"])
	assert.Contains(t, line["error"], "dial tcp 10.0.0.5:3306: i/o timeout")
}

// A repository missing from the repos config is something the operator fixes,
// not something a retry fixes, so the fixed message names it instead of
// calling it an internal error. The wrap around it still stays out.
func TestWebhookOpsFailureMessageNamesUnconfiguredRepository(t *testing.T) {
	err := fmt.Errorf("resolve via 10.0.0.5: %w", ErrRepoNotConfigured)

	assert.Equal(t, `checks scan failed for repository "acme/store": the repository is not declared in this server's repos config. `+
		`Name a repository this server serves, or add it to the repos config.`,
		webhookOpsFailureMessage("checks_scan", "acme/store", err))
	assert.Equal(t, "checks repos failed: SchemaBot hit an internal error. Retry in a moment. "+
		"If it keeps failing, a SchemaBot operator can find the cause in the server log.",
		webhookOpsFailureMessage("checks_repos", "", errors.New("dial tcp 10.0.0.5:3306: i/o timeout")))
}

// A GitHub App that is not installed on the repository is fixed by installing
// it, so the message says so instead of telling the operator to retry.
func TestWebhookOpsFailureDoesNotAdviseRetryForUninstalledApp(t *testing.T) {
	err := fmt.Errorf("resolve GitHub App %q installation for acme/store: %w: 404", "app", ghclient.ErrAppNotInstalled)

	msg := webhookOpsFailureMessage("checks_inspect", "acme/store", err)

	assert.NotContains(t, msg, "Retry in a moment")
	assert.Contains(t, msg, "not installed")
}

// A repository missing from the repos config is the operator's to fix, so the
// failure logs at WARN rather than paging anyone as a server fault.
func TestWebhookOpsUnconfiguredRepoLogsAtWarn(t *testing.T) {
	svc := New(&inspectStorage{}, inspectTestConfig(), nil, slog.New(slog.DiscardHandler))
	logs := captureServiceLogs(svc)
	rec := httptest.NewRecorder()

	svc.writeWebhookOpsError(rec, "checks_scan", "acme/store", fmt.Errorf("resolve: %w", ErrRepoNotConfigured), "environment", "staging")

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	line := requireLogLine(t, decodeLogLines(t, logs.Bytes()), "webhook operator request failed: the repository is not declared in the repos config")
	assert.Equal(t, "WARN", line["level"])
	assert.Equal(t, "checks_scan", line["operation"])
}

// Each failure an operator fixes in config, in GitHub, or in the request
// answers what to change, never "Retry", and logs at WARN with the
// identifiers the route has in scope. Every case goes through the route an
// operator's CLI calls.
func TestWebhookOpsOperatorFixableCausesNameTheFix(t *testing.T) {
	notFound := &gh.ErrorResponse{Response: &http.Response{StatusCode: http.StatusNotFound}, Message: "Not Found"}
	inspectThrough := func(client checksInspectClient, resolveErr error) checksInspectClientResolver {
		return func(context.Context, *ServerConfig, string, *slog.Logger) (checksInspectClient, error) {
			return client, resolveErr
		}
	}
	withApps := func(repo RepoConfig) *ServerConfig {
		cfg := inspectTestConfig()
		cfg.Apps = map[string]GitHubAppConfig{"main": {AppID: "1", PrivateKey: "key"}}
		cfg.Repos = map[string]RepoConfig{"acme/store": repo}
		return cfg
	}

	tests := []struct {
		name    string
		cfg     *ServerConfig
		resolve checksInspectClientResolver
		send    func(t *testing.T, svc *Service) *httptest.ResponseRecorder
		wantMsg string
		wantLog string
		// wantAttrs are the identifiers the route logs beside the cause.
		wantAttrs map[string]any
	}{
		{
			name:    "a pull request number GitHub does not have",
			cfg:     inspectTestConfig(),
			resolve: inspectThrough(&inspectGitHubClient{prErr: fmt.Errorf("fetch pull request acme/store#9999: %w", notFound)}, nil),
			send: func(t *testing.T, svc *Service) *httptest.ResponseRecorder {
				return inspectQueryAs(t, svc, "", "/api/checks/inspect?repo=acme/store&pull_request=9999&environment=staging")
			},
			wantMsg:   `checks inspect failed for repository "acme/store": GitHub has no pull request #9999 in this repository. Check the pull request number.`,
			wantLog:   "webhook operator request failed: the request names something the operator must correct",
			wantAttrs: map[string]any{"operation": "checks_inspect", "pr": float64(9999), "environment": "staging"},
		},
		{
			name:    "a GitHub App not installed on the repository",
			cfg:     inspectTestConfig(),
			resolve: inspectThrough(nil, fmt.Errorf("resolve GitHub App %q installation for acme/store: %w: 404", "main", ghclient.ErrAppNotInstalled)),
			send: func(t *testing.T, svc *Service) *httptest.ResponseRecorder {
				return inspectQueryAs(t, svc, "", "/api/checks/inspect?repo=acme/store&pull_request=7&environment=staging")
			},
			wantMsg:   `checks inspect failed for repository "acme/store": the GitHub App that serves this repository is not installed on it. Install the App on the repository.`,
			wantLog:   "webhook operator request failed: the GitHub App is not installed on the repository",
			wantAttrs: map[string]any{"operation": "checks_inspect", "pr": float64(7), "environment": "staging"},
		},
		{
			name: "a repository whose github_app names no configured App",
			cfg:  withApps(RepoConfig{GitHubApp: "retired"}),
			send: func(t *testing.T, svc *Service) *httptest.ResponseRecorder {
				return inspectQueryAs(t, svc, "", "/api/checks/inspect?repo=acme/store&pull_request=7&environment=staging")
			},
			wantMsg: `checks inspect failed for repository "acme/store": the repos config sets this repository's github_app to "retired", ` +
				`which is not one of the apps in the server config. Set its github_app to a configured App.`,
			wantLog:   "webhook operator request failed: the repository's github_app is not a configured App",
			wantAttrs: map[string]any{"operation": "checks_inspect", "pr": float64(7), "environment": "staging"},
		},
		{
			name: "a repository with no github_app",
			cfg:  withApps(RepoConfig{}),
			send: func(t *testing.T, svc *Service) *httptest.ResponseRecorder {
				return postJSON(t, svc.handleChecksScan, "/api/checks/scan",
					ChecksScanRequest{Repo: "acme/store", Environment: "staging", CheckName: "SchemaBot (staging)", Page: 2})
			},
			wantMsg: `checks scan failed for repository "acme/store": the repos config gives this repository no github_app. ` +
				`Set its github_app to one of the apps in the server config.`,
			wantLog:   "webhook operator request failed: the repository's github_app is not a configured App",
			wantAttrs: map[string]any{"operation": "checks_scan", "environment": "staging", "check_name": "SchemaBot (staging)", "page": float64(2)},
		},
		{
			name: "a server with no GitHub webhook runtime",
			cfg:  inspectTestConfig(),
			send: func(t *testing.T, svc *Service) *httptest.ResponseRecorder {
				return postJSON(t, svc.handleChecksSynthesize, "/api/checks/synthesize",
					ChecksSynthesizeRequest{Repo: "acme/store", Environment: "staging", PRs: []int{7, 9}, MergeQueueHeads: []string{"group-412"}})
			},
			wantMsg: `checks synthesize failed for repository "acme/store": this server has no GitHub App configured, so it cannot replay webhook deliveries to create Check Runs. ` +
				`Configure a GitHub App on this server, or send the request to the SchemaBot server that receives this repository's webhooks.`,
			wantLog: "webhook operator request failed: no GitHub webhook runtime is configured on this server",
			wantAttrs: map[string]any{"operation": "checks_synthesize", "environment": "staging",
				"prs": []any{float64(7), float64(9)}, "merge_queue_heads": []any{"group-412"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &inspectStorage{checks: &inspectCheckStore{}, applies: &inspectApplyStore{}}
			svc := New(store, tt.cfg, nil, slog.New(slog.DiscardHandler))
			svc.checksInspectClientFor = tt.resolve
			logs := captureServiceLogs(svc)

			rec := tt.send(t, svc)

			require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
			resp := decodeErrorResponse(t, rec)
			assert.Equal(t, tt.wantMsg, resp.Error)
			assert.NotContains(t, resp.Error, "Retry")

			line := requireLogLine(t, decodeLogLines(t, logs.Bytes()), tt.wantLog)
			assert.Equal(t, "WARN", line["level"])
			assert.Equal(t, "acme/store", line["repo"])
			for key, want := range tt.wantAttrs {
				assert.Equal(t, want, line[key], key)
			}
			assert.NotEmpty(t, line["error"])
		})
	}
}
