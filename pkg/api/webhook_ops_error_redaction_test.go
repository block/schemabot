package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"testing"

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
	assert.InDelta(t, 7, line["pull_request"], 0)
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
