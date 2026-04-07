package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	gh "github.com/google/go-github/v68/github"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
)

// fakeClientFactory returns a pre-built InstallationClient for any installation ID.
type fakeClientFactory struct {
	client *ghclient.InstallationClient
}

func (f *fakeClientFactory) ForInstallation(_ int64) (*ghclient.InstallationClient, error) {
	return f.client, nil
}

// setupGitHubServer creates an httptest server and a go-github client pointed at it.
// Uses go-github's own testing pattern: real client, fake server.
func setupGitHubServer(t *testing.T) (*gh.Client, *http.ServeMux) {
	t.Helper()
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	var err error
	client.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)
	return client, mux
}

// prWebhookPayloadOpts configures how buildPRWebhookRequest constructs the payload.
type prWebhookPayloadOpts struct {
	action  string // "opened", "synchronize", "reopened", "closed", etc.
	headSHA string
	headRef string
}

// buildPRWebhookRequest constructs a valid pull_request webhook POST request with HMAC signature.
func buildPRWebhookRequest(t *testing.T, opts prWebhookPayloadOpts, secret []byte) *http.Request {
	t.Helper()

	if opts.action == "" {
		opts.action = "opened"
	}
	if opts.headSHA == "" {
		opts.headSHA = "abc123"
	}
	if opts.headRef == "" {
		opts.headRef = "feature-branch"
	}

	payload := map[string]any{
		"action": opts.action,
		"pull_request": map[string]any{
			"number": 1,
			"head": map[string]any{
				"sha": opts.headSHA,
				"ref": opts.headRef,
			},
			"user": map[string]any{
				"login": "testuser",
			},
		},
		"repository": map[string]any{
			"full_name": "octocat/hello-world",
		},
		"installation": map[string]any{
			"id": 12345,
		},
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-GitHub-Event", "pull_request")

	if len(secret) > 0 {
		mac := hmac.New(sha256.New, secret)
		mac.Write(body)
		sig := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		req.Header.Set("X-Hub-Signature-256", sig)
	}

	return req
}

// webhookPayloadOpts configures how buildWebhookRequest constructs the payload.
type webhookPayloadOpts struct {
	comment   string
	userType  string // "User" or "Bot"
	userLogin string
	isPR      bool // whether the issue has a pull_request field
}

// buildWebhookRequest constructs a valid issue_comment webhook POST request with HMAC signature.
func buildWebhookRequest(t *testing.T, opts webhookPayloadOpts, secret []byte) *http.Request {
	t.Helper()

	if opts.userLogin == "" {
		opts.userLogin = "testuser"
	}
	if opts.userType == "" {
		opts.userType = "User"
	}

	payload := map[string]any{
		"action": "created",
		"comment": map[string]any{
			"id":   42,
			"body": opts.comment,
			"user": map[string]any{
				"login": opts.userLogin,
				"type":  opts.userType,
			},
		},
		"repository": map[string]any{
			"full_name": "octocat/hello-world",
		},
		"installation": map[string]any{
			"id": 12345,
		},
	}

	issue := map[string]any{
		"number": 1,
	}
	if opts.isPR {
		issue["pull_request"] = map[string]any{
			"url": "https://api.github.com/repos/octocat/hello-world/pulls/1",
		}
	}
	payload["issue"] = issue

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-GitHub-Event", "issue_comment")

	if len(secret) > 0 {
		mac := hmac.New(sha256.New, secret)
		mac.Write(body)
		sig := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		req.Header.Set("X-Hub-Signature-256", sig)
	}

	return req
}
