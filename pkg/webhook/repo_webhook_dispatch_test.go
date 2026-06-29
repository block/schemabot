package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

func repoWebhookSig(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func repoWebhookRequest(t *testing.T, targetType string, body []byte) *http.Request {
	t.Helper()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", strings.NewReader(string(body)))
	if targetType != "" {
		r.Header.Set(headerHookTargetType, targetType)
	}
	return r
}

// Repo-webhook dispatch only engages for repository-targeted deliveries and
// only when a repo-webhook secret is configured; App-webhook deliveries
// ("integration") and unconfigured deployments are unaffected.
func TestRepoWebhookTargeted(t *testing.T) {
	body := []byte(`{}`)
	configured := &Handler{repoWebhookSecret: []byte("s"), logger: testLogger()}
	unconfigured := &Handler{logger: testLogger()}

	assert.True(t, configured.repoWebhookTargeted(repoWebhookRequest(t, "repository", body)))
	assert.True(t, configured.repoWebhookTargeted(repoWebhookRequest(t, "Repository", body)), "target type is case-insensitive")
	assert.False(t, configured.repoWebhookTargeted(repoWebhookRequest(t, "integration", body)), "App-webhook deliveries are not repo-webhook")
	assert.False(t, configured.repoWebhookTargeted(repoWebhookRequest(t, "", body)))
	assert.False(t, unconfigured.repoWebhookTargeted(repoWebhookRequest(t, "repository", body)), "no repo-webhook secret means no repo-webhook dispatch")
}

// The payload-supplied installation id always wins; the out-of-band id resolved
// for repo-webhook deliveries (which carry no installation id) is the fallback.
func TestEffectiveInstallationID(t *testing.T) {
	h := &Handler{logger: testLogger()}
	ctx := t.Context()

	assert.Equal(t, int64(7), h.effectiveInstallationID(ctx, 7), "payload id is used when present")
	assert.Equal(t, int64(99), h.effectiveInstallationID(withResolvedInstallationID(ctx, 99), 0), "resolved id used when payload is 0")
	assert.Equal(t, int64(7), h.effectiveInstallationID(withResolvedInstallationID(ctx, 99), 7), "payload id takes precedence over resolved id")
	assert.Equal(t, int64(0), h.effectiveInstallationID(ctx, 0), "0 when neither is available")
}

// A repository-targeted delivery is authenticated against the repo-webhook
// secret and attributed to the single shared App; a bad signature is rejected.
func TestAuthenticateWebhook_RepoWebhookMode(t *testing.T) {
	secret := []byte("repo-secret")
	h := &Handler{repoWebhookSecret: secret, logger: testLogger()}
	body := []byte(`{"action":"opened","repository":{"full_name":"octocat/hello-world"}}`)

	valid := repoWebhookRequest(t, hookTargetTypeRepo, body)
	valid.Header.Set(headerSignature256, repoWebhookSig(secret, body))
	name, _, status, ok := h.authenticateWebhook(valid, body)
	require.True(t, ok)
	assert.Equal(t, defaultAppName, name)
	assert.Empty(t, status)

	bad := repoWebhookRequest(t, hookTargetTypeRepo, body)
	bad.Header.Set(headerSignature256, repoWebhookSig([]byte("wrong-secret"), body))
	_, _, status, ok = h.authenticateWebhook(bad, body)
	assert.False(t, ok)
	assert.Equal(t, "invalid_signature", status)
}

// repoWebhookIssueComment builds a repository-targeted issue_comment delivery
// signed with the repo-webhook secret. It deliberately omits the installation
// object: repository-level webhook payloads carry no installation id, so the
// handler must resolve it out of band before any command runs.
func repoWebhookIssueComment(t *testing.T, comment string, repoSecret []byte) *http.Request {
	t.Helper()
	payload := map[string]any{
		"action": "created",
		"comment": map[string]any{
			"id":   42,
			"body": comment,
			"user": map[string]any{"login": "testuser", "type": "User"},
		},
		"issue": map[string]any{
			"number":       1,
			"pull_request": map[string]any{"url": "https://api.github.com/repos/octocat/hello-world/pulls/1"},
		},
		"repository": map[string]any{"full_name": "octocat/hello-world"},
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-GitHub-Event", "issue_comment")
	req.Header.Set(headerHookTargetType, hookTargetTypeRepo)
	req.Header.Set(headerSignature256, repoWebhookSig(repoSecret, body))
	return req
}

// A repository-targeted delivery resolves its installation id out of band and
// injects it onto the request context so command handlers can act on the PR —
// even though the payload carries no installation object. The "help" command
// posts a comment via the resolved installation client, proving the full path:
// repo-secret auth → installation resolution → context injection → handler.
func TestServeHTTPRepoWebhookResolvesInstallation(t *testing.T) {
	client, mux := setupGitHubServer(t)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	installClient := ghclient.NewInstallationClient(client, testLogger())
	factory := &fakeClientFactory{client: installClient}
	service := api.New(nil, &api.ServerConfig{Repos: map[string]api.RepoConfig{"octocat/hello-world": {}}}, nil, testLogger())
	secret := []byte("repo-secret")
	h := &Handler{
		service:           service,
		ghClients:         ghclient.NewSingleClientSet(defaultAppName, factory),
		logger:            testLogger(),
		repoWebhookSecret: secret,
	}

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, repoWebhookIssueComment(t, "schemabot help", secret))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "help posted")
}

// If the installation id cannot be resolved for a repository-targeted delivery,
// the handler must fail closed with 500 rather than proceed without an
// installation client. GitHub retries on 5xx, so a transient resolution failure
// is recoverable; silently dropping the event is not.
func TestServeHTTPRepoWebhookResolutionFailureFailsClosed(t *testing.T) {
	factory := &fakeClientFactory{installIDErr: errors.New("github unavailable")}
	service := api.New(nil, &api.ServerConfig{Repos: map[string]api.RepoConfig{"octocat/hello-world": {}}}, nil, testLogger())
	secret := []byte("repo-secret")
	h := &Handler{
		service:           service,
		ghClients:         ghclient.NewSingleClientSet(defaultAppName, factory),
		logger:            testLogger(),
		repoWebhookSecret: secret,
	}

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, repoWebhookIssueComment(t, "schemabot help", secret))

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}
