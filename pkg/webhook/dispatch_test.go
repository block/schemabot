package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

// signWebhookBody returns the GitHub-style "sha256=<hex>" signature header
// value for the given body and secret. Mirrors what GitHub computes on
// every webhook delivery so multi-App dispatch tests can simulate signed
// requests for any configured App.
func signWebhookBody(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// newMultiAppTestHandler builds a Handler wired for multi-App dispatch
// with two Apps (app-a → ID 1001, app-b → ID 1002) plus a repos map that
// owns org-a/repo-x by app-a and org-b/repo-y by app-b. The returned
// secrets map is keyed by App name so callers can sign requests for a
// specific App.
func newMultiAppTestHandler(t *testing.T) (*Handler, map[string][]byte) {
	t.Helper()
	secrets := map[string][]byte{
		"app-a": []byte("secret-a"),
		"app-b": []byte("secret-b"),
	}
	appByID := map[int64]string{
		1001: "app-a",
		1002: "app-b",
	}
	clients := map[string]ghclient.GitHubClientFactory{
		"app-a": &fakeClientFactory{},
		"app-b": &fakeClientFactory{},
	}
	cfg := &api.ServerConfig{
		Apps: map[string]api.GitHubAppConfig{
			"app-a": {AppID: "1001", PrivateKey: "x", WebhookSecret: "secret-a"},
			"app-b": {AppID: "1002", PrivateKey: "x", WebhookSecret: "secret-b"},
		},
		Repos: map[string]api.RepoConfig{
			"org-a/repo-x": {GitHubApp: "app-a"},
			"org-b/repo-y": {GitHubApp: "app-b"},
		},
	}
	svc := api.New(&emptyStorage{}, cfg, nil, testLogger())
	h := NewHandlerWithDispatch(svc, ghclient.NewClientSet(clients), secrets, appByID, testLogger())
	return h, secrets
}

// newPingRequest constructs a minimal signed ping webhook for the given
// App. Ping uses an empty payload so handler routing returns 200 on the
// default case after authentication succeeds — keeps the assertions
// focused on dispatch behaviour rather than handler internals.
func newPingRequest(t *testing.T, appID int64, secret []byte, repo string) *http.Request {
	t.Helper()
	body := []byte(`{"action":"","repository":{"full_name":"` + repo + `"}}`)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-GitHub-Event", "ping")
	req.Header.Set(headerHookTargetType, hookTargetTypeApp)
	if appID != 0 {
		req.Header.Set(headerHookTargetID, strFromInt(appID))
	}
	if len(secret) > 0 {
		req.Header.Set(headerSignature256, signWebhookBody(secret, body))
	}
	return req
}

func strFromInt(n int64) string {
	const digits = "0123456789"
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = digits[n%10]
		n /= 10
	}
	return string(buf[i:])
}

func TestDispatch_HeaderKeyedHMACSuccess(t *testing.T) {
	h, secrets := newMultiAppTestHandler(t)

	req := newPingRequest(t, 1001, secrets["app-a"], "org-a/repo-x")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "valid signature for the App that owns the repo should be accepted")
}

func TestDispatch_RejectsMissingTargetID(t *testing.T) {
	h, secrets := newMultiAppTestHandler(t)

	req := newPingRequest(t, 0, secrets["app-a"], "org-a/repo-x")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestDispatch_RejectsInvalidTargetType(t *testing.T) {
	h, secrets := newMultiAppTestHandler(t)

	req := newPingRequest(t, 1001, secrets["app-a"], "org-a/repo-x")
	req.Header.Set(headerHookTargetType, "user")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestDispatch_RejectsUnknownAppID(t *testing.T) {
	h, secrets := newMultiAppTestHandler(t)

	// 9999 is not in webhookAppByID — must fail closed before HMAC.
	req := newPingRequest(t, 9999, secrets["app-a"], "org-a/repo-x")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestDispatch_RejectsInvalidSignatureForKnownApp(t *testing.T) {
	h, _ := newMultiAppTestHandler(t)

	// Use the wrong App's secret to sign the body for App A.
	req := newPingRequest(t, 1001, []byte("wrong-secret"), "org-a/repo-x")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestDispatch_RejectsAppRepoMismatch(t *testing.T) {
	h, secrets := newMultiAppTestHandler(t)

	// App-A signs a delivery for a repo owned by App-B. HMAC succeeds
	// against App-A's secret but the App-mismatch check must fail closed.
	req := newPingRequest(t, 1001, secrets["app-a"], "org-b/repo-y")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestDispatch_RejectsUnknownRepoInMultiAppMode(t *testing.T) {
	h, secrets := newMultiAppTestHandler(t)

	// A valid signature from App-A for a repo that isn't declared in
	// the repos map must still be rejected — in multi-App mode every
	// webhook must be attributable to a configured repo.
	req := newPingRequest(t, 1001, secrets["app-a"], "unknown-org/unknown-repo")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestDispatch_LegacySingleAppStillWorks(t *testing.T) {
	// Legacy NewHandler (no webhookAppByID) should accept a signed
	// request without requiring any App-ID header — full back-compat.
	secret := []byte("legacy-secret")
	cfg := &api.ServerConfig{} // no Apps
	svc := api.New(&emptyStorage{}, cfg, nil, testLogger())
	h := NewHandler(svc, &fakeClientFactory{}, secret, testLogger())

	body := []byte(`{"action":"","repository":{"full_name":"some/repo"}}`)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-GitHub-Event", "ping")
	req.Header.Set(headerSignature256, signWebhookBody(secret, body))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestFactoryForRepo_MultiAppResolution(t *testing.T) {
	h, _ := newMultiAppTestHandler(t)

	// app-a owns org-a/repo-x — factoryForRepo must resolve to app-a's
	// factory (not the default name).
	got, err := h.factoryForRepo("org-a/repo-x")
	require.NoError(t, err)
	assert.NotNil(t, got)

	// Unknown repo in multi-App mode must fail closed.
	_, err = h.factoryForRepo("unknown/repo")
	require.Error(t, err)
}
