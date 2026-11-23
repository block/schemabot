package webhook

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// During a webhook secret rotation the server holds both the new secret and
// the one being rotated out. A delivery signed with either is accepted, so the
// window between deploying the new secret and changing it on GitHub refuses
// nothing; a delivery signed with any other secret is still refused. The same
// holds for the single-App, multi-App, and repo-webhook verification paths.
func TestAuthenticateWebhook_AcceptsSecretBeingRotatedOut(t *testing.T) {
	body := []byte(`{"action":"opened","repository":{"full_name":"org-a/repo-x"}}`)
	current := []byte("new-secret")
	previous := []byte("old-secret")

	singleApp := func(opts ...HandlerOption) *Handler {
		h := &Handler{webhookSecretsByApp: map[string][]byte{defaultAppName: current}, logger: testLogger()}
		for _, opt := range opts {
			opt(h)
		}
		return h
	}
	multiApp := func(opts ...HandlerOption) *Handler {
		h := &Handler{
			webhookSecretsByApp: map[string][]byte{"app-a": current, "app-b": []byte("app-b-secret")},
			webhookAppByID:      map[int64]string{1001: "app-a", 1002: "app-b"},
			logger:              testLogger(),
		}
		for _, opt := range opts {
			opt(h)
		}
		return h
	}
	repoWebhook := func(opts ...HandlerOption) *Handler {
		h := &Handler{repoWebhookSecret: current, logger: testLogger()}
		for _, opt := range opts {
			opt(h)
		}
		return h
	}

	appRequest := func(t *testing.T, signWith []byte) *http.Request {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", strings.NewReader(string(body)))
		req.Header.Set(headerSignature256, signWebhookBody(signWith, body))
		return req
	}
	multiAppRequest := func(t *testing.T, signWith []byte) *http.Request {
		req := appRequest(t, signWith)
		req.Header.Set(headerHookTargetType, hookTargetTypeApp)
		req.Header.Set(headerHookTargetID, "1001")
		return req
	}
	repoRequest := func(t *testing.T, signWith []byte) *http.Request {
		req := repoWebhookRequest(t, hookTargetTypeRepo, body)
		req.Header.Set(headerSignature256, repoWebhookSig(signWith, body))
		return req
	}

	tests := []struct {
		name     string
		handler  *Handler
		request  func(*testing.T, []byte) *http.Request
		signWith []byte
		wantOK   bool
	}{
		{"single-App current secret", singleApp(WithPreviousWebhookSecret(previous)), appRequest, current, true},
		{"single-App previous secret", singleApp(WithPreviousWebhookSecret(previous)), appRequest, previous, true},
		{"single-App unknown secret", singleApp(WithPreviousWebhookSecret(previous)), appRequest, []byte("other"), false},
		{"single-App previous secret after rotation ends", singleApp(), appRequest, previous, false},

		{"multi-App current secret", multiApp(WithPreviousWebhookSecrets(map[string][]byte{"app-a": previous})), multiAppRequest, current, true},
		{"multi-App previous secret", multiApp(WithPreviousWebhookSecrets(map[string][]byte{"app-a": previous})), multiAppRequest, previous, true},
		{"multi-App unknown secret", multiApp(WithPreviousWebhookSecrets(map[string][]byte{"app-a": previous})), multiAppRequest, []byte("other"), false},
		// app-b's previous secret must not authenticate a delivery addressed to app-a.
		{"multi-App another App's previous secret", multiApp(WithPreviousWebhookSecrets(map[string][]byte{"app-b": previous})), multiAppRequest, previous, false},

		{"repo-webhook current secret", repoWebhook(WithPreviousRepoWebhookSecret(previous)), repoRequest, current, true},
		{"repo-webhook previous secret", repoWebhook(WithPreviousRepoWebhookSecret(previous)), repoRequest, previous, true},
		{"repo-webhook unknown secret", repoWebhook(WithPreviousRepoWebhookSecret(previous)), repoRequest, []byte("other"), false},
		// The App webhook's previous secret does not authenticate repo-webhook deliveries.
		{"repo-webhook signed with the App's previous secret", repoWebhook(WithPreviousWebhookSecret(previous)), repoRequest, previous, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, status, ok := tc.handler.authenticateWebhook(tc.request(t, tc.signWith), body)
			assert.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				assert.Empty(t, status)
			} else {
				assert.Equal(t, "invalid_signature", status)
			}
		})
	}
}
