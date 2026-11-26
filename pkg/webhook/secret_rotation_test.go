package webhook

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

		// With no rotation configured, a delivery signed with an empty key,
		// which anyone can compute, is refused: an unset previous secret never
		// stands in as a secret that verifies.
		{"single-App empty key with no rotation", singleApp(), appRequest, []byte{}, false},
		{"multi-App empty key with no rotation", multiApp(), multiAppRequest, []byte{}, false},
		{"repo-webhook empty key with no rotation", repoWebhook(), repoRequest, []byte{}, false},
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

// A delivery verified with the secret being rotated out is logged, since that
// line is how an operator knows GitHub still signs with the old secret and the
// rotation cannot end yet. The line names the App, the config key to remove
// and the delivery, and carries no secret material. A delivery verified with
// the current secret logs nothing.
func TestAuthenticateWebhook_LogsDeliveriesVerifiedWithThePreviousSecret(t *testing.T) {
	body := []byte(`{"action":"opened","repository":{"full_name":"org-a/repo-x"}}`)
	current := []byte("new-secret")
	previous := []byte("old-secret")

	deliver := func(t *testing.T, signWith []byte) string {
		var logs bytes.Buffer
		h := &Handler{
			webhookSecretsByApp: map[string][]byte{defaultAppName: current},
			logger:              slog.New(slog.NewJSONHandler(&logs, nil)),
		}
		WithPreviousWebhookSecret(previous)(h)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", strings.NewReader(string(body)))
		req.Header.Set(headerSignature256, signWebhookBody(signWith, body))
		req.Header.Set(headerDeliveryID, "delivery-7f3c")
		_, _, _, ok := h.authenticateWebhook(req, body)
		require.True(t, ok)
		return logs.String()
	}

	logged := deliver(t, previous)
	assert.Contains(t, logged, "webhook delivery verified with the secret being rotated out")
	assert.Contains(t, logged, `"app_name":"default"`)
	assert.Contains(t, logged, `"config_key":"previous-webhook-secret"`)
	assert.Contains(t, logged, `"delivery_id":"delivery-7f3c"`)
	assert.NotContains(t, logged, string(previous))
	assert.NotContains(t, logged, string(current))

	assert.Empty(t, deliver(t, current))
}

// A handler built with a secret being rotated out but no current secret to
// pair it with can never check the previous secret, so it warns at
// construction instead of carrying a rotation setting that does nothing.
func TestNewHandlerWarnsWhenAPreviousSecretHasNoCurrentSecret(t *testing.T) {
	newHandler := func(webhookSecret []byte, opts ...HandlerOption) string {
		var logs bytes.Buffer
		NewHandler(nil, nil, webhookSecret, slog.New(slog.NewJSONHandler(&logs, nil)), opts...)
		return logs.String()
	}

	logged := newHandler(nil, WithPreviousWebhookSecret([]byte("old-secret")))
	assert.Contains(t, logged, "previous webhook secret is ignored")
	assert.Contains(t, logged, `"config_key":"previous-webhook-secret"`)
	assert.NotContains(t, logged, "old-secret")

	logged = newHandler([]byte("app-secret"), WithPreviousRepoWebhookSecret([]byte("old-repo-secret")))
	assert.Contains(t, logged, "previous webhook secret is ignored")
	assert.Contains(t, logged, `"config_key":"previous-repo-webhook-secret"`)

	assert.Empty(t, newHandler([]byte("new-secret"), WithPreviousWebhookSecret([]byte("old-secret"))))
}
