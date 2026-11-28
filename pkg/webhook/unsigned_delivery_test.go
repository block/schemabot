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

// An embedder that builds the handler with an empty webhook secret cannot
// prove any delivery came from GitHub, so every delivery is refused with the
// same answer as a bad signature, and the log names the missing secret. Only
// the explicit opt-in for tests and local harnesses accepts it unsigned.
func TestUnsignedDeliveryWithoutSecret(t *testing.T) {
	deliver := func(t *testing.T, h *Handler) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", strings.NewReader(`{"zen":"keep it simple"}`))
		req.Header.Set("X-GitHub-Event", "ping")
		req.Header.Set(headerDeliveryID, "delivery-1")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	t.Run("without the option the delivery is refused and the log names the missing secret", func(t *testing.T) {
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		h := NewHandler(nil, nil, nil, logger)

		rec := deliver(t, h)
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		assert.Contains(t, rec.Body.String(), "invalid webhook dispatch")
		assert.NotContains(t, rec.Body.String(), "secret", "the response must not say which check failed")
		assert.Contains(t, logs.String(), "auth_status=missing_webhook_secret")
		assert.Contains(t, logs.String(), "delivery_id=delivery-1")
	})

	t.Run("with the option the delivery is accepted unsigned", func(t *testing.T) {
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		h := NewHandler(nil, nil, nil, logger, WithInsecureSkipSignatureVerification())

		rec := deliver(t, h)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "event type 'ping' ignored")
		assert.NotContains(t, logs.String(), "missing_webhook_secret")
	})
}

// The opt-in for unsigned deliveries only applies when no secret is
// configured. A handler given both still verifies every delivery against the
// secret, so a forged or unsigned delivery is refused rather than accepted.
func TestInsecureOptionDoesNotOverrideConfiguredSecret(t *testing.T) {
	secret := []byte("configured-secret")
	body := `{"zen":"keep it simple"}`
	h := NewHandler(nil, nil, secret, testLogger(), WithInsecureSkipSignatureVerification())

	deliver := func(sig string) int {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", strings.NewReader(body))
		req.Header.Set("X-GitHub-Event", "ping")
		if sig != "" {
			req.Header.Set(headerSignature256, sig)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	assert.Equal(t, http.StatusUnauthorized, deliver(""), "unsigned delivery")
	assert.Equal(t, http.StatusUnauthorized, deliver(signWebhookBody([]byte("other-secret"), []byte(body))), "wrongly signed delivery")
	assert.Equal(t, http.StatusOK, deliver(signWebhookBody(secret, []byte(body))), "correctly signed delivery")
}
