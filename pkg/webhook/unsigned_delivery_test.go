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

const unsignedDeliveryWarning = "webhook signature verification is skipped because no webhook secret is configured"

// An embedder that builds the handler with an empty webhook secret still has
// unsigned deliveries accepted. Without the explicit opt-in the handler warns,
// once per handler, naming the option; with the opt-in it skips silently.
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

	t.Run("without the option the delivery is accepted and the warning names the option", func(t *testing.T) {
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		h := NewHandler(nil, nil, nil, logger)

		rec := deliver(t, h)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "event type 'ping' ignored")
		assert.Contains(t, logs.String(), "level=WARN")
		assert.Contains(t, logs.String(), unsignedDeliveryWarning)
		assert.Contains(t, logs.String(), "WithInsecureSkipSignatureVerification")
		assert.Contains(t, logs.String(), "delivery_id=delivery-1")
	})

	t.Run("the warning is logged once per handler", func(t *testing.T) {
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		h := NewHandler(nil, nil, nil, logger)

		require.Equal(t, http.StatusOK, deliver(t, h).Code)
		require.Equal(t, http.StatusOK, deliver(t, h).Code)
		assert.Equal(t, 1, strings.Count(logs.String(), unsignedDeliveryWarning))
	})

	t.Run("with the option the delivery is accepted and nothing is warned", func(t *testing.T) {
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		h := NewHandler(nil, nil, nil, logger, WithInsecureSkipSignatureVerification())

		rec := deliver(t, h)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "event type 'ping' ignored")
		assert.NotContains(t, logs.String(), unsignedDeliveryWarning)
		assert.NotContains(t, logs.String(), "level=WARN")
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
