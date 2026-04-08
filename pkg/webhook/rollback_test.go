package webhook

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebhookRollbackDispatch(t *testing.T) {
	h, _, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback apply_abc123",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "rollback started")
}

func TestWebhookRollbackConfirmDispatch(t *testing.T) {
	h, _, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback-confirm -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "rollback-confirm started")
}

func TestWebhookRollbackMissingApplyID(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-comments:
		assert.Contains(t, body, "Missing Apply ID")
		assert.Contains(t, body, "schemabot rollback <apply-id>")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for comment")
	}
}

func TestWebhookApplyDispatch(t *testing.T) {
	h, _, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "apply started")
}
