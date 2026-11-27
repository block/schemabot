package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// An engine declines a cancel it cannot carry out, and the apply runs on.
// The progress answer reports the cancel request as failed with the engine's
// reason, so the CLI that sent it can say the cancel did not take effect
// instead of waiting for an end that is not coming.
func TestProgressByApplyIDReportsADeclinedCancel(t *testing.T) {
	const reason = "cancel is supported for PostgreSQL index builds only: this schema change runs as plain DDL that commits or fails on its own"
	mock := &mockTernClient{
		isRemote:     true,
		progressResp: &ternv1.ProgressResponse{ApplyId: "remote-apply-declined", State: ternv1.State_STATE_RUNNING},
	}
	apply := activeTestApply("apply-declined-cancel")
	apply.ExternalID = "remote-apply-declined"
	controls := &memoryControlRequestStore{}
	_, _, err := controls.RequestPending(t.Context(), &storage.ApplyControlRequest{
		ApplyID: apply.ID, Operation: storage.ControlOperationCancel, Status: storage.ControlRequestPending, RequestedBy: "cli:alice",
	})
	require.NoError(t, err)
	require.NoError(t, controls.FailPending(t.Context(), apply.ID, storage.ControlOperationCancel, reason))
	svc := New(&mockStorageWithApplyStores{
		applies:  &staticApplyStore{apply: apply},
		tasks:    &capturingTaskStore{},
		controls: controls,
	}, testServerConfig(), map[string]tern.Client{"default/staging": mock}, slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/progress/apply/apply-declined-cancel", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var resp apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	cancel := resp.ControlRequest("cancel")
	require.NotNil(t, cancel, "the poll reports the cancel request")
	assert.Equal(t, "failed", cancel.Status)
	assert.Equal(t, reason, cancel.ErrorMessage)
	assert.Equal(t, "cli:alice", cancel.RequestedBy)
}
