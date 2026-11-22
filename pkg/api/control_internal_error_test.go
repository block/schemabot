package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// sensitiveControlFailure is the kind of text a storage driver or an engine
// call returns: an address and credentials that must stay in the server log.
const sensitiveControlFailure = "dial tcp 10.0.0.5:3306: user:pass@tcp(db.internal:3306)/schemabot: connection refused"

// failingControlRequestStore fails every control request read and write with
// sensitiveControlFailure.
type failingControlRequestStore struct {
	storage.ControlRequestStore
}

func (failingControlRequestStore) GetPending(context.Context, int64, storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	return nil, errors.New(sensitiveControlFailure)
}

func (failingControlRequestStore) GetByOperation(context.Context, int64, storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	return nil, errors.New(sensitiveControlFailure)
}

func (failingControlRequestStore) RequestPending(context.Context, *storage.ApplyControlRequest) (*storage.ApplyControlRequest, bool, error) {
	return nil, false, errors.New(sensitiveControlFailure)
}

func (failingControlRequestStore) RequestPendingCommand(context.Context, *storage.ApplyControlRequest) (*storage.ApplyControlRequest, bool, error) {
	return nil, false, errors.New(sensitiveControlFailure)
}

func postControlRoute(t *testing.T, svc *Service, route string) (*httptest.ResponseRecorder, apitypes.ErrorResponse) {
	t.Helper()
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	body := `{"environment": "staging", "apply_id": "apply-internal-error"}`
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, route, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	var resp apitypes.ErrorResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp), "%s: %s", route, w.Body.String())
	return w, resp
}

func newControlInternalErrorService(applies storage.ApplyStore, controls storage.ControlRequestStore, logger *slog.Logger) *Service {
	svc, _ := newQueueApplyTestService(executeApplyTestPlan(), &mockTernClient{}, applies)
	svc.storage = &mockStorageWithApplyStores{
		plans:     &staticPlanStore{plan: executeApplyTestPlan()},
		applies:   applies,
		tasks:     &capturingTaskStore{},
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
		controls:  controls,
	}
	svc.logger = logger
	return svc
}

// A control request that cannot load its apply answers 500 with a fixed message
// that names the operation and the apply and says nothing was changed. The
// storage error stays in the server log with the apply identifier and path.
func TestControlRoutesAnswerFixedMessageWhenApplyLookupFails(t *testing.T) {
	for _, operation := range []string{"cutover", "stop", "cancel", "start", "release", "revert", "skip-revert"} {
		t.Run(operation, func(t *testing.T) {
			var logBuf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
			svc := newControlInternalErrorService(&staticApplyStore{err: errors.New(sensitiveControlFailure)}, &memoryControlRequestStore{}, logger)

			w, resp := postControlRoute(t, svc, "/api/"+operation)

			require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
			assert.Equal(t, controlNotAttemptedMessage(operation, "apply-internal-error"), resp.Error)
			assert.Contains(t, resp.Error, "The "+operation+" was not attempted for apply \"apply-internal-error\"")
			assert.Contains(t, resp.Error, "nothing was changed")
			assert.NotContains(t, w.Body.String(), "10.0.0.5")
			assert.NotContains(t, w.Body.String(), "user:pass")
			assert.NotContains(t, w.Body.String(), "db.internal")

			line := requireLogLine(t, decodeLogLines(t, logBuf.Bytes()), "failed to load apply for control request")
			assert.Equal(t, "apply-internal-error", line["apply_id"])
			assert.Equal(t, "staging", line["environment"])
			assert.Equal(t, "/api/"+operation, line["path"])
			assert.Contains(t, line["error"], "user:pass")
		})
	}
}

// A control request that fails inside the operation, after its apply loaded,
// answers 500 with a fixed message that names the operation and the apply and
// says the request may already be recorded. The storage error stays in the
// server log with the apply's full triage attributes.
func TestControlRoutesAnswerFixedMessageWhenOperationFails(t *testing.T) {
	for _, operation := range []string{"cutover", "stop"} {
		t.Run(operation, func(t *testing.T) {
			var logBuf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
			apply := activeTestApply("apply-internal-error")
			svc := newControlInternalErrorService(&staticApplyStore{apply: apply}, failingControlRequestStore{}, logger)

			w, resp := postControlRoute(t, svc, "/api/"+operation)

			require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
			assert.Equal(t, controlInternalFailureMessage(operation, apply), resp.Error)
			assert.Contains(t, resp.Error, operation+" failed for apply apply-internal-error")
			assert.Contains(t, resp.Error, "may already be recorded")
			assert.Contains(t, resp.Error, "server log")
			assert.NotContains(t, w.Body.String(), "10.0.0.5")
			assert.NotContains(t, w.Body.String(), "user:pass")
			assert.NotContains(t, w.Body.String(), "db.internal")

			line := requireLogLine(t, decodeLogLines(t, logBuf.Bytes()), operation+" failed")
			assert.Equal(t, "apply-internal-error", line["apply_id"])
			assert.Equal(t, "testdb", line["database"])
			assert.Equal(t, "staging", line["environment"])
			assert.Contains(t, line["error"], "user:pass")
		})
	}
}

// A refusal the requester caused or must act on keeps its own text and its 4xx
// status. Stop on an apply whose engine cannot pause is refused with the reason
// and the command to use instead.
func TestControlRoutesKeepRefusalTextOn4xx(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
	apply := activeTestApply("apply-internal-error")
	apply.Engine = storage.EnginePlanetScale
	svc := newControlInternalErrorService(&staticApplyStore{apply: apply}, &memoryControlRequestStore{}, logger)

	w, resp := postControlRoute(t, svc, "/api/stop")

	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	assert.Equal(t, "stop rejected: stop is not supported for this schema change; use cancel to permanently cancel it", resp.Error)
}

// A rollback plan failure that wraps a cause records no control request, so its
// fixed message says no rollback was started; an error with no apply in scope
// still gets a fixed message without naming one. A terminal error that wraps no
// cause is text SchemaBot wrote and keeps its next step, and an outer wrap
// around it cannot add raw text to the answer.
func TestWriteControlError_FixedMessageVariants(t *testing.T) {
	apply := activeTestApply("apply-internal-error")
	wrappedCause := terminalControlf("rollback plan for database %q: %w", "testdb", errors.New(sensitiveControlFailure))
	authored := terminalControlf("plan %s is missing server-side routing metadata field %q; create a new plan and retry rollback", "plan-1", "target")
	tests := []struct {
		name            string
		opName          string
		apply           *storage.Apply
		err             error
		wantMsg         string
		wantLogContains string
	}{
		{
			name:            "rollback plan",
			opName:          "rollback plan",
			apply:           apply,
			err:             wrappedCause,
			wantMsg:         "rollback plan failed for apply apply-internal-error: SchemaBot hit an internal error. No rollback was started. A SchemaBot operator can find the cause in the server log.",
			wantLogContains: "user:pass",
		},
		{
			name:            "no apply in scope",
			opName:          "rollback plan",
			apply:           nil,
			err:             wrappedCause,
			wantMsg:         "rollback plan failed: SchemaBot hit an internal error. No rollback was started. A SchemaBot operator can find the cause in the server log.",
			wantLogContains: "user:pass",
		},
		{
			name:            "authored terminal error keeps its text",
			opName:          "rollback plan",
			apply:           apply,
			err:             authored,
			wantMsg:         `rollback plan failed: plan plan-1 is missing server-side routing metadata field "target"; create a new plan and retry rollback`,
			wantLogContains: "create a new plan and retry rollback",
		},
		{
			name:            "outer wrap around an authored terminal error adds nothing",
			opName:          "rollback plan",
			apply:           apply,
			err:             fmt.Errorf("load rollback source: %w: %s", authored, sensitiveControlFailure),
			wantMsg:         `rollback plan failed: plan plan-1 is missing server-side routing metadata field "target"; create a new plan and retry rollback`,
			wantLogContains: "user:pass",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var logBuf bytes.Buffer
			svc := New(nil, testServerConfig(), map[string]tern.Client{}, slog.New(slog.NewJSONHandler(&logBuf, nil)))
			w := httptest.NewRecorder()

			svc.writeControlError(w, tc.opName, tc.apply, tc.err)

			require.Equal(t, http.StatusInternalServerError, w.Code)
			var resp apitypes.ErrorResponse
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
			assert.Equal(t, tc.wantMsg, resp.Error)
			assert.NotContains(t, w.Body.String(), "10.0.0.5")
			assert.NotContains(t, w.Body.String(), "user:pass")
			line := requireLogLine(t, decodeLogLines(t, logBuf.Bytes()), tc.opName+" failed")
			assert.Contains(t, line["error"], tc.wantLogContains)
		})
	}
}
