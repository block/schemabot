package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
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

// The text a control error shows the requester is the text SchemaBot wrote on
// the typed error, so a wrap added on the way up to the PR comment stays out
// of it, and an error with no such text shows nothing.
func TestAuthoredControlFailureMessage(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		want     string
		authored bool
	}{
		{
			name:     "a wrapped conflict shows its own text",
			err:      fmt.Errorf("execute start: %w", controlConflictf("apply %s already completed and cannot be started", "apply-123")),
			want:     "apply apply-123 already completed and cannot be started",
			authored: true,
		},
		{
			name:     "a wrapped engine-unavailable answer shows its own text",
			err:      fmt.Errorf("execute cutover: %w", controlHTTPCodeErrorf(http.StatusServiceUnavailable, apitypes.ErrCodeEngineUnavailable, "the engine for apply %s is not available; retry in a moment", "apply-123")),
			want:     "the engine for apply apply-123 is not available; retry in a moment",
			authored: true,
		},
		{
			name:     "a wrapped authored terminal failure shows its own text",
			err:      fmt.Errorf("execute revert: %w", authoredTerminalControlf("revert window for apply %s has closed", "apply-123")),
			want:     "revert window for apply apply-123 has closed",
			authored: true,
		},
		{
			name: "a terminal failure that wraps an error shows nothing",
			err:  terminalControlf("apply %s is inconsistent: %w", "apply-123", errors.New(sensitiveControlFailure)),
		},
		{
			name: "an untyped failure shows nothing",
			err:  fmt.Errorf("execute stop: %w", errors.New(sensitiveControlFailure)),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := AuthoredControlFailureMessage(tc.err)

			assert.Equal(t, tc.authored, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

// An apply with no deployment recorded cannot be routed on any attempt, so
// the control target refuses it as a terminal failure whose text says
// nothing was changed and a retry fails the same way, while the cause stays
// in the server log.
func TestControlTargetRefusesApplyWithoutDeploymentAsTerminal(t *testing.T) {
	apply := activeTestApply("apply-internal-error")
	apply.Deployment = ""
	svc := newControlInternalErrorService(&staticApplyStore{apply: apply}, &memoryControlRequestStore{}, slog.New(slog.DiscardHandler))

	_, _, _, err := svc.controlTarget(t.Context(), "skip-revert", apply.ApplyIdentifier, apply.Environment)

	require.Error(t, err)
	assert.True(t, IsTerminalControlError(err), "a retry meets the same record")
	shown, ok := AuthoredControlFailureMessage(err)
	require.True(t, ok)
	assert.Equal(t, controlMissingDeploymentMessage("skip-revert", "apply-internal-error"), shown)
	assert.NotContains(t, shown, "missing stored deployment metadata")
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
	for _, operation := range []string{"cutover", "stop", "cancel", "start", "revert", "skip-revert"} {
		t.Run(operation, func(t *testing.T) {
			var logBuf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
			apply := activeTestApply("apply-internal-error")
			if operation == "revert" || operation == "skip-revert" {
				apply.Engine = storage.EnginePlanetScale
				apply.State = state.Apply.RevertWindow
			}
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
			assert.Equal(t, DefaultDeployment, line["deployment"])
			assert.Contains(t, line["error"], "user:pass")
		})
	}
}

// A control request that SchemaBot cannot route to its apply is never
// attempted, so each answer says nothing was changed. A storage layer that is
// not available can recover, so that answer suggests a retry; an apply with
// no recorded deployment fails the same way every time, so that answer does
// not. Each failure is logged with the path and apply it was for.
func TestControlRoutesAnswerNotAttemptedBeforeTheOperation(t *testing.T) {
	missingDeployment := activeTestApply("apply-internal-error")
	missingDeployment.Deployment = ""
	tests := []struct {
		name      string
		setup     func(svc *Service)
		applies   storage.ApplyStore
		wantMsg   string
		wantLog   string
		wantRetry bool
	}{
		{
			name:      "storage not available",
			applies:   &staticApplyStore{apply: activeTestApply("apply-internal-error")},
			setup:     func(svc *Service) { svc.storage = nil },
			wantMsg:   controlNotAttemptedMessage("cutover", "apply-internal-error"),
			wantLog:   "storage not available for control request",
			wantRetry: true,
		},
		{
			name:      "apply lookup fails",
			applies:   &staticApplyStore{err: errors.New(sensitiveControlFailure)},
			setup:     func(*Service) {},
			wantMsg:   controlNotAttemptedMessage("cutover", "apply-internal-error"),
			wantLog:   "failed to load apply for control request",
			wantRetry: true,
		},
		{
			name:      "apply store not available",
			applies:   nil,
			setup:     func(*Service) {},
			wantMsg:   controlNotAttemptedMessage("cutover", "apply-internal-error"),
			wantLog:   "apply store not available for control request",
			wantRetry: true,
		},
		{
			name:    "apply has no recorded deployment",
			applies: &staticApplyStore{apply: missingDeployment},
			setup:   func(*Service) {},
			wantMsg: controlMissingDeploymentMessage("cutover", "apply-internal-error"),
			wantLog: "control request apply is missing stored deployment metadata",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var logBuf bytes.Buffer
			svc := newControlInternalErrorService(tc.applies, &memoryControlRequestStore{}, slog.New(slog.NewJSONHandler(&logBuf, nil)))
			tc.setup(svc)

			w, resp := postControlRoute(t, svc, "/api/cutover")

			require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
			assert.Equal(t, tc.wantMsg, resp.Error)
			assert.Contains(t, resp.Error, "nothing was changed")
			assert.Equal(t, tc.wantRetry, strings.Contains(resp.Error, "Retry"), "only a failure that can recover suggests a retry")
			line := requireLogLine(t, decodeLogLines(t, logBuf.Bytes()), tc.wantLog)
			assert.Equal(t, "apply-internal-error", line["apply_id"])
			assert.Equal(t, "/api/cutover", line["path"])
			assert.Equal(t, "cutover", line["operation"])
		})
	}
}

// Every engine is classified as one that can stop or one whose stops are
// refused before anything is recorded, so a new engine cannot accept stops it
// will never honor.
func TestStopRefusalsClassifyEveryEngine(t *testing.T) {
	assert.ElementsMatch(t, storage.Engines, slices.Collect(maps.Keys(stopRefusals)))
}

// A stop of a PostgreSQL schema change is refused up front: the engine has no
// stopped phase, so the request is never recorded. The refusal says what can't
// be paused and gives the command to run instead, for this apply: cancel for a
// concurrent index build that is its only statement, progress for anything
// else, and both when its stored progress cannot be read.
func TestStopOfPostgresApplyIsRefusedBeforeItIsRecorded(t *testing.T) {
	const cancel = "`schemabot cancel apply-postgres-stop -e staging`"
	const progress = "`schemabot progress apply-postgres-stop -e staging`"
	operation := func(metadata string) *storage.ApplyOperation {
		return &storage.ApplyOperation{ID: 11, ApplyID: 1, ProgressMetadata: metadata}
	}
	tests := []struct {
		name       string
		operations *staticApplyOperationStore
		code       string
		want       string
	}{
		{
			name: "a lone concurrent index build is pointed at cancel",
			operations: &staticApplyOperationStore{operations: []*storage.ApplyOperation{
				operation(`{"executor_operation":"concurrent-index-build","step":"1","steps_total":"1"}`),
			}},
			code: apitypes.ErrCodeStopUnsupportedCancel,
			want: "stop rejected: PostgreSQL can't pause an index build. To end it: " + cancel,
		},
		{
			name: "plain DDL is left to finish",
			operations: &staticApplyOperationStore{operations: []*storage.ApplyOperation{
				operation(`{"step":"1","steps_total":"1","statement":"ALTER TABLE users ADD COLUMN email text"}`),
			}},
			code: apitypes.ErrCodeStopUnsupportedFinishes,
			want: "stop rejected: PostgreSQL can't pause this schema change, and it finishes on its own. To follow it: " + progress,
		},
		{
			name: "an index build among other statements is left to finish",
			operations: &staticApplyOperationStore{operations: []*storage.ApplyOperation{
				operation(`{"executor_operation":"concurrent-index-build","step":"2","steps_total":"3"}`),
			}},
			code: apitypes.ErrCodeStopUnsupportedFinishes,
			want: "stop rejected: PostgreSQL can't pause this schema change, and it finishes on its own. To follow it: " + progress,
		},
		{
			name:       "unreadable progress names the remedy for each kind of statement",
			operations: &staticApplyOperationStore{err: errors.New(sensitiveControlFailure)},
			code:       apitypes.ErrCodeStopUnsupported,
			want: "stop rejected: PostgreSQL can't pause a schema change. If it is a single index build, end it with " + cancel +
				"; anything else finishes on its own. To follow it: " + progress,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apply := activeTestApply("apply-postgres-stop")
			apply.ID = 1
			apply.Engine = storage.EnginePostgres
			apply.DatabaseType = storage.DatabaseTypePostgres
			requests := &memoryControlRequestStore{}
			svc := newControlInternalErrorService(&staticApplyStore{apply: apply}, requests, slog.New(slog.DiscardHandler))
			svc.storage.(*mockStorageWithApplyStores).operations = tt.operations

			w, resp := postControlRoute(t, svc, "/api/stop")

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			assert.Equal(t, tt.want, resp.Error)
			assert.Equal(t, tt.code, resp.ErrorCode, "the code names the remedy for a client that renders the command itself")
			assert.NotContains(t, w.Body.String(), "10.0.0.5")
			assert.Empty(t, requests.requests, "a refused stop records no control request")
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

	// A coded refusal keeps its code, which the CLI acts on.
	coded := httptest.NewRecorder()
	svc.writeControlError(coded, "stop", apply, controlConflictCodef(apitypes.ErrCodeAlreadyStopped, "schema change is already stopped"))
	require.Equal(t, http.StatusConflict, coded.Code)
	var codedResp apitypes.ErrorResponse
	require.NoError(t, json.Unmarshal(coded.Body.Bytes(), &codedResp))
	assert.Equal(t, "already_stopped", codedResp.ErrorCode)
	assert.Equal(t, "stop rejected: schema change is already stopped", codedResp.Error)
}

// A rollback plan the engine refuses is the schema files' to fix, so the
// answer carries the refusal and where the rollback's ignore_tables came from,
// whether the engine ran locally or on a remote data plane, and without the
// gRPC status. A refused drop of a withheld table is SchemaBot's own refusal
// and keeps its text too, and so does a rollback source plan or apply that
// SchemaBot finds invalid before it asks the engine. Any other engine failure
// can carry driver text, so it answers the fixed message and stays in the
// server log.
func TestRollbackPlanAnswerCarriesOnlyRefusals(t *testing.T) {
	ignored, err := engine.NewIgnoredTables([]string{"flyway_schema_history"})
	require.NoError(t, err)
	refusal := ignored.RefuseDeclared("payments", []string{"flyway_schema_history"})
	require.Error(t, refusal)
	withheldDrop := &ternv1.PlanResponse{
		PlanId: "plan_rollback_unhonored",
		Changes: []*ternv1.SchemaChange{{
			Namespace: "payments",
			TableChanges: []*ternv1.TableChange{{
				TableName: "flyway_schema_history", ChangeType: ternv1.ChangeType_CHANGE_TYPE_DROP,
				Ddl: "DROP TABLE `flyway_schema_history`",
			}},
		}},
	}
	uncapturedSource := rollbackSourcePlan()
	uncapturedSource.Namespaces["payments"].OriginalFilesCaptured = false
	tests := []struct {
		name        string
		client      *mockTernClient
		source      *storage.Plan
		setupApply  func(*storage.Apply)
		wantContain []string
		wantExact   string
		wantFixed   bool
	}{
		{
			name:        "local engine refusal",
			client:      &mockTernClient{planErr: fmt.Errorf("plan namespace payments: %w", &engine.PlanRefusedError{Err: refusal})},
			wantContain: []string{"Remove the entry or the schema file", "This rollback uses the plan's recorded ignore_tables, not schemabot.yaml"},
		},
		{
			name:        "remote engine refusal",
			client:      &mockTernClient{planErr: fmt.Errorf("remote plan: %w", status.Error(codes.FailedPrecondition, refusal.Error()))},
			wantContain: []string{"Remove the entry or the schema file", "This rollback uses the plan's recorded ignore_tables, not schemabot.yaml"},
		},
		{
			name:        "refused drop of a withheld table",
			client:      &mockTernClient{planResp: withheldDrop},
			wantContain: []string{"ignore_tables withholds", "Upgrade that deployment to a build that supports ignore_tables"},
		},
		{
			name:      "source plan has no captured schema files",
			client:    &mockTernClient{},
			source:    uncapturedSource,
			wantExact: `rollback plan failed: rollback source plan is invalid: no original schema files available for rollback namespace "payments"`,
		},
		{
			name:       "source apply has no recorded deployment",
			client:     &mockTernClient{},
			setupApply: func(apply *storage.Apply) { apply.Deployment = "" },
			wantExact:  `rollback plan failed: rollback source apply is invalid: apply "apply_rollback" is missing stored deployment metadata`,
		},
		{
			name:      "engine failure",
			client:    &mockTernClient{planErr: status.Error(codes.Internal, sensitiveControlFailure)},
			wantFixed: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := tc.source
			if source == nil {
				source = rollbackSourcePlan()
			}
			svc, apply := newRollbackExemptServiceWithClient(&rollbackSourcePlanStore{source: source}, tc.client)
			if tc.setupApply != nil {
				tc.setupApply(apply)
			}
			var logBuf bytes.Buffer
			svc.logger = slog.New(slog.NewJSONHandler(&logBuf, nil))
			_, planErr := svc.ExecuteRollbackPlanForApply(t.Context(), apply)
			require.Error(t, planErr)

			w := httptest.NewRecorder()
			svc.writeControlError(w, "rollback plan", apply, planErr)

			require.Equal(t, http.StatusInternalServerError, w.Code)
			var resp apitypes.ErrorResponse
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
			assert.NotContains(t, resp.Error, "rpc error")
			assert.NotContains(t, resp.Error, "user:pass")
			if tc.wantFixed {
				assert.Equal(t, controlInternalFailureMessage("rollback plan", apply), resp.Error)
				assert.NotContains(t, resp.Error, "recorded ignore_tables")
				line := requireLogLine(t, decodeLogLines(t, logBuf.Bytes()), "rollback plan failed")
				assert.Contains(t, line["error"], "user:pass")
				return
			}
			if tc.wantExact != "" {
				assert.Equal(t, tc.wantExact, resp.Error)
				return
			}
			assert.True(t, strings.HasPrefix(resp.Error, `rollback plan failed: rollback plan for database "payments" (staging): `), resp.Error)
			for _, want := range tc.wantContain {
				assert.Contains(t, resp.Error, want)
			}
		})
	}
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
			name:            "an error formatted without %w is still a cause",
			opName:          "rollback plan",
			apply:           apply,
			err:             terminalControlf("rollback plan for database %q: %v", "testdb", errors.New(sensitiveControlFailure)),
			wantMsg:         "rollback plan failed for apply apply-internal-error: SchemaBot hit an internal error. No rollback was started. A SchemaBot operator can find the cause in the server log.",
			wantLogContains: "user:pass",
		},
		{
			name:            "two wrapped errors are causes",
			opName:          "rollback plan",
			apply:           apply,
			err:             terminalControlf("rollback plan for database %q: %w and %w", "testdb", errors.New("first"), errors.New(sensitiveControlFailure)),
			wantMsg:         "rollback plan failed for apply apply-internal-error: SchemaBot hit an internal error. No rollback was started. A SchemaBot operator can find the cause in the server log.",
			wantLogContains: "user:pass",
		},
		{
			name:            "a message SchemaBot wrote in full keeps its wrapped text",
			opName:          "rollback plan",
			apply:           apply,
			err:             authoredTerminalControlf("rollback source apply is invalid: %w", errors.New(`apply "apply-internal-error" is missing stored deployment metadata`)),
			wantMsg:         `rollback plan failed: rollback source apply is invalid: apply "apply-internal-error" is missing stored deployment metadata`,
			wantLogContains: "missing stored deployment metadata",
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
