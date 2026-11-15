package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestIsInternalControlError verifies that control operation errors are
// classified so callers log internal failures at error severity and
// operator-actionable rejections at warning severity.
func TestIsInternalControlError(t *testing.T) {
	t.Run("conflict rejection is not internal", func(t *testing.T) {
		assert.False(t, IsInternalControlError(controlConflictf("schema change is already terminal (current state: %s)", "completed")))
	})

	t.Run("client-facing status is not internal", func(t *testing.T) {
		assert.False(t, IsInternalControlError(controlHTTPErrorf(http.StatusNotFound, "apply not found: %s", "apply-123")))
		assert.False(t, IsInternalControlError(controlHTTPErrorf(http.StatusBadRequest, "environment is required")))
	})

	t.Run("wrapped rejection keeps its classification", func(t *testing.T) {
		wrapped := fmt.Errorf("execute start: %w", controlConflictf("schema change is still running; stop it before starting it again"))
		assert.False(t, IsInternalControlError(wrapped))
	})

	t.Run("unclassified error is internal", func(t *testing.T) {
		assert.True(t, IsInternalControlError(errors.New("control request store is not available")))
		assert.True(t, IsInternalControlError(fmt.Errorf("record start control request for apply %s: %w", "apply-123", errors.New("storage unavailable"))))
	})

	t.Run("explicit server status is internal", func(t *testing.T) {
		assert.True(t, IsInternalControlError(controlHTTPErrorf(http.StatusBadGateway, "tern unavailable")))
	})
}

// TestIsTerminalControlError verifies the retry disposition durable command
// processing hinges on: typed terminal errors and operator-actionable
// rejections must never be re-driven, while untyped internal failures and
// remote unavailability stay retryable.
func TestIsTerminalControlError(t *testing.T) {
	t.Run("typed terminal error is terminal", func(t *testing.T) {
		assert.True(t, IsTerminalControlError(terminalControlf("plan not found for apply %s", "apply-123")))
	})

	t.Run("wrapped terminal error stays terminal", func(t *testing.T) {
		wrapped := fmt.Errorf("rollback command plan: %w", terminalControlf("rollback source plan is invalid"))
		assert.True(t, IsTerminalControlError(wrapped))
	})

	t.Run("guardrail conflict is terminal", func(t *testing.T) {
		assert.True(t, IsTerminalControlError(controlConflictf("apply is in state %q; only completed applies can be rolled back", "running")))
	})

	t.Run("client-facing status is terminal", func(t *testing.T) {
		assert.True(t, IsTerminalControlError(controlHTTPErrorf(http.StatusBadRequest, "apply is required")))
		assert.True(t, IsTerminalControlError(controlHTTPErrorf(http.StatusNotFound, "apply not found: %s", "apply-123")))
	})

	t.Run("untyped error is not terminal", func(t *testing.T) {
		assert.False(t, IsTerminalControlError(errors.New("storage unavailable")))
		assert.False(t, IsTerminalControlError(fmt.Errorf("get rollback source plan: %w", errors.New("connection reset"))))
	})

	t.Run("remote unavailability is not terminal", func(t *testing.T) {
		assert.False(t, IsTerminalControlError(&RemoteDeploymentUnavailableError{
			Deployment: "tenant-a",
			Target:     "orders-staging",
			Err:        errors.New("connection refused"),
		}))
	})

	t.Run("explicit server status without terminal marker is not terminal", func(t *testing.T) {
		assert.False(t, IsTerminalControlError(controlHTTPErrorf(http.StatusBadGateway, "tern unavailable")))
	})
}

// A failed control operation is triaged from logs alone, so the failure line
// must carry the apply's full triage attributes — including external_id, the
// join key to the data plane's logs — alongside the error itself.
func TestWriteControlError_LogsCarryFullApplyAttrs(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	svc := New(nil, testServerConfig(), nil, logger)
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-42",
		Database:        "appdb",
		DatabaseType:    "mysql",
		Deployment:      "east",
		Environment:     "staging",
		Repository:      "org/repo",
		PullRequest:     123,
		State:           state.Apply.Running,
		ExternalID:      "remote-apply-7",
	}

	svc.writeControlError(httptest.NewRecorder(), "stop", apply, errors.New("storage unavailable"))

	lines := decodeLogLines(t, logBuf.Bytes())
	line := requireLogLine(t, lines, "stop failed")
	assert.Equal(t, "apply-42", line["apply_id"])
	assert.Equal(t, "appdb", line["database"])
	assert.Equal(t, "mysql", line["database_type"])
	assert.Equal(t, "staging", line["environment"])
	assert.Equal(t, "org/repo", line["repo"])
	assert.Equal(t, float64(123), line["pr"])
	assert.Equal(t, "east", line["deployment"])
	assert.Equal(t, state.Apply.Running, line["state"])
	assert.Equal(t, "remote-apply-7", line["external_id"])
	assert.Contains(t, line, "error")
}

// A control request is answered from the Tern client of the deployment stored
// on its apply. When SchemaBot cannot build that client, for example because
// the deployment's DSN file is unreadable, the request answers 503 so durable
// callers retry once the outage clears, and the message is fixed because the
// raw error names the DSN source and stays in the server log. A deployment
// that is missing from the server configuration answers 404, since retrying
// cannot change that.
func TestControlRoutesAnswerForTernClientFailure(t *testing.T) {
	const clientUnavailableMessage = "SchemaBot could not connect to the schema change engine for this deployment. Retry shortly."
	missingDSNFile := filepath.Join(t.TempDir(), "missing-dsn-file")

	tests := []struct {
		name          string
		deployment    string
		wantStatus    int
		wantMessage   string
		wantTerminal  bool
		wantInternal  bool
		wantLogged    bool
		wantLogDetail string
	}{
		{
			name:          "client cannot be built",
			deployment:    "brokendb",
			wantStatus:    http.StatusServiceUnavailable,
			wantMessage:   clientUnavailableMessage,
			wantInternal:  true,
			wantLogged:    true,
			wantLogDetail: "resolve DSN for brokendb/staging",
		},
		{
			name:         "named deployment not in the server configuration",
			deployment:   "ghost",
			wantStatus:   http.StatusNotFound,
			wantMessage:  `unknown deployment "ghost"`,
			wantTerminal: true,
		},
		{
			name:         "default deployment not in the server configuration",
			deployment:   DefaultDeployment,
			wantStatus:   http.StatusNotFound,
			wantMessage:  "not found in server configuration",
			wantTerminal: true,
		},
	}

	newService := func(deployment string, logger *slog.Logger) *Service {
		apply := activeTestApply("apply-client-failure")
		apply.Deployment = deployment
		return New(&mockStorageWithApplyStores{
			applies:  &staticApplyStore{apply: apply},
			tasks:    &capturingTaskStore{},
			controls: &memoryControlRequestStore{},
		}, &ServerConfig{
			Databases: map[string]DatabaseConfig{
				"brokendb": {
					Type: storage.DatabaseTypeMySQL,
					Environments: map[string]EnvironmentConfig{
						"staging": {DSN: "file:" + missingDSNFile},
					},
				},
			},
			TernDeployments: TernConfig{"other": TernEndpoints{"staging": "localhost:9090"}},
		}, nil, logger)
	}

	for _, tc := range tests {
		t.Run(tc.name+"/http", func(t *testing.T) {
			for _, route := range []string{"/api/cutover", "/api/stop"} {
				var logBuf bytes.Buffer
				svc := newService(tc.deployment, slog.New(slog.NewJSONHandler(&logBuf, nil)))
				mux := http.NewServeMux()
				svc.ConfigureRoutes(mux)

				body := `{"environment": "staging", "apply_id": "apply-client-failure"}`
				req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, route, strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, req)

				require.Equal(t, tc.wantStatus, w.Code, "%s: %s", route, w.Body.String())
				var resp apitypes.ErrorResponse
				require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
				assert.Contains(t, resp.Error, tc.wantMessage, route)
				assert.NotContains(t, resp.Error, missingDSNFile, route)
				if tc.wantStatus == http.StatusServiceUnavailable {
					assert.Equal(t, tc.wantMessage, resp.Error, route)
				}
				assertTernClientFailureLogged(t, logBuf.Bytes(), tc.wantLogged, tc.wantLogDetail, route)
			}
		})

		t.Run(tc.name+"/in-process", func(t *testing.T) {
			ctrlReq := apitypes.ControlRequest{ApplyID: "apply-client-failure", Environment: "staging"}
			executors := map[string]func(svc *Service) error{
				"cutover": func(svc *Service) error {
					_, err := svc.ExecuteCutover(t.Context(), ctrlReq)
					return err
				},
				"stop": func(svc *Service) error {
					_, err := svc.ExecuteStop(t.Context(), ctrlReq)
					return err
				},
			}
			for operation, execute := range executors {
				var logBuf bytes.Buffer
				svc := newService(tc.deployment, slog.New(slog.NewJSONHandler(&logBuf, nil)))

				err := execute(svc)

				require.Error(t, err, operation)
				assert.Equal(t, tc.wantStatus, ControlOperationHTTPStatus(err), operation)
				assert.Equal(t, tc.wantInternal, IsInternalControlError(err), operation)
				assert.Equal(t, tc.wantTerminal, IsTerminalControlError(err), operation)
				assert.Contains(t, err.Error(), tc.wantMessage, operation)
				assert.NotContains(t, err.Error(), missingDSNFile, operation)
				if tc.wantStatus == http.StatusServiceUnavailable {
					assert.Equal(t, tc.wantMessage, err.Error(), operation)
				}
				assertTernClientFailureLogged(t, logBuf.Bytes(), tc.wantLogged, tc.wantLogDetail, operation)
			}
		})
	}
}

// assertTernClientFailureLogged checks that the server log keeps the raw
// client-construction error for triage when a request was answered with the
// fixed 503 message.
func assertTernClientFailureLogged(t *testing.T, logs []byte, wantRawError bool, detail, route string) {
	t.Helper()
	if !wantRawError {
		return
	}
	line := requireLogLine(t, decodeLogLines(t, logs), "failed to create Tern client")
	assert.Equal(t, "apply-client-failure", line["apply_id"], route)
	assert.Equal(t, "brokendb", line["deployment"], route)
	assert.Contains(t, line["error"], detail, route)
	assert.Contains(t, line["error"], "missing-dsn-file", route)
}

// A client-construction failure must not be mistaken for a missing
// configuration entry: the retry decision for durable control callers hangs on
// the difference.
func TestControlTernClientFailure(t *testing.T) {
	t.Run("missing configuration entry is a 404 with its own message", func(t *testing.T) {
		status, message := controlTernClientFailure(fmt.Errorf("unknown deployment %q: %w", "ghost", errTernDeploymentNotConfigured))
		assert.Equal(t, http.StatusNotFound, status)
		assert.Equal(t, `unknown deployment "ghost": tern deployment not configured`, message)
	})

	t.Run("any other failure is a 503 with a fixed message", func(t *testing.T) {
		status, message := controlTernClientFailure(errors.New("resolve DSN for orders/staging: open /secrets/dsn: no such file"))
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.Equal(t, "SchemaBot could not connect to the schema change engine for this deployment. Retry shortly.", message)
	})

	t.Run("503 is retryable", func(t *testing.T) {
		_, message := controlTernClientFailure(io.ErrUnexpectedEOF)
		err := controlHTTPErrorf(http.StatusServiceUnavailable, "%s", message)
		assert.True(t, IsInternalControlError(err))
		assert.False(t, IsTerminalControlError(err))
	})
}
