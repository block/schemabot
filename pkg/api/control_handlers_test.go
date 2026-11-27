package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/secrets"
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

// A failed control is counted as "rejected" when the requester can act on the
// error and as "error" when SchemaBot failed, so the error status alerts only
// on SchemaBot's own failures.
func TestControlMetricStatusSeparatesRejectionFromFailure(t *testing.T) {
	assert.Equal(t, "rejected", controlMetricStatus(controlConflictf("schema change is not in its revert window (current state: %s)", "running")))
	assert.Equal(t, "rejected", controlMetricStatus(fmt.Errorf("execute stop: %w", controlHTTPErrorf(http.StatusNotFound, "apply not found: %s", "apply-123"))))
	assert.Equal(t, "error", controlMetricStatus(errors.New("control request store is not available")))
	assert.Equal(t, "error", controlMetricStatus(controlHTTPErrorf(http.StatusBadGateway, "tern unavailable")))
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
// on its apply. When SchemaBot cannot build that client because the secrets
// lookup for the deployment's DSN failed, the request answers 503 so durable
// callers retry once the backend recovers, and the message is fixed because
// the raw error names the DSN source and stays in the server log. When the DSN
// names a file that does not exist, every retry fails the same way, so the
// request answers a terminal 500 that says an operator has to fix the
// configuration. A deployment that is missing from the server configuration
// answers 404, since retrying cannot change that. Each answer carries the
// error code clients decide a retry from, and no message repeats the
// failure's own text.
func TestControlRoutesAnswerForTernClientFailure(t *testing.T) {
	missingDSNFile := filepath.Join(t.TempDir(), "missing-dsn-file")
	const flakyResolver = "controlflaky"
	secrets.Register(flakyResolver, func(string) (string, error) {
		return "", errors.New("secrets backend timed out reading flaky-dsn-secret")
	})
	t.Cleanup(func() { secrets.Unregister(flakyResolver) })

	tests := []struct {
		name          string
		deployment    string
		dsn           string
		wantStatus    int
		wantMessage   string
		wantCode      string
		wantRetryable bool
		wantTerminal  bool
		wantInternal  bool
		wantLogged    bool
		wantLogDetail string
	}{
		{
			name:          "client cannot be built",
			deployment:    "brokendb",
			dsn:           flakyResolver + ":orders",
			wantStatus:    http.StatusServiceUnavailable,
			wantMessage:   controlEngineUnavailableMessage,
			wantCode:      apitypes.ErrCodeEngineUnavailable,
			wantRetryable: true,
			wantInternal:  true,
			wantLogged:    true,
			wantLogDetail: "flaky-dsn-secret",
		},
		{
			name:       "client configuration cannot work",
			deployment: "brokendb",
			dsn:        "file:" + missingDSNFile,
			wantStatus: http.StatusInternalServerError,
			wantMessage: `the server configuration for database "testdb" (deployment="brokendb", environment="staging") cannot produce a client for the schema change engine, so retrying will not help. ` +
				"A SchemaBot operator can find the cause in the server log and fix the configuration",
			wantTerminal:  true,
			wantInternal:  true,
			wantLogged:    true,
			wantLogDetail: "missing-dsn-file",
		},
		{
			name:         "named deployment not in the server configuration",
			deployment:   "ghost",
			wantStatus:   http.StatusNotFound,
			wantMessage:  `no tern client configured for database "testdb" (deployment="ghost", environment="staging") — add this database to the server config`,
			wantCode:     apitypes.ErrCodeDeploymentNotFound,
			wantTerminal: true,
		},
		{
			name:         "default deployment not in the server configuration",
			deployment:   DefaultDeployment,
			wantStatus:   http.StatusNotFound,
			wantMessage:  `no tern client configured for database "testdb" (deployment="` + DefaultDeployment + `", environment="staging") — add this database to the server config`,
			wantCode:     apitypes.ErrCodeDeploymentNotFound,
			wantTerminal: true,
		},
	}

	newService := func(deployment, dsn string, logger *slog.Logger) *Service {
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
						"staging": {DSN: dsn},
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
				svc := newService(tc.deployment, tc.dsn, slog.New(slog.NewJSONHandler(&logBuf, nil)))
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
				assert.Equal(t, tc.wantMessage, resp.Error, route)
				assert.Equal(t, tc.wantCode, resp.ErrorCode, route)
				retryable, _ := resp.RetryAfter()
				assert.Equal(t, tc.wantRetryable, retryable, "%s: whether a client following the error code retries", route)
				assertTernClientFailureLogged(t, logBuf.Bytes(), tc.wantLogged, tc.wantLogDetail, strings.TrimPrefix(route, "/api/"))
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
				svc := newService(tc.deployment, tc.dsn, slog.New(slog.NewJSONHandler(&logBuf, nil)))

				err := execute(svc)

				require.Error(t, err, operation)
				assert.Equal(t, tc.wantStatus, ControlOperationHTTPStatus(err), operation)
				assert.Equal(t, tc.wantInternal, IsInternalControlError(err), operation)
				assert.Equal(t, tc.wantTerminal, IsTerminalControlError(err), operation)
				assert.Equal(t, tc.wantMessage, err.Error(), operation)
				assert.Equal(t, tc.wantCode, ControlOperationErrorCode(err), operation)
				assertTernClientFailureLogged(t, logBuf.Bytes(), tc.wantLogged, tc.wantLogDetail, operation)
			}
		})
	}
}

// An in-process control request names its environment the way the HTTP routes
// accept it: spelling case does not change which environment is meant. Every
// in-process control operation therefore answers a mixed-case environment
// exactly as it answers the lowercase one, and a different environment is still
// refused.
func TestExecuteControlOperationsFoldEnvironmentCase(t *testing.T) {
	executors := map[string]func(svc *Service, req apitypes.ControlRequest) error{
		"stop": func(svc *Service, req apitypes.ControlRequest) error {
			_, err := svc.ExecuteStop(t.Context(), req)
			return err
		},
		"cancel": func(svc *Service, req apitypes.ControlRequest) error {
			_, err := svc.ExecuteCancel(t.Context(), req)
			return err
		},
		"start": func(svc *Service, req apitypes.ControlRequest) error {
			_, err := svc.ExecuteStart(t.Context(), req)
			return err
		},
		"cutover": func(svc *Service, req apitypes.ControlRequest) error {
			_, err := svc.ExecuteCutover(t.Context(), req)
			return err
		},
		"release": func(svc *Service, req apitypes.ControlRequest) error {
			_, err := svc.ExecuteRelease(t.Context(), req)
			return err
		},
		"revert": func(svc *Service, req apitypes.ControlRequest) error {
			_, err := svc.ExecuteRevert(t.Context(), req)
			return err
		},
		"skip-revert": func(svc *Service, req apitypes.ControlRequest) error {
			_, err := svc.ExecuteSkipRevert(t.Context(), req)
			return err
		},
	}
	outcome := func(err error) string {
		if err == nil {
			return "accepted"
		}
		return err.Error()
	}

	// Each operation gets an apply in a state it acts on where one is cheap to
	// build, so the mixed-case request must be accepted, not merely refused the
	// same way. The rest are refused by a gate after the environment check.
	fixtures := map[string]func() *Service{
		"start": func() *Service {
			apply := stoppedTestApply("apply-env-case")
			task := &storage.Task{ID: 30, TaskIdentifier: "task-env-case", ApplyID: apply.ID, State: state.Task.Stopped}
			return newControlTestServiceWithTasks(&mockTernClient{}, apply, []*storage.Task{task})
		},
		"cutover": func() *Service {
			apply := activeTestApply("apply-env-case")
			apply.State = state.Apply.WaitingForCutover
			return newControlTestService(&mockTernClient{}, apply)
		},
	}
	acceptedOn := map[string]bool{"stop": true, "cancel": true, "start": true, "cutover": true}

	for operation, execute := range executors {
		t.Run(operation, func(t *testing.T) {
			fixture := fixtures[operation]
			if fixture == nil {
				fixture = func() *Service { return newControlTestService(&mockTernClient{}, activeTestApply("apply-env-case")) }
			}
			run := func(environment string) error {
				return execute(fixture(), apitypes.ControlRequest{ApplyID: "apply-env-case", Environment: environment})
			}

			lowercase := run("staging")
			mixedCase := run("StAgInG")

			if acceptedOn[operation] {
				assert.NoError(t, lowercase)
				assert.NoError(t, mixedCase, "a mixed-case environment must be accepted")
			}
			assert.NotContains(t, outcome(mixedCase), "belongs to environment")
			assert.Equal(t, outcome(lowercase), outcome(mixedCase),
				"a mixed-case environment must get the answer the lowercase spelling gets")

			err := run("production")
			require.Error(t, err)
			assert.Equal(t, http.StatusBadRequest, ControlOperationHTTPStatus(err))
			assert.Contains(t, err.Error(), `apply "apply-env-case" belongs to environment "staging", not "production"`)
		})
	}
}

// A stop sent from the PR-comment path under a mixed-case environment reaches
// the data plane under the apply's own canonical environment.
func TestExecuteStopSendsCanonicalEnvironmentToDataPlane(t *testing.T) {
	mock := &mockTernClient{stopResp: &ternv1.StopResponse{Accepted: true, StoppedCount: 1}}
	apply := activeTestApply("apply-stop-env-case")
	task := &storage.Task{ID: 20, TaskIdentifier: "task-stop-env-case", ApplyID: apply.ID, State: state.Task.Running}
	svc := newControlTestServiceWithTasks(mock, apply, []*storage.Task{task})

	resp, err := svc.ExecuteStop(t.Context(), apitypes.ControlRequest{
		ApplyID:     "apply-stop-env-case",
		Environment: "StAgInG",
		Caller:      "github:stopper@octocat/hello-world#1",
	})

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	require.NotNil(t, mock.stopReq, "the immediate stop reaches the data plane")
	assert.Equal(t, "staging", mock.stopReq.Environment)
	pending, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.NotNil(t, pending, "the durable stop request is recorded")
}

// assertTernClientFailureLogged checks that the server log keeps the raw
// client-construction error for triage, with the operation that was asked for,
// when a request was answered without the raw error.
func assertTernClientFailureLogged(t *testing.T, logs []byte, wantRawError bool, detail, operation string) {
	t.Helper()
	if !wantRawError {
		return
	}
	line := requireLogLine(t, decodeLogLines(t, logs), "failed to create Tern client")
	assert.Equal(t, operation, line["operation"])
	assert.Equal(t, "apply-client-failure", line["apply_id"], operation)
	assert.Equal(t, "brokendb", line["deployment"], operation)
	assert.Contains(t, line["error"], detail, operation)
}

// A client-construction failure must not be mistaken for a missing
// configuration entry: the retry decision for durable control callers hangs on
// the difference.
func TestControlTernClientFailure(t *testing.T) {
	apply := activeTestApply("apply-client-failure")
	const notConfiguredMessage = `no tern client configured for database "testdb" (deployment="ghost", environment="staging") — add this database to the server config`

	t.Run("missing configuration entry is a terminal 404 with the deployment-not-found code", func(t *testing.T) {
		err := controlTernClientFailure(apply, "ghost", fmt.Errorf("unknown deployment %q: %w", "ghost", errTernDeploymentNotConfigured))
		assert.Equal(t, http.StatusNotFound, ControlOperationHTTPStatus(err))
		assert.Equal(t, apitypes.ErrCodeDeploymentNotFound, ControlOperationErrorCode(err))
		assert.Equal(t, notConfiguredMessage, err.Error())
		assert.True(t, IsTerminalControlError(err))
	})

	// The 404 message names the apply's own database, deployment, and
	// environment. Text that a caller wrapped around the not-configured error
	// never reaches the requester.
	t.Run("missing configuration entry does not repeat the text wrapped around it", func(t *testing.T) {
		wrapped := fmt.Errorf("dial tern at db-7.internal:9090 as user:secret: %w",
			fmt.Errorf("unknown deployment %q: %w", "ghost", errTernDeploymentNotConfigured))
		err := controlTernClientFailure(apply, "ghost", wrapped)
		assert.Equal(t, http.StatusNotFound, ControlOperationHTTPStatus(err))
		assert.Equal(t, notConfiguredMessage, err.Error())
	})

	t.Run("any other failure is a retryable 503 with a fixed message and the engine-unavailable code", func(t *testing.T) {
		err := controlTernClientFailure(apply, "ghost", errors.New("resolve DSN for orders/staging: open /secrets/dsn: no such file"))
		assert.Equal(t, http.StatusServiceUnavailable, ControlOperationHTTPStatus(err))
		assert.Equal(t, apitypes.ErrCodeEngineUnavailable, ControlOperationErrorCode(err))
		assert.True(t, apitypes.IsRetryableErrorCode(ControlOperationErrorCode(err)))
		assert.Equal(t, controlEngineUnavailableMessage, err.Error())
		assert.True(t, IsInternalControlError(err))
		assert.False(t, IsTerminalControlError(err))
	})
}

// A stop or cancel reports how many tables it acts on and how many it leaves
// alone because they already finished. A table counts once however many tasks
// it carries: the steps of a staged change on one table, or one task per shard.
func TestStopRequestCountsCountTables(t *testing.T) {
	task := func(id int64, namespace, table, taskState string) *storage.Task {
		return &storage.Task{ID: id, Namespace: namespace, TableName: table, State: taskState}
	}
	cases := map[string]struct {
		tasks       []*storage.Task
		wantActive  int64
		wantSkipped int64
	}{
		"staged change on one table with one step done": {
			tasks: []*storage.Task{
				task(1, "public", "users", state.Task.Completed),
				task(2, "public", "users", state.Task.FailedRetryable),
				task(3, "public", "users", state.Task.Pending),
				task(4, "public", "users", state.Task.Pending),
			},
			wantActive: 1,
		},
		"one table done and one still running": {
			tasks: []*storage.Task{
				task(1, "app", "orders", state.Task.Completed),
				task(2, "app", "customers", state.Task.Running),
			},
			wantActive:  1,
			wantSkipped: 1,
		},
		"one table across shards": {
			tasks: []*storage.Task{
				task(1, "commerce", "orders", state.Task.Running),
				task(2, "commerce", "orders", state.Task.Completed),
			},
			wantActive: 1,
		},
		"same table name in two namespaces": {
			tasks: []*storage.Task{
				task(1, "billing", "accounts", state.Task.Running),
				task(2, "identity", "accounts", state.Task.Running),
			},
			wantActive: 2,
		},
		"tasks that name no table count one each": {
			tasks: []*storage.Task{
				task(1, "app", "", state.Task.Running),
				task(2, "app", "", state.Task.Completed),
			},
			wantActive:  1,
			wantSkipped: 1,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			active, skipped := stopRequestCounts(tc.tasks)
			assert.Equal(t, tc.wantActive, active, "tables acted on")
			assert.Equal(t, tc.wantSkipped, skipped, "tables already finished")
		})
	}
}
