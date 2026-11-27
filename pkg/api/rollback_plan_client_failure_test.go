package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/secrets"
	"github.com/block/schemabot/pkg/storage"
)

// rollbackPlanFakeDSN stands in for connection details a Tern client failure
// can carry. The failing secrets lookup and the missing DSN file both name it,
// so it appears in the raw error and must appear nowhere a requester can see.
const rollbackPlanFakeDSN = "user:pw@tcp(10.0.0.5:3306)"

// rollbackPlanFlakyResolver is a secrets prefix whose lookups always fail, the
// way a secrets backend does during an outage.
const rollbackPlanFlakyResolver = "rollbackplanflaky"

// A rollback plan request whose engine client cannot be built gets one of three
// answers. When the secrets lookup for the deployment's DSN fails, the request
// answers 503 with a fixed message, so a caller can retry once the backend
// recovers, and the raw cause stays in the server log. When the DSN names a
// file that does not exist, a retry fails the same way, so the request answers
// 500 with no retry code and says an operator has to fix the configuration.
// When the deployment is missing from the server configuration, the request
// answers 404. The terminal answers are built from the apply's own database,
// deployment, and environment. Each answer reads the same through the
// in-process call the webhook makes and through the HTTP route the CLI calls,
// including the error code clients decide a retry from.
func TestRollbackPlanAnswersForTernClientFailure(t *testing.T) {
	missingDSNFile := filepath.Join(t.TempDir(), rollbackPlanFakeDSN)
	secrets.Register(rollbackPlanFlakyResolver, func(string) (string, error) {
		return "", errors.New("secrets backend timed out reading " + rollbackPlanFakeDSN)
	})
	t.Cleanup(func() { secrets.Unregister(rollbackPlanFlakyResolver) })
	misconfigured := `the server configuration for database "orders" (deployment="brokendb", environment="staging") cannot produce a client for the schema change engine, so retrying will not help. ` +
		"A SchemaBot operator can find the cause in the server log and fix the configuration"

	tests := []struct {
		name         string
		deployment   string
		dsn          string
		wantStatus   int
		wantCode     string
		wantMessage  string
		wantHTTP     string
		wantTerminal bool
		wantLogged   bool
	}{
		{
			name:         "client cannot be built",
			deployment:   "brokendb",
			dsn:          rollbackPlanFlakyResolver + ":orders",
			wantStatus:   http.StatusServiceUnavailable,
			wantCode:     apitypes.ErrCodeEngineUnavailable,
			wantMessage:  controlEngineUnavailableMessage,
			wantHTTP:     "rollback plan failed: " + controlEngineUnavailableMessage,
			wantTerminal: false,
			wantLogged:   true,
		},
		{
			name:         "client configuration cannot work",
			deployment:   "brokendb",
			dsn:          "file:" + missingDSNFile,
			wantStatus:   http.StatusInternalServerError,
			wantMessage:  misconfigured,
			wantHTTP:     "rollback plan failed: " + misconfigured,
			wantTerminal: true,
			wantLogged:   true,
		},
		{
			name:         "deployment not in the server configuration",
			deployment:   "ghost",
			wantStatus:   http.StatusNotFound,
			wantCode:     apitypes.ErrCodeDeploymentNotFound,
			wantMessage:  `no tern client configured for database "orders" (deployment="ghost", environment="staging") — add this database to the server config`,
			wantHTTP:     `rollback plan rejected: no tern client configured for database "orders" (deployment="ghost", environment="staging") — add this database to the server config`,
			wantTerminal: true,
		},
	}

	newService := func(deployment, dsn string, logger *slog.Logger) (*Service, *storage.Apply) {
		now := time.Now().UTC()
		apply := rollbackGuardrailApply("apply-client-failure", 1, 10, now)
		apply.Deployment = deployment
		plan := rollbackGuardrailPlan(10, true)
		plan.Deployment = deployment
		plan.Target = "orders-staging"
		svc := New(&mockStorageWithApplyStores{
			plans:   &staticPlanStore{plansByID: map[int64]*storage.Plan{plan.ID: plan}},
			applies: &staticApplyStore{applies: []*storage.Apply{apply}},
			tasks:   &capturingTaskStore{tasks: []*storage.Task{rollbackGuardrailTask(1, 10, now)}},
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
		return svc, apply
	}

	for _, tc := range tests {
		t.Run(tc.name+"/in-process", func(t *testing.T) {
			var logBuf bytes.Buffer
			svc, apply := newService(tc.deployment, tc.dsn, slog.New(slog.NewJSONHandler(&logBuf, nil)))

			resp, err := svc.ExecuteRollbackPlanForApply(t.Context(), apply)

			require.Error(t, err)
			assert.Nil(t, resp)
			assert.Equal(t, tc.wantStatus, ControlOperationHTTPStatus(err))
			assert.Equal(t, tc.wantCode, ControlOperationErrorCode(err))
			assert.Equal(t, tc.wantTerminal, IsTerminalControlError(err), "whether a durable caller stops retrying")
			assert.Equal(t, tc.wantMessage, err.Error())
			assertNoRollbackPlanDSNFragment(t, err.Error())
			assertRollbackPlanClientFailureLogged(t, logBuf.Bytes(), tc.wantLogged)
		})

		t.Run(tc.name+"/http", func(t *testing.T) {
			var logBuf bytes.Buffer
			svc, _ := newService(tc.deployment, tc.dsn, slog.New(slog.NewJSONHandler(&logBuf, nil)))
			mux := http.NewServeMux()
			svc.ConfigureRoutes(mux)

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/rollback/plan",
				strings.NewReader(`{"apply_id":"apply-client-failure","environment":"staging"}`))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			require.Equal(t, tc.wantStatus, w.Code, w.Body.String())
			assertNoRollbackPlanDSNFragment(t, w.Body.String())
			var resp apitypes.ErrorResponse
			require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
			assert.Equal(t, tc.wantHTTP, resp.Error)
			assert.Equal(t, tc.wantCode, resp.ErrorCode)
			retryable, _ := resp.RetryAfter()
			assert.Equal(t, !tc.wantTerminal, retryable, "whether a client following the error code retries")
			assertRollbackPlanClientFailureLogged(t, logBuf.Bytes(), tc.wantLogged)
		})
	}
}

// A server configuration that can never produce an engine client fails every
// attempt the same way, so a rollback plan is refused as terminal and the
// requester is told an operator has to fix the configuration, rather than
// being told to retry.
func TestRollbackPlanMisconfiguredEngineClientIsTerminal(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("no-colon-here"), 0o600))
	localDSN := "u:p@tcp(127.0.0.1:1)/x"
	cases := map[string]ServerConfig{
		"empty endpoint":          {TernDeployments: TernConfig{"d": TernEndpoints{"staging": ""}}},
		"endpoint without a port": {TernDeployments: TernConfig{"d": TernEndpoints{"staging": "no-port"}}},
		"malformed token": {Databases: map[string]DatabaseConfig{"d": {Type: storage.DatabaseTypeMySQL,
			Environments: map[string]EnvironmentConfig{"staging": {DSN: localDSN, TokenSecretRef: "file:" + tokenFile}}}}},
		"token file that does not exist": {Databases: map[string]DatabaseConfig{"d": {Type: storage.DatabaseTypeMySQL,
			Environments: map[string]EnvironmentConfig{"staging": {DSN: localDSN, TokenSecretRef: "file:" + filepath.Join(t.TempDir(), "missing")}}}}},
		"revert window that does not parse": {Databases: map[string]DatabaseConfig{"d": {Type: storage.DatabaseTypeMySQL,
			Environments: map[string]EnvironmentConfig{"staging": {DSN: localDSN, RevertWindowDuration: "soon"}}}}},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			now := time.Now().UTC()
			apply := rollbackGuardrailApply("a1", 1, 10, now)
			apply.Deployment = "d"
			plan := rollbackGuardrailPlan(10, true)
			plan.Deployment = "d"
			plan.Target = "t"
			svc := New(&mockStorageWithApplyStores{
				plans:   &staticPlanStore{plansByID: map[int64]*storage.Plan{plan.ID: plan}},
				applies: &staticApplyStore{applies: []*storage.Apply{apply}},
				tasks:   &capturingTaskStore{tasks: []*storage.Task{rollbackGuardrailTask(1, 10, now)}},
			}, &cfg, nil, slog.New(slog.DiscardHandler))

			_, err := svc.ExecuteRollbackPlanForApply(t.Context(), apply)

			require.Error(t, err)
			assert.True(t, IsTerminalControlError(err), "a configuration error repeats on every attempt")
			assert.Equal(t, http.StatusInternalServerError, ControlOperationHTTPStatus(err))
			assert.Empty(t, ControlOperationErrorCode(err), "no code tells a client to retry")
			assert.Contains(t, err.Error(), "fix the configuration")
		})
	}
}

// assertNoRollbackPlanDSNFragment checks that no part of the fake DSN reached
// text a requester reads.
func assertNoRollbackPlanDSNFragment(t *testing.T, text string) {
	t.Helper()
	for _, fragment := range []string{"10.0.0.5", "user:pw", "3306", "tcp("} {
		assert.NotContains(t, text, fragment)
	}
}

// assertRollbackPlanClientFailureLogged checks that the server log keeps the
// raw client-construction error, DSN source included, for triage when the
// request was answered with the fixed 503 message.
func assertRollbackPlanClientFailureLogged(t *testing.T, logs []byte, wantRawError bool) {
	t.Helper()
	if !wantRawError {
		return
	}
	line := requireLogLine(t, decodeLogLines(t, logs), "failed to create Tern client")
	assert.Equal(t, "rollback_plan", line["operation"])
	assert.Equal(t, "apply-client-failure", line["apply_id"])
	assert.Equal(t, "brokendb", line["deployment"])
	assert.Contains(t, line["error"], "resolve DSN for brokendb/staging")
	assert.Contains(t, line["error"], rollbackPlanFakeDSN)
}
