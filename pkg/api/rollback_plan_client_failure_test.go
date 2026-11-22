package api

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
)

// rollbackPlanFakeDSN stands in for connection details a Tern client failure
// can carry. It names the DSN file the server cannot read, so it appears in
// the raw error and must appear nowhere a requester can see.
const rollbackPlanFakeDSN = "user:pw@tcp(10.0.0.5:3306)"

// A rollback plan request whose engine client cannot be built gets one of two
// answers. When the deployment's DSN does not resolve, the request answers 503
// with a fixed message, so a caller can retry once the outage clears, and the
// raw cause, which names the DSN source, stays in the server log. When the
// deployment is missing from the server configuration, the request answers 404,
// since retrying cannot change that, with a message built from the apply's own
// database, deployment, and environment. Both answers carry the error code
// clients decide a retry from, through the in-process call the webhook makes
// and through the HTTP route the CLI calls.
func TestRollbackPlanAnswersForTernClientFailure(t *testing.T) {
	missingDSNFile := filepath.Join(t.TempDir(), rollbackPlanFakeDSN)

	tests := []struct {
		name         string
		deployment   string
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
			wantStatus:   http.StatusServiceUnavailable,
			wantCode:     apitypes.ErrCodeEngineUnavailable,
			wantMessage:  controlEngineUnavailableMessage,
			wantHTTP:     "rollback plan failed: " + controlEngineUnavailableMessage,
			wantTerminal: false,
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

	newService := func(deployment string, logger *slog.Logger) (*Service, *storage.Apply) {
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
						"staging": {DSN: "file:" + missingDSNFile},
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
			svc, apply := newService(tc.deployment, slog.New(slog.NewJSONHandler(&logBuf, nil)))

			resp, err := svc.ExecuteRollbackPlanForApply(t.Context(), apply)

			require.Error(t, err)
			assert.Nil(t, resp)
			assert.Equal(t, tc.wantStatus, ControlOperationHTTPStatus(err))
			assert.Equal(t, tc.wantCode, controlOperationErrorCode(err))
			assert.Equal(t, tc.wantTerminal, IsTerminalControlError(err), "whether a durable caller stops retrying")
			assert.Equal(t, tc.wantMessage, err.Error())
			assertNoRollbackPlanDSNFragment(t, err.Error())
			assertRollbackPlanClientFailureLogged(t, logBuf.Bytes(), tc.wantLogged)
		})

		t.Run(tc.name+"/http", func(t *testing.T) {
			var logBuf bytes.Buffer
			svc, _ := newService(tc.deployment, slog.New(slog.NewJSONHandler(&logBuf, nil)))
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
