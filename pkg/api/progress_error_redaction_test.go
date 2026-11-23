package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

const sensitiveStorageError = "dial tcp 10.0.0.5:3306: connect: connection refused (user:pass@db.internal)"

// An operator polling an apply's progress while storage is failing gets a fixed
// message that names the apply and points at the server log. The storage error
// carries connection details, so it reaches only the server log, next to the
// apply's identifiers.
func TestProgressByApplyIDStorageFailuresAnswerFixedMessages(t *testing.T) {
	storageErr := errors.New(sensitiveStorageError)

	tests := []struct {
		name     string
		apply    func() *storage.Apply
		applies  *staticApplyStore
		tasksErr error
		client   tern.Client
		wantMsg  string
		wantLog  string
	}{
		{
			name:    "apply lookup fails",
			apply:   func() *storage.Apply { return activeTestApply("apply-lookup-fails") },
			applies: &staticApplyStore{err: storageErr},
			wantMsg: "progress for apply apply-lookup-fails failed: could not read the apply from storage; see server logs, then retry",
			wantLog: "failed to get apply",
		},
		{
			name: "task read fails for a finished apply",
			apply: func() *storage.Apply {
				apply := activeTestApply("apply-tasks-fail")
				apply.State = state.Apply.Completed
				return apply
			},
			tasksErr: storageErr,
			wantMsg:  "progress for apply apply-tasks-fail failed: could not read its tasks from storage; see server logs, then retry",
			wantLog:  "failed to read apply progress from storage",
		},
		{
			name: "task read fails for a queued remote apply",
			apply: func() *storage.Apply {
				apply := activeTestApply("apply-queued-tasks-fail")
				apply.ExternalID = ""
				return apply
			},
			tasksErr: storageErr,
			client:   &mockTernClient{isRemote: true},
			wantMsg:  "progress for apply apply-queued-tasks-fail failed: could not read its tasks from storage; see server logs, then retry",
			wantLog:  "failed to read queued remote apply progress from storage",
		},
		{
			name: "active apply has no stored deployment",
			apply: func() *storage.Apply {
				apply := activeTestApply("apply-no-deployment")
				apply.Deployment = ""
				return apply
			},
			client:  &mockTernClient{},
			wantMsg: "progress for apply apply-no-deployment is unavailable: SchemaBot has no deployment recorded for it; a SchemaBot operator can find the cause in the server log",
			wantLog: "active apply is missing stored deployment metadata",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))

			apply := tc.apply()
			applies := tc.applies
			if applies == nil {
				applies = &staticApplyStore{apply: apply}
			}
			var clients map[string]tern.Client
			if tc.client != nil {
				clients = map[string]tern.Client{"default/staging": tc.client}
			}
			svc := New(&mockStorageWithApplyStores{
				applies:  applies,
				tasks:    &capturingTaskStore{err: tc.tasksErr},
				controls: &memoryControlRequestStore{},
			}, testServerConfig(), clients, logger)
			mux := http.NewServeMux()
			svc.ConfigureRoutes(mux)

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/progress/apply/"+apply.ApplyIdentifier, nil)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
			var resp apitypes.ErrorResponse
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
			assert.Equal(t, apitypes.ErrCodeStorageError, resp.ErrorCode)
			assert.Equal(t, tc.wantMsg, resp.Error)
			assert.NotContains(t, w.Body.String(), "10.0.0.5")
			assert.NotContains(t, w.Body.String(), "user:pass")

			record := logRecordContaining(t, logs.String(), tc.wantLog)
			assert.Contains(t, record, "apply_id="+apply.ApplyIdentifier)
			if tc.applies == nil {
				// The apply is in scope, so the record carries its triage
				// identifiers, not only the apply ID.
				assert.Contains(t, record, "database="+apply.Database)
				assert.Contains(t, record, "environment="+apply.Environment)
				assert.Contains(t, record, "database_type="+apply.DatabaseType)
			}
			if tc.applies != nil || tc.tasksErr != nil {
				assert.Contains(t, record, "user:pass@db.internal", "the raw error belongs in the server log")
			} else {
				assert.Contains(t, record, `error="apply \"`+apply.ApplyIdentifier+`\" is missing stored deployment metadata"`, "the cause belongs in the server log")
			}
		})
	}
}

// logRecordContaining returns the one text log record whose message contains
// msg, so assertions check that record's own attributes rather than another
// line that happens to carry the same value.
func logRecordContaining(t *testing.T, logs, msg string) string {
	t.Helper()
	var matches []string
	for line := range strings.SplitSeq(logs, "\n") {
		if strings.Contains(line, msg) {
			matches = append(matches, line)
		}
	}
	require.Len(t, matches, 1, "log records containing %q:\n%s", msg, logs)
	return matches[0]
}

// A server config with an environment that has no route cannot be listed. The
// answer says so without repeating the configured database name, and the
// server log carries the cause.
func TestHandleDatabaseListConfigFailureAnswersFixedMessage(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	config := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"leakdb": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {Target: "leakdb"},
				},
			},
		},
	}
	svc := New(&mockStorage{}, config, nil, logger)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases?name=leakdb", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
	var resp apitypes.ErrorResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "failed to list databases; see server logs", resp.Error)
	assert.NotContains(t, w.Body.String(), "leakdb")
	record := logRecordContaining(t, logs.String(), "database list failed")
	assert.Contains(t, record, "name=leakdb", "the record carries the request's filters")
	assert.Contains(t, record, "database_type=")
	assert.Contains(t, record, "app=")
	assert.Contains(t, record, "error=", "the record carries the cause")
}
