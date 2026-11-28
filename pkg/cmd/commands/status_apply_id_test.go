package commands

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
)

func statusByApplyIDServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	t.Setenv("SCHEMABOT_TOKEN", "fictional-status-test")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/progress/apply/apply-example-1", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}

// An apply that exists but whose engine client is unavailable answers 503, and
// `status <apply_id>` reports that as an error with the server's message so the
// command exits non-zero, rather than printing "No schema change found".
func TestStatusByApplyIDReportsEngineUnavailableAsError(t *testing.T) {
	server := statusByApplyIDServer(t, http.StatusServiceUnavailable,
		`{"error":"SchemaBot could not read this apply's progress from its schema change engine. Retry in a moment. If it keeps failing, a SchemaBot operator can find the cause in the server log.","error_code":"engine_unavailable"}`)

	err := (&StatusCmd{ApplyIDArg: "apply-example-1", JSON: true}).Run(&Globals{Endpoint: server.URL})

	require.Error(t, err)
	assert.False(t, client.IsNotFound(err))
	assert.Contains(t, err.Error(), "SchemaBot could not read this apply's progress from its schema change engine")
	var apiErr *client.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusServiceUnavailable, apiErr.Status)
	assert.Equal(t, apitypes.ErrCodeEngineUnavailable, apiErr.ErrorCode)
	retry, _ := apiErr.RetryAfter()
	assert.True(t, retry, "an engine_unavailable answer is one the CLI may retry")
}

// An apply that exists but whose deployment is missing from the server
// configuration answers 404 deployment_not_found. `status <apply_id>` reports
// that as an error with the server's message so the command exits non-zero,
// rather than printing "No schema change found".
func TestStatusByApplyIDReportsMissingDeploymentAsError(t *testing.T) {
	server := statusByApplyIDServer(t, http.StatusNotFound,
		`{"error":"no tern client configured for database \"ledger\" (deployment=\"ledger\", environment=\"production\") — add this database to the server config","error_code":"deployment_not_found"}`)

	err := (&StatusCmd{ApplyIDArg: "apply-example-1", JSON: true}).Run(&Globals{Endpoint: server.URL})

	require.Error(t, err)
	assert.Contains(t, err.Error(), `no tern client configured for database "ledger"`)
	assert.Contains(t, err.Error(), "add this database to the server config")
	var apiErr *client.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusNotFound, apiErr.Status)
	assert.Equal(t, apitypes.ErrCodeDeploymentNotFound, apiErr.ErrorCode)
	retry, _ := apiErr.RetryAfter()
	assert.False(t, retry, "a deployment_not_found answer is not one the CLI retries")
}

// An apply ID the server has no record of is an error, in text and JSON
// output alike, so the command exits non-zero and a script never reads the
// absence as success. The error names the apply and carries the not_found
// code JSON output reports.
func TestStatusByApplyIDReportsUnknownApplyAsNotFound(t *testing.T) {
	server := statusByApplyIDServer(t, http.StatusNotFound,
		`{"error":"apply not found","error_code":"not_found"}`)

	for _, outputJSON := range []bool{false, true} {
		err := (&StatusCmd{ApplyIDArg: "apply-example-1", JSON: outputJSON}).Run(&Globals{Endpoint: server.URL})

		var notFound *NotFoundError
		require.ErrorAs(t, err, &notFound, "json=%v", outputJSON)
		assert.Equal(t, apitypes.ErrCodeNotFound, notFound.ErrorCode())
		assert.Contains(t, err.Error(), `no schema change found for apply "apply-example-1" on this server`)
		assert.Contains(t, err.Error(), "check the ID against the applies")
		assert.Equal(t, 1, ExitCodeFor(err))
	}
}
