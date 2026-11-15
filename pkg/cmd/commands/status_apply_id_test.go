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
		`{"error":"the schema change engine for this apply is unavailable; try again shortly","error_code":"engine_unavailable"}`)

	err := (&StatusCmd{ApplyIDArg: "apply-example-1", JSON: true}).Run(&Globals{Endpoint: server.URL})

	require.Error(t, err)
	assert.False(t, client.IsNotFound(err))
	assert.Contains(t, err.Error(), "the schema change engine for this apply is unavailable")
	var apiErr *client.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusServiceUnavailable, apiErr.Status)
	assert.Equal(t, apitypes.ErrCodeEngineUnavailable, apiErr.ErrorCode)
	retry, _ := apiErr.RetryAfter()
	assert.True(t, retry, "an engine_unavailable answer is one the CLI may retry")
}

func TestStatusByApplyIDReportsUnknownApplyAsNotFound(t *testing.T) {
	server := statusByApplyIDServer(t, http.StatusNotFound,
		`{"error":"apply not found","error_code":"not_found"}`)

	err := (&StatusCmd{ApplyIDArg: "apply-example-1", JSON: true}).Run(&Globals{Endpoint: server.URL})

	require.NoError(t, err)
}
