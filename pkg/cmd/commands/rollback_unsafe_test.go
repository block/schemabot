package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rollbackAPIServer serves a rollback plan and accepts the lock and apply
// calls that follow it. It records each request line and the options of every
// apply submitted, so a test can tell whether a rollback reached the apply and
// what consent it carried.
func rollbackAPIServer(t *testing.T, plan apitypes.PlanResponse) (endpoint string, requests func() []string, applyOptions func() []map[string]string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	var submitted []map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		line := r.Method + " " + r.URL.Path
		mu.Lock()
		seen = append(seen, line)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch line {
		case "POST /api/rollback/plan":
			assert.NoError(t, json.NewEncoder(w).Encode(plan))
		case "GET /api/status":
			assert.NoError(t, json.NewEncoder(w).Encode(apitypes.StatusResponse{}))
		case "GET /api/locks/shop/mysql", "POST /api/locks/acquire":
			assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"lock": nil}))
		case "POST /api/apply":
			var request apitypes.ApplyRequest
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			mu.Lock()
			submitted = append(submitted, request.Options)
			mu.Unlock()
			assert.NoError(t, json.NewEncoder(w).Encode(apitypes.ApplyResponse{Accepted: true, ApplyID: "apply-example-91"}))
		default:
			assert.Fail(t, "unexpected rollback request", line)
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	requests = func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
	applyOptions = func() []map[string]string {
		mu.Lock()
		defer mu.Unlock()
		return append([]map[string]string(nil), submitted...)
	}
	return server.URL, requests, applyOptions
}

// A rollback whose plan drops a table needs --allow-unsafe, the same consent an
// apply needs. With -y and without the flag it is refused before any lock or
// apply request, and the refusal names the command that permits it. With the
// flag, the apply carries allow_unsafe; a rollback with no unsafe changes never
// sends it.
func TestRollbackUnsafeChangesRequireAllowUnsafe(t *testing.T) {
	dropPlan := apitypes.PlanResponse{PlanID: "plan-example-rollback", Database: "shop", DatabaseType: "mysql", Environment: "production",
		Changes: []*apitypes.SchemaChangeResponse{{Namespace: "shop", TableChanges: []*apitypes.TableChangeResponse{{TableName: "refunds", ChangeType: "drop", DDL: "DROP TABLE `refunds`;"}}}}}
	indexPlan := apitypes.PlanResponse{PlanID: "plan-example-rollback", Database: "shop", DatabaseType: "mysql", Environment: "production",
		Changes: []*apitypes.SchemaChangeResponse{{Namespace: "shop", TableChanges: []*apitypes.TableChangeResponse{{TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD INDEX `idx_status` (`status`);"}}}}}
	lockAndApply := []string{"POST /api/rollback/plan", "GET /api/status", "GET /api/locks/shop/mysql", "POST /api/locks/acquire", "POST /api/apply"}

	t.Run("auto-approve without allow-unsafe is refused", func(t *testing.T) {
		t.Setenv("SCHEMABOT_TOKEN", "fictional-rollback-test")
		endpoint, requests, applyOptions := rollbackAPIServer(t, dropPlan)
		var runErr error
		output := stripAnsi(captureStdout(func() {
			cmd := RollbackCmd{ApplyID: "apply-example-90", Environment: "production", AutoApprove: true}
			runErr = cmd.Run(&Globals{Endpoint: endpoint})
		}))
		require.ErrorIs(t, runErr, ErrSilent)
		assert.Contains(t, output, "Apply blocked: 1 unsafe change(s) detected")
		assert.Contains(t, output, "refunds: DROP TABLE removes all data")
		assert.Contains(t, output, "rollback apply-example-90 -e production --allow-unsafe")
		assert.Equal(t, []string{"POST /api/rollback/plan", "GET /api/status"}, requests())
		assert.Empty(t, applyOptions())
	})

	t.Run("interactive without allow-unsafe is refused before the prompt", func(t *testing.T) {
		t.Setenv("SCHEMABOT_TOKEN", "fictional-rollback-test")
		endpoint, requests, applyOptions := rollbackAPIServer(t, dropPlan)
		var runErr error
		output := stripAnsi(captureStdout(func() {
			cmd := RollbackCmd{ApplyID: "apply-example-90", Environment: "production"}
			runErr = cmd.Run(&Globals{Endpoint: endpoint})
		}))
		require.ErrorIs(t, runErr, ErrSilent)
		assert.Contains(t, output, "rollback apply-example-90 -e production --allow-unsafe")
		assert.NotContains(t, output, "Do you want to apply this rollback?")
		assert.Equal(t, []string{"POST /api/rollback/plan", "GET /api/status"}, requests())
		assert.Empty(t, applyOptions())
	})

	t.Run("auto-approve with allow-unsafe sends consent", func(t *testing.T) {
		t.Setenv("SCHEMABOT_TOKEN", "fictional-rollback-test")
		endpoint, requests, applyOptions := rollbackAPIServer(t, dropPlan)
		var runErr error
		output := stripAnsi(captureStdout(func() {
			cmd := RollbackCmd{ApplyID: "apply-example-90", Environment: "production", AutoApprove: true, AllowUnsafe: true}
			runErr = cmd.Run(&Globals{Endpoint: endpoint})
		}))
		require.NoError(t, runErr)
		assert.Contains(t, output, "Unsafe Changes (--allow-unsafe enabled)")
		assert.Contains(t, output, "Rollback started: apply-example-91")
		assert.Equal(t, lockAndApply, requests())
		submitted := applyOptions()
		require.Len(t, submitted, 1)
		assert.Equal(t, "true", submitted[0]["allow_unsafe"])
	})

	t.Run("safe rollback does not send allow-unsafe", func(t *testing.T) {
		t.Setenv("SCHEMABOT_TOKEN", "fictional-rollback-test")
		endpoint, requests, applyOptions := rollbackAPIServer(t, indexPlan)
		var runErr error
		output := stripAnsi(captureStdout(func() {
			cmd := RollbackCmd{ApplyID: "apply-example-90", Environment: "production", AutoApprove: true}
			runErr = cmd.Run(&Globals{Endpoint: endpoint})
		}))
		require.NoError(t, runErr)
		assert.NotContains(t, output, "Unsafe Changes")
		assert.Equal(t, lockAndApply, requests())
		submitted := applyOptions()
		require.Len(t, submitted, 1)
		assert.NotContains(t, submitted[0], "allow_unsafe")
	})
}
