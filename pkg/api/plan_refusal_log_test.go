package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

// A plan the engine refuses is the PR author's to fix, so the control plane
// logs it at Warn with the same triage fields, whether a remote data plane
// answered FailedPrecondition or a local engine returned the refusal. Any
// other plan failure is still logged at Error.
func TestExecutePlanLogsARefusedPlanAtWarn(t *testing.T) {
	tests := []struct {
		name      string
		planErr   error
		wantLevel string
		wantMsg   string
	}{
		{
			name:      "remote refusal",
			planErr:   status.Error(codes.FailedPrecondition, "the schema files drop table orders_seq"),
			wantLevel: "WARN",
			wantMsg:   "ExecutePlan: engine refused the plan; it plans once the change the refusal names is made to the schema files or the target",
		},
		{
			name:      "local refusal",
			planErr:   fmt.Errorf("plan namespace: %w", &engine.PlanRefusedError{Err: errors.New("the schema files drop table orders_seq")}),
			wantLevel: "WARN",
			wantMsg:   "ExecutePlan: engine refused the plan; it plans once the change the refusal names is made to the schema files or the target",
		},
		{
			name:      "server fault",
			planErr:   status.Error(codes.Internal, "read shard schema: timeout"),
			wantLevel: "ERROR",
			wantMsg:   "ExecutePlan: client.Plan failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockTernClient{isRemote: true, planErr: tt.planErr}
			svc := namespaceSelectionService(t, client, &capturingPlanStore{})
			var logs bytes.Buffer
			svc.logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))

			_, err := svc.ExecutePlan(t.Context(), placedNamespacesRequest())
			require.Error(t, err)

			entry := planFailureLogEntry(t, &logs)
			assert.Equal(t, tt.wantLevel, entry["level"])
			assert.Equal(t, tt.wantMsg, entry["msg"])
			assert.Equal(t, "orders", entry["database"])
			assert.Equal(t, "production", entry["environment"])
			assert.Equal(t, "eu", entry["deployment"])
			assert.Equal(t, storage.DatabaseTypeMySQL, entry["type"])
		})
	}
}

// The plan handler and the webhook log a failed plan once more after
// ExecutePlan returns it. A refusal, local or from a remote data plane, is
// logged there at Warn too, so an alert on Error logs does not fire for it;
// every other failure stays at Error.
func TestPlanFailureLogLevel(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantLevel string
	}{
		{"remote refusal", fmt.Errorf("plan diff on deployment %q: %w", "eu", status.Error(codes.FailedPrecondition, "the schema files drop table orders_seq")), "WARN"},
		{"local refusal", fmt.Errorf("plan namespace: %w", &engine.PlanRefusedError{Err: errors.New("the schema files drop table orders_seq")}), "WARN"},
		{"server fault", status.Error(codes.Internal, "read shard schema: timeout"), "ERROR"},
		{"plain error", errors.New("connection reset by peer"), "ERROR"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, nil))

			PlanFailureLog(logger, tt.err)("plan failed", "database", "orders", "error", tt.err)

			var entry map[string]any
			require.NoError(t, json.Unmarshal(logs.Bytes(), &entry))
			assert.Equal(t, tt.wantLevel, entry["level"])
			assert.Equal(t, "plan failed", entry["msg"])
			assert.Equal(t, "orders", entry["database"])
		})
	}
}

// The primary plans, and a rollout member's data plane answers its PlanDiff
// with a failure. Either way the member blocks the rollout: the plan lists it
// as unplanned. A refusal reaches the handler still typed, so its log line is
// at Warn with the refusal's text; a data plane fault stays at Error.
func TestPlanHandlerLogsARefusedRolloutMemberAtWarn(t *testing.T) {
	const refusal = "the schema files drop table orders_seq; remove the drop or list the table under drop_tables"
	tests := []struct {
		name      string
		diffErr   error
		wantLevel string
	}{
		{"remote refusal", status.Error(codes.FailedPrecondition, refusal), "WARN"},
		{"server fault", status.Error(codes.Internal, "read shard schema: timeout"), "ERROR"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockTernClient{
				isRemote:    true,
				planResp:    reviewedUsersPlan("ALTER TABLE `users` ADD COLUMN `email` varchar(255)"),
				planDiffErr: tt.diffErr,
			}
			svc := multiTargetService(t, client, &recordingPlanStore{})
			var logs bytes.Buffer
			svc.logger = slog.New(slog.NewJSONHandler(&logs, nil))
			request := planDiffReq(t)
			request.RendersRollout = true

			rec := postJSON(t, svc.handlePlan, "/api/plan", request)

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var resp apitypes.PlanResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.NotNil(t, resp.Rollout)
			require.Len(t, resp.Rollout.Attention, 1)
			assert.Equal(t, "eu/testapp-002", resp.Rollout.Attention[0].Member)
			assert.Equal(t, apitypes.PlanMemberUnplanned, resp.Rollout.Attention[0].Reason, "the member blocks the rollout")

			line := requireLogLine(t, decodeLogLines(t, logs.Bytes()), "plan deployment diff failed; deployment will block the review rollup")
			assert.Equal(t, tt.wantLevel, line["level"])
			assert.Equal(t, "testapp-002", line["target"])
			assert.Contains(t, line["error"], status.Convert(tt.diffErr).Message())
		})
	}
}

// planFailureLogEntry returns the one log entry ExecutePlan wrote for the
// failed Plan call.
func planFailureLogEntry(t *testing.T, logs *bytes.Buffer) map[string]any {
	t.Helper()
	var found []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry))
		if msg, _ := entry["msg"].(string); strings.HasPrefix(msg, "ExecutePlan: ") && entry["error"] != nil {
			found = append(found, entry)
		}
	}
	require.Len(t, found, 1, "logs: %s", logs.String())
	return found[0]
}
