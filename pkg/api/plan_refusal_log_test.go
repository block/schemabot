package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
			wantMsg:   "ExecutePlan: engine refused the plan; the PR's schema files must change before it can plan",
		},
		{
			name:      "local refusal",
			planErr:   fmt.Errorf("plan namespace: %w", &engine.PlanRefusedError{Err: errors.New("the schema files drop table orders_seq")}),
			wantLevel: "WARN",
			wantMsg:   "ExecutePlan: engine refused the plan; the PR's schema files must change before it can plan",
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
