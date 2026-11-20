package tern

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// A plan the engine refuses is the PR author's to fix, so the data plane logs
// it at Warn rather than as a failure. Any other engine error is still logged
// at Error. Both are returned to the caller unchanged.
func TestLocalClientPlanLogsARefusedPlanAtWarn(t *testing.T) {
	tests := []struct {
		name      string
		planErr   error
		wantLevel string
		wantMsg   string
	}{
		{
			name:      "refusal",
			planErr:   fmt.Errorf("plan namespace: %w", &engine.PlanRefusedError{Err: errors.New("the schema files drop table orders_seq")}),
			wantLevel: "WARN",
			wantMsg:   "plan refused: the schema files must change before this database can plan",
		},
		{
			name:      "engine failure",
			planErr:   errors.New("read shard schema: timeout"),
			wantLevel: "ERROR",
			wantMsg:   "plan failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			c := &LocalClient{
				config:  LocalConfig{Database: "commerce", Type: storage.DatabaseTypeVitess},
				storage: &fakePlanStorage{plans: &fakePlanStore{}},
				planetscaleEngine: namedPlanEngine{name: "planetscale", planFn: func(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) {
					return nil, tt.planErr
				}},
				logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})),
			}

			_, err := c.Plan(t.Context(), &ternv1.PlanRequest{Database: "commerce"})
			require.ErrorIs(t, err, tt.planErr)

			lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
			require.Len(t, lines, 1, "logs: %s", logs.String())
			var entry map[string]any
			require.NoError(t, json.Unmarshal([]byte(lines[0]), &entry))
			assert.Equal(t, tt.wantLevel, entry["level"])
			assert.Equal(t, tt.wantMsg, entry["msg"])
			assert.Equal(t, "commerce", entry["database"])
		})
	}
}
