package api

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// The execution-mode verdict crosses the wire as a free-form string, and
// everything except "blocked" executes downstream. These tests pin the
// boundary invariant: only the closed vocabulary — empty, blocked, direct —
// passes through unchanged; anything else is blocked with a locally-produced
// reason, in both top-level and sharded plan shapes.
func TestNormalizeExecutionVerdictsBlocksUnrecognizedModes(t *testing.T) {
	s := &Service{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	empty := &ternv1.TableChange{TableName: "accounts"}
	blocked := &ternv1.TableChange{
		TableName:     "users",
		ExecutionMode: engine.ExecutionModeBlocked,
		ModeReason:    "requires copy-and-swap",
	}
	direct := &ternv1.TableChange{TableName: "orders", ExecutionMode: engine.ExecutionModeDirect, ModeReason: "runs as native DDL"}
	unknown := &ternv1.TableChange{
		TableName:     "events",
		ExecutionMode: "future-mode",
		ModeReason:    "untrusted planner reason",
	}
	shardedUnknown := &ternv1.TableChange{TableName: "payments", ExecutionMode: "future-mode"}
	shardedEmpty := &ternv1.TableChange{TableName: "ledger"}
	resp := &ternv1.PlanResponse{
		Changes: []*ternv1.SchemaChange{{
			Namespace:    "public",
			TableChanges: []*ternv1.TableChange{empty, nil, blocked, direct, unknown},
		}, nil},
		Shards: []*ternv1.ShardPlan{{
			Namespace: "public",
			Shard:     "0",
			Changes:   []*ternv1.TableChange{shardedUnknown, nil, shardedEmpty},
		}, nil},
	}

	s.normalizeExecutionVerdicts(resp, "appdb", "primary")

	assert.Empty(t, empty.ExecutionMode)
	assert.Empty(t, shardedEmpty.ExecutionMode)
	assert.Equal(t, engine.ExecutionModeBlocked, blocked.ExecutionMode)
	assert.Equal(t, "requires copy-and-swap", blocked.ModeReason)
	assert.Equal(t, engine.ExecutionModeDirect, direct.ExecutionMode)
	assert.Equal(t, "runs as native DDL", direct.ModeReason)

	for _, tc := range []*ternv1.TableChange{unknown, shardedUnknown} {
		assert.Equal(t, engine.ExecutionModeBlocked, tc.ExecutionMode)
		assert.Contains(t, tc.ModeReason, `"future-mode"`)
		assert.NotContains(t, tc.ModeReason, "untrusted planner reason")
	}
}

func TestNormalizeExecutionVerdictsHandlesNilResponse(t *testing.T) {
	s := &Service{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	assert.NotPanics(t, func() { s.normalizeExecutionVerdicts(nil, "appdb", "primary") })
}

func TestRecognizedExecutionModeIsCaseInsensitive(t *testing.T) {
	assert.True(t, recognizedExecutionMode(""))
	assert.True(t, recognizedExecutionMode("blocked"))
	assert.True(t, recognizedExecutionMode("Blocked"))
	assert.True(t, recognizedExecutionMode("direct"))
	assert.True(t, recognizedExecutionMode("DIRECT"))
	assert.False(t, recognizedExecutionMode("future-mode"))
	assert.False(t, recognizedExecutionMode(" blocked"))
}
