package api

import (
	"testing"

	"github.com/stretchr/testify/assert"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

func TestBlockPostgresPlanWithoutClassifierVerdicts(t *testing.T) {
	missingVerdict := &ternv1.TableChange{TableName: "accounts"}
	unknownVerdict := &ternv1.TableChange{
		TableName:     "orders",
		ExecutionMode: "future-mode",
		ModeReason:    "untrusted planner reason",
	}
	shardedChange := &ternv1.TableChange{TableName: "events"}
	resp := &ternv1.PlanResponse{
		Changes: []*ternv1.SchemaChange{{
			Namespace:    "public",
			TableChanges: []*ternv1.TableChange{missingVerdict, nil, unknownVerdict},
		}},
		Shards: []*ternv1.ShardPlan{{
			Namespace: "public",
			Shard:     "0",
			Changes:   []*ternv1.TableChange{shardedChange, nil},
		}},
	}

	blockPostgresPlanWithoutClassifierVerdicts(resp)

	for _, change := range []*ternv1.TableChange{missingVerdict, unknownVerdict, shardedChange} {
		assert.Equal(t, "blocked", change.ExecutionMode)
		assert.Equal(t, postgresVerdictUnavailableReason, change.ModeReason)
	}
}

func TestBlockPostgresPlanWithoutClassifierVerdictsHandlesNilResponse(t *testing.T) {
	assert.NotPanics(t, func() { blockPostgresPlanWithoutClassifierVerdicts(nil) })
}
