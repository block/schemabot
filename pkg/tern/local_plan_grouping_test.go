package tern

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
)

// The statement grouping on a plan request is the caller's decision, and the
// engine's copy prediction is only as good as the value that reaches it. A
// caller that states a grouping is taken at its word; one that leaves the
// field absent predates the choice, and the engine then predicts the joined
// batch — the shape that errs toward disclosing a discard rather than
// promising a resume the apply will not perform.
func TestPlanGroupedExecutionReachesTheEngine(t *testing.T) {
	cases := []struct {
		name  string
		field *bool
		want  bool
	}{
		{"absent predicts the joined batch", nil, true},
		{"explicit ungrouped is honored", new(false), false},
		{"explicit grouped is honored", new(true), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got *engine.PlanRequest
			eng := namedPlanEngine{name: "spirit", planFn: func(_ context.Context, req *engine.PlanRequest) (*engine.PlanResult, error) {
				got = req
				return &engine.PlanResult{}, nil
			}}
			c := &LocalClient{config: LocalConfig{Database: "commerce", Type: storage.DatabaseTypeMySQL}}

			_, err := c.planNamespaceWithEngine(t.Context(), eng,
				&ternv1.PlanRequest{Database: "commerce", GroupedExecution: tc.field},
				"commerce", schema.SchemaFiles{}, nil)
			require.NoError(t, err)
			require.NotNil(t, got, "the engine must have been asked to plan")
			assert.Equal(t, tc.want, got.GroupedExecution)
		})
	}
}
