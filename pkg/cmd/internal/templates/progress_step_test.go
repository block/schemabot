package templates

import (
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
)

func TestWriteProgressRendersCurrentStep(t *testing.T) {
	data := ParseProgressResponse(&apitypes.ProgressResponse{
		State: state.Apply.Running,
		Metadata: map[string]string{
			"step": "2", "steps_total": "3",
			"statement": "CREATE INDEX orders_ref_idx ON public.orders (ref)",
		},
	})
	out := captureStdout(t, func() { WriteProgress(data) })
	assert.Contains(t, out, "step 2 of 3 · CREATE INDEX orders_ref_idx ON public.orders (ref)\n")
}

func TestWriteProgressRendersConcurrentIndexBuildWork(t *testing.T) {
	data := ParseProgressResponse(&apitypes.ProgressResponse{
		State: state.Apply.Running,
		Metadata: map[string]string{
			"step": "2", "steps_total": "3", "statement": "CREATE INDEX CONCURRENTLY orders_ref_idx ON public.orders (ref)",
			"executor_operation": "concurrent-index-build", "blocks_done": "2500", "blocks_total": "10000",
			"tuples_done": "12000", "tuples_total": "50000", "attempt": "2",
		},
	})
	out := captureStdout(t, func() { WriteProgress(data) })
	assert.Contains(t, out, "step 2 of 3 · CREATE INDEX CONCURRENTLY orders_ref_idx ON public.orders (ref)\n"+
		"building index: 25% of blocks (2,500/10,000) · 12,000/50,000 tuples · attempt 2\n")
}
