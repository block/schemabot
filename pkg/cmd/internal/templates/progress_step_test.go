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
