package templates

import (
	"maps"
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
)

func TestRenderApplyStatusCommentStep(t *testing.T) {
	metadata := map[string]string{
		"step": "2", "steps_total": "3",
		"statement": "CREATE INDEX orders_ref_idx ON public.orders (ref)",
	}
	running := ApplyStatusFromProgress(&apitypes.ProgressResponse{State: state.Apply.Running, Metadata: metadata}, "")
	out := RenderApplyStatusComment(running)
	assert.Contains(t, out, "step 2 of 3 · `CREATE INDEX orders_ref_idx ON public.orders (ref)`\n")

	completed := ApplyStatusFromProgress(&apitypes.ProgressResponse{State: state.Apply.Completed, Metadata: metadata}, "")
	assert.NotContains(t, RenderApplyStatusComment(completed), "step 2 of 3")

	malformed := ApplyStatusFromProgress(&apitypes.ProgressResponse{State: state.Apply.Running, Metadata: map[string]string{
		"step": "two", "steps_total": "3", "statement": "CREATE TABLE ignored (id int)",
	}}, "")
	assert.NotContains(t, RenderApplyStatusComment(malformed), "step ")
}

func TestRenderApplyStatusCommentBuildWork(t *testing.T) {
	base := map[string]string{
		"step": "2", "steps_total": "3", "statement": "CREATE INDEX CONCURRENTLY orders_ref_idx ON public.orders (ref)",
		"executor_operation": "concurrent-index-build",
	}

	t.Run("heap scan", func(t *testing.T) {
		metadata := maps.Clone(base)
		metadata["server_phase"] = "building index: scanning table"
		metadata["blocks_done"], metadata["blocks_total"] = "2500", "10000"
		out := RenderApplyStatusComment(ApplyStatusFromProgress(&apitypes.ProgressResponse{State: state.Apply.Running, Metadata: metadata}, ""))
		assert.Contains(t, out, "step 2 of 3 · `CREATE INDEX CONCURRENTLY orders_ref_idx ON public.orders (ref)`\n"+
			"building index: 25% of blocks (2,500/10,000)\n")
	})

	t.Run("lockers wait and retry", func(t *testing.T) {
		metadata := maps.Clone(base)
		metadata["server_phase"] = "waiting for writers before validation"
		metadata["lockers_done"], metadata["lockers_total"], metadata["attempt"] = "2", "5", "2"
		out := RenderApplyStatusComment(ApplyStatusFromProgress(&apitypes.ProgressResponse{State: state.Apply.Running, Metadata: metadata}, ""))
		assert.Contains(t, out, "waiting on 3 of 5 lockers · attempt 2\n")
	})

	t.Run("other operation is suppressed", func(t *testing.T) {
		metadata := maps.Clone(base)
		metadata["executor_operation"], metadata["server_phase"] = "brief", "building index: scanning table"
		metadata["blocks_done"], metadata["blocks_total"] = "25", "100"
		out := RenderApplyStatusComment(ApplyStatusFromProgress(&apitypes.ProgressResponse{State: state.Apply.Running, Metadata: metadata}, ""))
		assert.NotContains(t, out, "building index:")
	})
}

func TestRenderApplyStatusCommentClampsHostileStatement(t *testing.T) {
	statement := "`DROP`\n\x01" + strings.Repeat("x", 2000)
	data := ApplyStatusFromProgress(&apitypes.ProgressResponse{State: state.Apply.Running, Metadata: map[string]string{
		"step": "1", "steps_total": "2", "statement": statement,
	}}, "")
	out := RenderApplyStatusComment(data)
	want := "step 1 of 2 · `'DROP' " + strings.Repeat("x", 152) + "…`\n"
	assert.Contains(t, out, want)
	assert.NotContains(t, out, "`DROP`")
	assert.NotContains(t, out, "\x01")
}
