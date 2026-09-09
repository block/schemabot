package templates

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// The statement is engine-reported text, so the terminal line it lands on is
// bounded the same way as the PR comment: line breaks and control bytes fold
// into spaces and text past the shared width is cut with an ellipsis, so a
// pathological DDL cannot flood or corrupt the operator's terminal.
func TestWriteProgressClampsStatementToOneBoundedLine(t *testing.T) {
	data := ParseProgressResponse(&apitypes.ProgressResponse{
		State: state.Apply.Running,
		Metadata: map[string]string{
			"step": "1", "steps_total": "2",
			"statement": "CREATE\r\n\x1bINDEX " + strings.Repeat("x", 2000),
		},
	})
	out := captureStdout(t, func() { WriteProgress(data) })
	_, stepLine, found := strings.Cut(out, "\nstep ")
	require.True(t, found, "step line rendered:\n%s", out)
	stepLine, _, _ = strings.Cut(stepLine, "\n")
	assert.Equal(t, "1 of 2 · CREATE INDEX "+strings.Repeat("x", ui.MaxStatementRunes-len("CREATE INDEX ")-1)+"…", stepLine)
	assert.NotContains(t, stepLine, "\x1b")
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
