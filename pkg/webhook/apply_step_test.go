package webhook

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A running single-deployment apply whose driver has persisted a statement
// position renders that position in the PR comment, so an operator reading the
// PR sees which statement of the sequence is executing without opening the
// progress API.
func TestFormatApplyStatusComment_RendersStoredStatementPosition(t *testing.T) {
	ops := []*storage.ApplyOperation{{
		ID:               1,
		Deployment:       "eu",
		State:            state.ApplyOperation.Running,
		ProgressMetadata: `{"step":"2","steps_total":"3","statement":"CREATE INDEX CONCURRENTLY idx_orders_status ON orders (status)"}`,
	}}

	displayByOp := resolveDisplayByOperation(t.Context(), nil, runningApply(), ops)
	require.Len(t, displayByOp, 1)
	assert.Equal(t, apitypes.ProgressStep{Step: 2, StepsTotal: 3, Statement: "CREATE INDEX CONCURRENTLY idx_orders_status ON orders (status)"}, displayByOp[1].Step)

	out := formatApplyStatusComment(runningApply(), ops, false, nil, displayByOp, nil, nil, "")
	assert.Contains(t, out, "step 2 of 3 · `CREATE INDEX CONCURRENTLY idx_orders_status ON orders (status)`")
}

// Each deployment of a fanned-out apply carries its own statement position:
// the per-deployment detail block renders the position persisted on that
// operation, and a sibling with no stored position renders none.
func TestFormatApplyStatusComment_RendersStatementPositionPerDeployment(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "eu", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyBarrier,
			ProgressMetadata: `{"step":"1","steps_total":"2","statement":"ALTER TABLE orders ADD COLUMN note text"}`},
		{ID: 2, Deployment: "us", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyBarrier},
	}

	displayByOp := resolveDisplayByOperation(t.Context(), nil, runningApply(), ops)
	require.Len(t, displayByOp, 1)
	assert.NotContains(t, displayByOp, int64(2))

	out := formatApplyStatusComment(runningApply(), ops, false, nil, displayByOp, nil, nil, "")
	assert.Contains(t, out, "**Deployments**: 2 running")
	assert.Equal(t, 1, strings.Count(out, "step 1 of 2 · `ALTER TABLE orders ADD COLUMN note text`"))
}

// A running concurrent index build persists the server's build work alongside
// the statement position, and the PR comment renders it under the position, so
// an operator sees the build advance even though no rows are copied. The tuple
// load still carries the finished heap scan's block counters; only the tuples
// the phase owns are shown.
func TestFormatApplyStatusComment_RendersStoredBuildWork(t *testing.T) {
	ops := []*storage.ApplyOperation{{
		ID:         1,
		Deployment: "eu",
		State:      state.ApplyOperation.Running,
		ProgressMetadata: `{"step":"2","steps_total":"3","statement":"CREATE INDEX CONCURRENTLY idx_orders_status ON orders (status)",` +
			`"executor_operation":"concurrent-index-build","server_phase":"building index: loading tuples in tree",` +
			`"blocks_done":"10000","blocks_total":"10000","tuples_done":"12000","tuples_total":"50000","lockers_done":"0","lockers_total":"0"}`,
	}}

	displayByOp := resolveDisplayByOperation(t.Context(), nil, runningApply(), ops)
	require.Len(t, displayByOp, 1)
	assert.Equal(t, apitypes.BuildWork{
		Operation: "concurrent-index-build", ServerPhase: "building index: loading tuples in tree",
		BlocksDone: 10000, BlocksTotal: 10000, TuplesDone: 12000, TuplesTotal: 50000,
	}, displayByOp[1].BuildWork)

	out := formatApplyStatusComment(runningApply(), ops, false, nil, displayByOp, nil, nil, "")
	assert.Contains(t, out, "step 2 of 3 · `CREATE INDEX CONCURRENTLY idx_orders_status ON orders (status)`\n"+
		"building index: 12,000/50,000 tuples\n")
}

// Each deployment of a fanned-out apply renders the build work persisted on its
// own operation: a deployment waiting on lockers reports them, and a sibling
// still scanning reports its blocks.
func TestFormatApplyStatusComment_RendersBuildWorkPerDeployment(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "eu", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyBarrier,
			ProgressMetadata: `{"step":"1","steps_total":"1","statement":"CREATE INDEX CONCURRENTLY idx ON t (c)",` +
				`"executor_operation":"concurrent-index-build","server_phase":"waiting for writers before build","lockers_done":"1","lockers_total":"3"}`},
		{ID: 2, Deployment: "us", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyBarrier,
			ProgressMetadata: `{"step":"1","steps_total":"1","statement":"CREATE INDEX CONCURRENTLY idx ON t (c)",` +
				`"executor_operation":"concurrent-index-build","server_phase":"building index: scanning table","blocks_done":"1","blocks_total":"4"}`},
	}

	displayByOp := resolveDisplayByOperation(t.Context(), nil, runningApply(), ops)
	require.Len(t, displayByOp, 2)

	out := formatApplyStatusComment(runningApply(), ops, false, nil, displayByOp, nil, nil, "")
	assert.Contains(t, out, "**Deployments**: 2 running")
	assert.Equal(t, 1, strings.Count(out, "\nwaiting on 2 of 3 lockers\n"))
	assert.Equal(t, 1, strings.Count(out, "\nbuilding index: 25% of blocks (1/4)\n"))
}

// A malformed build counter drops the build work line but keeps the readable
// statement position, so one bad counter does not hide where the apply is.
func TestFormatApplyStatusComment_KeepsPositionWhenBuildWorkIsMalformed(t *testing.T) {
	ops := []*storage.ApplyOperation{{ID: 1, Deployment: "eu", State: state.ApplyOperation.Running,
		ProgressMetadata: `{"step":"2","steps_total":"3","statement":"CREATE INDEX CONCURRENTLY idx ON t (c)","executor_operation":"concurrent-index-build","blocks_done":"many"}`}}

	displayByOp := resolveDisplayByOperation(t.Context(), nil, runningApply(), ops)
	require.Len(t, displayByOp, 1)
	assert.Equal(t, apitypes.BuildWork{}, displayByOp[1].BuildWork)

	out := formatApplyStatusComment(runningApply(), ops, false, nil, displayByOp, nil, nil, "")
	assert.Contains(t, out, "step 2 of 3 · `CREATE INDEX CONCURRENTLY idx ON t (c)`\n")
	assert.NotContains(t, out, "building index")
}

// Stored progress metadata that cannot be decoded, or that carries an
// impossible position, contributes no position rather than a wrong one, and
// the rest of the comment still renders.
func TestFormatApplyStatusComment_OmitsUnreadableStatementPosition(t *testing.T) {
	for name, metadata := range map[string]string{
		"malformed json":        `{"step":`,
		"non-numeric step":      `{"step":"two","steps_total":"3"}`,
		"step exceeds total":    `{"step":"4","steps_total":"3"}`,
		"no position published": `{"phase":"preflight"}`,
	} {
		t.Run(name, func(t *testing.T) {
			ops := []*storage.ApplyOperation{{ID: 1, Deployment: "eu", State: state.ApplyOperation.Running, ProgressMetadata: metadata}}

			displayByOp := resolveDisplayByOperation(t.Context(), nil, runningApply(), ops)
			assert.Empty(t, displayByOp)

			out := formatApplyStatusComment(runningApply(), ops, false, nil, displayByOp, nil, nil, "")
			assert.Contains(t, out, "## Schema Change Status")
			assert.NotContains(t, out, "\nstep ")
		})
	}
}
