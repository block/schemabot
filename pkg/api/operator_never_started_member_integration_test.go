//go:build integration

package api

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// payments-001 and payments-002 are targets of one deployment and share its
// data-plane apply. The control plane runs the rollout with on_failure:
// continue, so once payments-001 fails it drives payments-002. The data plane
// halts on failure, so it settled the shared apply as failed when payments-001
// failed and will never start payments-002. That drive fails payments-002's
// tasks with the reason and ends. The operator derives payments-002 failed
// from them and hands its lease back, nothing claims it again, and the
// rollout settles failed with one terminal summary.
func TestOperator_MemberItsRemoteApplySettledBeforeSettlesFailed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)
	resetMatrixTables(t, ctx, db)

	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "never-started-member",
		parentState:     state.Apply.Pending,
		cutoverPolicy:   storage.CutoverPolicyRolling,
		onFailure:       storage.OnFailureContinue,
		deployments:     []string{"region-a"},
		targets:         []string{"payments-001", "payments-002"},
		opState:         state.ApplyOperation.Pending,
		taskState:       state.Task.Pending,
	})

	neverStarts := "remote apply remote-apply-shared settled as failed before this operation started, so the data plane will not run it"
	rec := &driveRecorder{}
	svc := newMatrixService(t, stor, matrixTargetClients(stor, rec, "region-a", map[string]matrixOutcome{
		"payments-001": {taskState: state.Task.Failed, errMsg: "duplicate key name 'idx_orders_source'"},
		"payments-002": {
			taskState: state.Task.Failed,
			errMsg:    neverStarts,
			returnErr: fmt.Errorf("poll remote apply remote-apply-shared for operation %d: %w", seed.opID("payments-002"), tern.ErrRemoteOperationNeverStarts),
		},
	}))

	for driver := 1; driver <= 3; driver++ {
		driveNextOperation(t, ctx, svc, driver)
	}

	assert.Equal(t, []string{"payments-001", "payments-002"}, rec.resumeTargets(),
		"payments-002 is driven once and not claimed again")
	op, err := stor.ApplyOperations().Get(ctx, seed.opID("payments-002"))
	require.NoError(t, err)
	require.NotNil(t, op)
	assert.Equal(t, state.ApplyOperation.Failed, op.State)
	assert.Empty(t, op.LeaseOwner, "the ended drive hands its lease back")
	assert.Empty(t, op.LeaseToken)
	tasks, err := stor.Tasks().GetByApplyOperationID(ctx, op.ID)
	require.NoError(t, err)
	require.NotEmpty(t, tasks)
	for _, task := range tasks {
		assert.Equal(t, neverStarts, task.ErrorMessage, "task %s carries the reason it never ran", task.TaskIdentifier)
	}
	assert.Equal(t, state.Apply.Failed, getApply(t, ctx, stor, seed.applyID).State)
	assert.Equal(t, 1, svc.matrixSummary.count(), "exactly one terminal summary once settled")
}
