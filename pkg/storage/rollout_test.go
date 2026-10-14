package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
)

// A converged placeholder is the row apply creation writes for a member whose
// target already holds the change: completed, never started, no remote
// identity. Every other way a row reaches completed leaves a mark that a
// driver claimed it or a data plane received it, and a settled row that never
// started in any other state is not a placeholder either.
func TestApplyOperation_IsConvergedPlaceholder(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name string
		op   ApplyOperation
		want bool
	}{
		{name: "converged work", op: ApplyOperation{State: state.ApplyOperation.Completed, OperationKind: ApplyOperationKindWork, CompletedAt: &now}, want: true},
		{name: "converged finalizer", op: ApplyOperation{State: state.ApplyOperation.Completed, OperationKind: ApplyOperationKindGroupFinalizer, CompletedAt: &now}, want: true},
		{name: "proto-prefixed completed state", op: ApplyOperation{State: "STATE_COMPLETED"}, want: true},
		{name: "completed after a claim", op: ApplyOperation{State: state.ApplyOperation.Completed, StartedAt: &now}},
		{name: "completed with a remote apply id", op: ApplyOperation{State: state.ApplyOperation.Completed, ExternalID: "remote-apply"}},
		{name: "completed with a legacy remote apply id", op: ApplyOperation{State: state.ApplyOperation.Completed, EngineResumeContext: "remote-apply"}},
		{name: "completed with a remote operation id", op: ApplyOperation{State: state.ApplyOperation.Completed, ExternalOperationID: "remote-operation"}},
		{name: "pending never started", op: ApplyOperation{State: state.ApplyOperation.Pending}},
		{name: "failed never started", op: ApplyOperation{State: state.ApplyOperation.Failed}},
		{name: "cancelled never started", op: ApplyOperation{State: state.ApplyOperation.Cancelled}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := tc.op
			assert.Equal(t, tc.want, op.IsConvergedPlaceholder())
		})
	}
}
