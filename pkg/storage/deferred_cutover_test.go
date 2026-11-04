package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
)

// Settled work and finalizers never wait at a cutover, so they do not widen
// the apply.
func TestDeferredCutoverHeldAtOnceCountsOnlyWorkStillToDrive(t *testing.T) {
	ops := make([]*ApplyOperation, 0, 4)
	for _, opState := range []string{state.ApplyOperation.Completed, state.ApplyOperation.Pending, state.ApplyOperation.Pending} {
		ops = append(ops, &ApplyOperation{OperationKind: ApplyOperationKindWork, State: opState})
	}
	ops = append(ops, &ApplyOperation{OperationKind: ApplyOperationKindGroupFinalizer, State: state.ApplyOperation.Pending})
	assert.Equal(t, 2, DeferredCutoverHeldAtOnce(ops))
}

// A deferred apply holds at its cutover only the operations the claim lets run
// together (ST-10): one member at a time under rolling, and every deployment at
// once under barrier and parallel.
func TestDeferredCutoverHeldAtOnceCountsOperationsHeldAtOnce(t *testing.T) {
	shards := func(policy, deployment string, count int) []*ApplyOperation {
		ops := make([]*ApplyOperation, 0, count)
		for range count {
			ops = append(ops, &ApplyOperation{
				OperationKind: ApplyOperationKindWork, State: state.ApplyOperation.Pending,
				CutoverPolicy: policy, Deployment: deployment, Target: deployment,
			})
		}
		return ops
	}
	for _, tc := range []struct {
		name string
		ops  []*ApplyOperation
		want int
	}{
		{
			name: "rolling holds one deployment at a time",
			ops:  append(append(shards(CutoverPolicyRolling, "eu", 4), shards(CutoverPolicyRolling, "us", 2)...), shards(CutoverPolicyRolling, "ap", 3)...),
			want: 4,
		},
		{
			name: "barrier holds every deployment together",
			ops:  append(shards(CutoverPolicyBarrier, "eu", 4), shards(CutoverPolicyBarrier, "us", 2)...),
			want: 6,
		},
		{
			name: "parallel holds every deployment together",
			ops:  append(shards(CutoverPolicyParallel, "eu", 4), shards(CutoverPolicyParallel, "us", 2)...),
			want: 6,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, DeferredCutoverHeldAtOnce(tc.ops))
		})
	}
}

func TestDeferredCutoverDriversLeavesOneDriverPerServer(t *testing.T) {
	assert.Equal(t, 0, DeferredCutoverDrivers(0))
	assert.Equal(t, 1, DeferredCutoverDrivers(1), "a single driver is not reserved")
	assert.Equal(t, 1, DeferredCutoverDrivers(2))
	assert.Equal(t, 3, DeferredCutoverDrivers(4))
}
